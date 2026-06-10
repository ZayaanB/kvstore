#include "kvstore.hpp"

#include <atomic>
#include <cassert>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr int kNumWorkers = 4;
constexpr int kKeysPerWorker = 500;
constexpr const char* kDbPath = "test_db/kvstore_test.db";

std::string MakeKey(int worker_id, int idx) {
    return "key_" + std::to_string(worker_id) + "_" + std::to_string(idx);
}

std::string MakeValue(int worker_id, int idx, int version) {
    return "val_w" + std::to_string(worker_id) + "_i" + std::to_string(idx) +
           "_v" + std::to_string(version);
}

void PrintHeader(const std::string& title) {
    std::cout << "\n=== " << title << " ===\n";
}

} // namespace

int main() {
    // Clean slate for the test run.
    std::error_code ec;
    fs::remove_all("test_db", ec);
    fs::create_directories("test_db", ec);

    // -------------------------------------------------------------------
    // Phase 1: Concurrent writers/readers stress test.
    // -------------------------------------------------------------------
    PrintHeader("Phase 1: Concurrent stress test (writes + overlapping reads)");
    {
        auto store = std::make_unique<kvstore::KVStore>(kDbPath);

        std::atomic<int> total_reads{0};
        std::atomic<int> total_writes{0};
        std::atomic<bool> stop_readers{false};

        std::vector<std::thread> writers;
        writers.reserve(kNumWorkers);
        for (int w = 0; w < kNumWorkers; ++w) {
            writers.emplace_back([&, w]() {
                for (int i = 0; i < kKeysPerWorker; ++i) {
                    const std::string key = MakeKey(w, i);
                    const std::string value = MakeValue(w, i, 1);
                    bool ok = store->Set(key, value);
                    assert(ok && "Set should succeed");
                    total_writes.fetch_add(1, std::memory_order_relaxed);

                    // Overlapping key space: also touch a neighbor's key
                    // range to create cross-shard contention.
                    int neighbor = (w + 1) % kNumWorkers;
                    std::string shared_key = "shared_key_" + std::to_string(i % 50);
                    store->Set(shared_key, MakeValue(neighbor, i, 1));
                }
            });
        }

        // Reader threads concurrently hammer Get() on keys being written.
        std::vector<std::thread> readers;
        readers.reserve(static_cast<std::size_t>(kNumWorkers));
        for (int r = 0; r < kNumWorkers; ++r) {
            readers.emplace_back([&, r]() {
                while (!stop_readers.load(std::memory_order_relaxed)) {
                    for (int i = 0; i < kKeysPerWorker; i += 7) {
                        std::string key = MakeKey(r, i);
                        auto val = store->Get(key);
                        if (val.has_value()) {
                            total_reads.fetch_add(1, std::memory_order_relaxed);
                        }
                        std::string shared_key = "shared_key_" + std::to_string(i % 50);
                        (void)store->Get(shared_key);
                    }
                    std::this_thread::yield();
                }
            });
        }

        for (auto& t : writers) t.join();

        // A concurrent compaction while readers are still active.
        bool compacted = store->Compact();
        assert(compacted && "Compaction should succeed");

        stop_readers.store(true, std::memory_order_relaxed);
        for (auto& t : readers) t.join();

        std::cout << "Total writes performed: " << total_writes.load() << "\n";
        std::cout << "Total successful reads observed: " << total_reads.load() << "\n";
        std::cout << "Store size after phase 1: " << store->Size() << "\n";

        // Verify all worker-specific keys are present and correct.
        for (int w = 0; w < kNumWorkers; ++w) {
            for (int i = 0; i < kKeysPerWorker; ++i) {
                auto val = store->Get(MakeKey(w, i));
                assert(val.has_value() && "Key must exist after writes");
                assert(*val == MakeValue(w, i, 1) && "Value must match last write");
            }
        }
        std::cout << "Phase 1 verification PASSED.\n";

        // store goes out of scope -> destructor flushes & closes WAL,
        // simulating an orderly shutdown before the "crash" phase.
    }

    // -------------------------------------------------------------------
    // Phase 2: Simulate abrupt termination by writing extra records via a
    // raw handle, then dropping the object WITHOUT calling Compact, and
    // truncating the file to simulate a torn final write.
    // -------------------------------------------------------------------
    PrintHeader("Phase 2: Crash simulation (extra writes + torn record)");
    {
        auto store = std::make_unique<kvstore::KVStore>(kDbPath);

        // Additional writes after recovery from phase 1's clean state.
        for (int i = 0; i < 100; ++i) {
            std::string key = "crash_key_" + std::to_string(i);
            std::string value = "crash_val_" + std::to_string(i);
            bool ok = store->Set(key, value);
            assert(ok);
        }

        // Delete some pre-existing keys to test delete-record replay.
        for (int i = 0; i < 50; ++i) {
            bool ok = store->Delete(MakeKey(0, i));
            assert(ok);
        }

        std::cout << "Pre-crash size: " << store->Size() << "\n";

        // "Crash": drop the object without graceful shutdown semantics
        // beyond what the destructor does (it still flushes the OS file
        // handle since std::ofstream destructor closes the stream; data
        // already reached the OS via flush() on every Set/Delete).
    }

    // Simulate a torn write at the tail of the WAL (e.g., power loss mid
    // append): append a few garbage bytes that do not form a full record.
    {
        std::ofstream raw(std::string(kDbPath) + ".wal",
                          std::ios::binary | std::ios::app);
        assert(raw.is_open());
        const char garbage[] = {0x01, 0x02, 0x03}; // incomplete header
        raw.write(garbage, sizeof(garbage));
        raw.flush();
    }

    // -------------------------------------------------------------------
    // Phase 3: Restart and validate recovered state.
    // -------------------------------------------------------------------
    PrintHeader("Phase 3: Restart & recovery validation");
    {
        auto store = std::make_unique<kvstore::KVStore>(kDbPath);

        std::cout << "Recovered size: " << store->Size() << "\n";

        // Keys 0..49 of worker 0 were deleted in phase 2.
        for (int i = 0; i < 50; ++i) {
            auto val = store->Get(MakeKey(0, i));
            assert(!val.has_value() && "Deleted key must not be present after recovery");
        }

        // Remaining keys of worker 0 must still be present.
        for (int i = 50; i < kKeysPerWorker; ++i) {
            auto val = store->Get(MakeKey(0, i));
            assert(val.has_value() && "Non-deleted key must survive recovery");
            assert(*val == MakeValue(0, i, 1));
        }

        // Other workers' keys must be fully intact.
        for (int w = 1; w < kNumWorkers; ++w) {
            for (int i = 0; i < kKeysPerWorker; ++i) {
                auto val = store->Get(MakeKey(w, i));
                assert(val.has_value());
                assert(*val == MakeValue(w, i, 1));
            }
        }

        // Crash-phase writes must be present.
        for (int i = 0; i < 100; ++i) {
            auto val = store->Get("crash_key_" + std::to_string(i));
            assert(val.has_value());
            assert(*val == "crash_val_" + std::to_string(i));
        }

        // Torn trailing garbage must not corrupt anything beyond it
        // (recovery stops cleanly at the truncated record).
        std::cout << "Phase 3 verification PASSED.\n";

        // -------------------------------------------------------------
        // Phase 4: Post-recovery compaction + concurrent ops, then a
        // second restart to ensure compacted log replays correctly.
        // -------------------------------------------------------------
        PrintHeader("Phase 4: Compaction after recovery + second restart");

        std::vector<std::thread> threads;
        for (int w = 0; w < kNumWorkers; ++w) {
            threads.emplace_back([&, w]() {
                for (int i = 0; i < 100; ++i) {
                    std::string key = "post_recovery_" + std::to_string(w) + "_" +
                                       std::to_string(i);
                    store->Set(key, "pr_val_" + std::to_string(i));
                }
            });
        }
        for (auto& t : threads) t.join();

        bool compacted = store->Compact();
        assert(compacted);

        std::size_t size_before_restart = store->Size();
        std::cout << "Size before second restart: " << size_before_restart << "\n";
    }

    {
        auto store = std::make_unique<kvstore::KVStore>(kDbPath);
        std::cout << "Size after second restart: " << store->Size() << "\n";

        // Verify deleted keys remain deleted after compaction + restart.
        for (int i = 0; i < 50; ++i) {
            auto val = store->Get(MakeKey(0, i));
            assert(!val.has_value());
        }

        // Verify post-recovery writes survived compaction + restart.
        for (int w = 0; w < kNumWorkers; ++w) {
            for (int i = 0; i < 100; ++i) {
                std::string key = "post_recovery_" + std::to_string(w) + "_" +
                                   std::to_string(i);
                auto val = store->Get(key);
                assert(val.has_value());
                assert(*val == "pr_val_" + std::to_string(i));
            }
        }

        // Verify crash-phase keys survived compaction + restart.
        for (int i = 0; i < 100; ++i) {
            auto val = store->Get("crash_key_" + std::to_string(i));
            assert(val.has_value());
            assert(*val == "crash_val_" + std::to_string(i));
        }

        std::cout << "Phase 4 verification PASSED.\n";
    }

    PrintHeader("ALL TESTS PASSED");
    return 0;
}