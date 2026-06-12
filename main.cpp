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

}

int main() {
    // clean slate for the test run.
    std::error_code ec;
    fs::remove_all("test_db", ec);
    fs::create_directories("test_db", ec);

    // phase 1: concurrent writers/readers stress test.
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
                    [[maybe_unused]] bool ok = store->Set(key, value);
                    assert(ok && "Set should succeed");
                    total_writes.fetch_add(1, std::memory_order_relaxed);

                    // also touch a neighbor's key range to create cross-shard contention.
                    int neighbor = (w + 1) % kNumWorkers;
                    std::string shared_key = "shared_key_" + std::to_string(i % 50);
                    store->Set(shared_key, MakeValue(neighbor, i, 1));
                }
            });
        }

        // reader threads concurrently hammer get() on keys being written.
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

        // a concurrent compaction while readers are still active.
        [[maybe_unused]] bool compacted = store->Compact();
        assert(compacted && "Compaction should succeed");

        stop_readers.store(true, std::memory_order_relaxed);
        for (auto& t : readers) t.join();

        std::cout << "Total writes performed: " << total_writes.load() << "\n";
        std::cout << "Total successful reads observed: " << total_reads.load() << "\n";
        std::cout << "Store size after phase 1: " << store->Size() << "\n";

        // verify all worker-specific keys are present and correct.
        for (int w = 0; w < kNumWorkers; ++w) {
            for (int i = 0; i < kKeysPerWorker; ++i) {
                auto val = store->Get(MakeKey(w, i));
                assert(val.has_value() && "Key must exist after writes");
                assert(*val == MakeValue(w, i, 1) && "Value must match last write");
            }
        }
        std::cout << "Phase 1 verification PASSED.\n";

        // store goes out of scope here, destructor flushes and closes the wal.
    }

    // phase 2: simulate abrupt termination via extra writes plus a torn trailing record.
    PrintHeader("Phase 2: Crash simulation (extra writes + torn record)");
    {
        auto store = std::make_unique<kvstore::KVStore>(kDbPath);

        // additional writes after recovery from phase 1's clean state.
        for (int i = 0; i < 100; ++i) {
            std::string key = "crash_key_" + std::to_string(i);
            std::string value = "crash_val_" + std::to_string(i);
            [[maybe_unused]] bool ok = store->Set(key, value);
            assert(ok);
        }

        // delete some pre-existing keys to test delete-record replay.
        for (int i = 0; i < 50; ++i) {
            [[maybe_unused]] bool ok = store->Delete(MakeKey(0, i));
            assert(ok);
        }

        std::cout << "Pre-crash size: " << store->Size() << "\n";

        // "crash": drop the object without an orderly shutdown beyond the destructor.
    }

    // simulate a torn write at the tail of the wal with a few incomplete header bytes.
    {
        std::ofstream raw(std::string(kDbPath) + ".wal",
                          std::ios::binary | std::ios::app);
        assert(raw.is_open());
        const char garbage[] = {0x01, 0x02, 0x03}; // incomplete header
        raw.write(garbage, sizeof(garbage));
        raw.flush();
    }

    // phase 3: restart and validate recovered state.
    PrintHeader("Phase 3: Restart & recovery validation");
    {
        auto store = std::make_unique<kvstore::KVStore>(kDbPath);

        std::cout << "Recovered size: " << store->Size() << "\n";

        // keys 0..49 of worker 0 were deleted in phase 2.
        for (int i = 0; i < 50; ++i) {
            auto val = store->Get(MakeKey(0, i));
            assert(!val.has_value() && "Deleted key must not be present after recovery");
        }

        // remaining keys of worker 0 must still be present.
        for (int i = 50; i < kKeysPerWorker; ++i) {
            auto val = store->Get(MakeKey(0, i));
            assert(val.has_value() && "Non-deleted key must survive recovery");
            assert(*val == MakeValue(0, i, 1));
        }

        // other workers' keys must be fully intact.
        for (int w = 1; w < kNumWorkers; ++w) {
            for (int i = 0; i < kKeysPerWorker; ++i) {
                auto val = store->Get(MakeKey(w, i));
                assert(val.has_value());
                assert(*val == MakeValue(w, i, 1));
            }
        }

        // crash-phase writes must be present.
        for (int i = 0; i < 100; ++i) {
            auto val = store->Get("crash_key_" + std::to_string(i));
            assert(val.has_value());
            assert(*val == "crash_val_" + std::to_string(i));
        }

        // torn trailing garbage didn't corrupt anything before it.
        std::cout << "Phase 3 verification PASSED.\n";

        // phase 4: post-recovery compaction plus concurrent ops, then a second restart.
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

        [[maybe_unused]] bool compacted = store->Compact();
        assert(compacted);

        std::size_t size_before_restart = store->Size();
        std::cout << "Size before second restart: " << size_before_restart << "\n";
    }

    {
        auto store = std::make_unique<kvstore::KVStore>(kDbPath);
        std::cout << "Size after second restart: " << store->Size() << "\n";

        // verify deleted keys remain deleted after compaction and restart.
        for (int i = 0; i < 50; ++i) {
            auto val = store->Get(MakeKey(0, i));
            assert(!val.has_value());
        }

        // verify post-recovery writes survived compaction and restart.
        for (int w = 0; w < kNumWorkers; ++w) {
            for (int i = 0; i < 100; ++i) {
                std::string key = "post_recovery_" + std::to_string(w) + "_" +
                                   std::to_string(i);
                auto val = store->Get(key);
                assert(val.has_value());
                assert(*val == "pr_val_" + std::to_string(i));
            }
        }

        // verify crash-phase keys survived compaction and restart.
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
