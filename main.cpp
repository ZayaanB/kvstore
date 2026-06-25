#include "kvstore.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
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

    PrintHeader("Phase 5: TTL expiry");
    {
        auto store = std::make_unique<kvstore::KVStore>(kDbPath);

        // a key with no ttl never expires.
        store->Set("ttl_permanent", "stays_forever");

        // a key that expires almost immediately.
        [[maybe_unused]] bool ok = store->SetWithTtl("ttl_short", "soon_gone", 1);
        assert(ok);
        assert(store->Get("ttl_short").has_value() && "key should be readable before it expires");

        // a key with a long ttl that should survive a restart.
        store->SetWithTtl("ttl_long", "around_for_a_while", 3600);

        // wait for the short-lived key to expire.
        std::this_thread::sleep_for(std::chrono::seconds(2));
        assert(!store->Get("ttl_short").has_value() && "key should be expired by now");
        assert(store->Get("ttl_permanent").has_value());
        assert(store->Get("ttl_long").has_value());

        // compaction should drop the expired entry from disk.
        [[maybe_unused]] bool compacted = store->Compact();
        assert(compacted);
        std::cout << "Phase 5a (in-memory expiry) PASSED.\n";
    }
    {
        auto store = std::make_unique<kvstore::KVStore>(kDbPath);
        // the expired key shouldn't come back after a restart.
        assert(!store->Get("ttl_short").has_value());
        // the long-lived and permanent keys should survive recovery.
        assert(store->Get("ttl_permanent").value() == "stays_forever");
        assert(store->Get("ttl_long").value() == "around_for_a_while");
        std::cout << "Phase 5b (ttl survives restart) PASSED.\n";
    }

    PrintHeader("Phase 6: Iterator and range scan");
    {
        auto store = std::make_unique<kvstore::KVStore>(kDbPath);

        // isolated keyspace so leftover keys from earlier phases don't affect assertions.
        store->Set("scan_a", "1");
        store->Set("scan_b", "2");
        store->Set("scan_c", "3");
        store->Set("scan_d", "4");
        store->Delete("scan_b");
        store->SetWithTtl("scan_expired", "gone", 1);

        // give the ttl entry time to expire before scanning.
        std::this_thread::sleep_for(std::chrono::seconds(2));

        // items() returns everything live and non-expired, sorted by key.
        auto all = store->Items();
        [[maybe_unused]] auto find_scan = [&all](const std::string& key) {
            return std::find_if(all.begin(), all.end(),
                                 [&key](const kvstore::KeyValue& kv) { return kv.key == key; });
        };
        assert(find_scan("scan_a") != all.end());
        assert(find_scan("scan_b") == all.end()); // deleted
        assert(find_scan("scan_c") != all.end());
        assert(find_scan("scan_d") != all.end());
        assert(find_scan("scan_expired") == all.end()); // expired

        // results must be sorted by key.
        for (std::size_t i = 1; i < all.size(); ++i) {
            assert(all[i - 1].key <= all[i].key);
        }

        // range() with an explicit [start, end) window.
        auto window = store->Range("scan_a", "scan_d");
        assert(find_scan("scan_a") != all.end());
        [[maybe_unused]] bool has_a = false, has_c = false, has_d = false;
        for (const auto& kv : window) {
            if (kv.key == "scan_a") has_a = true;
            if (kv.key == "scan_c") has_c = true;
            if (kv.key == "scan_d") has_d = true;
        }
        assert(has_a && has_c && "range start is inclusive");
        assert(!has_d && "range end is exclusive");

        // an open-ended range (empty end) includes everything from start onward.
        auto open_ended = store->Range("scan_c", "");
        [[maybe_unused]] bool open_has_c = false, open_has_d = false, open_has_a = false;
        for (const auto& kv : open_ended) {
            if (kv.key == "scan_c") open_has_c = true;
            if (kv.key == "scan_d") open_has_d = true;
            if (kv.key == "scan_a") open_has_a = true;
        }
        assert(open_has_c && open_has_d && !open_has_a);

        std::cout << "Phase 6 verification PASSED.\n";
    }

    PrintHeader("ALL TESTS PASSED");
    return 0;
}
