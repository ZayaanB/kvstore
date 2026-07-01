#include "kvstore.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
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
    // clean slate.
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

                    // cross-shard contention.
                    int neighbor = (w + 1) % kNumWorkers;
                    std::string shared_key = "shared_key_" + std::to_string(i % 50);
                    store->Set(shared_key, MakeValue(neighbor, i, 1));
                }
            });
        }

        // readers hammering Get while writers run.
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

        // compact while readers run.
        [[maybe_unused]] bool compacted = store->Compact();
        assert(compacted && "Compaction should succeed");

        stop_readers.store(true, std::memory_order_relaxed);
        for (auto& t : readers) t.join();

        std::cout << "Total writes performed: " << total_writes.load() << "\n";
        std::cout << "Total successful reads observed: " << total_reads.load() << "\n";
        std::cout << "Store size after phase 1: " << store->Size() << "\n";

        // verify every key.
        for (int w = 0; w < kNumWorkers; ++w) {
            for (int i = 0; i < kKeysPerWorker; ++i) {
                auto val = store->Get(MakeKey(w, i));
                assert(val.has_value() && "Key must exist after writes");
                assert(*val == MakeValue(w, i, 1) && "Value must match last write");
            }
        }
        std::cout << "Phase 1 verification PASSED.\n";
    }

    PrintHeader("Phase 2: Crash simulation (extra writes + torn record)");
    {
        auto store = std::make_unique<kvstore::KVStore>(kDbPath);

        // more writes after recovery.
        for (int i = 0; i < 100; ++i) {
            std::string key = "crash_key_" + std::to_string(i);
            std::string value = "crash_val_" + std::to_string(i);
            [[maybe_unused]] bool ok = store->Set(key, value);
            assert(ok);
        }

        // delete some keys (tests delete replay).
        for (int i = 0; i < 50; ++i) {
            [[maybe_unused]] bool ok = store->Delete(MakeKey(0, i));
            assert(ok);
        }

        std::cout << "Pre-crash size: " << store->Size() << "\n";

        // "crash" by dropping the store.
    }

    // append a torn (incomplete) record.
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

        // worker 0 keys 0..49 were deleted.
        for (int i = 0; i < 50; ++i) {
            auto val = store->Get(MakeKey(0, i));
            assert(!val.has_value() && "Deleted key must not be present after recovery");
        }

        // the rest must survive.
        for (int i = 50; i < kKeysPerWorker; ++i) {
            auto val = store->Get(MakeKey(0, i));
            assert(val.has_value() && "Non-deleted key must survive recovery");
            assert(*val == MakeValue(0, i, 1));
        }

        // other workers intact.
        for (int w = 1; w < kNumWorkers; ++w) {
            for (int i = 0; i < kKeysPerWorker; ++i) {
                auto val = store->Get(MakeKey(w, i));
                assert(val.has_value());
                assert(*val == MakeValue(w, i, 1));
            }
        }

        // crash-phase writes present.
        for (int i = 0; i < 100; ++i) {
            auto val = store->Get("crash_key_" + std::to_string(i));
            assert(val.has_value());
            assert(*val == "crash_val_" + std::to_string(i));
        }

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

        // deletes still gone.
        for (int i = 0; i < 50; ++i) {
            auto val = store->Get(MakeKey(0, i));
            assert(!val.has_value());
        }

        // post-recovery writes survived.
        for (int w = 0; w < kNumWorkers; ++w) {
            for (int i = 0; i < 100; ++i) {
                std::string key = "post_recovery_" + std::to_string(w) + "_" +
                                   std::to_string(i);
                auto val = store->Get(key);
                assert(val.has_value());
                assert(*val == "pr_val_" + std::to_string(i));
            }
        }

        // crash-phase keys survived.
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

        store->Set("ttl_permanent", "stays_forever");

        // expires in 1s.
        [[maybe_unused]] bool ok = store->SetWithTtl("ttl_short", "soon_gone", 1);
        assert(ok);
        assert(store->Get("ttl_short").has_value() && "key should be readable before it expires");

        // long ttl.
        store->SetWithTtl("ttl_long", "around_for_a_while", 3600);

        // let the short key expire.
        std::this_thread::sleep_for(std::chrono::seconds(2));
        assert(!store->Get("ttl_short").has_value() && "key should be expired by now");
        assert(store->Get("ttl_permanent").has_value());
        assert(store->Get("ttl_long").has_value());

        // compaction drops it from disk.
        [[maybe_unused]] bool compacted = store->Compact();
        assert(compacted);
        std::cout << "Phase 5a (in-memory expiry) PASSED.\n";
    }
    {
        auto store = std::make_unique<kvstore::KVStore>(kDbPath);
        // expired key stays gone.
        assert(!store->Get("ttl_short").has_value());
        // others survive.
        assert(store->Get("ttl_permanent").value() == "stays_forever");
        assert(store->Get("ttl_long").value() == "around_for_a_while");
        std::cout << "Phase 5b (ttl survives restart) PASSED.\n";
    }

    PrintHeader("Phase 6: Iterator and range scan");
    {
        auto store = std::make_unique<kvstore::KVStore>(kDbPath);

        // isolated keys for this phase.
        store->Set("scan_a", "1");
        store->Set("scan_b", "2");
        store->Set("scan_c", "3");
        store->Set("scan_d", "4");
        store->Delete("scan_b");
        store->SetWithTtl("scan_expired", "gone", 1);

        // let the ttl key expire.
        std::this_thread::sleep_for(std::chrono::seconds(2));

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

        // sorted by key.
        for (std::size_t i = 1; i < all.size(); ++i) {
            assert(all[i - 1].key <= all[i].key);
        }

        // explicit [start, end).
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

        // open-ended range.
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

    PrintHeader("Phase 7: CRC corruption detection");
    {
        const std::string crc_db = "test_db/kvstore_crc.db";
        const std::string crc_wal = crc_db + ".wal";
        std::error_code rm_ec;
        fs::remove(crc_wal, rm_ec);

        // two records; the 2nd is last on disk.
        {
            auto store = std::make_unique<kvstore::KVStore>(crc_db);
            [[maybe_unused]] bool ok = store->Set("crc_keep", "good");
            assert(ok);
            ok = store->Set("crc_corrupt", "target");
            assert(ok);
        }

        // corrupt the last record's payload so its crc no longer matches.
        {
            std::fstream f(crc_wal, std::ios::binary | std::ios::in | std::ios::out);
            assert(f.is_open());
            f.seekg(0, std::ios::end);
            const std::streamoff size = f.tellg();
            assert(size > 5);
            // byte just before the 4-byte crc.
            const std::streamoff pos = size - 5;
            f.seekg(pos);
            char b = 0;
            f.read(&b, 1);
            b = static_cast<char>(b ^ 0xFF);
            f.seekp(pos);
            f.write(&b, 1);
            f.flush();
        }

        std::uintmax_t size_before_reopen = 0;
        {
            std::error_code sz_ec;
            size_before_reopen = fs::file_size(crc_wal, sz_ec);
            assert(!sz_ec);

            auto store = std::make_unique<kvstore::KVStore>(crc_db);
            // bad-crc record is dropped; earlier record survives.
            assert(!store->Get("crc_corrupt").has_value() &&
                   "record with mismatched CRC must be dropped during recovery");
            assert(store->Get("crc_keep").value() == "good");

            // recovery must truncate the corrupt tail, not leave it in place.
            std::error_code sz_ec2;
            const std::uintmax_t size_after = fs::file_size(crc_wal, sz_ec2);
            assert(!sz_ec2 && size_after < size_before_reopen &&
                   "corrupt tail must be truncated during recovery");

            // writes after recovery must land at the clean boundary.
            [[maybe_unused]] bool ok = store->Set("crc_after", "fresh");
            assert(ok);
        }

        // reopen once more: the post-recovery write must survive.
        {
            auto store = std::make_unique<kvstore::KVStore>(crc_db);
            assert(store->Get("crc_keep").value() == "good");
            assert(store->Get("crc_after").value() == "fresh" &&
                   "writes appended after recovery must persist across restart");
            assert(!store->Get("crc_corrupt").has_value());
        }
        std::cout << "Phase 7 (CRC corruption detection) PASSED.\n";
    }

    PrintHeader("ALL TESTS PASSED");
    return 0;
}
