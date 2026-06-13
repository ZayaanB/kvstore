#include "kvstore.hpp"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr int kOpsPerThread = 20000;
constexpr int kKeyspaceSize = 10000;

// builds a key from an index, kept simple so different threads overlap on shards.
std::string MakeKey(int idx) {
    return "bench_key_" + std::to_string(idx % kKeyspaceSize);
}

// pure write throughput: every thread does kOpsPerThread Set() calls.
double RunWriteBench(kvstore::KVStore& store, int num_threads) {
    std::vector<std::thread> threads;
    threads.reserve(static_cast<std::size_t>(num_threads));

    auto start = std::chrono::steady_clock::now();
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&store, t]() {
            std::string value(64, 'x'); // fixed-size payload keeps the comparison fair.
            for (int i = 0; i < kOpsPerThread; ++i) {
                store.Set(MakeKey(t * kOpsPerThread + i), value);
            }
        });
    }
    for (auto& th : threads) th.join();
    auto end = std::chrono::steady_clock::now();

    double seconds = std::chrono::duration<double>(end - start).count();
    double total_ops = static_cast<double>(num_threads) * kOpsPerThread;
    return total_ops / seconds;
}

// mixed read/write throughput: each thread does mostly Get with some Set on a shared keyspace.
double RunMixedBench(kvstore::KVStore& store, int num_threads) {
    std::vector<std::thread> threads;
    threads.reserve(static_cast<std::size_t>(num_threads));

    auto start = std::chrono::steady_clock::now();
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&store, t]() {
            std::mt19937 rng(static_cast<unsigned>(t) + 1);
            std::uniform_int_distribution<int> key_dist(0, kKeyspaceSize - 1);
            std::uniform_int_distribution<int> op_dist(0, 9); // < 8 means read.
            std::string value(64, 'y');

            for (int i = 0; i < kOpsPerThread; ++i) {
                int key_idx = key_dist(rng);
                if (op_dist(rng) < 8) {
                    (void)store.Get(MakeKey(key_idx));
                } else {
                    store.Set(MakeKey(key_idx), value);
                }
            }
        });
    }
    for (auto& th : threads) th.join();
    auto end = std::chrono::steady_clock::now();

    double seconds = std::chrono::duration<double>(end - start).count();
    double total_ops = static_cast<double>(num_threads) * kOpsPerThread;
    return total_ops / seconds;
}

void PrintRow(const std::string& label, int threads, double ops_per_sec) {
    std::cout << std::left << std::setw(8) << label
              << "threads=" << std::setw(2) << threads << "  "
              << std::fixed << std::setprecision(0) << std::setw(10) << ops_per_sec
              << " ops/sec\n";
}

void ResetDb(const std::string& dir) {
    fs::remove_all(dir);
    fs::create_directories(dir);
}

} // namespace

int main() {
    const std::string db_dir = "bench_db";
    const std::string db_path = db_dir + "/bench.db";

    std::cout << "kvstore benchmark - " << kOpsPerThread << " ops/thread, "
              << kKeyspaceSize << " distinct keys\n\n";

    const std::vector<int> thread_counts = {1, 2, 4, 8};

    std::cout << "-- write throughput (Set only) --\n";
    for (int n : thread_counts) {
        // fresh store per run so the working set size stays comparable.
        ResetDb(db_dir);
        kvstore::KVStore store(db_path);
        double ops = RunWriteBench(store, n);
        PrintRow("write", n, ops);
    }

    std::cout << "\n-- mixed throughput (80% Get / 20% Set) --\n";
    for (int n : thread_counts) {
        ResetDb(db_dir);
        kvstore::KVStore store(db_path);
        // pre-populate so reads have something to find.
        std::string value(64, 'z');
        for (int i = 0; i < kKeyspaceSize; ++i) {
            store.Set(MakeKey(i), value);
        }
        double ops = RunMixedBench(store, n);
        PrintRow("mixed", n, ops);
    }

    std::cout << "\n-- compaction cost (on a populated store) --\n";
    {
        ResetDb(db_dir);
        kvstore::KVStore store(db_path);
        std::string value(64, 'w');
        for (int i = 0; i < kKeyspaceSize; ++i) {
            store.Set(MakeKey(i), value);
        }
        auto start = std::chrono::steady_clock::now();
        store.Compact();
        auto end = std::chrono::steady_clock::now();
        double ms = std::chrono::duration<double, std::milli>(end - start).count();
        std::cout << "compact(): " << std::fixed << std::setprecision(2) << ms
                  << " ms for " << store.Size() << " live keys\n";
    }

    fs::remove_all(db_dir);
    return 0;
}