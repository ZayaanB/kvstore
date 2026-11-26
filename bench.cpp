#include "kvstore.hpp"
#include "metrics.hpp"
#include "benchmark_build.hpp"
#include "benchmarks/stats.hpp"

#include <cerrno>
#include <charconv>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <latch>
#include <memory>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#ifdef __linux__
#include <sys/vfs.h>
#include <sys/sysmacros.h>
#endif
#include <thread>

namespace fs = std::filesystem;

namespace {

using Clock = std::chrono::steady_clock;
using bench::Counts;
using bench::Rate;

enum Operation { Read, Write, Compact, Reopen, OperationCount };
constexpr std::array kOperations{"get", "set", "compact", "reopen"};
constexpr std::array<std::string_view, 5> kWorkloads{"read", "write", "mixed", "compaction", "reopen"};
constexpr int kKeyBytes = 16;
constexpr int kValueBytes = 64;

struct Options {
    bool latency = false;
    bool engine_metrics = false;
    unsigned trials = 3;
    unsigned seed = 1;
    unsigned keys = 1000;
    unsigned ops = 500;
    unsigned read_ops = 1'000'000;
    unsigned warmup = 1000;
    std::vector<unsigned> threads{1, 2, 4, 8, 16};
    std::string workload;
    std::string json;
    fs::path temp_root = fs::temp_directory_path();
};

struct Workload {
    std::string_view name;
    bool hot;
    unsigned threads;
};

using Operations = std::array<Counts, OperationCount>;
enum class Outcome { Ok, Miss, Error };

double Seconds(Clock::duration duration) {
    return std::chrono::duration<double>(duration).count();
}

void Quote(std::ostream& out, std::string_view value) {
    constexpr std::string_view hex = "0123456789abcdef";
    out << '"';
    for (unsigned char byte : std::basic_string_view<unsigned char>(
             reinterpret_cast<const unsigned char*>(value.data()), value.size())) {
        if (byte == '"' || byte == '\\') out << '\\' << static_cast<char>(byte);
        else if (byte < 32) out << "\\u00" << hex[byte >> 4] << hex[byte & 15];
        else out << static_cast<char>(byte);
    }
    out << '"';
}

struct TempDb {
    explicit TempDb(const fs::path& root) {
        auto pattern = (fs::absolute(root) / "kvstore-bench-XXXXXX").string();
        if (!::mkdtemp(pattern.data())) {
            throw std::system_error(errno, std::generic_category(), "mkdtemp");
        }
        directory = std::move(pattern);
    }
    ~TempDb() {
        std::error_code error;
        fs::remove_all(directory, error);
    }
    fs::path directory;
};

struct Trial {
    double seconds = 0;
    Operations operations;
    std::vector<std::string> errors;
    std::optional<double> cpu_seconds;
    std::optional<kvstore::StorageTotals> storage;
    std::optional<double> maintenance_seconds;
    std::optional<std::uintmax_t> peak_rss_bytes;
    std::optional<std::uintmax_t> wal_bytes;
    std::optional<std::uintmax_t> filesystem_available_bytes;
};

struct Worker {
    Operations operations;
    Clock::time_point end;
    double maintenance_seconds = 0;
    std::uint64_t iterations = 0;
    std::string error;
};

struct Request {
    Operation operation;
    unsigned key;
    bool miss;
};

Request Next(std::mt19937& rng, const Options& options, Workload workload, std::uint64_t index) {
    const auto selector = rng();
    const auto sample = rng();
    const auto keyspace = workload.hot && selector % 10 < 9
        ? std::max(1u, options.keys / 100) : options.keys;
    const unsigned key = static_cast<unsigned>(sample % keyspace);
    const bool write = workload.name == "write" || (workload.name != "read" && index % 5 == 4);
    const bool miss = !write && (selector / 10) % 16 == 0;
    return {write ? Write : Read, key, miss};
}

template <class Action>
void Measure(Worker& worker, Operation operation, bool latency, Action action) {
    auto& counts = worker.operations[operation];
    Clock::time_point start;
    Clock::time_point end;
    if (latency) start = Clock::now();
    Outcome outcome = Outcome::Error;
    try {
        outcome = action();
        if (latency) end = Clock::now();
        if (outcome == Outcome::Error && worker.error.empty()) {
            worker.error = std::string(kOperations[operation]) + " returned an unexpected result";
        }
    } catch (const std::exception& error) {
        if (latency) end = Clock::now();
        if (worker.error.empty()) worker.error = std::string(kOperations[operation]) + ": " + error.what();
    } catch (...) {
        if (latency) end = Clock::now();
        if (worker.error.empty()) worker.error = std::string(kOperations[operation]) + ": unknown exception";
    }
    if (outcome == Outcome::Ok) ++counts.ok;
    else if (outcome == Outcome::Miss) ++counts.misses;
    else ++counts.errors;
    if (latency) {
        const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        counts.latency.Add(static_cast<std::uint64_t>(ns));
    }
}

unsigned OpsPerWorker(const Options& options, Workload workload) {
    if (workload.name == "reopen") return 1;
    if (workload.name == "read") return options.read_ops;
    if (workload.name == "compaction") return std::max(5u, options.ops);
    return options.ops;
}

std::optional<double> CpuSeconds() {
    rusage usage{};
    if (::getrusage(RUSAGE_SELF, &usage) != 0) return std::nullopt;
    const auto seconds = [](timeval time) {
        return static_cast<double>(time.tv_sec) + static_cast<double>(time.tv_usec) * 1e-6;
    };
    return seconds(usage.ru_utime) + seconds(usage.ru_stime);
}

void RunWorkers(kvstore::KVStore& store, const Options& options, Workload workload,
                const std::vector<std::string>& keys, const std::string& value,
                std::vector<bool>& written, Trial& trial) {
    const bool compact = workload.name == "compaction";
    const std::uint64_t minimum_ops = OpsPerWorker(options, workload);
    const auto workers = static_cast<std::ptrdiff_t>(workload.threads);
    std::latch ready(workers + static_cast<int>(compact));
    std::latch start_gate(1);
    std::latch compaction_start(workers + 1);
    bool cancelled = false; // published by start_gate
    std::vector<Worker> results(workload.threads + static_cast<unsigned>(compact));
    std::vector<std::jthread> threads;
    threads.reserve(results.size());
    const auto wait_for_start = [&] {
        ready.count_down();
        start_gate.wait();
        return !cancelled;
    };
    try {
        for (unsigned id = 0; id < workload.threads; ++id) {
            threads.emplace_back([&, id] {
                auto& result = results[id];
                std::mt19937 rng(options.seed + id);
                if (!wait_for_start()) return;
                for (; result.iterations < minimum_ops; ++result.iterations) {
                    // Release the compactor and the first timed writes together.
                    if (compact && result.iterations == 4) compaction_start.arrive_and_wait();
                    const auto request = Next(rng, options, workload, result.iterations);
                    Measure(result, request.operation, options.latency, [&] {
                        if (request.operation == Write) {
                            return store.Set(keys[request.key], value) ? Outcome::Ok : Outcome::Error;
                        }
                        const auto read = store.Get(request.miss ? keys.back() : keys[request.key]);
                        if (request.miss) return read ? Outcome::Error : Outcome::Miss;
                        return read ? Outcome::Ok : Outcome::Error;
                    });
                }
                result.end = Clock::now();
            });
        }
        if (compact) {
            threads.emplace_back([&] {
                if (!wait_for_start()) return;
                compaction_start.arrive_and_wait();
                auto& result = results.back();
                const auto compact_start = Clock::now();
                Measure(result, Compact, options.latency, [&] {
                    return store.Compact() ? Outcome::Ok : Outcome::Error;
                });
                result.end = Clock::now();
                result.maintenance_seconds = Seconds(result.end - compact_start);
            });
        }
    } catch (...) {
        cancelled = true;
        start_gate.count_down();
        throw;
    }
    ready.wait();
    const auto cpu_start = CpuSeconds();
    const auto start = Clock::now();
    start_gate.count_down();
    for (auto& thread : threads) thread.join();
    const auto cpu_end = CpuSeconds();
    if (cpu_start && cpu_end) trial.cpu_seconds = *cpu_end - *cpu_start;
    auto end = start;
    for (const auto& worker : results) {
        end = std::max(end, worker.end);
        if (!worker.error.empty()) trial.errors.push_back(worker.error);
        for (std::size_t op = 0; op < OperationCount; ++op) {
            trial.operations[op].Merge(worker.operations[op]);
        }
    }
    trial.seconds = Seconds(end - start);
    if (compact) trial.maintenance_seconds = results.back().maintenance_seconds;

    if (trial.errors.empty() && workload.name != "read") {
        for (unsigned id = 0; id < workload.threads; ++id) {
            std::mt19937 rng(options.seed + id);
            for (std::uint64_t i = 0; i < results[id].iterations; ++i) {
                const auto request = Next(rng, options, workload, i);
                if (request.operation == Write) written[request.key] = true;
            }
        }
    }
}

Trial RunTrial(const Options& options, Workload workload) {
    Trial trial;
    try {
        const TempDb db(options.temp_root);
        const auto path = (db.directory / "store").string();
        auto metrics = options.engine_metrics ? std::make_shared<kvstore::Metrics>() : nullptr;
        auto store = std::make_unique<kvstore::KVStore>(path, metrics);
        auto keys = std::vector<std::string>{};
        keys.reserve(options.keys + 1);
        const std::string initial(kValueBytes, 'a');
        const std::string updated(kValueBytes, 'b');
        for (unsigned i = 0; i <= options.keys; ++i) {
            const auto digits = std::to_string(i);
            keys.push_back(std::string(kKeyBytes - digits.size(), '0') + digits);
            if (i < options.keys && !store->Set(keys.back(), initial)) {
                throw std::runtime_error("failed to populate database");
            }
        }
        std::vector<bool> written(options.keys);
        std::mt19937 rng(options.seed);
        for (unsigned i = 0; i < options.warmup; ++i) {
            const auto request = Next(rng, options, {"read", workload.hot, 1}, i);
            const auto actual = store->Get(keys[request.key]);
            if (actual != initial) throw std::runtime_error("warmup value mismatch");
        }
        const auto storage_start = metrics ? metrics->Storage() : kvstore::StorageTotals{};
        if (workload.name == "reopen") {
            store.reset();
            Worker result;
            const auto cpu_start = CpuSeconds();
            const auto start = Clock::now();
            Measure(result, Reopen, options.latency, [&] {
                store = std::make_unique<kvstore::KVStore>(path, metrics);
                return Outcome::Ok;
            });
            const auto end = Clock::now();
            const auto cpu_end = CpuSeconds();
            if (cpu_start && cpu_end) trial.cpu_seconds = *cpu_end - *cpu_start;
            trial.seconds = Seconds(end - start);
            trial.maintenance_seconds = trial.seconds;
            trial.operations = std::move(result.operations);
            if (!result.error.empty()) trial.errors.push_back(result.error);
        } else {
            RunWorkers(*store, options, workload, keys, updated, written, trial);
        }
        if (metrics) {
            const auto storage_end = metrics->Storage();
            trial.storage = kvstore::StorageTotals{
                storage_end.wal_syncs - storage_start.wal_syncs,
                storage_end.failed_wal_syncs - storage_start.failed_wal_syncs,
                storage_end.wal_sync_ns - storage_start.wal_sync_ns,
                storage_end.wal_written_bytes - storage_start.wal_written_bytes,
                storage_end.compaction_written_bytes - storage_start.compaction_written_bytes
            };
        }
        rusage usage{};
        if (::getrusage(RUSAGE_SELF, &usage) == 0) {
#ifdef __linux__
            trial.peak_rss_bytes = static_cast<std::uintmax_t>(usage.ru_maxrss) * 1024;
#elif defined(__APPLE__)
            trial.peak_rss_bytes = static_cast<std::uintmax_t>(usage.ru_maxrss);
#endif
        }
        std::error_code error;
        const auto bytes = fs::file_size(path + ".wal", error);
        if (!error) trial.wal_bytes = bytes;
        const auto space = fs::space(db.directory, error);
        if (!error) trial.filesystem_available_bytes = space.available;
        if (trial.errors.empty()) {
            const auto verify = [&](const kvstore::KVStore& target) {
                if (target.Size() != options.keys || target.Get(keys.back())) {
                    throw std::runtime_error("verification key count or absent key mismatch");
                }
                for (unsigned i = 0; i < options.keys; ++i) {
                    if (target.Get(keys[i]) != (written[i] ? updated : initial)) {
                        throw std::runtime_error("verification value mismatch at key " + keys[i]);
                    }
                }
            };
            verify(*store);
            if (workload.name != "reopen") {
                store.reset();
                const kvstore::KVStore recovered(path);
                verify(recovered);
            }
        }
    } catch (const std::exception& error) {
        trial.errors.push_back(error.what());
    }
    return trial;
}

template <class T>
void OptionalNumber(std::ostream& out, const std::optional<T>& value) {
    if (value) out << *value;
    else out << "null";
}

struct Totals {
    std::uint64_t ok = 0;
    std::uint64_t misses = 0;
    std::uint64_t errors = 0;

    void Add(const Counts& counts) {
        ok += counts.ok;
        misses += counts.misses;
        errors += counts.errors;
    }
    std::uint64_t Completed() const { return ok + misses; }
    void Json(std::ostream& out, double seconds) const {
        const auto attempted = Completed() + errors;
        out << "\"ok\":" << ok << ",\"misses\":" << misses << ",\"errors\":" << errors
            << ",\"completed\":" << Completed() << ",\"attempted\":" << attempted
            << ",\"ops_per_second\":" << Rate(Completed(), seconds)
            << ",\"attempted_ops_per_second\":" << Rate(attempted, seconds)
            << ",\"failure_rate\":" << Rate(errors, static_cast<double>(attempted));
    }
};

Totals Primary(const Operations& operations) {
    Totals total;
    for (auto operation : {Read, Write, Reopen}) total.Add(operations[operation]);
    return total;
}

void OperationsJson(std::ostream& out, const Operations& operations, double seconds) {
    out << '{';
    for (std::size_t i = 0; i < OperationCount; ++i) {
        if (i) out << ',';
        Quote(out, kOperations[i]);
        out << ":{";
        Totals total;
        total.Add(operations[i]);
        total.Json(out, seconds);
        out << ",\"latency\":";
        operations[i].latency.Json(out);
        out << '}';
    }
    out << '}';
}

void TrialJson(std::ostream& out, const Trial& trial, unsigned number) {
    out << "{\"trial\":" << number << ",\"duration_seconds\":" << trial.seconds
        << ",\"passed\":" << (trial.errors.empty() ? "true" : "false") << ',';
    Primary(trial.operations).Json(out, trial.seconds);
    out << ",\"failed_operations\":" << Primary(trial.operations).errors + trial.operations[Compact].errors;
    out << ",\"maintenance_seconds\":";
    OptionalNumber(out, trial.maintenance_seconds);
    out << ",\"operations\":";
    OperationsJson(out, trial.operations, trial.seconds);
    out << ",\"resources\":{\"measured_process_cpu_seconds\":";
    OptionalNumber(out, trial.cpu_seconds);
    out << ",\"process_lifetime_peak_rss_bytes\":";
    OptionalNumber(out, trial.peak_rss_bytes);
    out << ",\"final_wal_bytes\":";
    OptionalNumber(out, trial.wal_bytes);
    out << ",\"filesystem_available_bytes\":";
    OptionalNumber(out, trial.filesystem_available_bytes);
    if (trial.storage) {
        const auto& storage = *trial.storage;
        out << ",\"wal_sync_count\":" << storage.wal_syncs
            << ",\"failed_wal_sync_count\":" << storage.failed_wal_syncs
            << ",\"bytes_written\":" << storage.wal_written_bytes + storage.compaction_written_bytes
            << ",\"compaction_bytes_processed\":" << storage.compaction_written_bytes
            << ",\"wal_sync_latency\":{\"samples\":" << storage.wal_syncs << ",\"mean_seconds\":";
        if (storage.wal_syncs > 0) out << Rate(storage.wal_sync_ns, static_cast<double>(storage.wal_syncs)) * 1e-9;
        else out << "null";
        out << '}';
    } else {
        out << ",\"wal_sync_count\":null,\"failed_wal_sync_count\":null,\"wal_sync_latency\":null,"
               "\"bytes_written\":null,\"compaction_bytes_processed\":null";
    }
    out << "},\"error_messages\":[";
    for (std::size_t i = 0; i < trial.errors.size(); ++i) {
        if (i) out << ',';
        Quote(out, trial.errors[i]);
    }
    out << "]}";
}

void ParametersJson(std::ostream& out, const Options& options, Workload workload) {
    const bool reopen = workload.name == "reopen";
    const bool compact = workload.name == "compaction";
    const int get = reopen || workload.name == "write" ? 0 : workload.name == "read" ? 100 : 80;
    const int set = reopen ? 0 : 100 - get;
    out << "\"workload\":";
    Quote(out, workload.name);
    out << ",\"distribution\":";
    Quote(out, reopen ? "n/a" : workload.hot ? "hot" : "uniform");
    out << ",\"threads\":" << workload.threads << ",\"compaction_threads\":" << (compact ? 1 : 0)
        << ",\"seed\":" << options.seed << ",\"keys\":" << options.keys
        << ",\"key_bytes\":" << kKeyBytes << ",\"value_bytes\":" << kValueBytes
        << ",\"min_operations_per_worker\":" << OpsPerWorker(options, workload)
        << ",\"warmup_reads\":" << options.warmup
        << ",\"mix_percent\":{\"get\":" << get << ",\"set\":" << set
        << ",\"reopen\":" << (reopen ? 100 : 0) << '}'
        << ",\"expected_read_miss_fraction\":" << (get ? "0.0625" : "0")
        << ",\"cache_conditions\":";
    Quote(out, reopen ? "warm WAL page cache (no eviction requested)"
                      : "prepopulated in-memory index; no OS cache eviction");
    out << ",\"load_model\":\"closed-loop\",\"durability\":\"fdatasync-per-write\"";
}

void EnvironmentJson(std::ostream& out, const fs::path& root) {
    out << '{';
    const auto field = [&](std::string_view key, std::string_view value) {
        Quote(out, key);
        out << ':';
        if (value.empty()) out << "null";
        else Quote(out, value);
        out << ',';
    };
    field("revision", KVSTORE_BENCH_REVISION);
    field("source_state_at_configure", KVSTORE_BENCH_SOURCE_STATE);
    field("build_type", KVSTORE_BENCH_BUILD_TYPE);
    field("compiler", KVSTORE_BENCH_COMPILER);
    utsname system{};
    if (::uname(&system) == 0) {
        field("os", std::string(system.sysname) + " " + system.release);
        field("architecture", system.machine);
    } else {
        field("os", "");
        field("architecture", "");
    }
    std::ifstream cpu_info("/proc/cpuinfo");
    std::string line;
    std::string cpu;
    while (std::getline(cpu_info, line)) {
        if (line.starts_with("model name")) {
            const auto colon = line.find(':');
            const auto first = line.find_first_not_of(" \t", colon + 1);
            if (first != std::string::npos) cpu = line.substr(first);
            break;
        }
    }
    field("cpu", cpu);
    std::string filesystem;
    std::string device;
#ifdef __linux__
    struct statfs storage{};
    if (::statfs(root.c_str(), &storage) == 0) {
        std::ostringstream magic;
        magic << "0x" << std::hex << storage.f_type;
        filesystem = magic.str();
    }
    struct stat info{};
    if (::stat(root.c_str(), &info) == 0) {
        const auto id = std::to_string(major(info.st_dev)) + ":" + std::to_string(minor(info.st_dev));
        std::error_code error;
        const auto path = fs::canonical(fs::path("/sys/dev/block") / id, error);
        device = error ? id : path.filename().string();
        std::ifstream model(path.parent_path() / "device/model");
        if (!error && std::getline(model, line)) {
            line.erase(line.find_last_not_of(' ') + 1);
            device += " (" + line + ')';
        }
    }
#endif
    field("filesystem_magic", filesystem);
    field("storage_device", device);
    out << "\"hardware_threads\":" << std::thread::hardware_concurrency()
        << ",\"shards\":" << kvstore::kNumShards << '}';
}

bool RunWorkload(std::ostream& out, std::ostream& console, const Options& options, Workload workload) {
    out << "{\"parameters\":{";
    ParametersJson(out, options, workload);
    out << "},\"trials\":[";
    Operations pooled;
    double seconds = 0;
    std::vector<double> rates;
    bool passed = true;
    for (unsigned i = 1; i <= options.trials; ++i) {
        const auto trial = RunTrial(options, workload);
        const auto total = Primary(trial.operations);
        const auto rate = Rate(total.Completed(), trial.seconds);
        if (i > 1) out << ',';
        TrialJson(out, trial, i);
        out.flush();
        console << workload.name << ' ' << (workload.name == "reopen" ? "n/a" : workload.hot ? "hot" : "uniform")
                << " threads=" << workload.threads << " trial=" << i << "  " << rate << " ops/s";
        if (trial.maintenance_seconds) console << "  " << *trial.maintenance_seconds * 1000 << " ms maintenance";
        if (!trial.errors.empty()) {
            console << "  FAILED (" << trial.errors.front() << ')';
            passed = false;
        } else {
            seconds += trial.seconds;
            rates.push_back(rate);
            for (std::size_t op = 0; op < OperationCount; ++op) pooled[op].Merge(trial.operations[op]);
        }
        console << '\n';
    }
    out << "],\"summary\":{\"passed\":" << (passed ? "true" : "false")
        << ",\"repetitions\":" << options.trials << ",\"valid_trials\":" << rates.size()
        << ",\"failed_trials\":" << options.trials - rates.size()
        << ",\"duration_seconds\":" << seconds;
    if (rates.empty()) {
        out << ",\"ops_per_second\":null,\"median_ops_per_second\":null,"
               "\"min_ops_per_second\":null,\"max_ops_per_second\":null";
    } else {
        const auto total = Primary(pooled);
        const auto [minimum, maximum] = std::minmax_element(rates.begin(), rates.end());
        out << ',';
        total.Json(out, seconds);
        out << ",\"median_ops_per_second\":" << bench::Median(rates)
            << ",\"min_ops_per_second\":" << *minimum << ",\"max_ops_per_second\":" << *maximum;
        console << "  average " << Rate(total.Completed(), seconds) << " ops/s, median "
                << bench::Median(rates) << ", range " << *minimum << "–" << *maximum << '\n';
    }
    out << ",\"operations\":";
    OperationsJson(out, pooled, seconds);
    out << "}}";
    return passed;
}

unsigned Number(std::string_view token, unsigned minimum, unsigned maximum) {
    unsigned result = 0;
    const auto [end, error] = std::from_chars(token.data(), token.data() + token.size(), result);
    if (error != std::errc{} || end != token.data() + token.size() || result < minimum || result > maximum) {
        throw std::invalid_argument("invalid numeric option: " + std::string(token));
    }
    return result;
}

Options Parse(int argc, char** argv) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        if (std::string_view(argv[i]) == "--smoke") {
            options.trials = 2;
            options.threads = {1, 2};
            options.keys = 32;
            options.read_ops = 5000;
            options.ops = 20;
            options.warmup = 32;
        }
    }
    for (int i = 1; i < argc; ++i) {
        const std::string_view flag = argv[i];
        if (flag == "--smoke") continue;
        if (++i == argc) throw std::invalid_argument("missing value for " + std::string(flag));
        const std::string_view value = argv[i];
        if (flag == "--mode") {
            if (value != "throughput" && value != "latency") throw std::invalid_argument("unknown mode");
            options.latency = value == "latency";
        } else if (flag == "--engine-metrics") {
            if (value != "on" && value != "off") throw std::invalid_argument("unknown engine metrics setting");
            options.engine_metrics = value == "on";
        } else if (flag == "--json") options.json = value;
        else if (flag == "--temp-root") options.temp_root = value;
        else if (flag == "--trials") options.trials = Number(value, 1, 1000);
        else if (flag == "--seed") options.seed = Number(value, 0, UINT32_MAX);
        else if (flag == "--keys") options.keys = Number(value, 1, 10'000'000);
        else if (flag == "--ops") options.ops = Number(value, 1, UINT32_MAX);
        else if (flag == "--read-ops") options.read_ops = Number(value, 1, UINT32_MAX);
        else if (flag == "--warmup") options.warmup = Number(value, 0, UINT32_MAX);
        else if (flag == "--workload") {
            if (std::find(kWorkloads.begin(), kWorkloads.end(), value) == kWorkloads.end()) {
                throw std::invalid_argument("unknown workload");
            }
            options.workload = value;
        } else if (flag == "--threads") {
            options.threads.clear();
            auto remaining = value;
            while (true) {
                const auto comma = remaining.find(',');
                const auto count = Number(remaining.substr(0, comma), 1, 1024);
                if (std::find(options.threads.begin(), options.threads.end(), count) != options.threads.end()) {
                    throw std::invalid_argument("duplicate thread count");
                }
                options.threads.push_back(count);
                if (comma == std::string_view::npos) break;
                remaining.remove_prefix(comma + 1);
            }
        } else throw std::invalid_argument("unknown option: " + std::string(flag));
    }
    return options;
}

bool MathFixture() {
    Counts first;
    Counts second;
    first.ok = 6;
    first.misses = 2;
    first.errors = 2;
    second.ok = 10;
    second.misses = 2;
    for (unsigned i = 1; i <= 19; ++i) first.latency.Add(i);
    second.latency.Add(1000);
    const bool rates = Rate(first.Completed(), 1) == 8 && Rate(first.Attempted(), 1) == 10
        && Rate(second.Completed(), 3) == 4 && bench::Median({8, 4}) == 6
        && Rate(first.errors, static_cast<double>(first.Attempted())) == 0.2;
    first.Merge(second);
    bench::Histogram extremes;
    extremes.Add(UINT64_MAX);
    return rates && first.Completed() == 20 && first.Attempted() == 22
        && first.misses == 4 && first.errors == 2 && Rate(first.Completed(), 4) == 5
        && first.latency.count == 20 && first.latency.sum_ns / 20 == 59.5
        && first.latency.Percentile(50) == 10 && first.latency.Percentile(95) == 19
        && first.latency.Percentile(99) == 1007 && extremes.Percentile(99) >= 1e19;
}

} // namespace

int main(int argc, char** argv) {
    if (argc == 2 && std::string_view(argv[1]) == "--help") {
        std::cout << "Usage: kvstore_bench [--smoke] [--mode throughput|latency] [--json PATH|-]\n"
                     "       [--engine-metrics on|off] [--trials N] [--threads N[,N...]] [--seed N] [--keys N]\n"
                     "       [--ops N] [--read-ops N] [--warmup N] [--temp-root DIR]\n"
                     "       [--workload read|write|mixed|compaction|reopen]\n";
        return 0;
    }
    if (argc == 2 && std::string_view(argv[1]) == "--self-test") {
        if (MathFixture()) return 0;
        std::cerr << "Benchmark math fixture failed\n";
        return 1;
    }
    try {
        const auto options = Parse(argc, argv);
        std::ofstream file;
        // Preserve console-only invocation without building a JSON artifact in memory.
        std::ostream discard(nullptr);
        std::ostream* output = &discard;
        if (options.json == "-") output = &std::cout;
        else if (!options.json.empty()) {
            file.open(options.json);
            if (!file) throw std::runtime_error("cannot open JSON output: " + options.json);
            output = &file;
        }
        auto& out = *output;
        auto& console = options.json == "-" ? std::cerr : std::cout;
        const auto mode = options.latency ? "latency" : "throughput";
        console << "kvstore benchmark (" << mode << ", engine metrics "
                << (options.engine_metrics ? "on" : "off") << ", closed-loop)\n" << std::fixed << std::setprecision(1);
        out << std::setprecision(17) << "{\"schema_version\":1,\"telemetry_mode\":";
        Quote(out, mode);
        out << ",\"engine_metrics\":" << (options.engine_metrics ? "true" : "false")
            << ",\"environment\":";
        EnvironmentJson(out, options.temp_root);
        out << ",\"workloads\":[";
        bool passed = true;
        bool first = true;
        const auto run = [&](Workload workload) {
            if (!first) out << ',';
            first = false;
            passed = RunWorkload(out, console, options, workload) && passed;
        };
        for (const auto workload : kWorkloads) {
            if (!options.workload.empty() && workload != options.workload) continue;
            if (workload == "reopen") {
                run({workload, false, 1});
                continue;
            }
            for (const bool hot : {false, true}) {
                for (const unsigned threads : options.threads) run({workload, hot, threads});
            }
        }
        out << "],\"passed\":" << (passed ? "true" : "false") << "}\n";
        out.flush();
        if (!options.json.empty() && !out) throw std::runtime_error("writing JSON failed");
        return passed ? 0 : 1;
    } catch (const std::exception& error) {
        std::cerr << "kvstore_bench: " << error.what() << '\n';
        return 1;
    }
}
