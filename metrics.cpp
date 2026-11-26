#include "metrics.hpp"

#include <algorithm>
#include <iomanip>
#include <locale>
#include <sstream>

namespace kvstore {
namespace {

template <class Enum>
constexpr std::size_t Index(Enum value) {
    return static_cast<std::size_t>(value);
}

constexpr auto kRelaxed = std::memory_order_relaxed;
constexpr std::array<const char*, Index(Operation::Count)> kOperations{
    "get", "set", "set_ttl", "delete", "size", "range", "compact", "recovery", "flush", "disk_lookup"
};
constexpr std::array<const char*, Index(Outcome::Count)> kOutcomes{"ok", "miss", "error"};
constexpr std::array<const char*, Index(WalSync::Count)> kSyncs{"append", "rollback", "open", "recovery"};
constexpr std::array<const char*, Index(WriteSource::Count)> kSources{"wal", "compaction"};

double Seconds(std::uint64_t ns) {
    return static_cast<double>(ns) * 1e-9;
}

} // namespace

void Metrics::Histogram::Add(Clock::duration duration) noexcept {
    const auto ns = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count());
    const auto bucket = std::lower_bound(kBounds.begin(), kBounds.end(), ns);
    bins[static_cast<std::size_t>(bucket - kBounds.begin())].fetch_add(1, kRelaxed);
    sum_ns.fetch_add(ns, kRelaxed);
}

void Metrics::Histogram::Write(std::ostream& out, const char* name, const std::string& label) const {
    std::uint64_t count = 0;
    for (std::size_t i = 0; i < bins.size(); ++i) {
        count += bins[i].load(kRelaxed);
        out << name << "_bucket{" << label << ",le=\"";
        if (i == kBounds.size()) out << "+Inf";
        else out << Seconds(kBounds[i]);
        out << "\"} " << count << '\n';
    }
    out << name << "_count{" << label << "} " << count << '\n'
        << name << "_sum{" << label << "} " << Seconds(sum_ns.load(kRelaxed)) << '\n';
}

void Metrics::Observe(Operation operation, Outcome outcome, Clock::duration duration) noexcept {
    operations_[Index(operation)][Index(outcome)].fetch_add(1, kRelaxed);
    latencies_[Index(operation)].Add(duration);
}

void Metrics::Observe(WalSync sync, bool ok, Clock::duration duration) noexcept {
    syncs_[Index(sync)][ok ? 0 : 1].fetch_add(1, kRelaxed);
    sync_latencies_[Index(sync)].Add(duration);
}

void Metrics::AddWritten(WriteSource source, std::uint64_t bytes) noexcept {
    written_[Index(source)].fetch_add(bytes, kRelaxed);
}

StorageTotals Metrics::Storage() const noexcept {
    StorageTotals totals;
    for (std::size_t i = 0; i < syncs_.size(); ++i) {
        const auto failed = syncs_[i][1].load(kRelaxed);
        totals.failed_wal_syncs += failed;
        totals.wal_syncs += failed + syncs_[i][0].load(kRelaxed);
        totals.wal_sync_ns += sync_latencies_[i].sum_ns.load(kRelaxed);
    }
    totals.wal_written_bytes = written_[Index(WriteSource::Wal)].load(kRelaxed);
    totals.compaction_written_bytes = written_[Index(WriteSource::Compaction)].load(kRelaxed);
    return totals;
}

std::string Metrics::Prometheus() const {
    std::ostringstream out;
    out.imbue(std::locale::classic());
    out << std::setprecision(12) << "# HELP kvstore_operations_total Completed operations by engine operation and outcome.\n"
           "# TYPE kvstore_operations_total counter\n";
    for (std::size_t operation = 0; operation < operations_.size(); ++operation) {
        for (std::size_t outcome = 0; outcome < kOutcomes.size(); ++outcome) {
            out << "kvstore_operations_total{operation=\"" << kOperations[operation]
                << "\",outcome=\"" << kOutcomes[outcome] << "\"} "
                << operations_[operation][outcome].load(kRelaxed) << '\n';
        }
    }
    out << "# HELP kvstore_operation_duration_seconds Observed wall time of attempted engine operations in seconds.\n"
           "# TYPE kvstore_operation_duration_seconds histogram\n";
    for (std::size_t i = 0; i < latencies_.size(); ++i) {
        latencies_[i].Write(out, "kvstore_operation_duration_seconds", std::string("operation=\"") + kOperations[i] + '"');
    }
    out << "# HELP kvstore_wal_syncs_total Logical WAL file sync calls by phase and outcome.\n"
           "# TYPE kvstore_wal_syncs_total counter\n";
    for (std::size_t i = 0; i < syncs_.size(); ++i) {
        out << "kvstore_wal_syncs_total{phase=\"" << kSyncs[i] << "\",outcome=\"ok\"} " << syncs_[i][0].load(kRelaxed) << '\n'
            << "kvstore_wal_syncs_total{phase=\"" << kSyncs[i] << "\",outcome=\"error\"} " << syncs_[i][1].load(kRelaxed) << '\n';
    }
    out << "# HELP kvstore_wal_sync_duration_seconds Observed wall time of WAL file sync calls in seconds.\n"
           "# TYPE kvstore_wal_sync_duration_seconds histogram\n";
    for (std::size_t i = 0; i < sync_latencies_.size(); ++i) {
        sync_latencies_[i].Write(out, "kvstore_wal_sync_duration_seconds", std::string("phase=\"") + kSyncs[i] + '"');
    }
    out << "# HELP kvstore_storage_written_bytes_total Bytes accepted by write syscalls by storage source, including rolled-back writes.\n"
           "# TYPE kvstore_storage_written_bytes_total counter\n";
    for (std::size_t i = 0; i < written_.size(); ++i) {
        out << "kvstore_storage_written_bytes_total{source=\"" << kSources[i] << "\"} " << written_[i].load(kRelaxed) << '\n';
    }
    return out.str();
}

} // namespace kvstore
