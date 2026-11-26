#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iosfwd>
#include <string>

namespace kvstore {

enum class Operation { Get, Set, SetTtl, Delete, Size, Range, Compact, Recovery, Count };
enum class Outcome { Ok, Miss, Error, Count };
enum class WalSync { Append, Rollback, Open, Recovery, Compact, Count };
enum class WriteSource { Wal, Compaction, Count };

struct StorageTotals {
    std::uint64_t wal_syncs = 0;
    std::uint64_t failed_wal_syncs = 0;
    std::uint64_t wal_sync_ns = 0;
    std::uint64_t wal_written_bytes = 0;
    std::uint64_t compaction_written_bytes = 0;
};

// A recorder may be shared by worker threads. Exporting it never takes engine locks.
class Metrics {
public:
    using Clock = std::chrono::steady_clock;

    void Observe(Operation operation, Outcome outcome, Clock::duration duration) noexcept;
    void Observe(WalSync sync, bool ok, Clock::duration duration) noexcept;
    void AddWritten(WriteSource source, std::uint64_t bytes) noexcept;
    StorageTotals Storage() const noexcept;
    std::string Prometheus() const;

private:
    using Counter = std::atomic<std::uint64_t>;
    // Nanoseconds. Upper bounds are shared by all operation and sync histograms.
    static constexpr std::array<std::uint64_t, 22> kBounds{
        50, 100, 200, 400, 800, 1'600, 3'200, 6'400, 12'800, 25'600, 51'200,
        100'000, 250'000, 500'000, 1'000'000, 2'500'000, 5'000'000, 10'000'000,
        25'000'000, 100'000'000, 500'000'000, 5'000'000'000
    };

    struct Histogram {
        std::array<Counter, kBounds.size() + 1> bins{};
        Counter sum_ns{0};
        void Add(Clock::duration duration) noexcept;
        void Write(std::ostream& out, const char* name, const std::string& label) const;
    };

    std::array<std::array<Counter, static_cast<std::size_t>(Outcome::Count)>,
               static_cast<std::size_t>(Operation::Count)> operations_{};
    std::array<Histogram, static_cast<std::size_t>(Operation::Count)> latencies_{};
    std::array<std::array<Counter, 2>, static_cast<std::size_t>(WalSync::Count)> syncs_{};
    std::array<Histogram, static_cast<std::size_t>(WalSync::Count)> sync_latencies_{};
    std::array<Counter, static_cast<std::size_t>(WriteSource::Count)> written_{};
};

} // namespace kvstore
