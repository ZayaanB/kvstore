#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <numeric>
#include <ostream>
#include <vector>

namespace bench {

inline double Rate(std::uint64_t count, double seconds) {
    return seconds > 0 ? static_cast<double>(count) / seconds : 0;
}

// Exact through 63 ns, then 32 bins per power of two. Percentiles use
// nearest rank and return the inclusive upper bound of the selected bin.
struct Histogram {
    static constexpr std::size_t kSteps = 32;
    std::array<std::uint64_t, 64 * kSteps> bins{};
    std::uint64_t count = 0;
    double sum_ns = 0;

    void Add(std::uint64_t ns) {
        const auto width = std::bit_width(ns);
        const auto shift = width > 6 ? width - 6 : 0;
        ++bins[static_cast<std::size_t>(shift) * kSteps + (ns >> shift)];
        ++count;
        sum_ns += static_cast<double>(ns);
    }

    void Merge(const Histogram& other) {
        for (std::size_t i = 0; i < bins.size(); ++i) bins[i] += other.bins[i];
        count += other.count;
        sum_ns += other.sum_ns;
    }

    double Percentile(unsigned percent) const {
        const auto rank = count / 100 * percent + (count % 100 * percent + 99) / 100;
        std::uint64_t cumulative = 0;
        for (std::size_t i = 0; i < bins.size(); ++i) {
            cumulative += bins[i];
            if (cumulative >= rank && bins[i] != 0) {
                const auto shift = i < 64 ? 0 : i / kSteps - 1;
                const auto mantissa = i < 64 ? i : i % kSteps + kSteps;
                return std::ldexp(static_cast<double>(mantissa + 1), static_cast<int>(shift)) - 1;
            }
        }
        return 0;
    }

    void Json(std::ostream& out) const {
        out << "{\"samples\":" << count;
        if (count == 0) {
            out << ",\"mean_seconds\":null,\"p50_seconds\":null,\"p95_seconds\":null,\"p99_seconds\":null}";
            return;
        }
        out << ",\"mean_seconds\":" << sum_ns / static_cast<double>(count) * 1e-9
            << ",\"p50_seconds\":" << Percentile(50) * 1e-9
            << ",\"p95_seconds\":" << Percentile(95) * 1e-9
            << ",\"p99_seconds\":" << Percentile(99) * 1e-9 << '}';
    }
};

struct Counts {
    std::uint64_t ok = 0;
    std::uint64_t misses = 0;
    std::uint64_t errors = 0;
    Histogram latency;

    std::uint64_t Completed() const { return ok + misses; }
    std::uint64_t Attempted() const { return Completed() + errors; }

    void Merge(const Counts& other) {
        ok += other.ok;
        misses += other.misses;
        errors += other.errors;
        latency.Merge(other.latency);
    }
};

inline double Median(std::vector<double> values) {
    std::sort(values.begin(), values.end());
    const auto middle = values.size() / 2;
    return values.size() % 2 ? values[middle] : std::midpoint(values[middle - 1], values[middle]);
}

} // namespace bench
