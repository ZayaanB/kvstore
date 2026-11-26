#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <stdexcept>
#include <stop_token>
#include <string_view>

namespace kvstore::mapreduce {

// An already-open snapshot scan. The source owns shard traversal, key range
// selection, and TTL evaluation at the snapshot's captured timestamp.
// Row views last until the next Next() call. Return false at EOF and throw on error.
class SnapshotCursor {
public:
    virtual ~SnapshotCursor() = default;
    virtual bool Next(std::string_view& key, std::string_view& value) = 0;
};

class Values {
public:
    virtual ~Values() = default;
    // The value view lasts until the next Next() call. False ends this group.
    virtual bool Next(std::string_view& value) = 0;
};

using Emit = std::function<void(std::string_view key, std::string_view value)>;
using Write = std::function<void(std::string_view bytes)>;
using Mapper = std::function<void(std::string_view key, std::string_view value, const Emit&)>;
using Reducer = std::function<void(std::string_view key, Values&, const Write&)>;

struct Options {
    // In-memory map buffer, including sort entries. Merge holds at most one
    // record per input (each bounded by this limit), plus fixed I/O overhead.
    // Cursor and callback allocations are owned by the caller.
    std::size_t shuffle_bytes = 4u << 20;
    // Logical bytes of all concurrent temporary files, including staged output.
    std::uint64_t temporary_bytes = 1ull << 30;
    std::size_t partitions = 16;
    std::size_t merge_fan_in = 8;
};

struct Stats {
    std::size_t spill_runs = 0;
    std::size_t merge_passes = 0; // Maximum across partitions.
    std::uint64_t peak_temporary_bytes = 0;
};

class PublicationUncertain : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// Calls callbacks synchronously. Emit, Values and Write are valid only during
// their callback. Reducers run in partition order, then bytewise key order.
// Values within a group have unspecified order. A reducer may leave values unread.
// The destination's parent must exist. Run replaces the destination by rename;
// PublicationUncertain means rename succeeded but the directory sync failed.
Stats Run(SnapshotCursor& snapshot, const Mapper& mapper, const Reducer& reducer,
          const std::filesystem::path& destination, const Options& options = {},
          std::stop_token stop = {});

}
