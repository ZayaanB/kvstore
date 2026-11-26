#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace kvstore {

class Metrics;

inline constexpr std::size_t kNumShards = 16;
inline constexpr std::size_t kDefaultMemtableBudget = 16u << 20;
inline constexpr std::int64_t kNoExpiry = 0;
inline constexpr std::uint32_t kMaxKeyLen = 1u << 20;
inline constexpr std::uint32_t kMaxValueLen = 1u << 28;

struct KeyValue {
    std::string key;
    std::string value;
};

class KVStore {
public:
    // Opens or creates a database directory. An existing legacy WAL requires migration.
    // The budget is shared by the shards and persisted when the database is created.
    explicit KVStore(std::string path, std::shared_ptr<Metrics> metrics = {},
                     std::optional<std::size_t> memtable_budget = std::nullopt);
    ~KVStore();
    KVStore(const KVStore&) = delete;
    KVStore& operator=(const KVStore&) = delete;

    // I/O failures return false. After an uncertain failure, writes return false
    // and reads throw until the store is reopened. Allocation errors can throw.
    bool Set(const std::string& key, const std::string& value);
    bool SetWithTtl(const std::string& key, const std::string& value, std::int64_t ttl_seconds);
    std::optional<std::string> Get(const std::string& key) const;
    bool Delete(const std::string& key);

    // Flush all pending memtables synchronously.
    bool Compact();

    // Aggregate reads are rolling views across shards.
    std::size_t Size() const;
    std::vector<KeyValue> Items() const;
    // Live pairs in [start, end). Empty end means no upper bound.
    std::vector<KeyValue> Range(const std::string& start, const std::string& end) const;

    const std::string& DatabasePath() const;
    // Active paths in shard order. A flush may retire any of these paths.
    std::vector<std::string> WalPaths() const;
    std::uintmax_t WalBytes() const;
    // Rolling snapshot of committed active WAL bytes, available with a recorder.
    std::optional<std::uint64_t> TelemetryWalBytes() const noexcept;

    // Replay a legacy <source>.wal into a new destination directory while
    // holding both process locks. Throws on validation or publication errors.
    static void MigrateLegacy(const std::string& source, const std::string& destination,
                              std::size_t memtable_budget = kDefaultMemtableBudget);

private:
    struct Engine;
    std::unique_ptr<Engine> engine_;
};

}
