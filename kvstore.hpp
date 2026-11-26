#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
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

enum class OpenMode { Strict, Degraded, Existing };

class ShardUnavailable : public std::runtime_error {
public:
    explicit ShardUnavailable(std::size_t shard);
};

struct SnapshotRow {
    std::string key;
    std::string value;
    std::optional<std::int64_t> expires_at;
};

class KVStore {
public:
    class Snapshot {
    public:
        ~Snapshot();
        std::int64_t CapturedAt() const;
        bool Next(SnapshotRow& row);

    private:
        friend class KVStore;
        struct Cursor;
        explicit Snapshot(std::unique_ptr<Cursor> cursor);
        std::unique_ptr<Cursor> cursor_;
    };

    // Opens or creates a database directory. An existing legacy WAL requires migration.
    // The budget is shared by the shards and persisted when the database is created.
    explicit KVStore(std::string path, std::shared_ptr<Metrics> metrics = {},
                     std::optional<std::size_t> memtable_budget = std::nullopt,
                     OpenMode mode = OpenMode::Strict);
    ~KVStore();
    KVStore(const KVStore&) = delete;
    KVStore& operator=(const KVStore&) = delete;

    // I/O failures return false. Reads throw after an uncertain failure.
    // Degraded mode isolates the affected shard. Allocation errors can throw.
    bool Set(const std::string& key, const std::string& value);
    bool SetWithTtl(const std::string& key, const std::string& value, std::int64_t ttl_seconds);
    std::optional<std::string> Get(const std::string& key) const;
    bool Delete(const std::string& key);

    // Flush pending writes and merge tables synchronously.
    bool Compact();

    // Aggregate reads are rolling views across shards.
    std::size_t Size() const;
    std::vector<KeyValue> Items() const;
    // Live pairs in [start, end). Empty end means no upper bound.
    std::vector<KeyValue> Range(const std::string& start, const std::string& end) const;

    // Holds off writes until EOF or destruction. Consume and destroy on the calling thread.
    std::unique_ptr<Snapshot> OpenSnapshot() const;
    std::vector<std::size_t> UnavailableShards() const;
    // Saves a verified working copy to a new directory.
    void Checkpoint(const std::string& destination) const;
    // Replaces one shard from a checkpoint. Both databases must be closed.
    static void RestoreShard(const std::string& database, const std::string& checkpoint, std::size_t shard);

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
    // Copies live rows into a new database using rendezvous hashing.
    static void Rehash(const std::string& source, const std::string& destination);

private:
    struct Engine;
    std::shared_ptr<Engine> engine_;
};

}
