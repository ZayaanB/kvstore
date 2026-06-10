#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <fstream>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace kvstore {

constexpr std::size_t kNumShards = 16;

// Binary WAL record types
enum class RecordType : std::uint8_t {
    kSet = 1,
    kDelete = 2,
};

// On-disk record header (followed by key bytes, then value bytes for kSet)
struct RecordHeader {
    RecordType type;
    std::uint32_t key_len;
    std::uint32_t value_len; // 0 for delete
};

// A single shard: its own map + its own lock for fine-grained concurrency.
struct Shard {
    mutable std::shared_mutex mutex;
    std::unordered_map<std::string, std::string> data;
};

// Production-grade embedded KV store with WAL durability,
// crash recovery and online compaction.
class KVStore {
public:
    // Opens (creating if necessary) the store at the given path.
    // Performs crash recovery by replaying the WAL.
    explicit KVStore(std::string path);

    ~KVStore();

    KVStore(const KVStore&) = delete;
    KVStore& operator=(const KVStore&) = delete;

    // Insert or update a key. Durable: WAL-flushed before returning true.
    bool Set(const std::string& key, const std::string& value);

    // Retrieve a value if present.
    std::optional<std::string> Get(const std::string& key) const;

    // Remove a key. Durable: WAL-flushed before returning true.
    bool Delete(const std::string& key);

    // Compact the WAL: write only live key/value pairs to a fresh log file
    // and atomically swap it in as the active log, without blocking readers
    // or writers for more than the per-shard critical sections.
    bool Compact();

    // Number of live keys across all shards (approximate under concurrency).
    std::size_t Size() const;

    // Path to the active WAL file (for test introspection).
    const std::string& WalPath() const { return wal_path_; }

private:
    static std::size_t ShardIndex(const std::string& key);

    Shard& ShardFor(const std::string& key);
    const Shard& ShardFor(const std::string& key) const;

    // Append a record to the WAL. Caller must hold wal_mutex_.
    bool AppendRecordLocked(RecordType type, const std::string& key,
                             const std::string& value);

    // Replays the WAL on startup to rebuild in-memory state.
    void Recover();

    // Opens (or re-opens) the WAL output stream in append mode.
    void OpenWalForAppend();

    std::string path_;
    std::string wal_path_;
    std::string wal_tmp_path_;

    std::array<Shard, kNumShards> shards_;

    // Guards the WAL output stream itself (separate from per-shard locks
    // so different shards can mutate concurrently while WAL writes are
    // serialized for on-disk ordering and atomic swap during compaction).
    mutable std::mutex wal_mutex_;
    std::ofstream wal_stream_;

    // Guards against concurrent Compact() invocations.
    std::atomic<bool> compaction_in_progress_{false};
};

} // namespace kvstore