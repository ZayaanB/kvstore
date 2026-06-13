#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace kvstore {

// 16 shards is a decent default for spreading out lock contention.
constexpr std::size_t kNumShards = 16;

// sentinel meaning "no expiry" for entry.expires_at and the ttl wal field.
constexpr std::int64_t kNoExpiry = 0;

// what kind of mutation a wal record represents.
enum class RecordType : std::uint8_t {
    kSet = 1,
    kDelete = 2,
    kSetTtl = 3,
};

// header written before every record.
// kSet: followed by key bytes then value bytes.
// kDelete: followed by key bytes only.
// kSetTtl: followed by key bytes, then value bytes, then an 8-byte expires_at.
struct RecordHeader {
    RecordType type;
    std::uint32_t key_len;
    std::uint32_t value_len;
};

// one value slot in a shard: the payload plus an optional expiry.
struct Entry {
    std::string value;
    // unix epoch seconds, or kNoExpiry if this entry never expires.
    std::int64_t expires_at = kNoExpiry;
};

// one shard of the keyspace: its own map and its own lock.
struct Shard {
    mutable std::shared_mutex mutex;
    std::unordered_map<std::string, Entry> data;
};

// a single key/value pair as returned by range scans.
struct KeyValue {
    std::string key;
    std::string value;
};

// a simple embedded key-value store with a write-ahead log for durability.
class KVStore {
public:
    // opens (or creates) the store at the given path and replays its wal.
    explicit KVStore(std::string path);

    ~KVStore();

    // not copyable - this thing owns a file handle and a bunch of mutexes.
    KVStore(const KVStore&) = delete;
    KVStore& operator=(const KVStore&) = delete;

    // insert or overwrite a key with no expiry.
    bool Set(const std::string& key, const std::string& value);

    // insert or overwrite a key that expires after ttl_seconds from now.
    bool SetWithTtl(const std::string& key, const std::string& value,
                     std::int64_t ttl_seconds);

    // look up a key, empty optional if it's not present or has expired.
    std::optional<std::string> Get(const std::string& key) const;

    // remove a key, still durable/logged even if it wasn't present.
    bool Delete(const std::string& key);

    // rewrite the log down to just the live, non-expired keys and swap it in atomically.
    bool Compact();

    // total number of keys across all shards right now, including expired-but-not-purged ones.
    std::size_t Size() const;

    // snapshot of every live, non-expired key/value pair, sorted by key.
    std::vector<KeyValue> Items() const;

    // snapshot of every live, non-expired key in [start, end) (end exclusive), sorted by key.
    // an empty end means "no upper bound".
    std::vector<KeyValue> Range(const std::string& start, const std::string& end) const;

    // mostly useful for tests that want to peek at the log file directly.
    const std::string& WalPath() const { return wal_path_; }

private:
    static std::size_t ShardIndex(const std::string& key);

    Shard& ShardFor(const std::string& key);
    const Shard& ShardFor(const std::string& key) const;

    // current wall-clock time as unix epoch seconds.
    static std::int64_t NowSeconds();

    // true if expires_at is set and is at or before now.
    static bool IsExpired(std::int64_t expires_at);

    // writes one record to the wal stream and flushes it, caller must hold wal_mutex_.
    bool AppendRecordLocked(RecordType type, const std::string& key,
                             const std::string& value, std::int64_t expires_at);

    // reads the wal from the start and replays every record into the shards.
    void Recover();

    // (re)opens the wal file for appending.
    void OpenWalForAppend();

    // shared logic for Set and SetWithTtl.
    bool SetInternal(const std::string& key, const std::string& value,
                      std::int64_t expires_at);

    std::string path_;
    std::string wal_path_;
    std::string wal_tmp_path_;

    std::array<Shard, kNumShards> shards_;

    // guards wal writes and doubles as the lock held during compaction's file swap.
    mutable std::mutex wal_mutex_;
    std::ofstream wal_stream_;

    // stops two compactions from running at the same time.
    std::atomic<bool> compaction_in_progress_{false};
};

}
