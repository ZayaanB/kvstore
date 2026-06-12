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

// 16 shards is a decent default for spreading out lock contention.
constexpr std::size_t kNumShards = 16;

// what kind of mutation a wal record represents.
enum class RecordType : std::uint8_t {
    kSet = 1,
    kDelete = 2,
};

// header written before every record, followed by key bytes (and value bytes for a set).
struct RecordHeader {
    RecordType type;
    std::uint32_t key_len;
    std::uint32_t value_len;
};

// one shard of the keyspace: its own map and its own lock.
struct Shard {
    mutable std::shared_mutex mutex;
    std::unordered_map<std::string, std::string> data;
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

    // insert or overwrite a key.
    bool Set(const std::string& key, const std::string& value);

    // look up a key, empty optional if it's not present.
    std::optional<std::string> Get(const std::string& key) const;

    // remove a key, still durable/logged even if it wasn't present.
    bool Delete(const std::string& key);

    // rewrite the log down to just the live keys and swap it in atomically.
    bool Compact();

    // total number of keys across all shards right now.
    std::size_t Size() const;

    // mostly useful for tests that want to peek at the log file directly.
    const std::string& WalPath() const { return wal_path_; }

private:
    static std::size_t ShardIndex(const std::string& key);

    Shard& ShardFor(const std::string& key);
    const Shard& ShardFor(const std::string& key) const;

    // writes one record to the wal stream and flushes it, caller must hold wal_mutex_.
    bool AppendRecordLocked(RecordType type, const std::string& key,
                             const std::string& value);

    // reads the wal from the start and replays every record into the shards.
    void Recover();

    // (re)opens the wal file for appending.
    void OpenWalForAppend();

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
