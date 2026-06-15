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

    // number of independent keyspace partitions.
    constexpr std::size_t kNumShards = 16;

    // sentinel value meaning this entry has no expiry.
    constexpr std::int64_t kNoExpiry = 0;

    // type tag written at the start of every log record.
    enum class RecordType : std::uint8_t {
        kSet = 1,
        kDelete = 2,
        kSetTtl = 3,
    };

    // fixed-size prefix for every log record, followed by key and value bytes on disk.
    struct RecordHeader {
        RecordType type;
        std::uint32_t key_len;
        std::uint32_t value_len;
    };

    // a stored value and its optional expiry time.
    struct Entry {
        std::string value;
        std::int64_t expires_at = kNoExpiry;
    };

    // one partition of the keyspace with its own map and lock.
    struct Shard {
        mutable std::shared_mutex mutex;
        std::unordered_map<std::string, Entry> data;
    };

    // a key/value pair returned by scans.
    struct KeyValue {
        std::string key;
        std::string value;
    };

    // embedded key-value store backed by a write-ahead log.
    class KVStore {
    public:
        // opens or creates a store at path, replaying the log if one exists.
        explicit KVStore(std::string path);

        ~KVStore();

        // non-copyable.
        KVStore(const KVStore&) = delete;
        KVStore& operator=(const KVStore&) = delete;

        // insert or overwrite a key.
        bool Set(const std::string& key, const std::string& value);

        // insert or overwrite a key that expires after ttl_seconds.
        bool SetWithTtl(const std::string& key, const std::string& value,
                        std::int64_t ttl_seconds);

        // returns the value for key, or empty if absent or expired.
        std::optional<std::string> Get(const std::string& key) const;

        // removes a key.
        bool Delete(const std::string& key);

        // rewrites the log to contain only live keys, then swaps it in atomically.
        bool Compact();

        // number of live keys.
        std::size_t Size() const;

        // returns all live key/value pairs sorted by key.
        std::vector<KeyValue> Items() const;

        // returns live key/value pairs in [start, end), sorted by key.
        // an empty end means no upper bound.
        std::vector<KeyValue> Range(const std::string& start, const std::string& end) const;

        // path to the active log file.
        const std::string& WalPath() const { return wal_path_; }

    private:
        static std::size_t ShardIndex(const std::string& key);

        Shard& ShardFor(const std::string& key);
        const Shard& ShardFor(const std::string& key) const;

        static std::int64_t NowSeconds();
        static bool IsExpired(std::int64_t expires_at);

        bool AppendRecordLocked(RecordType type, const std::string& key,
                                const std::string& value, std::int64_t expires_at);

        void Recover();
        void OpenWalForAppend();

        bool SetInternal(const std::string& key, const std::string& value,
                        std::int64_t expires_at);

        std::string path_;
        std::string wal_path_;
        std::string wal_tmp_path_;

        std::array<Shard, kNumShards> shards_;

        mutable std::mutex wal_mutex_;
        std::ofstream wal_stream_;

        std::atomic<bool> compaction_in_progress_{false};
    };

}