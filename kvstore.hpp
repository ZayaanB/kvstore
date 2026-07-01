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

    constexpr std::size_t kNumShards = 16;

    // "no expiry" sentinel.
    constexpr std::int64_t kNoExpiry = 0;

    // sanity bounds; a bigger field means a corrupt header.
    constexpr std::uint32_t kMaxKeyLen = 1u << 20;    // 1 MiB
    constexpr std::uint32_t kMaxValueLen = 1u << 28;  // 256 MiB

    enum class RecordType : std::uint8_t {
        kSet = 1,
        kDelete = 2,
        kSetTtl = 3,
    };

    // record prefix. on-disk layout: [type][key_len][value_len][key][value][expires_at?][crc32].
    // sizeof(RecordHeader) is not the serialized size -- don't use it as a byte count.
    struct RecordHeader {
        RecordType type;
        std::uint32_t key_len;
        std::uint32_t value_len;
    };

    struct Entry {
        std::string value;
        std::int64_t expires_at = kNoExpiry;
    };

    // one keyspace partition, with its own lock.
    struct Shard {
        mutable std::shared_mutex mutex;
        std::unordered_map<std::string, Entry> data;
    };

    struct KeyValue {
        std::string key;
        std::string value;
    };

    // key-value store backed by a write-ahead log.
    class KVStore {
    public:
        // opens or creates a store at path, replaying any existing log.
        explicit KVStore(std::string path);

        ~KVStore();

        KVStore(const KVStore&) = delete;
        KVStore& operator=(const KVStore&) = delete;

        bool Set(const std::string& key, const std::string& value);

        // like Set, but the key expires after ttl_seconds.
        bool SetWithTtl(const std::string& key, const std::string& value,
                        std::int64_t ttl_seconds);

        // value, or empty if absent or expired.
        std::optional<std::string> Get(const std::string& key) const;

        bool Delete(const std::string& key);

        // rewrite the log with only live keys, swapped in atomically.
        bool Compact();

        std::size_t Size() const;

        std::vector<KeyValue> Items() const;

        // live pairs in [start, end); empty end means no upper bound.
        std::vector<KeyValue> Range(const std::string& start, const std::string& end) const;

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
        int wal_fd_ = -1;

        std::atomic<bool> compaction_in_progress_{false};
    };

}