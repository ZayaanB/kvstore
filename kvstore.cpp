#include "kvstore.hpp"

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <stdexcept>

namespace kvstore {

    namespace fs = std::filesystem;

    KVStore::KVStore(std::string path)
        : path_(std::move(path)),
        wal_path_(path_ + ".wal"),
        wal_tmp_path_(path_ + ".wal.tmp") {
        // make sure the directory the db lives in actually exists.
        fs::path p(path_);
        if (p.has_parent_path()) {
            std::error_code ec;
            fs::create_directories(p.parent_path(), ec);
        }

        Recover();
        OpenWalForAppend();
    }

    KVStore::~KVStore() {
        std::lock_guard<std::mutex> lock(wal_mutex_);
        if (wal_stream_.is_open()) {
            wal_stream_.flush();
            wal_stream_.close();
        }
    }

    std::size_t KVStore::ShardIndex(const std::string& key) {
        return std::hash<std::string>{}(key) % kNumShards;
    }

    Shard& KVStore::ShardFor(const std::string& key) {
        return shards_[ShardIndex(key)];
    }

    const Shard& KVStore::ShardFor(const std::string& key) const {
        return shards_[ShardIndex(key)];
    }

    std::int64_t KVStore::NowSeconds() {
        using namespace std::chrono;
        return duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    }

    bool KVStore::IsExpired(std::int64_t expires_at) {
        return expires_at != kNoExpiry && expires_at <= NowSeconds();
    }

    void KVStore::OpenWalForAppend() {
        std::lock_guard<std::mutex> lock(wal_mutex_);
        wal_stream_.open(wal_path_, std::ios::binary | std::ios::app | std::ios::out);
        if (!wal_stream_.is_open()) {
            throw std::runtime_error("KVStore: failed to open WAL file: " + wal_path_);
        }
    }

    void KVStore::Recover() {
        std::ifstream in(wal_path_, std::ios::binary);
        if (!in.is_open()) {
            // no log yet, so there's nothing to recover.
            return;
        }

        while (true) {
            RecordHeader hdr{};
            in.read(reinterpret_cast<char*>(&hdr.type), sizeof(hdr.type));
            if (in.eof()) break;
            in.read(reinterpret_cast<char*>(&hdr.key_len), sizeof(hdr.key_len));
            if (!in || in.eof()) break;
            in.read(reinterpret_cast<char*>(&hdr.value_len), sizeof(hdr.value_len));
            if (!in || in.eof()) break;

            // a length past these bounds means a corrupt header, so stop replaying.
            if (hdr.key_len > kMaxKeyLen || hdr.value_len > kMaxValueLen) break;

            std::string key(hdr.key_len, '\0');
            if (hdr.key_len > 0) {
                in.read(key.data(), static_cast<std::streamsize>(hdr.key_len));
                // partial key means the last write was cut short by a crash.
                if (!in) break;
            }

            std::string value;
            if (hdr.type == RecordType::kSet || hdr.type == RecordType::kSetTtl) {
                value.resize(hdr.value_len);
                if (hdr.value_len > 0) {
                    in.read(value.data(), static_cast<std::streamsize>(hdr.value_len));
                    // same deal - truncated value, stop replaying.
                    if (!in) break;
                }
            }

            std::int64_t expires_at = kNoExpiry;
            if (hdr.type == RecordType::kSetTtl) {
                in.read(reinterpret_cast<char*>(&expires_at), sizeof(expires_at));
                // truncated ttl field, stop replaying.
                if (!in) break;
            }

            switch (hdr.type) {
                case RecordType::kSet: {
                    Shard& shard = ShardFor(key);
                    std::unique_lock lock(shard.mutex);
                    shard.data[key] = Entry{std::move(value), kNoExpiry};
                    break;
                }
                case RecordType::kSetTtl: {
                    Shard& shard = ShardFor(key);
                    std::unique_lock lock(shard.mutex);
                    shard.data[key] = Entry{std::move(value), expires_at};
                    break;
                }
                case RecordType::kDelete: {
                    Shard& shard = ShardFor(key);
                    std::unique_lock lock(shard.mutex);
                    shard.data.erase(key);
                    break;
                }
                default:
                    // unrecognized record type, stop rather than read garbage.
                    return;
            }
        }
    }

    bool KVStore::AppendRecordLocked(RecordType type, const std::string& key,
                                    const std::string& value, std::int64_t expires_at) {
        RecordHeader hdr{};
        hdr.type = type;
        hdr.key_len = static_cast<std::uint32_t>(key.size());
        hdr.value_len = (type == RecordType::kDelete)
                            ? 0u
                            : static_cast<std::uint32_t>(value.size());

        wal_stream_.write(reinterpret_cast<const char*>(&hdr.type), sizeof(hdr.type));
        wal_stream_.write(reinterpret_cast<const char*>(&hdr.key_len), sizeof(hdr.key_len));
        wal_stream_.write(reinterpret_cast<const char*>(&hdr.value_len), sizeof(hdr.value_len));
        if (!key.empty()) {
            wal_stream_.write(key.data(), static_cast<std::streamsize>(key.size()));
        }
        if (type != RecordType::kDelete && !value.empty()) {
            wal_stream_.write(value.data(), static_cast<std::streamsize>(value.size()));
        }
        if (type == RecordType::kSetTtl) {
            wal_stream_.write(reinterpret_cast<const char*>(&expires_at), sizeof(expires_at));
        }

        // flush now so the record is on disk before we touch the in-memory map.
        wal_stream_.flush();
        return wal_stream_.good();
    }

    bool KVStore::SetInternal(const std::string& key, const std::string& value,
                                std::int64_t expires_at) {
        RecordType type = (expires_at == kNoExpiry) ? RecordType::kSet : RecordType::kSetTtl;

        // hold wal_mutex_ across the append and the apply so wal order and visible state can't diverge.
        std::lock_guard<std::mutex> wal_lock(wal_mutex_);
        if (!AppendRecordLocked(type, key, value, expires_at)) {
            return false;
        }

        Shard& shard = ShardFor(key);
        std::unique_lock lock(shard.mutex);
        shard.data[key] = Entry{value, expires_at};
        return true;
    }

    bool KVStore::Set(const std::string& key, const std::string& value) {
        return SetInternal(key, value, kNoExpiry);
    }

    bool KVStore::SetWithTtl(const std::string& key, const std::string& value,
                            std::int64_t ttl_seconds) {
        // ttl <= 0 means "expire immediately" - still a valid, durable write.
        std::int64_t expires_at = NowSeconds() + ttl_seconds;
        if (expires_at == kNoExpiry) {
            // extremely unlikely collision with the sentinel, nudge by one second.
            expires_at += 1;
        }
        return SetInternal(key, value, expires_at);
    }

    std::optional<std::string> KVStore::Get(const std::string& key) const {
        const Shard& shard = ShardFor(key);
        std::shared_lock lock(shard.mutex);
        auto it = shard.data.find(key);
        if (it == shard.data.end()) {
            return std::nullopt;
        }
        // expired entries are treated as absent, but stay on disk until compaction.
        if (IsExpired(it->second.expires_at)) {
            return std::nullopt;
        }
        return it->second.value;
    }

    bool KVStore::Delete(const std::string& key) {
        // hold wal_mutex_ across the append and the apply so wal order and visible state can't diverge.
        std::lock_guard<std::mutex> wal_lock(wal_mutex_);
        if (!AppendRecordLocked(RecordType::kDelete, key, std::string(), kNoExpiry)) {
            return false;
        }

        Shard& shard = ShardFor(key);
        std::unique_lock lock(shard.mutex);
        shard.data.erase(key);
        return true;
    }

    std::size_t KVStore::Size() const {
        std::size_t total = 0;
        for (const auto& shard : shards_) {
            std::shared_lock lock(shard.mutex);
            for (const auto& [key, entry] : shard.data) {
                if (!IsExpired(entry.expires_at)) {
                    ++total;
                }
            }
        }
        return total;
    }

    std::vector<KeyValue> KVStore::Items() const {
        return Range(std::string(), std::string());
    }

    std::vector<KeyValue> KVStore::Range(const std::string& start, const std::string& end) const {
        std::vector<KeyValue> result;

        // collect matching, non-expired entries shard by shard under shared locks.
        for (const auto& shard : shards_) {
            std::shared_lock lock(shard.mutex);
            for (const auto& [key, entry] : shard.data) {
                if (IsExpired(entry.expires_at)) {
                    continue;
                }
                if (key < start) {
                    continue;
                }
                if (!end.empty() && key >= end) {
                    continue;
                }
                result.push_back(KeyValue{key, entry.value});
            }
        }

        // shards are unordered internally, so sort the merged snapshot by key.
        std::sort(result.begin(), result.end(),
                [](const KeyValue& a, const KeyValue& b) { return a.key < b.key; });

        return result;
    }

    bool KVStore::Compact() {
        bool expected = false;
        if (!compaction_in_progress_.compare_exchange_strong(expected, true)) {
            // somebody's already compacting, don't pile on.
            return false;
        }

        struct CompactionGuard {
            std::atomic<bool>& flag;
            ~CompactionGuard() { flag.store(false); }
        } guard{compaction_in_progress_};

        // hold wal_mutex_ across the whole snapshot and swap so no write is lost mid-rename.
        std::lock_guard<std::mutex> wal_lock(wal_mutex_);

        // write a clean copy of every live, non-expired key/value pair to a temp file.
        {
            std::ofstream tmp(wal_tmp_path_, std::ios::binary | std::ios::trunc | std::ios::out);
            if (!tmp.is_open()) {
                return false;
            }

            // shard-by-shard shared locks let other shards (and readers here) keep going.
            for (auto& shard : shards_) {
                std::shared_lock lock(shard.mutex);
                for (const auto& [key, entry] : shard.data) {
                    // drop expired entries during compaction instead of carrying them forward.
                    if (IsExpired(entry.expires_at)) {
                        continue;
                    }

                    RecordType type = (entry.expires_at == kNoExpiry)
                                        ? RecordType::kSet
                                        : RecordType::kSetTtl;

                    RecordHeader hdr{};
                    hdr.type = type;
                    hdr.key_len = static_cast<std::uint32_t>(key.size());
                    hdr.value_len = static_cast<std::uint32_t>(entry.value.size());

                    tmp.write(reinterpret_cast<const char*>(&hdr.type), sizeof(hdr.type));
                    tmp.write(reinterpret_cast<const char*>(&hdr.key_len), sizeof(hdr.key_len));
                    tmp.write(reinterpret_cast<const char*>(&hdr.value_len), sizeof(hdr.value_len));
                    if (!key.empty()) {
                        tmp.write(key.data(), static_cast<std::streamsize>(key.size()));
                    }
                    if (!entry.value.empty()) {
                        tmp.write(entry.value.data(), static_cast<std::streamsize>(entry.value.size()));
                    }
                    if (type == RecordType::kSetTtl) {
                        tmp.write(reinterpret_cast<const char*>(&entry.expires_at), sizeof(entry.expires_at));
                    }
                }
            }

            tmp.flush();
            if (!tmp.good()) {
                tmp.close();
                std::error_code ec;
                fs::remove(wal_tmp_path_, ec);
                return false;
            }
            tmp.close();
        }

        // swap the temp file in (still holding wal_mutex_) so no write is lost.
        {
            if (wal_stream_.is_open()) {
                wal_stream_.flush();
                wal_stream_.close();
            }

            std::error_code ec;
            fs::rename(wal_tmp_path_, wal_path_, ec);
            if (ec) {
                // rename failed, try to reopen the old log and report failure.
                wal_stream_.open(wal_path_, std::ios::binary | std::ios::app | std::ios::out);
                return false;
            }

            wal_stream_.open(wal_path_, std::ios::binary | std::ios::app | std::ios::out);
            if (!wal_stream_.is_open()) {
                return false;
            }
        }

        // purge expired entries from the in-memory shards so scan and Size() reflect reality
        for (auto& shard : shards_) {
            std::unique_lock lock(shard.mutex);
            for (auto it = shard.data.begin(); it != shard.data.end(); ) {
                if (IsExpired(it->second.expires_at)) {
                    it = shard.data.erase(it);
                } else {
                    ++it;
                }
            }
        }

        return true;
    }

}
