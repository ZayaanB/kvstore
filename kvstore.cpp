#include "kvstore.hpp"

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

        std::string key(hdr.key_len, '\0');
        if (hdr.key_len > 0) {
            in.read(key.data(), static_cast<std::streamsize>(hdr.key_len));
            // partial key means the last write was cut short by a crash.
            if (!in) break;
        }

        std::string value;
        if (hdr.type == RecordType::kSet) {
            value.resize(hdr.value_len);
            if (hdr.value_len > 0) {
                in.read(value.data(), static_cast<std::streamsize>(hdr.value_len));
                // same deal - truncated value, stop replaying.
                if (!in) break;
            }
        }

        switch (hdr.type) {
            case RecordType::kSet: {
                Shard& shard = ShardFor(key);
                std::unique_lock lock(shard.mutex);
                shard.data[key] = std::move(value);
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
                                  const std::string& value) {
    RecordHeader hdr{};
    hdr.type = type;
    hdr.key_len = static_cast<std::uint32_t>(key.size());
    hdr.value_len = (type == RecordType::kSet)
                        ? static_cast<std::uint32_t>(value.size())
                        : 0u;

    wal_stream_.write(reinterpret_cast<const char*>(&hdr.type), sizeof(hdr.type));
    wal_stream_.write(reinterpret_cast<const char*>(&hdr.key_len), sizeof(hdr.key_len));
    wal_stream_.write(reinterpret_cast<const char*>(&hdr.value_len), sizeof(hdr.value_len));
    if (!key.empty()) {
        wal_stream_.write(key.data(), static_cast<std::streamsize>(key.size()));
    }
    if (type == RecordType::kSet && !value.empty()) {
        wal_stream_.write(value.data(), static_cast<std::streamsize>(value.size()));
    }

    // flush now so the record is on disk before we touch the in-memory map.
    wal_stream_.flush();
    return wal_stream_.good();
}

bool KVStore::Set(const std::string& key, const std::string& value) {
    // log first, apply second, so a crash in between is recoverable.
    {
        std::lock_guard<std::mutex> wal_lock(wal_mutex_);
        if (!AppendRecordLocked(RecordType::kSet, key, value)) {
            return false;
        }
    }

    Shard& shard = ShardFor(key);
    std::unique_lock lock(shard.mutex);
    shard.data[key] = value;
    return true;
}

std::optional<std::string> KVStore::Get(const std::string& key) const {
    const Shard& shard = ShardFor(key);
    std::shared_lock lock(shard.mutex);
    auto it = shard.data.find(key);
    if (it == shard.data.end()) {
        return std::nullopt;
    }
    return it->second;
}

bool KVStore::Delete(const std::string& key) {
    {
        std::lock_guard<std::mutex> wal_lock(wal_mutex_);
        if (!AppendRecordLocked(RecordType::kDelete, key, std::string())) {
            return false;
        }
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
        total += shard.data.size();
    }
    return total;
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

    // step 1: write a clean copy of every live key/value pair to a temp file.
    {
        std::ofstream tmp(wal_tmp_path_, std::ios::binary | std::ios::trunc | std::ios::out);
        if (!tmp.is_open()) {
            return false;
        }

        // shard-by-shard shared locks let other shards (and readers here) keep going.
        for (auto& shard : shards_) {
            std::shared_lock lock(shard.mutex);
            for (const auto& [key, value] : shard.data) {
                RecordHeader hdr{};
                hdr.type = RecordType::kSet;
                hdr.key_len = static_cast<std::uint32_t>(key.size());
                hdr.value_len = static_cast<std::uint32_t>(value.size());

                tmp.write(reinterpret_cast<const char*>(&hdr.type), sizeof(hdr.type));
                tmp.write(reinterpret_cast<const char*>(&hdr.key_len), sizeof(hdr.key_len));
                tmp.write(reinterpret_cast<const char*>(&hdr.value_len), sizeof(hdr.value_len));
                if (!key.empty()) {
                    tmp.write(key.data(), static_cast<std::streamsize>(key.size()));
                }
                if (!value.empty()) {
                    tmp.write(value.data(), static_cast<std::streamsize>(value.size()));
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

    // step 2: swap the temp file in while holding wal_mutex_ so no write is lost.
    {
        std::lock_guard<std::mutex> wal_lock(wal_mutex_);

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

    return true;
}

}
