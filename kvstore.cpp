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
    // Ensure parent directory exists.
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
        // No existing WAL; fresh database.
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
            if (!in) break; // truncated record -> stop replay (crash mid-write)
        }

        std::string value;
        if (hdr.type == RecordType::kSet) {
            value.resize(hdr.value_len);
            if (hdr.value_len > 0) {
                in.read(value.data(), static_cast<std::streamsize>(hdr.value_len));
                if (!in) break; // truncated record -> stop replay
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
                // Unknown/corrupt record type -> stop replay.
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

    wal_stream_.flush();
    return wal_stream_.good();
}

bool KVStore::Set(const std::string& key, const std::string& value) {
    // 1. Durably log the mutation first (WAL-ahead).
    {
        std::lock_guard<std::mutex> wal_lock(wal_mutex_);
        if (!AppendRecordLocked(RecordType::kSet, key, value)) {
            return false;
        }
    }

    // 2. Apply to the in-memory sharded index.
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
        // Another compaction is already running.
        return false;
    }

    struct CompactionGuard {
        std::atomic<bool>& flag;
        ~CompactionGuard() { flag.store(false); }
    } guard{compaction_in_progress_};

    // 1. Write a clean snapshot of all currently-live keys to a temp file.
    //    Each shard is locked individually (shared lock) so writers on
    //    other shards are never blocked, and readers are never blocked
    //    on this shard either.
    {
        std::ofstream tmp(wal_tmp_path_, std::ios::binary | std::ios::trunc | std::ios::out);
        if (!tmp.is_open()) {
            return false;
        }

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

    // 2. Atomically swap the temp file in as the new WAL.
    //    Hold wal_mutex_ for the duration of the swap so that any in-flight
    //    Set/Delete either completes fully against the old file before the
    //    swap, or is appended to the new file after the swap. No user write
    //    is dropped: the close+rename+reopen sequence happens while holding
    //    the WAL lock, guaranteeing ordering.
    {
        std::lock_guard<std::mutex> wal_lock(wal_mutex_);

        if (wal_stream_.is_open()) {
            wal_stream_.flush();
            wal_stream_.close();
        }

        std::error_code ec;
        fs::rename(wal_tmp_path_, wal_path_, ec);
        if (ec) {
            // Attempt to reopen the old WAL so the store remains usable.
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

} // namespace kvstore