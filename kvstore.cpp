#include "kvstore.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <stdexcept>

namespace kvstore {

    namespace fs = std::filesystem;

    namespace {

        // precomputed crc32 table.
        struct Crc32Table {
            std::uint32_t v[256];
            constexpr Crc32Table() : v{} {
                for (std::uint32_t i = 0; i < 256; ++i) {
                    std::uint32_t c = i;
                    for (int k = 0; k < 8; ++k) {
                        c = (c & 1u) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
                    }
                    v[i] = c;
                }
            }
        };
        constexpr Crc32Table kCrc32Table{};

        std::uint32_t Crc32(const void* data, std::size_t len) {
            const auto* p = static_cast<const std::uint8_t*>(data);
            std::uint32_t crc = 0xFFFFFFFFu;
            for (std::size_t i = 0; i < len; ++i) {
                crc = kCrc32Table.v[(crc ^ p[i]) & 0xFFu] ^ (crc >> 8);
            }
            return ~crc;
        }

        void AppendBytes(std::string& buf, const void* data, std::size_t len) {
            buf.append(static_cast<const char*>(data), len);
        }

        // build a record: header, payload, then a crc over it all.
        std::string SerializeRecord(RecordType type, const std::string& key,
                                    const std::string& value, std::int64_t expires_at) {
            const auto key_len = static_cast<std::uint32_t>(key.size());
            const std::uint32_t value_len =
                (type == RecordType::kDelete) ? 0u
                                              : static_cast<std::uint32_t>(value.size());
            const auto type_raw = static_cast<std::uint8_t>(type);

            std::string buf;
            buf.reserve(sizeof(type_raw) + sizeof(key_len) + sizeof(value_len) +
                        key.size() + value_len + sizeof(expires_at) + sizeof(std::uint32_t));

            AppendBytes(buf, &type_raw, sizeof(type_raw));
            AppendBytes(buf, &key_len, sizeof(key_len));
            AppendBytes(buf, &value_len, sizeof(value_len));
            if (!key.empty()) {
                AppendBytes(buf, key.data(), key.size());
            }
            if (type != RecordType::kDelete && value_len > 0) {
                AppendBytes(buf, value.data(), value_len);
            }
            if (type == RecordType::kSetTtl) {
                AppendBytes(buf, &expires_at, sizeof(expires_at));
            }

            const std::uint32_t crc = Crc32(buf.data(), buf.size());
            AppendBytes(buf, &crc, sizeof(crc));
            return buf;
        }

        // write every byte, retrying partial writes and EINTR.
        bool WriteFully(int fd, const char* data, std::size_t len) {
            std::size_t written = 0;
            while (written < len) {
                const ssize_t n = ::write(fd, data + written, len - written);
                if (n < 0) {
                    if (errno == EINTR) {
                        continue;
                    }
                    return false;
                }
                if (n == 0) {
                    return false;  // no progress; don't spin.
                }
                written += static_cast<std::size_t>(n);
            }
            return true;
        }

        // fsync the parent dir so a create/rename survives a crash.
        void SyncParentDir(const std::string& path) {
            fs::path p(path);
            const fs::path dir = p.has_parent_path() ? p.parent_path() : fs::path(".");
            const int dfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
            if (dfd >= 0) {
                ::fsync(dfd);
                ::close(dfd);
            }
        }

    }  // namespace

    KVStore::KVStore(std::string path)
        : path_(std::move(path)),
        wal_path_(path_ + ".wal"),
        wal_tmp_path_(path_ + ".wal.tmp") {
        // ensure the db directory exists.
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
        if (wal_fd_ >= 0) {
            ::fsync(wal_fd_);
            ::close(wal_fd_);
            wal_fd_ = -1;
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
        const bool existed = fs::exists(wal_path_);
        wal_fd_ = ::open(wal_path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (wal_fd_ < 0) {
            throw std::runtime_error("KVStore: failed to open WAL file: " + wal_path_);
        }
        if (!existed) {
            SyncParentDir(wal_path_);
        }
    }

    void KVStore::Recover() {
        std::ifstream in(wal_path_, std::ios::binary);
        if (!in.is_open()) {
            return;  // nothing to recover.
        }

        // read n bytes; false on a short read (torn tail).
        const auto read_into = [&in](std::string& sink, std::size_t n) -> bool {
            const std::size_t old_size = sink.size();
            sink.resize(old_size + n);
            in.read(sink.data() + old_size, static_cast<std::streamsize>(n));
            return in.gcount() == static_cast<std::streamsize>(n);
        };

        // offset just past the last fully-valid record.
        std::streamoff good_offset = 0;

        while (true) {
            // buffer the whole record so we can check its crc.
            std::string record;

            std::uint8_t type_raw = 0;
            in.read(reinterpret_cast<char*>(&type_raw), sizeof(type_raw));
            if (in.gcount() != static_cast<std::streamsize>(sizeof(type_raw))) break;
            AppendBytes(record, &type_raw, sizeof(type_raw));

            const RecordType type = static_cast<RecordType>(type_raw);
            if (type != RecordType::kSet && type != RecordType::kSetTtl &&
                type != RecordType::kDelete) {
                break;  // unknown type.
            }

            if (!read_into(record, sizeof(std::uint32_t))) break;  // key_len
            if (!read_into(record, sizeof(std::uint32_t))) break;  // value_len

            std::uint32_t key_len = 0;
            std::uint32_t value_len = 0;
            std::memcpy(&key_len, record.data() + 1, sizeof(key_len));
            std::memcpy(&value_len, record.data() + 1 + sizeof(key_len), sizeof(value_len));

            if (key_len > kMaxKeyLen || value_len > kMaxValueLen) break;  // corrupt header.

            const std::size_t key_off = record.size();
            if (key_len > 0 && !read_into(record, key_len)) break;

            std::size_t value_off = record.size();
            if (type == RecordType::kSet || type == RecordType::kSetTtl) {
                if (value_len > 0 && !read_into(record, value_len)) break;
            }

            std::int64_t expires_at = kNoExpiry;
            if (type == RecordType::kSetTtl) {
                const std::size_t exp_off = record.size();
                if (!read_into(record, sizeof(expires_at))) break;
                std::memcpy(&expires_at, record.data() + exp_off, sizeof(expires_at));
            }

            std::uint32_t stored_crc = 0;
            in.read(reinterpret_cast<char*>(&stored_crc), sizeof(stored_crc));
            if (in.gcount() != static_cast<std::streamsize>(sizeof(stored_crc))) break;

            if (Crc32(record.data(), record.size()) != stored_crc) break;  // bad crc.

            std::string key = record.substr(key_off, key_len);

            switch (type) {
                case RecordType::kSet: {
                    std::string value = record.substr(value_off, value_len);
                    Shard& shard = ShardFor(key);
                    std::unique_lock lock(shard.mutex);
                    shard.data[key] = Entry{std::move(value), kNoExpiry};
                    break;
                }
                case RecordType::kSetTtl: {
                    std::string value = record.substr(value_off, value_len);
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
            }

            good_offset = in.tellg();  // record boundary; stream is still good here.
        }

        // drop any torn/corrupt tail so later appends aren't stranded past it.
        in.close();
        std::error_code ec;
        const std::uintmax_t size = fs::file_size(wal_path_, ec);
        if (!ec && good_offset >= 0 &&
            static_cast<std::uintmax_t>(good_offset) < size) {
            if (::truncate(wal_path_.c_str(), good_offset) == 0) {
                const int fd = ::open(wal_path_.c_str(), O_WRONLY);
                if (fd >= 0) {
                    ::fsync(fd);
                    ::close(fd);
                }
            }
        }
    }

    bool KVStore::AppendRecordLocked(RecordType type, const std::string& key,
                                    const std::string& value, std::int64_t expires_at) {
        // reject oversized fields; recovery would treat them as corrupt.
        if (key.size() > kMaxKeyLen) {
            return false;
        }
        if (type != RecordType::kDelete && value.size() > kMaxValueLen) {
            return false;
        }

        const std::string record = SerializeRecord(type, key, value, expires_at);

        // tail offset, for rollback on failure.
        const off_t before = ::lseek(wal_fd_, 0, SEEK_END);
        if (before < 0) {
            return false;
        }

        if (!WriteFully(wal_fd_, record.data(), record.size())) {
            if (::ftruncate(wal_fd_, before) != 0) {}  // roll back partial write.
            return false;
        }

        // fsync before the record is visible in memory.
        if (::fdatasync(wal_fd_) != 0) {
            if (::ftruncate(wal_fd_, before) != 0) {}
            return false;
        }
        return true;
    }

    bool KVStore::SetInternal(const std::string& key, const std::string& value,
                                std::int64_t expires_at) {
        RecordType type = (expires_at == kNoExpiry) ? RecordType::kSet : RecordType::kSetTtl;

        // append + apply under one lock so log order matches memory.
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
        std::int64_t expires_at = NowSeconds() + ttl_seconds;
        if (expires_at == kNoExpiry) {
            expires_at += 1;  // avoid the no-expiry sentinel.
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
        // expired reads as absent; removed at compaction.
        if (IsExpired(it->second.expires_at)) {
            return std::nullopt;
        }
        return it->second.value;
    }

    bool KVStore::Delete(const std::string& key) {
        // append + apply under one lock so log order matches memory.
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

        // sort by key.
        std::sort(result.begin(), result.end(),
                [](const KeyValue& a, const KeyValue& b) { return a.key < b.key; });

        return result;
    }

    bool KVStore::Compact() {
        bool expected = false;
        if (!compaction_in_progress_.compare_exchange_strong(expected, true)) {
            return false;  // already compacting.
        }

        struct CompactionGuard {
            std::atomic<bool>& flag;
            ~CompactionGuard() { flag.store(false); }
        } guard{compaction_in_progress_};

        // lock across the snapshot and swap so no write is lost.
        std::lock_guard<std::mutex> wal_lock(wal_mutex_);

        // write live entries to a temp file.
        {
            const int tmp_fd =
                ::open(wal_tmp_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (tmp_fd < 0) {
                return false;
            }

            bool write_ok = true;
            for (auto& shard : shards_) {
                std::shared_lock lock(shard.mutex);
                for (const auto& [key, entry] : shard.data) {
                    if (IsExpired(entry.expires_at)) {  // drop expired.
                        continue;
                    }

                    const RecordType type = (entry.expires_at == kNoExpiry)
                                                ? RecordType::kSet
                                                : RecordType::kSetTtl;
                    const std::string record =
                        SerializeRecord(type, key, entry.value, entry.expires_at);
                    if (!WriteFully(tmp_fd, record.data(), record.size())) {
                        write_ok = false;
                        break;
                    }
                }
                if (!write_ok) {
                    break;
                }
            }

            // fsync before the rename.
            if (write_ok && ::fdatasync(tmp_fd) != 0) {
                write_ok = false;
            }
            ::close(tmp_fd);

            if (!write_ok) {
                std::error_code ec;
                fs::remove(wal_tmp_path_, ec);
                return false;
            }
        }

        // swap the temp file in.
        {
            if (wal_fd_ >= 0) {
                ::fsync(wal_fd_);
                ::close(wal_fd_);
                wal_fd_ = -1;
            }

            std::error_code ec;
            fs::rename(wal_tmp_path_, wal_path_, ec);
            if (ec) {
                // rename failed: keep the old log.
                std::error_code rm_ec;
                fs::remove(wal_tmp_path_, rm_ec);
                wal_fd_ = ::open(wal_path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
                return false;
            }

            // persist the rename.
            SyncParentDir(wal_path_);

            wal_fd_ = ::open(wal_path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
            if (wal_fd_ < 0) {
                return false;
            }
        }

        // drop expired from memory too.
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
