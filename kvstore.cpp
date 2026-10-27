#include "kvstore.hpp"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <limits>
#include <stdexcept>

namespace kvstore {

    namespace fs = std::filesystem;

    namespace {

        std::string AbsoluteStorePath(const std::string& path) {
            if (path.empty() || path.find('\0') != std::string::npos) {
                throw std::invalid_argument("KVStore: path must be nonempty and contain no NUL bytes");
            }
            // Resolve once so later chdir calls cannot redirect compaction.
            return fs::absolute(fs::path(path)).string();
        }

        // precomputed crc32 table
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

        // fixed little-endian encoding so the wal is portable across architectures
        void AppendU32Le(std::string& buf, std::uint32_t v) {
            const char b[4] = {
                static_cast<char>(v & 0xFFu),
                static_cast<char>((v >> 8) & 0xFFu),
                static_cast<char>((v >> 16) & 0xFFu),
                static_cast<char>((v >> 24) & 0xFFu),
            };
            buf.append(b, sizeof(b));
        }

        void AppendI64Le(std::string& buf, std::int64_t v) {
            const auto u = static_cast<std::uint64_t>(v);
            char b[8];
            for (int i = 0; i < 8; ++i) {
                b[i] = static_cast<char>((u >> (8 * i)) & 0xFFu);
            }
            buf.append(b, sizeof(b));
        }

        std::uint32_t LoadU32Le(const char* p) {
            const auto* u = reinterpret_cast<const unsigned char*>(p);
            return static_cast<std::uint32_t>(u[0]) |
                   (static_cast<std::uint32_t>(u[1]) << 8) |
                   (static_cast<std::uint32_t>(u[2]) << 16) |
                   (static_cast<std::uint32_t>(u[3]) << 24);
        }

        std::int64_t LoadI64Le(const char* p) {
            const auto* u = reinterpret_cast<const unsigned char*>(p);
            std::uint64_t v = 0;
            for (int i = 0; i < 8; ++i) {
                v |= static_cast<std::uint64_t>(u[i]) << (8 * i);
            }
            return static_cast<std::int64_t>(v);
        }

        // build a record with header, payload, then a crc over it all
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
            AppendU32Le(buf, key_len);
            AppendU32Le(buf, value_len);
            if (!key.empty()) {
                AppendBytes(buf, key.data(), key.size());
            }
            if (type != RecordType::kDelete && value_len > 0) {
                AppendBytes(buf, value.data(), value_len);
            }
            if (type == RecordType::kSetTtl) {
                AppendI64Le(buf, expires_at);
            }

            const std::uint32_t crc = Crc32(buf.data(), buf.size());
            AppendU32Le(buf, crc);
            return buf;
        }

        // write every byte, retrying partial writes and EINTR
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
                    return false;  // no progress, don't spin
                }
                written += static_cast<std::size_t>(n);
            }
            return true;
        }

        bool SyncFile(int fd, bool data_only = false) {
            int result;
            do {
                result = data_only ? ::fdatasync(fd) : ::fsync(fd);
            } while (result != 0 && errno == EINTR);
            return result == 0;
        }

        bool TruncateFile(int fd, off_t length) {
            int result;
            do {
                result = ::ftruncate(fd, length);
            } while (result != 0 && errno == EINTR);
            return result == 0;
        }

        // Read errors must never be mistaken for a torn tail.
        bool ReadFully(int fd, char* data, std::size_t length) {
            std::size_t done = 0;
            while (done < length) {
                const ssize_t n = ::read(fd, data + done, length - done);
                if (n < 0) {
                    if (errno == EINTR) continue;
                    throw std::runtime_error("KVStore: WAL read failed");
                }
                if (n == 0) return false;
                done += static_cast<std::size_t>(n);
            }
            return true;
        }

    }  // namespace

    KVStore::FileDescriptor::~FileDescriptor() { Reset(); }

    void KVStore::FileDescriptor::Reset(int fd) {
        if (value >= 0) ::close(value);
        value = fd;
    }

    int KVStore::FileDescriptor::Release() {
        const int fd = value;
        value = -1;
        return fd;
    }

    // A member so the directory fd uses the same RAII ownership as the WAL.
    bool KVStore::SyncParentDir(const std::string& path) {
        const fs::path p(path);
        const fs::path dir = p.has_parent_path() ? p.parent_path() : fs::path(".");
        FileDescriptor fd(::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
        return fd >= 0 && SyncFile(fd);
    }

    KVStore::KVStore(std::string path)
        : path_(AbsoluteStorePath(path)),
          wal_path_(path_ + ".wal"),
          wal_tmp_path_(path_ + ".wal.tmp"),
          lock_path_(path_ + ".lock") {
        const fs::path parent = fs::absolute(fs::path(path_)).parent_path();
        fs::create_directories(parent);
        // Persist newly created directory entries, including nested ancestors.
        for (fs::path dir = parent; dir != dir.root_path(); dir = dir.parent_path()) {
            if (!SyncParentDir(dir.string())) {
                throw std::runtime_error("KVStore: failed to sync database directory");
            }
        }
        AcquireProcessLock();
        Recover();
        OpenWalForAppend();
    }

    KVStore::~KVStore() = default;

    void KVStore::AcquireProcessLock() {
        lock_fd_.Reset(::open(lock_path_.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644));
        if (lock_fd_ < 0) {
            throw std::runtime_error("KVStore: failed to open lock file: " + lock_path_);
        }
        if (::flock(lock_fd_, LOCK_EX | LOCK_NB) != 0) {
            throw std::runtime_error(
                "KVStore: store at '" + path_ + "' is locked by another process");
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
        if (!OpenWalLocked()) {
            throw std::runtime_error("KVStore: failed to open WAL file: " + wal_path_);
        }
    }

    bool KVStore::OpenWalLocked() {
        if (wal_fd_ >= 0) return true;
        FileDescriptor fd(::open(wal_path_.c_str(),
                                 O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644));
        if (fd < 0) return false;
        // Sync even on reopen: an earlier failed create may have left the name behind.
        if (!SyncFile(fd, true) || !SyncParentDir(wal_path_)) return false;
        wal_fd_.Reset(fd.Release());
        return true;
    }

    void KVStore::Recover() {
        FileDescriptor fd(::open(wal_path_.c_str(), O_RDWR | O_CLOEXEC));
        if (fd < 0) {
            if (errno == ENOENT) return;
            throw std::runtime_error("KVStore: failed to open WAL for recovery");
        }
        struct stat info {};
        if (::fstat(fd, &info) != 0 || !S_ISREG(info.st_mode) || info.st_size < 0) {
            throw std::runtime_error("KVStore: WAL must be a readable regular file");
        }
        const auto size = static_cast<std::uint64_t>(info.st_size);
        std::uint64_t good_offset = 0;
        while (good_offset < size) {
            constexpr std::size_t header_size = 9;
            if (size - good_offset < header_size) break;
            char header[header_size];
            if (!ReadFully(fd, header, header_size)) {
                throw std::runtime_error("KVStore: WAL changed during recovery");
            }
            const auto type = static_cast<RecordType>(static_cast<std::uint8_t>(header[0]));
            const std::uint32_t key_len = LoadU32Le(header + 1);
            const std::uint32_t value_len = LoadU32Le(header + 5);
            if ((type != RecordType::kSet && type != RecordType::kSetTtl &&
                 type != RecordType::kDelete) || key_len > kMaxKeyLen ||
                value_len > kMaxValueLen || (type == RecordType::kDelete && value_len != 0)) {
                throw std::runtime_error("KVStore: corrupt WAL header; file preserved");
            }
            const std::uint64_t payload_size = static_cast<std::uint64_t>(key_len) +
                value_len + (type == RecordType::kSetTtl ? 8u : 0u);
            const std::uint64_t record_size = header_size + payload_size + 4;
            if (record_size > size - good_offset) break; // incomplete final record
            std::string record(header, header_size);
            record.resize(static_cast<std::size_t>(header_size + payload_size));
            if (!ReadFully(fd, record.data() + header_size,
                           static_cast<std::size_t>(payload_size))) {
                throw std::runtime_error("KVStore: WAL changed during recovery");
            }
            char checksum[4];
            if (!ReadFully(fd, checksum, sizeof(checksum))) {
                throw std::runtime_error("KVStore: WAL changed during recovery");
            }
            if (Crc32(record.data(), record.size()) != LoadU32Le(checksum)) {
                if (good_offset + record_size != size) {
                    throw std::runtime_error("KVStore: non-tail WAL corruption; file preserved");
                }
                break; // retain compatibility with recovery of a bad final CRC
            }
            std::string key = record.substr(header_size, key_len);
            auto& data = ShardFor(key).data; // construction is single-threaded
            if (type == RecordType::kDelete) {
                data.erase(key);
            } else {
                const auto expiry = type == RecordType::kSetTtl
                    ? LoadI64Le(record.data() + header_size + key_len + value_len)
                    : kNoExpiry;
                data[key] = Entry{record.substr(header_size + key_len, value_len), expiry};
            }
            good_offset += record_size;
        }
        if (good_offset < size &&
            (!TruncateFile(fd, static_cast<off_t>(good_offset)) || !SyncFile(fd))) {
            throw std::runtime_error("KVStore: failed to durably repair WAL tail");
        }
    }

    bool KVStore::AppendRecordLocked(RecordType type, const std::string& key,
                                    const std::string& value, std::int64_t expires_at) {
        if (failed_.load()) return false;
        // reject oversized fields, recovery would treat them as corrupt
        if (key.size() > kMaxKeyLen) {
            return false;
        }
        if (type != RecordType::kDelete && value.size() > kMaxValueLen) {
            return false;
        }

        const std::string record = SerializeRecord(type, key, value, expires_at);

        // tail offset, for rollback on failure
        const off_t before = ::lseek(wal_fd_, 0, SEEK_END);
        if (before < 0) {
            return false;
        }

        if (!WriteFully(wal_fd_, record.data(), record.size()) || !SyncFile(wal_fd_, true)) {
            // Both rollback and its durability must be established before continuing.
            if (!TruncateFile(wal_fd_, before) || !SyncFile(wal_fd_, true)) {
                failed_.store(true);
            }
            return false;
        }
        return true;
    }

    bool KVStore::SetInternal(const std::string& key, const std::string& value,
                                std::int64_t expires_at) {
        RecordType type = (expires_at == kNoExpiry) ? RecordType::kSet : RecordType::kSetTtl;

        // append + apply under one lock so log order matches memory
        std::lock_guard<std::mutex> wal_lock(wal_mutex_);
        if (!AppendRecordLocked(type, key, value, expires_at)) {
            return false;
        }

        Shard& shard = ShardFor(key);
        std::unique_lock lock(shard.mutex, std::defer_lock);
        try {
            lock.lock();
            shard.data[key] = Entry{value, expires_at};
        } catch (...) {
            // WAL may already be durable. Do not serve a divergent memory view.
            failed_.store(true);
            throw;
        }
        return true;
    }

    bool KVStore::Set(const std::string& key, const std::string& value) {
        return SetInternal(key, value, kNoExpiry);
    }

    bool KVStore::SetWithTtl(const std::string& key, const std::string& value,
                            std::int64_t ttl_seconds) {
        const auto now = NowSeconds();
        if ((ttl_seconds > 0 && now > std::numeric_limits<std::int64_t>::max() - ttl_seconds) ||
            (ttl_seconds < 0 && now < std::numeric_limits<std::int64_t>::min() - ttl_seconds)) {
            return false;
        }
        std::int64_t expires_at = now + ttl_seconds;
        if (expires_at == kNoExpiry) {
            expires_at += 1;  // avoid the no-expiry sentinel
        }
        return SetInternal(key, value, expires_at);
    }

    void KVStore::CheckReadable() const {
        if (failed_.load()) {
            throw std::runtime_error("KVStore: store requires close and reopen after failure");
        }
    }

    std::optional<std::string> KVStore::Get(const std::string& key) const {
        const Shard& shard = ShardFor(key);
        std::shared_lock lock(shard.mutex);
        CheckReadable();
        auto it = shard.data.find(key);
        if (it == shard.data.end()) {
            return std::nullopt;
        }
        // expired reads as absent, removed at compaction
        if (IsExpired(it->second.expires_at)) {
            return std::nullopt;
        }
        return it->second.value;
    }

    bool KVStore::Delete(const std::string& key) {
        // append + apply under one lock so log order matches memory
        std::lock_guard<std::mutex> wal_lock(wal_mutex_);
        if (!AppendRecordLocked(RecordType::kDelete, key, std::string(), kNoExpiry)) {
            return false;
        }

        Shard& shard = ShardFor(key);
        std::unique_lock lock(shard.mutex, std::defer_lock);
        try {
            lock.lock();
            shard.data.erase(key);
        } catch (...) {
            failed_.store(true);
            throw;
        }
        return true;
    }

    std::size_t KVStore::Size() const {
        CheckReadable();
        std::size_t total = 0;
        for (const auto& shard : shards_) {
            std::shared_lock lock(shard.mutex);
            CheckReadable();
            for (const auto& [key, entry] : shard.data) {
                if (!IsExpired(entry.expires_at)) {
                    ++total;
                }
            }
        }
        CheckReadable();
        return total;
    }

    std::vector<KeyValue> KVStore::Items() const {
        return Range(std::string(), std::string());
    }

    std::vector<KeyValue> KVStore::Range(const std::string& start, const std::string& end) const {
        CheckReadable();
        std::vector<KeyValue> result;

        for (const auto& shard : shards_) {
            std::shared_lock lock(shard.mutex);
            CheckReadable();
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

        std::sort(result.begin(), result.end(),
                [](const KeyValue& a, const KeyValue& b) { return a.key < b.key; });

        CheckReadable();
        return result;
    }

    bool KVStore::Compact() {
        bool expected = false;
        if (!compaction_in_progress_.compare_exchange_strong(expected, true)) {
            return false;  // already compacting
        }

        struct CompactionGuard {
            std::atomic<bool>& flag;
            ~CompactionGuard() { flag.store(false); }
        } guard{compaction_in_progress_};

        // lock across the snapshot and swap so no write is lost
        std::lock_guard<std::mutex> wal_lock(wal_mutex_);

        if (failed_.load()) return false;
        FileDescriptor tmp_fd(::open(wal_tmp_path_.c_str(),
            O_WRONLY | O_CREAT | O_TRUNC | O_APPEND | O_CLOEXEC, 0644));
        if (tmp_fd < 0) return false;
        struct TempCleanup {
            const std::string& path;
            ~TempCleanup() { ::unlink(path.c_str()); }
        } cleanup{wal_tmp_path_};

        for (auto& shard : shards_) {
            std::shared_lock lock(shard.mutex);
            CheckReadable();
            for (const auto& [key, entry] : shard.data) {
                if (IsExpired(entry.expires_at)) continue;
                const auto type = entry.expires_at == kNoExpiry
                    ? RecordType::kSet : RecordType::kSetTtl;
                const auto record = SerializeRecord(type, key, entry.value, entry.expires_at);
                if (!WriteFully(tmp_fd, record.data(), record.size())) return false;
            }
        }
        if (!SyncFile(tmp_fd, true)) return false;
        // Keep the old fd intact until replacement is durable. The temp fd already
        // has O_APPEND, avoiding an additional fallible reopen after the rename.
        if (::rename(wal_tmp_path_.c_str(), wal_path_.c_str()) != 0) return false;
        try {
            if (!SyncParentDir(wal_path_)) {
                failed_.store(true);
                return false;
            }
        } catch (...) {
            failed_.store(true);
            throw;
        }
        wal_fd_.Reset(tmp_fd.Release());

        // drop expired from memory too
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
