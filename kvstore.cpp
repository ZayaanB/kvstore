#include "kvstore.hpp"
#include "metrics.hpp"
#include "routing.hpp"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <charconv>
#include <cstdlib>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <string_view>
#include <utility>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <exception>
#include <filesystem>
#include <limits>
#include <stdexcept>
#include <system_error>

namespace kvstore {

    namespace fs = std::filesystem;

    namespace {
        using detail::Routing;
        using detail::ShardIndex;

        class OperationObservation {
        public:
            OperationObservation(Metrics* metrics, Operation operation) noexcept
                : metrics_(metrics), operation_(operation) {
                if (metrics_) {
                    start_ = Metrics::Clock::now();
                    exceptions_ = std::uncaught_exceptions();
                }
            }

            ~OperationObservation() {
                if (metrics_) {
                    const auto outcome = std::uncaught_exceptions() > exceptions_ ? Outcome::Error : outcome_;
                    metrics_->Observe(operation_, outcome, Metrics::Clock::now() - start_);
                }
            }

            void Success() noexcept { outcome_ = Outcome::Ok; }
            void Miss() noexcept { outcome_ = Outcome::Miss; }
            bool Result(bool ok) noexcept {
                if (ok) Success();
                return ok;
            }

        private:
            Metrics* metrics_;
            Operation operation_;
            Outcome outcome_ = Outcome::Error;
            Metrics::Clock::time_point start_;
            int exceptions_ = 0;
        };

        fs::path AbsolutePath(const std::string& path) {
            if (path.empty() || path.find('\0') != std::string::npos) {
                throw std::invalid_argument("KVStore: path must be nonempty and contain no NUL bytes");
            }
            return fs::absolute(fs::path(path));
        }

        std::string AbsoluteStorePath(const std::string& path) {
            // Resolve once so later chdir calls cannot redirect database I/O.
            auto absolute = AbsolutePath(path);
            if (!absolute.has_filename()) absolute = absolute.parent_path();
            if (!absolute.has_filename() || absolute.filename() == "." || absolute.filename() == "..") {
                throw std::invalid_argument("KVStore: invalid database root");
            }
            // A symlink to the database must use the target's sibling lock.
            if (fs::is_symlink(fs::symlink_status(absolute))) absolute = fs::canonical(absolute);
            if (!absolute.has_filename()) throw std::invalid_argument("KVStore: invalid database root");
            return absolute.string();
        }

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

        bool WriteFully(int fd, const char* data, std::size_t len,
                        Metrics* metrics, WriteSource source) {
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
                    return false;
                }
                written += static_cast<std::size_t>(n);
                if (metrics) metrics->AddWritten(source, static_cast<std::uint64_t>(n));
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

        bool SyncWal(int fd, bool data_only, Metrics* metrics, WalSync phase) {
            if (!metrics) return SyncFile(fd, data_only);
            const auto start = Metrics::Clock::now();
            const bool ok = SyncFile(fd, data_only);
            metrics->Observe(phase, ok, Metrics::Clock::now() - start);
            return ok;
        }

        bool TruncateFile(int fd, off_t length) {
            int result;
            do {
                result = ::ftruncate(fd, length);
            } while (result != 0 && errno == EINTR);
            return result == 0;
        }

        class StorageError : public std::runtime_error {
        public:
            using std::runtime_error::runtime_error;
        };

        // Read errors must never be mistaken for a torn tail.
        bool ReadFully(int fd, char* data, std::size_t length) {
            std::size_t done = 0;
            while (done < length) {
                const ssize_t n = ::read(fd, data + done, length - done);
                if (n < 0) {
                    if (errno == EINTR) continue;
                    throw StorageError("KVStore: file read failed");
                }
                if (n == 0) return false;
                done += static_cast<std::size_t>(n);
            }
            return true;
        }

        [[noreturn]] void Invalid(const char* reason) {
            throw StorageError(std::string("KVStore: ") + reason);
        }

        class Fd {
        public:
            explicit Fd(int fd = -1) : fd_(fd) {}
            ~Fd() { if (fd_ >= 0) ::close(fd_); }
            Fd(Fd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
            Fd& operator=(Fd&& other) noexcept {
                std::swap(fd_, other.fd_);
                return *this;
            }
            operator int() const { return fd_; }
        private:
            int fd_;
        };

        Fd AcquireLock(const std::string& path) {
            Fd fd(::open((path + ".lock").c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644));
            if (fd < 0 || ::flock(fd, LOCK_EX | LOCK_NB) != 0) {
                throw std::runtime_error("KVStore: cannot lock database at '" + path + "'");
            }
            return fd;
        }

        bool SyncDirectory(const char* path) {
            const Fd fd(::open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC));
            return fd >= 0 && SyncFile(fd);
        }

        void PrepareParent(const fs::path& path) {
            fs::create_directories(path);
            for (auto dir = path; dir != dir.root_path(); dir = dir.parent_path()) {
                if (!SyncDirectory(dir.parent_path().c_str())) Invalid("could not sync database ancestors");
            }
        }

        bool Exists(const fs::path& path) {
            return fs::exists(fs::symlink_status(path));
        }

        class Stage {
        public:
            explicit Stage(const fs::path& parent, const char* pattern = ".kvstore-stage-XXXXXX")
                : path_((parent / pattern).string()) {
                if (::mkdtemp(path_.data()) == nullptr) {
                    throw std::system_error(errno, std::generic_category(), "could not create staging directory");
                }
            }
            ~Stage() {
                std::error_code error;
                if (!path_.empty()) fs::remove_all(path_, error);
            }
            const std::string& Path() const { return path_; }
            void Release() noexcept { path_.clear(); }
            void Publish(const std::string& destination) {
                const auto parent = fs::path(destination).parent_path();
                if (Exists(destination) || ::rename(path_.c_str(), destination.c_str()) != 0) {
                    Invalid("could not publish database directory");
                }
                path_.clear();
                if (!SyncDirectory(parent.c_str())) {
                    Invalid("database directory was renamed, but publication sync failed; reopen destination to verify");
                }
            }
        private:
            std::string path_;
        };

        std::uint64_t FileSize(int fd) {
            struct stat info {};
            if (fd < 0 || ::fstat(fd, &info) != 0 || !S_ISREG(info.st_mode) || info.st_size < 0) {
                Invalid("expected a readable regular file");
            }
            return static_cast<std::uint64_t>(info.st_size);
        }

        void CopyDurable(const std::string& source, const std::string& destination) {
            const Fd input(::open(source.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK));
            const Fd output(::open(destination.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644));
            auto remaining = FileSize(input);
            std::array<char, 64 * 1024> buffer;
            while (remaining != 0) {
                const auto length = static_cast<std::size_t>(std::min<std::uint64_t>(buffer.size(), remaining));
                if (!ReadFully(input, buffer.data(), length) ||
                    !WriteFully(output, buffer.data(), length, nullptr, WriteSource::Compaction)) {
                    Invalid("could not copy checkpoint file");
                }
                remaining -= length;
            }
            if (!SyncFile(output)) Invalid("could not sync checkpoint file");
        }

        void ReadAt(int fd, std::uint64_t offset, char* data, std::size_t length) {
            while (length != 0) {
                const auto n = ::pread(fd, data, length, static_cast<off_t>(offset));
                if (n < 0 && errno == EINTR) continue;
                if (n <= 0) Invalid("short or failed positional read");
                const auto bytes = static_cast<std::size_t>(n);
                offset += bytes;
                data += bytes;
                length -= bytes;
            }
        }

        template<class Integer>
        void StoreLe(char* dest, Integer value) {
            for (std::size_t i = 0; i < sizeof(value); ++i) {
                dest[i] = static_cast<char>((value >> (i * 8)) & 0xffu);
            }
        }

        template<class Integer>
        Integer LoadLe(const char* source) {
            Integer value = 0;
            for (std::size_t i = 0; i < sizeof(value); ++i) {
                value |= static_cast<Integer>(static_cast<unsigned char>(source[i])) << (i * 8);
            }
            return value;
        }

        class Checksum {
        public:
            void Add(std::string_view bytes) {
                for (const auto byte : bytes) {
                    crc_ = kCrc32Table.v[(crc_ ^ static_cast<unsigned char>(byte)) & 0xffu] ^ (crc_ >> 8);
                }
            }
            std::uint32_t Value() const { return ~crc_; }
        private:
            std::uint32_t crc_ = 0xffffffffu;
        };

        std::uint32_t Crc(std::string_view bytes) {
            Checksum crc;
            crc.Add(bytes);
            return crc.Value();
        }

        std::int64_t NowSeconds() {
            using namespace std::chrono;
            return duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
        }

        enum class RecordType : unsigned char { Set = 1, Delete = 2, SetTtl = 3 };
        constexpr std::size_t kHeaderBytes = 25;
        constexpr std::size_t kChecksumBytes = 4;
        // Allow for the ordered-map node, two string objects and allocator overhead.
        constexpr std::size_t kRecordOverhead = 192;
        constexpr std::uint64_t kAutomaticCompactionTables = 8;

        struct Header {
            std::uint64_t sequence;
            RecordType type;
            std::uint32_t key_length;
            std::uint32_t value_length;
            std::int64_t expiry;

            std::uint64_t Bytes() const {
                return kHeaderBytes + kChecksumBytes + key_length + value_length;
            }
            std::size_t Charge() const { return kRecordOverhead + key_length + value_length; }
            bool Live(std::int64_t now) const {
                return type != RecordType::Delete && (expiry == kNoExpiry || expiry > now);
            }
        };

        bool ValidLengths(RecordType type, std::uint32_t key, std::uint32_t value) {
            return (type == RecordType::Set || type == RecordType::SetTtl || type == RecordType::Delete) &&
                   key <= kMaxKeyLen && value <= kMaxValueLen && (type != RecordType::Delete || value == 0);
        }

        Header DecodeHeader(const char* bytes) {
            const Header h{LoadLe<std::uint64_t>(bytes), static_cast<RecordType>(bytes[8]),
                           LoadLe<std::uint32_t>(bytes + 9), LoadLe<std::uint32_t>(bytes + 13),
                           std::bit_cast<std::int64_t>(LoadLe<std::uint64_t>(bytes + 17))};
            if (h.sequence == 0 || !ValidLengths(h.type, h.key_length, h.value_length) ||
                (h.type != RecordType::SetTtl && h.expiry != kNoExpiry)) {
                Invalid("invalid record header");
            }
            return h;
        }

        void EncodeRecord(const Header& h, std::string_view key, std::string_view value,
                          char (&header)[kHeaderBytes], char (&checksum)[kChecksumBytes]) {
            StoreLe(header, h.sequence);
            header[8] = static_cast<char>(h.type);
            StoreLe(header + 9, h.key_length);
            StoreLe(header + 13, h.value_length);
            StoreLe(header + 17, static_cast<std::uint64_t>(h.expiry));
            Checksum crc;
            crc.Add({header, sizeof(header)});
            crc.Add(key);
            crc.Add(value);
            StoreLe(checksum, crc.Value());
        }

        bool WriteRecord(int fd, const Header& h, std::string_view key, std::string_view value,
                         Metrics* metrics, WriteSource source) {
            char header[kHeaderBytes], checksum[kChecksumBytes];
            EncodeRecord(h, key, value, header, checksum);
            return WriteFully(fd, header, sizeof(header), metrics, source) &&
                   WriteFully(fd, key.data(), key.size(), metrics, source) &&
                   WriteFully(fd, value.data(), value.size(), metrics, source) &&
                   WriteFully(fd, checksum, sizeof(checksum), metrics, source);
        }

        struct Entry {
            Header header;
            std::string value;
        };
        using Memtable = std::map<std::string, Entry, std::less<>>;

        constexpr std::string_view kMetadataMagic = "KVDB0001";
        constexpr std::uint64_t kFormatVersion = 1;
        constexpr std::uint64_t kShardPointerVersion = 2;
        constexpr std::string_view kManifestMagic = "KVMAN001";
        constexpr std::string_view kTableMagic = "KVSST001";
        constexpr std::string_view kIdentityMagic = "KVID0001";
        constexpr std::size_t kBlockRecords = 16;

        // Fixed-size metadata is little endian and checksummed, including its magic.
        template<std::size_t N>
        using Fields = std::array<std::uint64_t, N>;

        template<std::size_t N>
        constexpr std::size_t kFieldsBytes = 8 + N * 8 + kChecksumBytes;

        template<std::size_t N>
        bool WriteFields(int fd, std::string_view magic, const Fields<N>& fields, Metrics* metrics = nullptr) {
            std::array<char, kFieldsBytes<N>> bytes{};
            std::copy(magic.begin(), magic.end(), bytes.begin());
            for (std::size_t i = 0; i < N; ++i) StoreLe(bytes.data() + 8 + i * 8, fields[i]);
            StoreLe(bytes.data() + bytes.size() - kChecksumBytes, Crc({bytes.data(), bytes.size() - kChecksumBytes}));
            return WriteFully(fd, bytes.data(), bytes.size(), metrics, WriteSource::Compaction);
        }

        template<std::size_t N>
        Fields<N> ReadFields(int fd, std::uint64_t offset, std::string_view magic) {
            std::array<char, kFieldsBytes<N>> bytes{};
            ReadAt(fd, offset, bytes.data(), bytes.size());
            if (std::string_view(bytes.data(), 8) != magic ||
                Crc({bytes.data(), bytes.size() - kChecksumBytes}) !=
                    LoadLe<std::uint32_t>(bytes.data() + bytes.size() - kChecksumBytes)) {
                Invalid("invalid on-disk metadata checksum or format");
            }
            Fields<N> fields{};
            for (std::size_t i = 0; i < N; ++i) fields[i] = LoadLe<std::uint64_t>(bytes.data() + 8 + i * 8);
            return fields;
        }

        class Table {
        public:
            struct Record {
                std::uint64_t offset;
                Header header;
                std::string key;
                Checksum prefix;
            };

            explicit Table(const std::string& path) : fd_(::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW)) {
                const auto size = FileSize(fd_);
                if (size < kFieldsBytes<5>) Invalid("short sorted table");
                const auto fields = ReadFields<5>(fd_, size - kFieldsBytes<5>, kTableMagic);
                index_ = fields[0];
                count_ = fields[1];
                last_ = fields[2];
                high_ = fields[3];
                low_ = fields[4];
                blocks_ = count_ == 0 ? 0 : (count_ - 1) / kBlockRecords + 1;
                const auto payload = size - kFieldsBytes<5>;
                if ((count_ == 0 ? index_ != 0 || last_ != 0 : last_ >= index_) ||
                    count_ > index_ / (kHeaderBytes + kChecksumBytes) ||
                    index_ > payload || (payload - index_) % 8 != 0 || (payload - index_) / 8 != blocks_ ||
                    low_ == 0 || low_ > high_ || (count_ != 0 && count_ - 1 > high_ - low_)) {
                    Invalid("invalid sorted table footer");
                }
            }

            std::uint64_t End() const { return index_; }
            std::uint64_t High() const { return high_; }
            std::uint64_t Low() const { return low_; }

            Record Read(std::uint64_t offset) const {
                if (offset >= index_ || index_ - offset < kHeaderBytes) Invalid("invalid sorted table offset");
                char bytes[kHeaderBytes];
                ReadAt(fd_, offset, bytes, sizeof(bytes));
                const auto header = DecodeHeader(bytes);
                if (header.Bytes() > index_ - offset) Invalid("truncated sorted table record");
                Record record{offset, header, std::string(header.key_length, '\0'), {}};
                ReadAt(fd_, offset + kHeaderBytes, record.key.data(), record.key.size());
                record.prefix.Add({bytes, sizeof(bytes)});
                record.prefix.Add(record.key);
                return record;
            }

            std::optional<Record> Find(std::string_view key) const {
                if (count_ == 0 || key < Read(0).key || key > Read(last_).key) return std::nullopt;
                const auto block = SearchBlock(key);
                const auto end = block + 1 == blocks_ ? index_ : BlockOffset(block + 1);
                for (auto offset = BlockOffset(block); offset < end;) {
                    auto record = Read(offset);
                    if (record.key == key) return record;
                    if (record.key > key) break;
                    offset += record.header.Bytes();
                }
                return std::nullopt;
            }

            std::uint64_t StartOffset(std::string_view key) const {
                if (count_ == 0 || key.empty()) return 0;
                if (key > Read(last_).key) return index_;
                return BlockOffset(SearchBlock(key));
            }

            std::string Value(const Record& record) const {
                std::string value(record.header.value_length, '\0');
                const auto offset = record.offset + kHeaderBytes + record.key.size();
                ReadAt(fd_, offset, value.data(), value.size());
                auto crc = record.prefix;
                crc.Add(value);
                VerifyChecksum(offset + value.size(), crc);
                return value;
            }

            void ValidateRecord(const Record& record) const {
                auto checksum = record.prefix;
                auto offset = record.offset + kHeaderBytes + record.key.size();
                auto remaining = static_cast<std::size_t>(record.header.value_length);
                std::array<char, 64 * 1024> buffer;
                while (remaining != 0) {
                    const auto bytes = std::min(buffer.size(), remaining);
                    ReadAt(fd_, offset, buffer.data(), bytes);
                    checksum.Add({buffer.data(), bytes});
                    offset += bytes;
                    remaining -= bytes;
                }
                VerifyChecksum(offset, checksum);
            }

            void Validate(std::size_t shard, std::uint64_t previous, Routing routing, bool merged = false) const {
                if (low_ - 1 != previous) Invalid("nonconsecutive sorted table sequence range");
                std::uint64_t offset = 0;
                std::uint64_t largest = 0;
                std::string previous_key;
                for (std::uint64_t i = 0; i < count_; ++i) {
                    if ((i % kBlockRecords == 0 && BlockOffset(i / kBlockRecords) != offset) ||
                        (i == count_ - 1 && last_ != offset)) Invalid("invalid sorted table index");
                    auto record = Read(offset);
                    if (ShardIndex(record.key, routing, kNumShards) != shard ||
                        (i != 0 && record.key <= previous_key) ||
                        record.header.sequence < low_ || record.header.sequence > high_) {
                        Invalid("invalid sorted table record order or shard");
                    }
                    ValidateRecord(record);
                    largest = std::max(largest, record.header.sequence);
                    previous_key = std::move(record.key);
                    offset += record.header.Bytes();
                }
                if (offset != index_ || (!merged && largest != high_)) Invalid("invalid sorted table extent");
            }

        private:
            std::uint64_t SearchBlock(std::string_view key) const {
                std::uint64_t low = 0;
                std::uint64_t high = blocks_;
                while (high - low > 1) {
                    const auto mid = low + (high - low) / 2;
                    if (key < Read(BlockOffset(mid)).key) high = mid;
                    else low = mid;
                }
                return low;
            }

            std::uint64_t BlockOffset(std::uint64_t block) const {
                char bytes[8];
                ReadAt(fd_, index_ + block * sizeof(bytes), bytes, sizeof(bytes));
                return LoadLe<std::uint64_t>(bytes);
            }
            void VerifyChecksum(std::uint64_t offset, const Checksum& crc) const {
                char bytes[kChecksumBytes];
                ReadAt(fd_, offset, bytes, sizeof(bytes));
                if (LoadLe<std::uint32_t>(bytes) != crc.Value()) Invalid("sorted table checksum mismatch");
            }

            Fd fd_;
            std::uint64_t index_, count_, last_, high_, low_, blocks_;
        };

        class TableCursor {
        public:
            explicit TableCursor(const std::string& path, std::string_view start = {})
                : table(path), offset_(table.StartOffset(start)) {}

            std::optional<Table::Record> Next() {
                if (offset_ == table.End()) return std::nullopt;
                auto record = table.Read(offset_);
                offset_ += record.header.Bytes();
                return record;
            }

            Table table;

        private:
            std::uint64_t offset_;
        };

        bool MergeTables(const std::string& older_path, const std::string& newer_path,
                         const std::string& output, std::optional<std::int64_t> prune_at,
                         Metrics* metrics) {
            TableCursor older(older_path), newer(newer_path);
            if (older.table.High() != newer.table.Low() - 1) Invalid("cannot merge nonconsecutive tables");
            const Fd fd(::open(output.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW, 0644));
            if (fd < 0) return false;
            auto a = older.Next();
            auto b = newer.Next();
            std::uint64_t offset = 0, last = 0, count = 0;
            while (a || b) {
                const bool use_older = a && (!b || a->key < b->key);
                const auto& row = use_older ? *a : *b;
                const auto& table = use_older ? older.table : newer.table;
                if (prune_at && !row.header.Live(*prune_at)) table.ValidateRecord(row);
                else {
                    if (!WriteRecord(fd, row.header, row.key, table.Value(row), metrics, WriteSource::Compaction)) {
                        return false;
                    }
                    last = offset;
                    offset += row.header.Bytes();
                    ++count;
                }
                if (use_older) a = older.Next();
                else {
                    if (a && a->key == b->key) {
                        older.table.ValidateRecord(*a);
                        a = older.Next();
                    }
                    b = newer.Next();
                }
            }
            const auto index = offset;
            offset = 0;
            for (std::uint64_t row = 0; row < count; ++row) {
                if (row % kBlockRecords == 0) {
                    char entry[8];
                    StoreLe(entry, offset);
                    if (!WriteFully(fd, entry, sizeof(entry), metrics, WriteSource::Compaction)) return false;
                }
                char header[kHeaderBytes];
                ReadAt(fd, offset, header, sizeof(header));
                offset += DecodeHeader(header).Bytes();
            }
            return WriteFields(fd, kTableMagic,
                               Fields<5>{index, count, last, newer.table.High(), older.table.Low()}, metrics) &&
                   SyncFile(fd, true);
        }

        // Sequential reader shared by the legacy migration and the active shard WAL.
        template<class Apply>
        std::uint64_t Replay(int fd, bool legacy, Apply apply, Metrics* metrics = nullptr,
                             bool repair = true) {
            const auto size = FileSize(fd);
            std::uint64_t offset = 0;
            const std::size_t header_bytes = legacy ? 9 : kHeaderBytes;
            const auto read = [&](char* bytes, std::size_t length) {
                if (!ReadFully(fd, bytes, length)) Invalid("WAL changed during replay");
            };
            while (size - offset >= header_bytes) {
                char bytes[kHeaderBytes];
                read(bytes, header_bytes);
                Header header{};
                std::size_t expiry_bytes = 0;
                if (legacy) {
                    header.type = static_cast<RecordType>(bytes[0]);
                    header.key_length = LoadLe<std::uint32_t>(bytes + 1);
                    header.value_length = LoadLe<std::uint32_t>(bytes + 5);
                    if (!ValidLengths(header.type, header.key_length, header.value_length)) {
                        Invalid("invalid legacy WAL header");
                    }
                    if (header.type == RecordType::SetTtl) expiry_bytes = 8;
                } else {
                    header = DecodeHeader(bytes);
                }
                const auto record_bytes = header_bytes + header.key_length + header.value_length +
                                          expiry_bytes + kChecksumBytes;
                if (record_bytes > size - offset) break;
                std::string key(header.key_length, '\0');
                std::string value(header.value_length, '\0');
                read(key.data(), key.size());
                read(value.data(), value.size());
                Checksum crc;
                crc.Add({bytes, header_bytes});
                crc.Add(key);
                crc.Add(value);
                if (expiry_bytes != 0) {
                    read(bytes, expiry_bytes);
                    crc.Add({bytes, expiry_bytes});
                    header.expiry = std::bit_cast<std::int64_t>(LoadLe<std::uint64_t>(bytes));
                }
                read(bytes, kChecksumBytes);
                if (crc.Value() != LoadLe<std::uint32_t>(bytes)) {
                    if (!legacy && repair && record_bytes == size - offset) break;
                    Invalid("WAL checksum mismatch");
                }
                apply(header, std::move(key), std::move(value));
                offset += record_bytes;
            }
            // Incomplete legacy tails are ignored without touching the source.
            if (!legacy && offset != size && (!repair || !TruncateFile(fd, static_cast<off_t>(offset)) ||
                                              !SyncWal(fd, false, metrics, WalSync::Recovery))) {
                Invalid("could not durably repair WAL tail");
            }
            return offset;
        }
    }

    struct KVStore::Engine {
        struct Shard {
            std::string directory;
            std::size_t index = 0;
            mutable std::shared_mutex mutex;
            mutable std::atomic<bool> unavailable{false};
            Memtable pending;
            std::size_t charged = 0;
            std::uint64_t base = 0;
            std::uint64_t tables = 0;
            std::uint64_t last_auto_compaction = 0;
            std::uint64_t flushed = 0;
            std::uint64_t sequence = 0;
            Fd wal;
            std::atomic<std::uint64_t> committed_wal_bytes{0};

            std::string WalPath(std::uint64_t generation) const {
                return directory + "/" + std::to_string(generation) + ".wal";
            }
            std::string TablePath(std::uint64_t generation) const {
                if (generation == base) return BasePath(generation);
                return directory + "/" + std::to_string(generation) + ".sst";
            }
            std::string BasePath(std::uint64_t generation) const {
                return directory + "/base-" + std::to_string(generation) + ".sst";
            }
            std::uint64_t FirstTable() const { return std::max(std::uint64_t{1}, base); }
        };

        explicit Engine(std::string directory, std::shared_ptr<Metrics> recorder = {},
                        OpenMode mode = OpenMode::Strict)
            : path(std::move(directory)), metrics(std::move(recorder)), open_mode(mode) {}

        static void ValidateBudget(std::uint64_t budget) {
            if (budget < kNumShards * kRecordOverhead || budget > std::numeric_limits<std::size_t>::max()) {
                throw std::invalid_argument("KVStore: invalid memtable budget");
            }
        }

        static std::string ShardDirectory(const std::string& directory, std::size_t i) {
            return directory + "/shard-" + std::to_string(i);
        }

        static void Initialize(const std::string& directory, std::size_t budget, Metrics* metrics) {
            ValidateBudget(budget);
            for (std::size_t i = 0; i < kNumShards; ++i) {
                Shard shard;
                shard.directory = ShardDirectory(directory, i);
                fs::create_directory(shard.directory);
                const Fd wal(::open(shard.WalPath(1).c_str(),
                                    O_WRONLY | O_CREAT | O_EXCL | O_APPEND | O_CLOEXEC, 0644));
                const Fd manifest(::open((shard.directory + "/MANIFEST").c_str(),
                                         O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644));
                if (wal < 0 || manifest < 0 || !SyncWal(wal, true, metrics, WalSync::Open) ||
                    !WriteFields(manifest, kManifestMagic, Fields<2>{0, 0}) ||
                    !SyncFile(manifest) || !SyncDirectory(shard.directory.c_str())) {
                    Invalid("could not initialize shard");
                }
            }
            const Fd meta(::open((directory + "/METADATA").c_str(),
                                 O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644));
            if (meta < 0 || !WriteFields(meta, kMetadataMagic, Fields<4>{
                    kFormatVersion, kNumShards, static_cast<std::uint64_t>(Routing::Rendezvous), budget}) ||
                !SyncFile(meta) || !SyncDirectory(directory.c_str())) {
                Invalid("could not initialize database metadata");
            }
        }

        Fields<4> ReadMetadata(std::optional<std::size_t> requested_budget = std::nullopt) {
            const Fd meta(::open((path + "/METADATA").c_str(), O_RDONLY | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW));
            if (FileSize(meta) != kFieldsBytes<4>) Invalid("invalid database metadata length");
            const auto fields = ReadFields<4>(meta, 0, kMetadataMagic);
            routing = static_cast<Routing>(fields[2]);
            if ((fields[0] != kFormatVersion && fields[0] != kShardPointerVersion) || fields[1] != kNumShards ||
                (routing != Routing::Modulo && routing != Routing::Rendezvous)) {
                Invalid("unsupported database format or routing");
            }
            ValidateBudget(fields[3]);
            if (requested_budget && *requested_budget != fields[3]) Invalid("memtable budget differs from database metadata");
            shard_budget = static_cast<std::size_t>(fields[3]) / kNumShards;
            return fields;
        }

        void Open(std::optional<std::size_t> requested_budget, bool repair = true) {
            ReadMetadata(requested_budget);
            if (!SyncDirectory(path.c_str())) Invalid("could not sync database directory");
            for (std::size_t i = 0; i < shards.size(); ++i) {
                auto& shard = shards[i];
                shard.index = i;
                try {
                    shard.directory = SelectedDirectory(path, i);
                    Recover(shard, i, repair);
                } catch (const StorageError&) {
                    if (open_mode != OpenMode::Degraded) throw;
                    MarkFailed(shard);
                } catch (const fs::filesystem_error&) {
                    if (open_mode != OpenMode::Degraded) throw;
                    MarkFailed(shard);
                }
            }
        }

        void OpenOrCreate(std::optional<std::size_t> budget) {
            const auto parent = fs::path(path).parent_path();
            if (open_mode != OpenMode::Strict && !Exists(path)) Invalid("database directory does not exist");
            PrepareParent(parent);
            process_lock = AcquireLock(path);
            if (Exists(path + ".wal")) Invalid("legacy WAL requires offline migration to a new path");
            if (!Exists(path)) {
                if (open_mode != OpenMode::Strict) Invalid("database directory does not exist");
                Stage stage(parent);
                Initialize(stage.Path(), budget.value_or(kDefaultMemtableBudget), metrics.get());
                stage.Publish(path);
            }
            Open(budget, open_mode != OpenMode::Existing);
        }

        static std::string RestorePrefix(std::size_t index) {
            return ".shard-" + std::to_string(index) + "-restore-";
        }

        static std::string ShardPointer(const std::string& root, std::size_t index) {
            return ShardDirectory(root, index) + ".CURRENT";
        }

        static std::string SelectedDirectory(const std::string& root, std::size_t index) {
            const auto pointer = ShardPointer(root, index);
            if (!Exists(pointer)) return ShardDirectory(root, index);
            const auto name = fs::read_symlink(pointer).string();
            const auto prefix = RestorePrefix(index);
            if (!name.starts_with(prefix) || name.size() != prefix.size() + 6 ||
                name.find_first_not_of("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", prefix.size()) !=
                    std::string::npos || !fs::is_directory(fs::symlink_status(root + "/" + name))) {
                Invalid("invalid restored shard pointer");
            }
            return root + "/" + name;
        }

        static Fields<2> ReadIdentity(const std::string& root) {
            const Fd fd(::open((root + "/IDENTITY").c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK));
            if (FileSize(fd) != kFieldsBytes<2>) Invalid("invalid database identity");
            return ReadFields<2>(fd, 0, kIdentityMagic);
        }

        static void EnsureIdentity(const std::string& root) {
            const auto identity = root + "/IDENTITY";
            if (Exists(identity)) {
                (void)ReadIdentity(root);
                if (!SyncDirectory(root.c_str())) Invalid("could not sync database identity");
                return;
            }
            const auto temporary = identity + ".tmp";
            const Fd random(::open("/dev/urandom", O_RDONLY | O_CLOEXEC));
            const Fd output(::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC |
                                                      O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK, 0644));
            Fields<2> id{};
            if (!ReadFully(random, reinterpret_cast<char*>(id.data()), sizeof(id)) ||
                !WriteFields(output, kIdentityMagic, id) || !SyncFile(output) ||
                ::rename(temporary.c_str(), identity.c_str()) != 0 || !SyncDirectory(root.c_str())) {
                Invalid("could not save database identity");
            }
        }

        static void RemoveUncommittedFiles(const Shard& shard) {
            bool removed = false;
            for (const auto& file : fs::directory_iterator(shard.directory)) {
                const auto name = file.path().filename();
                const auto extension = name.extension();
                auto stem = name.stem().string();
                const bool base = stem.starts_with("base-");
                if (base) stem.erase(0, 5);
                std::uint64_t generation = 0;
                const auto [end, error] = std::from_chars(stem.data(), stem.data() + stem.size(), generation);
                const bool numbered = error == std::errc{} && end == stem.data() + stem.size();
                if (name == "MANIFEST.tmp" ||
                    (numbered && ((!base && extension == ".wal" && generation != shard.tables + 1) ||
                                  (extension == ".sst" && (base ? generation != shard.base :
                                                           generation <= shard.base || generation > shard.tables))))) {
                    if (::unlink(file.path().c_str()) == 0) removed = true;
                }
                if (name.string().starts_with(".kvstore-merge-")) {
                    std::error_code ignored;
                    fs::remove_all(file.path(), ignored);
                }
            }
            if (removed) (void)SyncDirectory(shard.directory.c_str());
        }

        static void ReadShardManifest(Shard& shard) {
            const Fd manifest(::open((shard.directory + "/MANIFEST").c_str(), O_RDONLY | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW));
            const auto manifest_size = FileSize(manifest);
            if (manifest_size == kFieldsBytes<2>) {
                const auto fields = ReadFields<2>(manifest, 0, kManifestMagic);
                shard.tables = fields[0];
                shard.flushed = fields[1];
            } else if (manifest_size == kFieldsBytes<3>) {
                const auto fields = ReadFields<3>(manifest, 0, kManifestMagic);
                shard.tables = fields[0];
                shard.flushed = fields[1];
                shard.base = fields[2];
            } else Invalid("invalid shard manifest length");
            if (shard.base > shard.tables || shard.tables > shard.flushed ||
                shard.tables == std::numeric_limits<std::uint64_t>::max()) {
                Invalid("invalid shard manifest sequence");
            }
        }

        void Recover(Shard& shard, std::size_t index, bool repair = true) {
            ReadShardManifest(shard);
            std::uint64_t previous = 0;
            for (auto generation = shard.FirstTable(); generation <= shard.tables; ++generation) {
                const Table table(shard.TablePath(generation));
                table.Validate(index, previous, routing, generation == shard.base);
                previous = table.High();
            }
            if (previous != shard.flushed) Invalid("sorted table sequence does not match manifest");

            shard.wal = Fd(::open(shard.WalPath(shard.tables + 1).c_str(), O_RDWR | O_APPEND | O_CLOEXEC | O_NOFOLLOW));
            shard.sequence = shard.flushed;
            const auto recovered_bytes = Replay(shard.wal, false, [&](const Header& header, std::string key, std::string value) {
                if (header.sequence <= shard.sequence || header.sequence - shard.sequence != 1 ||
                    Index(key) != index || shard.charged >= shard_budget ||
                    (shard.charged != 0 && header.Charge() > shard_budget - shard.charged)) {
                    Invalid("unexpected shard WAL sequence, routing or budget");
                }
                shard.sequence = header.sequence;
                shard.charged += header.Charge();
                shard.pending.insert_or_assign(std::move(key), Entry{header, std::move(value)});
            }, metrics.get(), repair);
            if (metrics) shard.committed_wal_bytes.store(recovered_bytes, std::memory_order_relaxed);

            // The manifest rename may have survived a process crash before its fsync.
            // Make its observed state durable before removing any obsolete names.
            if (!SyncDirectory(shard.directory.c_str())) Invalid("could not sync recovered shard");
            RemoveUncommittedFiles(shard);
            if (shard.charged >= shard_budget && !Flush(shard)) Invalid("could not flush recovered WAL");
        }

        static void CopyShard(const Shard& shard, const std::string& destination) {
            CopyDurable(shard.directory + "/MANIFEST", destination + "/MANIFEST");
            for (auto generation = shard.FirstTable(); generation <= shard.tables; ++generation) {
                const auto source = shard.TablePath(generation);
                CopyDurable(source, destination + "/" + fs::path(source).filename().string());
            }
            const auto wal = shard.WalPath(shard.tables + 1);
            CopyDurable(wal, destination + "/" + fs::path(wal).filename().string());
            if (!SyncDirectory(destination.c_str())) Invalid("could not sync checkpoint shard");
        }

        bool WriteTable(int fd, const Shard& shard) {
            std::uint64_t offset = 0;
            std::uint64_t last = 0;
            for (const auto& [key, entry] : shard.pending) {
                if (!WriteRecord(fd, entry.header, key, entry.value, metrics.get(), WriteSource::Compaction)) return false;
                last = offset;
                offset += entry.header.Bytes();
            }
            const auto index = offset;
            offset = 0;
            std::size_t row = 0;
            for (const auto& [key, entry] : shard.pending) {
                if (row++ % kBlockRecords == 0) {
                    char bytes[8];
                    StoreLe(bytes, offset);
                    if (!WriteFully(fd, bytes, sizeof(bytes), metrics.get(), WriteSource::Compaction)) return false;
                }
                offset += entry.header.Bytes();
            }
            return WriteFields(fd, kTableMagic, Fields<5>{index, shard.pending.size(), last,
                                                        shard.sequence, shard.flushed + 1}, metrics.get());
        }

        static bool WriteManifest(int fd, std::uint64_t generation, std::uint64_t sequence, std::uint64_t base) {
            if (base == 0) return WriteFields(fd, kManifestMagic, Fields<2>{generation, sequence});
            return WriteFields(fd, kManifestMagic, Fields<3>{generation, sequence, base});
        }

        bool Flush(Shard& shard) {
            if (shard.pending.empty()) return true;
            OperationObservation observation(metrics.get(), Operation::Flush);
            const auto generation = shard.tables + 1;
            const auto retired = shard.WalPath(generation);
            const auto manifest_path = shard.directory + "/MANIFEST";
            const auto manifest_tmp = manifest_path + ".tmp";

            // Future generation names are invisible until a durable manifest selects them.
            const Fd table(::open(shard.TablePath(generation).c_str(),
                                  O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW, 0644));
            if (table < 0 || !WriteTable(table, shard) || !SyncFile(table, true)) {
                return false;
            }
            Fd next_wal(::open(shard.WalPath(generation + 1).c_str(),
                               O_RDWR | O_CREAT | O_TRUNC | O_APPEND | O_CLOEXEC | O_NOFOLLOW, 0644));
            const Fd manifest(::open(manifest_tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW, 0644));
            if (next_wal < 0 || manifest < 0 || !SyncWal(next_wal, true, metrics.get(), WalSync::Open) ||
                !WriteManifest(manifest, generation, shard.sequence, shard.base) ||
                !SyncFile(manifest) || !SyncDirectory(shard.directory.c_str()) ||
                ::rename(manifest_tmp.c_str(), manifest_path.c_str()) != 0) {
                return false;
            }
            if (!SyncDirectory(shard.directory.c_str())) {
                MarkFailed(shard);
                return false;
            }

            shard.wal = std::move(next_wal);
            if (metrics) shard.committed_wal_bytes.store(0, std::memory_order_relaxed);
            shard.tables = generation;
            shard.flushed = shard.sequence;
            shard.pending.clear();
            shard.charged = 0;

            // Retirement is housekeeping after commit. A failed unlink or sync
            // can only leave an obsolete WAL, which recovery ignores and retries.
            if (::unlink(retired.c_str()) == 0) (void)SyncDirectory(shard.directory.c_str());
            observation.Success();
            return true;
        }

        bool CommitAppend(Shard& shard, off_t before, std::uint64_t bytes, bool written) {
            if (!written || !SyncWal(shard.wal, true, metrics.get(), WalSync::Append)) {
                if (!TruncateFile(shard.wal, before) || !SyncWal(shard.wal, true, metrics.get(), WalSync::Rollback)) {
                    MarkFailed(shard);
                }
                return false;
            }
            if (metrics) {
                shard.committed_wal_bytes.store(static_cast<std::uint64_t>(before) + bytes,
                                                std::memory_order_relaxed);
            }
            return true;
        }

        template<class Key, class Value>
        bool Mutate(RecordType type, Key&& key, Value&& value, std::int64_t expiry) {
            if (key.size() > kMaxKeyLen || value.size() > kMaxValueLen) return false;
            std::lock_guard writer(write_mutex);
            auto& shard = shards[Index(key)];
            std::unique_lock lock(shard.mutex);
            if (failed.load() || shard.unavailable.load() ||
                shard.sequence >= std::numeric_limits<std::uint64_t>::max() - 1) return false;
            const Header header{shard.sequence + 1, type, static_cast<std::uint32_t>(key.size()),
                                static_cast<std::uint32_t>(value.size()), expiry};
            if (shard.charged != 0 && header.Charge() > shard_budget - shard.charged && !Flush(shard)) {
                return false;
            }
            if (!MaybeCompact(shard)) return false;
            const auto before = ::lseek(shard.wal, 0, SEEK_END);
            if (before < 0) return false;
            if (!CommitAppend(shard, before, header.Bytes(),
                              WriteRecord(shard.wal, header, key, value, metrics.get(), WriteSource::Wal))) return false;
            try {
                shard.pending.insert_or_assign(std::forward<Key>(key), Entry{header, std::forward<Value>(value)});
                shard.sequence = header.sequence;
                shard.charged += header.Charge();
                if (shard.charged < shard_budget || Flush(shard)) return true;
                MarkFailed(shard);
                return false;
            } catch (...) {
                MarkFailed(shard);
                throw;
            }
        }

        static Header SetHeader(std::uint64_t sequence, const KeyValue& entry) {
            return {sequence, RecordType::Set, static_cast<std::uint32_t>(entry.key.size()),
                    static_cast<std::uint32_t>(entry.value.size()), kNoExpiry};
        }

        bool SetBatch(std::span<const KeyValue> entries) {
            if (entries.empty()) return true;
            if (entries.size() == 1) {
                return Mutate(RecordType::Set, entries.front().key, entries.front().value, kNoExpiry);
            }
            const auto index = Index(entries.front().key);
            std::size_t charge = 0;
            std::size_t bytes = 0;
            for (const auto& entry : entries) {
                if (entry.key.size() > kMaxKeyLen || entry.value.size() > kMaxValueLen ||
                    Index(entry.key) != index) return false;
                const auto header = SetHeader(0, entry);
                if (header.Charge() > shard_budget - charge) return false;
                charge += header.Charge();
                bytes += static_cast<std::size_t>(header.Bytes());
            }

            std::lock_guard writer(write_mutex);
            auto& shard = shards[index];
            std::unique_lock lock(shard.mutex);
            const auto max_sequence = std::numeric_limits<std::uint64_t>::max() - 1;
            if (failed.load() || shard.unavailable.load() || shard.sequence > max_sequence ||
                entries.size() > max_sequence - shard.sequence) return false;
            if (charge > shard_budget - shard.charged && !Flush(shard)) return false;
            if (!MaybeCompact(shard)) return false;

            Memtable staged;
            std::string encoded;
            encoded.reserve(bytes);
            auto sequence = shard.sequence;
            for (const auto& entry : entries) {
                const auto header = SetHeader(++sequence, entry);
                staged.insert_or_assign(entry.key, Entry{header, entry.value});
                char prefix[kHeaderBytes], checksum[kChecksumBytes];
                EncodeRecord(header, entry.key, entry.value, prefix, checksum);
                encoded.append(prefix, sizeof(prefix)).append(entry.key).append(entry.value)
                       .append(checksum, sizeof(checksum));
            }
            const auto before = ::lseek(shard.wal, 0, SEEK_END);
            if (before < 0) return false;
            const bool written = WriteFully(shard.wal, encoded.data(), encoded.size(), metrics.get(), WriteSource::Wal);
            if (!CommitAppend(shard, before, encoded.size(), written)) return false;

            // Transfer the staged map nodes without allocating after the WAL sync.
            for (auto it = staged.begin(); it != staged.end();) {
                auto node = staged.extract(it++);
                const auto previous = shard.pending.find(node.key());
                if (previous == shard.pending.end()) shard.pending.insert(std::move(node));
                else previous->second = std::move(node.mapped());
            }
            shard.sequence = sequence;
            shard.charged += charge;
            return true;
        }

        bool FlushAll() {
            std::lock_guard writer(write_mutex);
            if (failed.load()) return false;
            for (auto& shard : shards) {
                std::unique_lock lock(shard.mutex);
                if (!Flush(shard)) return false;
            }
            return true;
        }

        std::string MergeRange(const Shard& shard, const std::string& scratch, std::uint64_t first,
                               std::uint64_t last, std::optional<std::int64_t> prune_at) {
            if (first == last) return shard.TablePath(first);
            const auto middle = first + (last - first) / 2;
            const auto older = MergeRange(shard, scratch, first, middle, std::nullopt);
            if (older.empty()) return {};
            const auto newer = MergeRange(shard, scratch, middle + 1, last, std::nullopt);
            if (newer.empty()) return {};
            const auto output = scratch + "/" + std::to_string(first) + "-" + std::to_string(last) + ".sst";
            if (!MergeTables(older, newer, output, prune_at, metrics.get())) return {};
            if (first != middle) (void)::unlink(older.c_str());
            if (middle + 1 != last) (void)::unlink(newer.c_str());
            return output;
        }

        bool Merge(Shard& shard) {
            const auto first = shard.FirstTable();
            if (first >= shard.tables) return true;
            const Stage scratch(shard.directory, ".kvstore-merge-XXXXXX");
            const auto merged = MergeRange(shard, scratch.Path(), first, shard.tables, NowSeconds());
            if (merged.empty()) return false;
            const auto base = shard.BasePath(shard.tables);
            const auto manifest_path = shard.directory + "/MANIFEST";
            const auto manifest_tmp = manifest_path + ".tmp";
            const Fd manifest(::open(manifest_tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC |
                                                           O_NONBLOCK | O_NOFOLLOW, 0644));
            if (manifest < 0 || !WriteManifest(manifest, shard.tables, shard.flushed, shard.tables) ||
                !SyncFile(manifest) || ::rename(merged.c_str(), base.c_str()) != 0 ||
                !SyncDirectory(shard.directory.c_str()) || ::rename(manifest_tmp.c_str(), manifest_path.c_str()) != 0) {
                return false;
            }
            if (!SyncDirectory(shard.directory.c_str())) {
                MarkFailed(shard);
                return false;
            }

            shard.base = shard.tables;
            try {
                RemoveUncommittedFiles(shard);
            } catch (const std::exception&) {
                // The manifest already controls visibility. Startup retries cleanup.
            }
            return true;
        }

        bool MaybeCompact(Shard& shard) {
            const auto first = shard.FirstTable();
            if (shard.tables < first || shard.tables - first + 1 < kAutomaticCompactionTables ||
                shard.tables == shard.last_auto_compaction) return true;

            // Retry a failed maintenance attempt only after another table is flushed.
            shard.last_auto_compaction = shard.tables;
            OperationObservation observation(metrics.get(), Operation::AutoCompact);
            try {
                observation.Result(Merge(shard));
            } catch (const StorageError&) {
                MarkFailed(shard);
                return false;
            } catch (const std::system_error&) {
                // Failure to create merge scratch space leaves the previous manifest usable.
            }
            return !failed.load() && !shard.unavailable.load();
        }

        bool Compact() {
            std::lock_guard writer(write_mutex);
            if (failed.load()) return false;
            for (const auto& shard : shards) if (shard.unavailable.load()) return false;
            for (auto& shard : shards) {
                std::unique_lock lock(shard.mutex);
                if (shard.unavailable.load()) return false;
                try {
                    if (!Flush(shard) || !Merge(shard)) return false;
                } catch (const StorageError&) {
                    MarkFailed(shard);
                    throw;
                }
            }
            return true;
        }

        void CheckReadable() const {
            if (failed.load()) Invalid("store requires close and reopen after failure");
        }

        void MarkFailed(const Shard& shard) const {
            shard.unavailable.store(true);
            if (open_mode != OpenMode::Degraded) failed.store(true);
        }

        void CheckShard(const Shard& shard) const {
            CheckReadable();
            if (shard.unavailable.load()) throw ShardUnavailable(shard.index);
        }

        void CheckAll() const {
            for (const auto& shard : shards) CheckShard(shard);
        }

        std::optional<std::string> Get(const std::string& key) const {
            if (key.size() > kMaxKeyLen) return std::nullopt;
            const auto& shard = shards[Index(key)];
            std::shared_lock lock(shard.mutex);
            CheckShard(shard);
            if (const auto entry = shard.pending.find(key); entry != shard.pending.end()) {
                if (entry->second.header.Live(NowSeconds())) return entry->second.value;
                return std::nullopt;
            }
            if (shard.tables == 0) return std::nullopt;
            std::lock_guard reader(disk_read_mutex);
            OperationObservation observation(metrics.get(), Operation::DiskLookup);
            try {
                CheckShard(shard);
                for (auto i = shard.tables; i >= shard.FirstTable(); --i) {
                    const Table table(shard.TablePath(i));
                    if (const auto record = table.Find(key)) {
                        if (record->header.Live(NowSeconds())) {
                            auto value = table.Value(*record);
                            observation.Success();
                            return value;
                        }
                        table.ValidateRecord(*record);
                        observation.Miss();
                        return std::nullopt;
                    }
                }
            } catch (const StorageError&) {
                MarkFailed(shard);
                throw;
            }
            observation.Miss();
            return std::nullopt;
        }

        static bool Shadowed(const Shard& shard, std::uint64_t generation, const std::string& key) {
            if (shard.pending.contains(key)) return true;
            for (auto newer = shard.tables; newer > generation; --newer) {
                if (Table(shard.TablePath(newer)).Find(key)) return true;
            }
            return false;
        }

        // Visit only visible versions. Memory is independent of disk file count.
        // The existing vector API collects and sorts its results outside this loop.
        template<class Visit>
        void Scan(std::string_view start, std::string_view end, Visit visit) const {
            if (!end.empty() && end <= start) return;
            const auto now = NowSeconds();
            for (const auto& shard : shards) {
                std::shared_lock lock(shard.mutex);
                CheckShard(shard);
                for (auto it = shard.pending.lower_bound(start); it != shard.pending.end(); ++it) {
                    const auto& [key, entry] = *it;
                    if (!end.empty() && key >= end) break;
                    if (entry.header.Live(now)) visit(key, [&]() { return entry.value; });
                }
                std::lock_guard reader(disk_read_mutex);
                try {
                    CheckShard(shard);
                    for (auto i = shard.tables; i >= shard.FirstTable(); --i) {
                        const Table table(shard.TablePath(i));
                        for (auto offset = table.StartOffset(start); offset < table.End();) {
                            const auto record = table.Read(offset);
                            if (!end.empty() && record.key >= end) break;
                            if (record.key >= start && !Shadowed(shard, i, record.key)) {
                                if (record.header.Live(now)) visit(record.key, [&]() { return table.Value(record); });
                                else table.ValidateRecord(record);
                            }
                            offset += record.header.Bytes();
                        }
                    }
                } catch (const StorageError&) {
                    MarkFailed(shard);
                    throw;
                }
            }
        }

        std::string path;
        std::shared_ptr<Metrics> metrics;
        OpenMode open_mode;
        Routing routing = Routing::Rendezvous;
        std::size_t Index(std::string_view key) const { return ShardIndex(key, routing, kNumShards); }
        Fd process_lock;
        std::array<Shard, kNumShards> shards;
        std::size_t shard_budget = 0;
        std::mutex write_mutex;
        mutable std::mutex disk_read_mutex;
        mutable std::atomic<bool> failed{false};
    };

    struct KVStore::Snapshot::Cursor {
        explicit Cursor(std::shared_ptr<Engine> source)
            : engine(std::move(source)), writer(engine->write_mutex), captured(NowSeconds()) {
            engine->CheckAll();
            SelectShard();
        }

        bool Next(SnapshotRow& row) {
            if (!writer.owns_lock()) return false;
            try {
                while (index < engine->shards.size()) {
                    const auto& shard = engine->shards[index];
                    engine->CheckShard(shard);
                    while (pending != shard.pending.end()) {
                        const auto& entry = *pending++;
                        if (!entry.second.header.Live(captured)) continue;
                        row = {entry.first, entry.second.value, Expiry(entry.second.header)};
                        return true;
                    }
                    if (ReadTable(shard, row)) return true;
                    ++index;
                    if (index < engine->shards.size()) SelectShard();
                }
                engine->CheckAll();
            } catch (...) {
                table.reset();
                writer.unlock();
                throw;
            }
            writer.unlock();
            return false;
        }

        static std::optional<std::int64_t> Expiry(const Header& header) {
            if (header.expiry == kNoExpiry) return std::nullopt;
            return header.expiry;
        }

        void SelectShard() {
            const auto& shard = engine->shards[index];
            pending = shard.pending.begin();
            generation = shard.tables;
        }

        bool ReadTable(const Engine::Shard& shard, SnapshotRow& row) {
            std::lock_guard reader(engine->disk_read_mutex);
            engine->CheckShard(shard);
            try {
                for (; generation >= shard.FirstTable(); --generation) {
                    if (!table) table.emplace(shard.TablePath(generation));
                    while (auto record = table->Next()) {
                        if (Engine::Shadowed(shard, generation, record->key)) continue;
                        if (record->header.Live(captured)) {
                            auto value = table->table.Value(*record);
                            row = {std::move(record->key), std::move(value), Expiry(record->header)};
                            return true;
                        }
                        table->table.ValidateRecord(*record);
                    }
                    table.reset();
                }
                return false;
            } catch (const StorageError&) {
                engine->MarkFailed(shard);
                throw;
            }
        }

        std::shared_ptr<Engine> engine;
        std::unique_lock<std::mutex> writer;
        std::int64_t captured;
        std::size_t index = 0;
        Memtable::const_iterator pending;
        std::uint64_t generation = 0;
        std::optional<TableCursor> table;
    };

    ShardUnavailable::ShardUnavailable(std::size_t shard)
        : std::runtime_error("KVStore: shard " + std::to_string(shard) + " is unavailable") {}

    KVStore::Snapshot::Snapshot(std::unique_ptr<Cursor> cursor) : cursor_(std::move(cursor)) {}
    KVStore::Snapshot::~Snapshot() = default;
    std::int64_t KVStore::Snapshot::CapturedAt() const { return cursor_->captured; }
    bool KVStore::Snapshot::Next(SnapshotRow& row) { return cursor_->Next(row); }

    std::unique_ptr<KVStore::Snapshot> KVStore::OpenSnapshot() const {
        auto cursor = std::make_unique<Snapshot::Cursor>(engine_);
        return std::unique_ptr<Snapshot>(new Snapshot(std::move(cursor)));
    }

    std::vector<std::size_t> KVStore::UnavailableShards() const {
        std::vector<std::size_t> result;
        for (const auto& shard : engine_->shards) {
            if (shard.unavailable.load()) result.push_back(shard.index);
        }
        return result;
    }

    void KVStore::Checkpoint(const std::string& destination_path) const {
        const auto destination = AbsoluteStorePath(destination_path);
        if (destination == engine_->path) Invalid("checkpoint requires a different destination");
        const auto parent = fs::path(destination).parent_path();
        PrepareParent(parent);
        const auto destination_lock = AcquireLock(destination);
        if (Exists(destination) || Exists(destination + ".wal")) Invalid("checkpoint destination already exists");
        std::lock_guard writer(engine_->write_mutex);
        engine_->CheckAll();
        Engine::EnsureIdentity(engine_->path);

        Stage stage(parent);
        CopyDurable(engine_->path + "/METADATA", stage.Path() + "/METADATA");
        CopyDurable(engine_->path + "/IDENTITY", stage.Path() + "/IDENTITY");
        for (const auto& shard : engine_->shards) {
            const auto target = Engine::ShardDirectory(stage.Path(), shard.index);
            fs::create_directory(target);
            Engine::CopyShard(shard, target);
        }
        Engine validation(stage.Path());
        validation.Open(std::nullopt, false);
        engine_->CheckAll();
        stage.Publish(destination);
    }

    void KVStore::RestoreShard(const std::string& database_path, const std::string& checkpoint_path,
                               std::size_t index) {
        if (index >= kNumShards) throw std::invalid_argument("KVStore: invalid shard number");
        const auto database = AbsoluteStorePath(database_path);
        const auto checkpoint = AbsoluteStorePath(checkpoint_path);
        if (database == checkpoint) Invalid("cannot restore a shard from its own database");
        const auto database_lock = AcquireLock(database);
        const auto checkpoint_lock = AcquireLock(checkpoint);
        Engine target(database), source(checkpoint);
        auto target_metadata = target.ReadMetadata();
        const auto source_metadata = source.ReadMetadata();
        if (!std::equal(target_metadata.begin() + 1, target_metadata.end(), source_metadata.begin() + 1) ||
            Engine::ReadIdentity(database) != Engine::ReadIdentity(checkpoint)) {
            Invalid("checkpoint belongs to a different database");
        }
        Engine::Shard copy;
        copy.directory = Engine::SelectedDirectory(checkpoint, index);
        Engine::ReadShardManifest(copy);
        const auto pattern = Engine::RestorePrefix(index) + "XXXXXX";
        Stage stage(database, pattern.c_str());
        Engine::CopyShard(copy, stage.Path());
        Engine::Shard validated;
        validated.directory = stage.Path();
        target.Recover(validated, index, false);

        // Pre-restore binaries understood modulo routing but ignored shard pointers.
        if (target.routing == Routing::Modulo && target_metadata[0] == kFormatVersion) {
            target_metadata[0] = kShardPointerVersion;
            const auto staged_metadata = stage.Path() + "/METADATA.tmp";
            const Fd metadata(::open(staged_metadata.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644));
            if (metadata < 0 || !WriteFields(metadata, kMetadataMagic, target_metadata) || !SyncFile(metadata) ||
                ::rename(staged_metadata.c_str(), (database + "/METADATA").c_str()) != 0) {
                Invalid("could not publish shard pointer format");
            }
        }
        const auto temporary = stage.Path() + "/.CURRENT.tmp";
        const auto name = fs::path(stage.Path()).filename().string();
        const auto pointer = Engine::ShardPointer(database, index);
        if (::symlink(name.c_str(), temporary.c_str()) != 0 || !SyncDirectory(stage.Path().c_str()) ||
            !SyncDirectory(database.c_str()) || ::rename(temporary.c_str(), pointer.c_str()) != 0) {
            Invalid("could not publish restored shard");
        }
        stage.Release();
        if (!SyncDirectory(database.c_str())) Invalid("shard pointer was renamed; reopen to verify the restore");
    }

    KVStore::KVStore(std::string path, std::shared_ptr<Metrics> metrics,
                     std::optional<std::size_t> memtable_budget, OpenMode mode)
        : engine_(std::make_shared<Engine>(AbsoluteStorePath(path), std::move(metrics), mode)) {
        OperationObservation observation(engine_->metrics.get(), Operation::Recovery);
        engine_->OpenOrCreate(memtable_budget);
        observation.Success();
    }

    KVStore::~KVStore() = default;

    bool KVStore::Set(const std::string& key, const std::string& value) {
        OperationObservation observation(engine_->metrics.get(), Operation::Set);
        return observation.Result(engine_->Mutate(RecordType::Set, key, value, kNoExpiry));
    }

    bool KVStore::SetBatch(std::span<const KeyValue> entries) {
        OperationObservation observation(engine_->metrics.get(), Operation::SetBatch);
        return observation.Result(engine_->SetBatch(entries));
    }

    std::size_t KVStore::ShardForKey(std::string_view key) const noexcept {
        return engine_->Index(key);
    }

    bool KVStore::SetWithTtl(const std::string& key, const std::string& value, std::int64_t ttl_seconds) {
        OperationObservation observation(engine_->metrics.get(), Operation::SetTtl);
        const auto now = NowSeconds();
        if ((ttl_seconds > 0 && now > std::numeric_limits<std::int64_t>::max() - ttl_seconds) ||
            (ttl_seconds < 0 && now < std::numeric_limits<std::int64_t>::min() - ttl_seconds)) return false;
        const auto expiry = now + ttl_seconds;
        return observation.Result(engine_->Mutate(RecordType::SetTtl, key, value, expiry == kNoExpiry ? 1 : expiry));
    }

    bool KVStore::Delete(const std::string& key) {
        OperationObservation observation(engine_->metrics.get(), Operation::Delete);
        return observation.Result(engine_->Mutate(RecordType::Delete, key, std::string{}, kNoExpiry));
    }

    std::optional<std::string> KVStore::Get(const std::string& key) const {
        OperationObservation observation(engine_->metrics.get(), Operation::Get);
        engine_->CheckReadable();
        auto result = engine_->Get(key);
        engine_->CheckReadable();
        if (result) observation.Success();
        else observation.Miss();
        return result;
    }

    bool KVStore::Compact() {
        OperationObservation observation(engine_->metrics.get(), Operation::Compact);
        return observation.Result(engine_->Compact());
    }

    std::size_t KVStore::Size() const {
        OperationObservation observation(engine_->metrics.get(), Operation::Size);
        engine_->CheckAll();
        std::size_t count = 0;
        engine_->Scan({}, {}, [&](const auto&, const auto&) { ++count; });
        engine_->CheckAll();
        observation.Success();
        return count;
    }

    std::vector<KeyValue> KVStore::Items() const { return Range({}, {}); }

    std::vector<KeyValue> KVStore::Range(const std::string& start, const std::string& end) const {
        OperationObservation observation(engine_->metrics.get(), Operation::Range);
        engine_->CheckAll();
        std::vector<KeyValue> result;
        engine_->Scan(start, end, [&](const auto& key, const auto& value) { result.push_back({key, value()}); });
        std::sort(result.begin(), result.end(), [](const auto& a, const auto& b) { return a.key < b.key; });
        engine_->CheckAll();
        observation.Success();
        return result;
    }

    const std::string& KVStore::DatabasePath() const { return engine_->path; }

    std::vector<std::string> KVStore::WalPaths() const {
        std::lock_guard writer(engine_->write_mutex);
        engine_->CheckAll();
        std::vector<std::string> result;
        result.reserve(kNumShards);
        for (const auto& shard : engine_->shards) result.push_back(shard.WalPath(shard.tables + 1));
        return result;
    }

    std::uintmax_t KVStore::WalBytes() const {
        std::lock_guard writer(engine_->write_mutex);
        engine_->CheckAll();
        std::uintmax_t bytes = 0;
        for (const auto& shard : engine_->shards) bytes += FileSize(shard.wal);
        return bytes;
    }

    std::optional<std::uint64_t> KVStore::TelemetryWalBytes() const noexcept {
        if (!engine_->metrics) return std::nullopt;
        std::uint64_t bytes = 0;
        for (const auto& shard : engine_->shards) {
            bytes += shard.committed_wal_bytes.load(std::memory_order_relaxed);
        }
        return bytes;
    }

    void KVStore::MigrateLegacy(const std::string& source_path, const std::string& destination_path,
                                std::size_t memtable_budget) {
        Engine::ValidateBudget(memtable_budget);
        // Match the legacy engine's exact resolution before adding the sidecar suffix.
        const auto source = AbsolutePath(source_path).string();
        const auto destination = AbsoluteStorePath(destination_path);
        if (source == destination) Invalid("migration requires a different destination");
        const auto source_lock = AcquireLock(source);
        const Fd legacy(::open((source + ".wal").c_str(), O_RDONLY | O_CLOEXEC | O_NONBLOCK));
        (void)FileSize(legacy);

        const auto parent = fs::path(destination).parent_path();
        PrepareParent(parent);
        const auto destination_lock = AcquireLock(destination);
        if (Exists(destination) || Exists(destination + ".wal")) Invalid("migration destination already exists");

        Stage stage(parent);
        Engine::Initialize(stage.Path(), memtable_budget, nullptr);
        std::array<std::uint64_t, kNumShards> expected{};
        {
            Engine staged(stage.Path());
            staged.Open(memtable_budget);
            Replay(legacy, true, [&](const Header& header, std::string key, std::string value) {
                const auto index = staged.Index(key);
                if (!staged.Mutate(header.type, std::move(key), std::move(value), header.expiry)) {
                    Invalid("could not replay legacy mutation");
                }
                ++expected[index];
            });
            if (!staged.FlushAll()) Invalid("could not flush migrated data");
        }
        {
            Engine validation(stage.Path());
            validation.Open(memtable_budget);
            for (std::size_t i = 0; i < kNumShards; ++i) {
                if (validation.shards[i].flushed != expected[i] || !validation.shards[i].pending.empty()) {
                    Invalid("migrated shard does not match replayed sequence");
                }
            }
        }
        stage.Publish(destination);
    }

    void KVStore::Rehash(const std::string& source_path, const std::string& destination_path) {
        const KVStore source(source_path, {}, std::nullopt, OpenMode::Existing);
        const auto budget = static_cast<std::size_t>(source.engine_->ReadMetadata()[3]);
        const auto destination = AbsoluteStorePath(destination_path);
        if (source.DatabasePath() == destination) Invalid("rehash requires a different destination");
        const auto parent = fs::path(destination).parent_path();
        PrepareParent(parent);
        const auto destination_lock = AcquireLock(destination);
        if (Exists(destination) || Exists(destination + ".wal")) Invalid("rehash destination already exists");

        Stage stage(parent);
        Engine::Initialize(stage.Path(), budget, nullptr);
        {
            Engine staged(stage.Path());
            staged.Open(budget);
            auto snapshot = source.OpenSnapshot();
            SnapshotRow row;
            while (snapshot->Next(row)) {
                const auto type = row.expires_at ? RecordType::SetTtl : RecordType::Set;
                if (!staged.Mutate(type, std::move(row.key), std::move(row.value),
                                   row.expires_at.value_or(kNoExpiry))) Invalid("could not rehash snapshot row");
            }
            if (!staged.FlushAll()) Invalid("could not flush rehashed database");
        }
        {
            Engine validation(stage.Path());
            validation.Open(budget, false);
        }
        stage.Publish(destination);
    }
}
