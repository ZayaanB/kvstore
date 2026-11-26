#include "kvstore.hpp"
#include "metrics.hpp"

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

namespace kvstore {

    namespace fs = std::filesystem;

    namespace {

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
            return absolute.string();
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

        // write every byte, retrying partial writes and EINTR
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
                    return false;  // no progress, don't spin
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

        // Read errors must never be mistaken for a torn tail.
        bool ReadFully(int fd, char* data, std::size_t length) {
            std::size_t done = 0;
            while (done < length) {
                const ssize_t n = ::read(fd, data + done, length - done);
                if (n < 0) {
                    if (errno == EINTR) continue;
                    throw std::runtime_error("KVStore: file read failed");
                }
                if (n == 0) return false;
                done += static_cast<std::size_t>(n);
            }
            return true;
        }

        [[noreturn]] void Invalid(const char* reason) {
            throw std::runtime_error(std::string("KVStore: ") + reason);
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
            explicit Stage(const fs::path& parent)
                : path_((parent / ".kvstore-stage-XXXXXX").string()) {
                if (::mkdtemp(path_.data()) == nullptr) Invalid("could not create staging directory");
            }
            ~Stage() {
                std::error_code error;
                if (!path_.empty()) fs::remove_all(path_, error);
            }
            const std::string& Path() const { return path_; }
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

        // FNV-1a over unsigned key bytes, modulo the versioned shard count.
        std::size_t ShardIndex(std::string_view key) {
            std::uint64_t hash = 14695981039346656037ull;
            for (const auto byte : key) {
                hash ^= static_cast<unsigned char>(byte);
                hash *= 1099511628211ull;
            }
            return hash % kNumShards;
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

        bool WriteRecord(int fd, const Header& h, std::string_view key, std::string_view value,
                         Metrics* metrics, WriteSource source) {
            char header[kHeaderBytes];
            StoreLe(header, h.sequence);
            header[8] = static_cast<char>(h.type);
            StoreLe(header + 9, h.key_length);
            StoreLe(header + 13, h.value_length);
            StoreLe(header + 17, static_cast<std::uint64_t>(h.expiry));
            Checksum crc;
            crc.Add({header, sizeof(header)});
            crc.Add(key);
            crc.Add(value);
            char checksum[kChecksumBytes];
            StoreLe(checksum, crc.Value());
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
        constexpr std::uint64_t kFnv1a64 = 1;
        constexpr std::string_view kManifestMagic = "KVMAN001";
        constexpr std::string_view kTableMagic = "KVSST001";
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
                if (count_ == 0 || count_ > index_ / (kHeaderBytes + kChecksumBytes) || last_ >= index_ ||
                    index_ > payload || (payload - index_) % 8 != 0 || (payload - index_) / 8 != blocks_ ||
                    low_ == 0 || low_ > high_ || count_ - 1 > high_ - low_) Invalid("invalid sorted table footer");
            }

            std::uint64_t End() const { return index_; }
            std::uint64_t High() const { return high_; }

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
                if (key < Read(0).key || key > Read(last_).key) return std::nullopt;
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
                if (key.empty()) return 0;
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

            void Validate(std::size_t shard, std::uint64_t previous) const {
                if (low_ - 1 != previous) Invalid("nonconsecutive sorted table sequence range");
                std::uint64_t offset = 0;
                std::uint64_t largest = 0;
                std::string previous_key;
                std::array<char, 64 * 1024> buffer;
                for (std::uint64_t i = 0; i < count_; ++i) {
                    if ((i % kBlockRecords == 0 && BlockOffset(i / kBlockRecords) != offset) ||
                        (i == count_ - 1 && last_ != offset)) Invalid("invalid sorted table index");
                    auto record = Read(offset);
                    if (ShardIndex(record.key) != shard || (i != 0 && record.key <= previous_key) ||
                        record.header.sequence < low_ || record.header.sequence > high_) {
                        Invalid("invalid sorted table record order or shard");
                    }
                    auto value_offset = offset + kHeaderBytes + record.key.size();
                    auto remaining = static_cast<std::size_t>(record.header.value_length);
                    while (remaining != 0) {
                        const auto bytes = std::min(buffer.size(), remaining);
                        ReadAt(fd_, value_offset, buffer.data(), bytes);
                        record.prefix.Add({buffer.data(), bytes});
                        value_offset += bytes;
                        remaining -= bytes;
                    }
                    VerifyChecksum(value_offset, record.prefix);
                    largest = std::max(largest, record.header.sequence);
                    previous_key = std::move(record.key);
                    offset += record.header.Bytes();
                }
                if (offset != index_ || largest != high_) Invalid("invalid sorted table extent");
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

        // Sequential reader shared by the legacy migration and the active shard WAL.
        template<class Apply>
        std::uint64_t Replay(int fd, bool legacy, Apply apply, Metrics* metrics = nullptr) {
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
                    if (!legacy && record_bytes == size - offset) break;
                    Invalid("WAL checksum mismatch");
                }
                apply(header, std::move(key), std::move(value));
                offset += record_bytes;
            }
            // Incomplete legacy tails are ignored without touching the source.
            if (!legacy && offset != size &&
                (!TruncateFile(fd, static_cast<off_t>(offset)) ||
                 !SyncWal(fd, false, metrics, WalSync::Recovery))) {
                Invalid("could not durably repair WAL tail");
            }
            return offset;
        }
    }

    struct KVStore::Engine {
        struct Shard {
            std::string directory;
            mutable std::shared_mutex mutex;
            Memtable pending;
            std::size_t charged = 0;
            std::uint64_t tables = 0;
            std::uint64_t flushed = 0;
            std::uint64_t sequence = 0;
            Fd wal;
            std::atomic<std::uint64_t> committed_wal_bytes{0};

            std::string WalPath(std::uint64_t generation) const {
                return directory + "/" + std::to_string(generation) + ".wal";
            }
            std::string TablePath(std::uint64_t generation) const {
                return directory + "/" + std::to_string(generation) + ".sst";
            }
        };

        explicit Engine(std::string directory, std::shared_ptr<Metrics> recorder = {})
            : path(std::move(directory)), metrics(std::move(recorder)) {}

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
            if (meta < 0 || !WriteFields(meta, kMetadataMagic, Fields<4>{kFormatVersion, kNumShards, kFnv1a64, budget}) ||
                !SyncFile(meta) || !SyncDirectory(directory.c_str())) {
                Invalid("could not initialize database metadata");
            }
        }

        void Open(std::optional<std::size_t> requested_budget) {
            const Fd meta(::open((path + "/METADATA").c_str(), O_RDONLY | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW));
            if (FileSize(meta) != kFieldsBytes<4>) Invalid("invalid database metadata length");
            const auto fields = ReadFields<4>(meta, 0, kMetadataMagic);
            if (fields[0] != kFormatVersion || fields[1] != kNumShards || fields[2] != kFnv1a64) {
                Invalid("unsupported database format or routing");
            }
            ValidateBudget(fields[3]);
            if (requested_budget && *requested_budget != fields[3]) Invalid("memtable budget differs from database metadata");
            shard_budget = static_cast<std::size_t>(fields[3]) / kNumShards;
            for (std::size_t i = 0; i < shards.size(); ++i) Recover(shards[i], i);
        }

        void OpenOrCreate(std::optional<std::size_t> budget) {
            const auto parent = fs::path(path).parent_path();
            PrepareParent(parent);
            process_lock = AcquireLock(path);
            if (Exists(path + ".wal")) Invalid("legacy WAL requires offline migration to a new path");
            if (!Exists(path)) {
                Stage stage(parent);
                Initialize(stage.Path(), budget.value_or(kDefaultMemtableBudget), metrics.get());
                stage.Publish(path);
            }
            Open(budget);
        }

        static void RemoveUncommittedFiles(const Shard& shard) {
            bool removed = false;
            for (const auto& file : fs::directory_iterator(shard.directory)) {
                const auto name = file.path().filename();
                const auto extension = name.extension();
                const auto stem = name.stem().string();
                std::uint64_t generation = 0;
                const auto [end, error] = std::from_chars(stem.data(), stem.data() + stem.size(), generation);
                const bool numbered = error == std::errc{} && end == stem.data() + stem.size();
                if (name == "MANIFEST.tmp" ||
                    (numbered && ((extension == ".wal" && generation != shard.tables + 1) ||
                                  (extension == ".sst" && generation > shard.tables)))) {
                    if (::unlink(file.path().c_str()) == 0) removed = true;
                }
            }
            if (removed) (void)SyncDirectory(shard.directory.c_str());
        }

        void Recover(Shard& shard, std::size_t index) {
            shard.directory = ShardDirectory(path, index);
            const Fd manifest(::open((shard.directory + "/MANIFEST").c_str(), O_RDONLY | O_CLOEXEC | O_NONBLOCK | O_NOFOLLOW));
            if (FileSize(manifest) != kFieldsBytes<2>) Invalid("invalid shard manifest length");
            const auto fields = ReadFields<2>(manifest, 0, kManifestMagic);
            shard.tables = fields[0];
            shard.flushed = fields[1];
            if (shard.tables > shard.flushed || shard.tables == std::numeric_limits<std::uint64_t>::max()) {
                Invalid("invalid shard manifest sequence");
            }

            std::uint64_t previous = 0;
            for (std::uint64_t generation = 1; generation <= shard.tables; ++generation) {
                const Table table(shard.TablePath(generation));
                table.Validate(index, previous);
                previous = table.High();
            }
            if (previous != shard.flushed) Invalid("sorted table sequence does not match manifest");

            shard.wal = Fd(::open(shard.WalPath(shard.tables + 1).c_str(), O_RDWR | O_APPEND | O_CLOEXEC | O_NOFOLLOW));
            shard.sequence = shard.flushed;
            const auto recovered_bytes = Replay(shard.wal, false, [&](const Header& header, std::string key, std::string value) {
                if (header.sequence <= shard.sequence || header.sequence - shard.sequence != 1 ||
                    ShardIndex(key) != index || shard.charged >= shard_budget ||
                    (shard.charged != 0 && header.Charge() > shard_budget - shard.charged)) {
                    Invalid("unexpected shard WAL sequence, routing or budget");
                }
                shard.sequence = header.sequence;
                shard.charged += header.Charge();
                shard.pending.insert_or_assign(std::move(key), Entry{header, std::move(value)});
            }, metrics.get());
            if (metrics) shard.committed_wal_bytes.store(recovered_bytes, std::memory_order_relaxed);

            // The manifest rename may have survived a process crash before its fsync.
            // Make its observed state durable before removing any obsolete names.
            if (!SyncDirectory(shard.directory.c_str())) Invalid("could not sync recovered shard");
            RemoveUncommittedFiles(shard);
            if (shard.charged >= shard_budget && !Flush(shard)) Invalid("could not flush recovered WAL");
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
                !WriteFields(manifest, kManifestMagic, Fields<2>{generation, shard.sequence}) ||
                !SyncFile(manifest) || !SyncDirectory(shard.directory.c_str()) ||
                ::rename(manifest_tmp.c_str(), manifest_path.c_str()) != 0) {
                return false;
            }
            if (!SyncDirectory(shard.directory.c_str())) {
                failed.store(true);
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

        template<class Key, class Value>
        bool Mutate(RecordType type, Key&& key, Value&& value, std::int64_t expiry) {
            if (key.size() > kMaxKeyLen || value.size() > kMaxValueLen) return false;
            std::lock_guard writer(write_mutex);
            auto& shard = shards[ShardIndex(key)];
            std::unique_lock lock(shard.mutex);
            if (failed.load() || shard.sequence >= std::numeric_limits<std::uint64_t>::max() - 1) return false;
            const Header header{shard.sequence + 1, type, static_cast<std::uint32_t>(key.size()),
                                static_cast<std::uint32_t>(value.size()), expiry};
            if (shard.charged != 0 && header.Charge() > shard_budget - shard.charged && !Flush(shard)) {
                return false;
            }
            const auto before = ::lseek(shard.wal, 0, SEEK_END);
            if (before < 0) return false;
            if (!WriteRecord(shard.wal, header, key, value, metrics.get(), WriteSource::Wal) ||
                !SyncWal(shard.wal, true, metrics.get(), WalSync::Append)) {
                if (!TruncateFile(shard.wal, before) || !SyncWal(shard.wal, true, metrics.get(), WalSync::Rollback)) {
                    failed.store(true);
                }
                return false;
            }
            if (metrics) {
                shard.committed_wal_bytes.store(static_cast<std::uint64_t>(before) + header.Bytes(),
                                                std::memory_order_relaxed);
            }
            try {
                shard.pending.insert_or_assign(std::forward<Key>(key), Entry{header, std::forward<Value>(value)});
                shard.sequence = header.sequence;
                shard.charged += header.Charge();
                if (shard.charged < shard_budget || Flush(shard)) return true;
                failed.store(true);
                return false;
            } catch (...) {
                failed.store(true);
                throw;
            }
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

        void CheckReadable() const {
            if (failed.load()) Invalid("store requires close and reopen after failure");
        }

        std::optional<std::string> Get(const std::string& key) const {
            if (key.size() > kMaxKeyLen) return std::nullopt;
            const auto& shard = shards[ShardIndex(key)];
            std::shared_lock lock(shard.mutex);
            CheckReadable();
            if (const auto entry = shard.pending.find(key); entry != shard.pending.end()) {
                if (entry->second.header.Live(NowSeconds())) return entry->second.value;
                return std::nullopt;
            }
            if (shard.tables == 0) return std::nullopt;
            std::lock_guard reader(disk_read_mutex);
            OperationObservation observation(metrics.get(), Operation::DiskLookup);
            for (auto i = shard.tables; i != 0; --i) {
                const Table table(shard.TablePath(i));
                if (const auto record = table.Find(key)) {
                    if (record->header.Live(NowSeconds())) {
                        auto value = table.Value(*record);
                        observation.Success();
                        return value;
                    }
                    observation.Miss();
                    return std::nullopt;
                }
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
                CheckReadable();
                for (auto it = shard.pending.lower_bound(start); it != shard.pending.end(); ++it) {
                    const auto& [key, entry] = *it;
                    if (!end.empty() && key >= end) break;
                    if (entry.header.Live(now)) visit(key, [&]() { return entry.value; });
                }
                std::lock_guard reader(disk_read_mutex);
                for (auto i = shard.tables; i != 0; --i) {
                    const Table table(shard.TablePath(i));
                    for (auto offset = table.StartOffset(start); offset < table.End();) {
                        const auto record = table.Read(offset);
                        if (!end.empty() && record.key >= end) break;
                        if (record.key >= start && record.header.Live(now) && !Shadowed(shard, i, record.key)) {
                            visit(record.key, [&]() { return table.Value(record); });
                        }
                        offset += record.header.Bytes();
                    }
                }
            }
        }

        std::string path;
        std::shared_ptr<Metrics> metrics;
        Fd process_lock;
        std::array<Shard, kNumShards> shards;
        std::size_t shard_budget = 0;
        std::mutex write_mutex;
        mutable std::mutex disk_read_mutex;
        std::atomic<bool> failed{false};
    };

    KVStore::KVStore(std::string path, std::shared_ptr<Metrics> metrics,
                     std::optional<std::size_t> memtable_budget)
        : engine_(std::make_unique<Engine>(AbsoluteStorePath(path), std::move(metrics))) {
        OperationObservation observation(engine_->metrics.get(), Operation::Recovery);
        engine_->OpenOrCreate(memtable_budget);
        observation.Success();
    }

    KVStore::~KVStore() = default;

    bool KVStore::Set(const std::string& key, const std::string& value) {
        OperationObservation observation(engine_->metrics.get(), Operation::Set);
        return observation.Result(engine_->Mutate(RecordType::Set, key, value, kNoExpiry));
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
        return observation.Result(engine_->FlushAll());
    }

    std::size_t KVStore::Size() const {
        OperationObservation observation(engine_->metrics.get(), Operation::Size);
        engine_->CheckReadable();
        std::size_t count = 0;
        engine_->Scan({}, {}, [&](const auto&, const auto&) { ++count; });
        engine_->CheckReadable();
        observation.Success();
        return count;
    }

    std::vector<KeyValue> KVStore::Items() const { return Range({}, {}); }

    std::vector<KeyValue> KVStore::Range(const std::string& start, const std::string& end) const {
        OperationObservation observation(engine_->metrics.get(), Operation::Range);
        engine_->CheckReadable();
        std::vector<KeyValue> result;
        engine_->Scan(start, end, [&](const auto& key, const auto& value) { result.push_back({key, value()}); });
        std::sort(result.begin(), result.end(), [](const auto& a, const auto& b) { return a.key < b.key; });
        engine_->CheckReadable();
        observation.Success();
        return result;
    }

    const std::string& KVStore::DatabasePath() const { return engine_->path; }

    std::vector<std::string> KVStore::WalPaths() const {
        std::lock_guard writer(engine_->write_mutex);
        std::vector<std::string> result;
        result.reserve(kNumShards);
        for (const auto& shard : engine_->shards) result.push_back(shard.WalPath(shard.tables + 1));
        return result;
    }

    std::uintmax_t KVStore::WalBytes() const {
        std::lock_guard writer(engine_->write_mutex);
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
                const auto index = ShardIndex(key);
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
}
