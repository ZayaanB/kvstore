#include "kvstore.hpp"
#include "metrics.hpp"
#include "routing.hpp"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdarg>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <limits>
#include <new>
#include <random>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace fs = std::filesystem;
namespace {
enum class Fault {
    None, PartialWrite, RollbackTruncate, RollbackSync, WriteZero, WriteInterrupted,
    SyncOnce, ReadError, ReadInterrupted, RecoveryTruncate, RecoverySync,
    DirectorySync, DirectoryOpen, CreateSync, CreateDirectorySync, RecoveryOpen,
    TempWrite, TempSync, Rename, AllocationAfterSync, AllocationInCompact,
    CrashWrite, CrashTableWrite, CrashTempSync, CrashRename, CrashDirectorySync,
    Retire, CrashBeforeRetire, CrashAfterRetire, CrashMigrationBeforePublish, CrashMigrationAfterPublish,
    MigrationValidation, CrashRestoreBeforePublish, CrashRestoreAfterPublish
};
std::atomic<Fault> fault{Fault::None};
std::atomic<int> phase{0};
std::atomic<bool> fail_allocation{false};
std::string root;

void Arm(Fault f) { phase = 0; fault = f; }
void Disarm() { fault = Fault::None; fail_allocation = false; }
void Check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}
template<class F> void Throws(F&& action, const char* message) {
    bool caught = false;
    try { action(); } catch (const std::exception&) { caught = true; }
    Disarm();
    Check(caught, message);
}
std::string Db(const std::string& name) { return root + "/" + name; }
std::string Bytes(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    Check(in.is_open(), "could not read test WAL");
    std::string bytes(static_cast<std::size_t>(fs::file_size(path)), '\0');
    in.read(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    Check(in.gcount() == static_cast<std::streamsize>(bytes.size()), "short read in test");
    return bytes;
}
std::size_t DescriptorCount() {
    std::size_t count = 0;
    for ([[maybe_unused]] const auto& entry : fs::directory_iterator("/proc/self/fd")) ++count;
    return count;
}
void Seed(const std::string& path) {
    kvstore::KVStore store(path);
    Check(store.Set("base", "original"), "seed write failed");
}
std::string ActiveWal(const std::string& path) {
    const kvstore::KVStore store(path);
    for (const auto& wal : store.WalPaths()) {
        if (fs::file_size(wal) != 0) return wal;
    }
    throw std::runtime_error("test fixture must have a nonempty WAL");
}
void Tear(const std::string& wal) {
    std::ofstream out(wal, std::ios::binary | std::ios::app);
    out.write("\1\2\3", 3);
}
void CheckUnavailable(kvstore::KVStore& store) {
    Check(!store.Set("later", "value"), "poisoned store acknowledged Set");
    Check(!store.Delete("base"), "poisoned store acknowledged Delete");
    Check(!store.Compact(), "poisoned store compacted");
    Throws([&] { (void)store.Get("base"); }, "poisoned store served Get");
    Throws([&] { (void)store.Size(); }, "poisoned store served Size");
    Throws([&] { (void)store.Items(); }, "poisoned store served Items");
}
}

// Fail exactly the first allocation after a successful WAL sync (or temp open).
[[gnu::noinline]] void* operator new(std::size_t size) {
    if (fail_allocation.exchange(false)) throw std::bad_alloc();
    if (void* p = std::malloc(size == 0 ? 1 : size)) return p;
    throw std::bad_alloc();
}
[[gnu::noinline]] void* operator new[](std::size_t size) { return ::operator new(size); }
[[gnu::noinline]] void operator delete(void* p) noexcept { std::free(p); }
[[gnu::noinline]] void operator delete[](void* p) noexcept { std::free(p); }
[[gnu::noinline]] void operator delete(void* p, std::size_t) noexcept { std::free(p); }
[[gnu::noinline]] void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

extern "C" int __real_open(const char*, int, ...);
extern "C" ssize_t __real_write(int, const void*, size_t);
extern "C" ssize_t __real_read(int, void*, size_t);
extern "C" int __real_ftruncate(int, off_t);
extern "C" int __real_fdatasync(int);
extern "C" int __real_fsync(int);
extern "C" int __real_rename(const char*, const char*);
extern "C" int __real_unlink(const char*);

namespace {
bool FdEndsWith(int fd, std::string_view suffix) {
    char reference[64];
    const int length = std::snprintf(reference, sizeof(reference), "/proc/self/fd/%d", fd);
    if (length <= 0 || static_cast<std::size_t>(length) >= sizeof(reference)) return false;
    char path[4096];
    const auto size = ::readlink(reference, path, sizeof(path));
    return size >= 0 && std::string_view(path, static_cast<std::size_t>(size)).ends_with(suffix);
}
}

extern "C" int __wrap_open(const char* path, int flags, ...) {
    mode_t mode = 0;
    if ((flags & O_CREAT) != 0) {
        va_list args;
        va_start(args, flags);
        mode = static_cast<mode_t>(va_arg(args, int));
        va_end(args);
    }
    const std::string_view name(path);
    const bool wal = name.ends_with(".wal");
    if ((fault == Fault::RecoveryOpen && wal) ||
        (fault == Fault::MigrationValidation && name.ends_with(".sst") && (flags & O_ACCMODE) == O_RDONLY) ||
        (fault == Fault::DirectoryOpen && phase == 1 && (flags & O_DIRECTORY) != 0)) {
        errno = EACCES;
        return -1;
    }
    const int fd = __real_open(path, flags, mode);
    if (fd >= 0 && name.ends_with(".sst") && fault == Fault::AllocationInCompact) {
        fail_allocation = true;
    }
    if (fd >= 0 && wal && (flags & O_APPEND) != 0 &&
        fault == Fault::CreateDirectorySync) phase = 1;
    return fd;
}
extern "C" ssize_t __wrap_write(int fd, const void* data, size_t len) {
    const auto f = fault.load();
    if (FdEndsWith(fd, ".wal")) {
        if (f == Fault::PartialWrite || f == Fault::RollbackTruncate) {
            if (phase == 0) { phase = 1; return __real_write(fd, data, std::min(len, size_t{3})); }
            if (phase == 1) { phase = 2; errno = ENOSPC; return -1; }
        }
        if (f == Fault::WriteZero && phase++ == 0) return 0;
        if (f == Fault::WriteInterrupted && phase++ == 0) { errno = EINTR; return -1; }
        if (f == Fault::CrashWrite) {
            (void)__real_write(fd, data, std::min(len, size_t{3}));
            ::_exit(77);
        }
    }
    if (FdEndsWith(fd, ".sst") && f == Fault::TempWrite) { errno = ENOSPC; return -1; }
    const auto result = __real_write(fd, data, len);
    if (result > 0 && FdEndsWith(fd, ".sst") && f == Fault::CrashTableWrite) ::_exit(77);
    return result;
}
extern "C" ssize_t __wrap_read(int fd, void* data, size_t len) {
    if (FdEndsWith(fd, ".wal") && fault == Fault::ReadError) { errno = EIO; return -1; }
    if (FdEndsWith(fd, ".wal") && fault == Fault::ReadInterrupted && phase++ == 0) {
        errno = EINTR; return -1;
    }
    return __real_read(fd, data, len);
}
extern "C" int __wrap_ftruncate(int fd, off_t len) {
    if (FdEndsWith(fd, ".wal") && (fault == Fault::RollbackTruncate || fault == Fault::RecoveryTruncate)) {
        errno = EIO; return -1;
    }
    return __real_ftruncate(fd, len);
}
extern "C" int __wrap_fdatasync(int fd) {
    const auto f = fault.load();
    if (FdEndsWith(fd, ".wal") && (f == Fault::RollbackSync || f == Fault::CreateSync ||
                       (f == Fault::SyncOnce && phase++ == 0))) {
        errno = EIO; return -1;
    }
    if (FdEndsWith(fd, ".sst") && f == Fault::TempSync) { errno = EIO; return -1; }
    const int result = __real_fdatasync(fd);
    if (result == 0 && FdEndsWith(fd, ".wal") && f == Fault::AllocationAfterSync) fail_allocation = true;
    if (result == 0 && FdEndsWith(fd, ".sst") && f == Fault::CrashTempSync) ::_exit(77);
    return result;
}
extern "C" int __wrap_fsync(int fd) {
    struct stat info {};
    const bool directory = ::fstat(fd, &info) == 0 && S_ISDIR(info.st_mode);
    const auto f = fault.load();
    if ((FdEndsWith(fd, ".wal") && f == Fault::RecoverySync) ||
        (directory && ((f == Fault::DirectorySync || f == Fault::CreateDirectorySync) && phase == 1))) {
        errno = EIO; return -1;
    }
    const int result = __real_fsync(fd);
    if (result == 0 && directory && f == Fault::CrashDirectorySync && phase == 1) ::_exit(77);
    return result;
}
extern "C" int __wrap_rename(const char* from, const char* to) {
    const bool manifest = std::string_view(to).ends_with("/MANIFEST");
    const bool restore = std::string_view(to).ends_with(".CURRENT");
    if (manifest && fault == Fault::Rename) { errno = EIO; return -1; }
    if (!manifest && fault == Fault::CrashMigrationBeforePublish) ::_exit(77);
    if (restore && fault == Fault::CrashRestoreBeforePublish) ::_exit(77);
    const int result = __real_rename(from, to);
    if (result == 0 && manifest) {
        phase = 1;
        if (fault == Fault::CrashRename) ::_exit(77);
    }
    if (result == 0 && !manifest && fault == Fault::CrashMigrationAfterPublish) ::_exit(77);
    if (result == 0 && restore && fault == Fault::CrashRestoreAfterPublish) ::_exit(77);
    return result;
}
extern "C" int __wrap_unlink(const char* path) {
    const bool retired = std::string_view(path).ends_with(".wal");
    if (retired && fault == Fault::Retire) { errno = EIO; return -1; }
    if (retired && fault == Fault::CrashBeforeRetire) ::_exit(77);
    const int result = __real_unlink(path);
    if (result == 0 && retired && fault == Fault::CrashAfterRetire) ::_exit(77);
    return result;
}

namespace {
void TestRollback() {
    for (const bool compact_first : {false, true}) {
        for (auto f : {Fault::PartialWrite, Fault::WriteZero, Fault::SyncOnce,
                       Fault::RollbackTruncate, Fault::RollbackSync}) {
            const auto path = Db("rollback-" + std::to_string(static_cast<int>(f)) +
                                 (compact_first ? "-compacted" : ""));
            {
                const auto metrics = std::make_shared<kvstore::Metrics>();
                kvstore::KVStore store(path, metrics);
                Check(store.Set("base", "original"), "seed failed");
                if (compact_first) Check(store.Compact(), "pre-failure compaction failed");
                const auto before = metrics->Storage();
                Arm(f);
                Check(!store.Set("broken", "value"), "injected write unexpectedly succeeded");
                Disarm();
                if (f == Fault::SyncOnce) {
                    const auto after = metrics->Storage();
                    Check(after.wal_syncs - before.wal_syncs == 2, "metrics missed append or rollback sync");
                    Check(after.failed_wal_syncs - before.failed_wal_syncs == 1, "metrics missed failed sync");
                    Check(after.wal_written_bytes - before.wal_written_bytes == 40, "metrics missed bytes rolled back");
                    Check(metrics->Prometheus().find("kvstore_operations_total{operation=\"set\",outcome=\"error\"} 1") !=
                        std::string::npos, "metrics missed failed Set");
                }
                if (f == Fault::RollbackTruncate || f == Fault::RollbackSync) {
                    CheckUnavailable(store);
                } else {
                    Check(!store.Get("broken"), "failed write became visible");
                    Check(store.Set("later", "value"), "healthy rollback prevented later write");
                }
            }
            kvstore::KVStore store(path);
            Check(store.Get("base") == "original", "rollback lost earlier acknowledged write");
            Check(!store.Get("broken"), "failed write survived durable rollback");
            const bool poisoned = f == Fault::RollbackTruncate || f == Fault::RollbackSync;
            Check(store.Get("later").has_value() != poisoned, "later write recovery mismatch");
        }
    }
    const auto path = Db("eintr");
    { kvstore::KVStore store(path); Arm(Fault::WriteInterrupted);
      Check(store.Set("key", "value"), "write EINTR was not retried"); Disarm(); }
    Arm(Fault::ReadInterrupted);
    kvstore::KVStore store(path);
    Disarm();
    Check(store.Get("key") == "value", "read EINTR was not retried");
}
void TestRecoveryFailures() {
    for (auto f : {Fault::ReadError, Fault::RecoveryTruncate, Fault::RecoverySync, Fault::RecoveryOpen}) {
        const auto path = Db("recovery-" + std::to_string(static_cast<int>(f)));
        Seed(path);
        const auto wal = ActiveWal(path);
        Tear(wal);
        const auto before = Bytes(wal);
        Arm(f);
        Throws([&] { kvstore::KVStore store(path); }, "recovery ignored I/O failure");
        if (f != Fault::RecoverySync) Check(Bytes(wal) == before, "failed recovery changed WAL");
        kvstore::KVStore store(path); // also verifies constructor released process lock
        Check(store.Get("base") == "original", "recovery lost seed");
        Check(store.Set("after", "value"), "recovery retry failed");
    }
    const auto path = Db("bad-file");
    Seed(path);
    const auto wal = ActiveWal(path);
    const auto saved = wal + ".saved";
    fs::rename(wal, saved);
    Throws([&] { kvstore::KVStore store(path); }, "missing WAL accepted");
    fs::create_directory(wal);
    Throws([&] { kvstore::KVStore store(path); }, "directory accepted as WAL");
    fs::remove(wal);
    fs::rename(saved, wal);
    kvstore::KVStore retry(path);
    Check(retry.Set("key", "value"), "constructor leaked lock");
}
void TestCorruptionAndTornRecords() {
    const auto path = Db("middle-corruption");
    { kvstore::KVStore store(path); Check(store.Set("first", "one"), "write failed");
      Check(store.Set("first", "two"), "write failed"); }
    const auto wal = ActiveWal(path);
    auto bytes = Bytes(wal);
    bytes[25] ^= 1;
    { std::ofstream out(wal, std::ios::binary | std::ios::trunc); out.write(bytes.data(), static_cast<std::streamsize>(bytes.size())); }
    Throws([&] { kvstore::KVStore store(path); }, "middle corruption silently truncated");
    Check(Bytes(wal) == bytes, "middle corruption was destructive");
    bytes[8] = 99;
    { std::ofstream out(wal, std::ios::binary | std::ios::trunc); out.write(bytes.data(), static_cast<std::streamsize>(bytes.size())); }
    Throws([&] { kvstore::KVStore store(path); }, "unknown header accepted");
    Check(Bytes(wal) == bytes, "unknown header was destructive");

    const auto source = Db("record-source");
    { kvstore::KVStore store(source); Check(store.SetWithTtl("base", "payload", 3600), "TTL write failed"); }
    auto record = Bytes(ActiveWal(source));
    // Retarget the copied record to sequence 2 with a normal WAL write.
    {
        kvstore::KVStore store(source);
        Check(store.SetWithTtl("base", "payload", 3600), "second TTL write failed");
    }
    record = Bytes(ActiveWal(source)).substr(record.size());
    // Exercise every truncation boundary: header, payload, expiry, and checksum.
    for (std::size_t n = 1; n < record.size(); ++n) {
        const auto torn = Db("torn-" + std::to_string(n));
        Seed(torn);
        const auto torn_wal = ActiveWal(torn);
        const auto prefix = fs::file_size(torn_wal);
        { std::ofstream out(torn_wal, std::ios::binary | std::ios::app);
          out.write(record.data(), static_cast<std::streamsize>(n)); }
        kvstore::KVStore store(torn);
        Check(store.Get("base") == "original", "torn record lost valid prefix");
        Check(fs::file_size(torn_wal) == prefix, "torn tail not repaired");
    }
}
void TestFlushFailures() {
    for (auto f : {Fault::TempWrite, Fault::TempSync, Fault::Rename,
                   Fault::DirectorySync, Fault::DirectoryOpen, Fault::AllocationInCompact}) {
        const auto path = Db("compact-" + std::to_string(static_cast<int>(f)));
        {
            kvstore::KVStore store(path);
            Check(store.Set("base", std::string(128, 'a')), "seed failed");
            const auto descriptors = DescriptorCount();
            Arm(f);
            if (f == Fault::AllocationInCompact) {
                Throws([&] { (void)store.Compact(); }, "allocation injection missed compaction");
            } else { Check(!store.Compact(), "compaction ignored injected failure"); Disarm(); }
            Check(DescriptorCount() == descriptors, "failed compaction leaked descriptor");
            if (f == Fault::DirectorySync || f == Fault::DirectoryOpen) CheckUnavailable(store);
            else {
                Check(store.Set("later", "value"), "failed compaction broke old WAL");
                Check(store.Compact(), "compaction guard not reset after failure");
            }
        }
        kvstore::KVStore store(path);
        Check(store.Get("base") == std::string(128, 'a'), "failed compaction lost data");
    }
}
void TestCreateFailuresAndMemoryFailure() {
    for (auto f : {Fault::CreateSync, Fault::CreateDirectorySync}) {
        const auto path = Db("create-" + std::to_string(static_cast<int>(f)));
        Arm(f);
        Throws([&] { kvstore::KVStore store(path); }, "create ignored sync failure");
        kvstore::KVStore store(path);
        Check(store.Set("key", "value"), "failed create could not reopen");
    }
    const auto path = Db("memory-failure");
    {
        kvstore::KVStore store(path);
        Check(store.Set("base", "original"), "seed failed");
        const std::string value(128, 'x');
        Arm(Fault::AllocationAfterSync);
        Throws([&] { (void)store.Set("base", value); }, "post-WAL allocation injection missed");
        CheckUnavailable(store);
    }
    kvstore::KVStore store(path);
    Check(store.Get("base") == std::string(128, 'x'), "durable uncertain write did not recover");
}
void TestEdges() {
    const auto path = Db("edges");
    {
        kvstore::KVStore store(path);
        Check(store.Set("", ""), "empty pair rejected");
        Check(store.Set(std::string("a\0b", 3), std::string("c\0d", 3)), "binary pair rejected");
        Check(!store.SetWithTtl("overflow", "value", std::numeric_limits<std::int64_t>::max()), "TTL overflow accepted");
        Check(store.SetWithTtl("negative", "value", std::numeric_limits<std::int64_t>::min()), "valid negative TTL rejected");
        Check(!store.Get("negative"), "negative TTL did not expire");
        Check(store.SetWithTtl("zero", "value", 0), "zero TTL rejected");
        Check(!store.Get("zero"), "zero TTL did not expire");
        const std::string too_long(kvstore::kMaxKeyLen + 1, 'k');
        Check(!store.Set(too_long, "value") && !store.Delete(too_long), "oversized key accepted");
        Throws([&] { kvstore::KVStore duplicate(path); }, "process lock accepted second instance");
        Check(store.Compact(), "edge compaction failed");
    }
    kvstore::KVStore store(path);
    Check(store.Get("") == "", "empty pair did not recover");
    Check(store.Get(std::string("a\0b", 3)) == std::string("c\0d", 3), "binary pair did not recover");
}
void TestPaths() {
    Throws([&] { kvstore::KVStore store(""); }, "empty path accepted");
    const std::string nul_path = Db("invalid-path") + std::string("\0suffix", 7);
    Throws([&] { kvstore::KVStore store(nul_path); }, "NUL path accepted");
    Check(!fs::exists(Db("invalid-path")), "NUL path touched truncated filename");
    const auto original_cwd = fs::current_path();
    struct RestoreCwd {
        fs::path path;
        ~RestoreCwd() { std::error_code ec; fs::current_path(path, ec); }
    } restore{original_cwd};
    fs::create_directories(Db("cwd-first"));
    fs::create_directories(Db("cwd-second"));
    fs::current_path(Db("cwd-first"));
    {
        kvstore::KVStore store("relative");
        const auto wals = store.WalPaths();
        Check(wals.size() == kvstore::kNumShards && fs::path(wals.front()).is_absolute(),
              "WAL paths did not bind to absolute location");
        Check(store.Set("before", "value"), "initial relative write failed");
        fs::current_path(Db("cwd-second"));
        Check(store.Compact(), "compaction after chdir failed");
        Check(store.Set("after", "value"), "post-chdir write failed");
        const auto current_wals = store.WalPaths();
        for (std::size_t i = 0; i < wals.size(); ++i) {
            Check(fs::path(current_wals[i]).parent_path() == fs::path(wals[i]).parent_path(), "shard location changed");
        }
        Check(!fs::exists("relative"), "compaction followed changed cwd");
    }
    kvstore::KVStore reopened(Db("cwd-first/relative"));
    Check(reopened.Get("before") == "value" && reopened.Get("after") == "value",
          "chdir redirected acknowledged writes away from original database");
}

void TestSymlinkLock() {
    const auto path = Db("real-store");
    const auto alias = Db("store-alias");
    {
        const kvstore::KVStore store(path);
        fs::create_directory_symlink(fs::path(path).filename(), alias);
        Throws([&] { kvstore::KVStore second(alias); }, "database alias bypassed process lock");
    }
    {
        kvstore::KVStore store(alias);
        Throws([&] { kvstore::KVStore second(path); }, "target bypassed database alias's process lock");
        Check(store.Set("key", "value"), "write through database alias failed");
    }
    const kvstore::KVStore store(path);
    Check(store.Get("key") == "value", "database alias wrote to the wrong target");
}

void TestModel() {
    const auto path = Db("model");
    auto store = std::make_unique<kvstore::KVStore>(path);
    std::map<std::string, std::string> model;
    std::mt19937 rng(12345);
    auto verify = [&] {
        const auto items = store->Items();
        Check(items.size() == model.size() && store->Size() == model.size(), "model size mismatch");
        std::size_t i = 0;
        for (const auto& [key, value] : model) {
            Check(items[i].key == key && items[i].value == value, "ordered model mismatch");
            Check(store->Get(key) == value, "model read mismatch");
            ++i;
        }
        const auto range = store->Range("key-1", "key-3");
        std::vector<kvstore::KeyValue> expected;
        for (const auto& [key, value] : model) {
            if (key >= "key-1" && key < "key-3") expected.push_back({key, value});
        }
        Check(range.size() == expected.size(), "model range size mismatch");
        for (i = 0; i < range.size(); ++i) {
            Check(range[i].key == expected[i].key && range[i].value == expected[i].value,
                  "model range value mismatch");
        }
        Check(store->Range("z", "a").empty(), "reversed range not empty");
    };
    for (int i = 0; i < 300; ++i) {
        const auto key = "key-" + std::to_string(rng() % 40);
        const auto value = std::string("value\0", 6) + std::to_string(i);
        switch (rng() % 5) {
            case 0:
                Check(store->Delete(key), "model delete failed"); model.erase(key); break;
            case 1:
                Check(store->SetWithTtl(key, value, 0), "model expiry failed"); model.erase(key); break;
            case 2:
                Check(store->SetWithTtl(key, value, 3600), "model TTL failed"); model[key] = value; break;
            default:
                Check(store->Set(key, value), "model set failed"); model[key] = value; break;
        }
        if (!model.contains(key)) Check(!store->Get(key), "deleted/expired model key visible");
        if (i % 30 == 0) {
            verify();
            Check(store->Compact(), "model compaction failed");
            store.reset();
            store = std::make_unique<kvstore::KVStore>(path);
            verify();
        }
    }
    verify();
    store.reset();
    store = std::make_unique<kvstore::KVStore>(path);
    verify();
}

void TestCorruptVisibilityRecord() {
    const auto path = Db("corrupt-visibility");
    kvstore::KVStore store(path, {}, kvstore::kNumShards * 192);
    Check(store.Set("base", "original") && store.Delete("base"), "could not flush tombstone");
    Check(!store.Get("base") && store.Items().empty(), "valid tombstone did not hide old value");
    fs::path table;
    for (const auto& file : fs::recursive_directory_iterator(path)) {
        if (file.path().filename() == "2.sst") table = file.path();
    }
    auto data = Bytes(table.string());
    data[25 + std::string_view("base").size()] ^= 1;
    std::ofstream(table, std::ios::binary | std::ios::trunc) << data;
    Throws([&] { (void)store.Get("base"); }, "lookup treated a corrupt tombstone as a miss");
    Throws([&] { (void)store.Items(); }, "scan accepted a corrupt tombstone");
}

void TestSnapshot() {
    kvstore::KVStore store(Db("snapshot"));
    Check(store.Set("expired", "old") && store.Set("gone", "old") &&
          store.Set("value", "old") && store.Compact(), "snapshot disk seed failed");
    Check(store.SetWithTtl("expired", "new", 0) && store.Delete("gone") &&
          store.SetWithTtl("value", "new", 3600), "snapshot memtable seed failed");
    std::promise<void> ready;
    std::future<bool> writer;
    auto snapshot = store.OpenSnapshot();
    writer = std::async(std::launch::async, [&] {
        ready.set_value();
        return store.Set("after", "new");
    });
    ready.get_future().wait();
    Check(writer.wait_for(std::chrono::milliseconds(20)) == std::future_status::timeout,
          "write advanced during snapshot");
    kvstore::SnapshotRow row;
    Check(snapshot->Next(row) && row.key == "value" && row.value == "new" &&
          row.expires_at && *row.expires_at > snapshot->CapturedAt(), "snapshot row mismatch");
    Check(!snapshot->Next(row) && !snapshot->Next(row), "snapshot exposed a shadowed row");
    Check(writer.get() && store.Get("after") == "new", "snapshot exhaustion did not release writer");
}

void TestMergeCrashBoundaries() {
    for (auto fault_point : {Fault::CrashTempSync, Fault::CrashRename, Fault::None}) {
        const auto path = Db("table-merge-" + std::to_string(static_cast<int>(fault_point)));
        {
            kvstore::KVStore store(path, {}, kvstore::kNumShards * 192);
            Check(store.Set("base", std::string(4096, 'a')) &&
                  store.Set("base", std::string(4096, 'b')) &&
                  store.Delete("base"), "merge fixture failed");
        }
        if (fault_point != Fault::None) {
            const pid_t child = ::fork();
            Check(child >= 0, "fork failed");
            if (child == 0) {
                kvstore::KVStore store(path);
                Arm(fault_point);
                (void)store.Compact();
                ::_exit(3);
            }
            int status = 0;
            Check(::waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 77,
                  "merge crash point was not reached");
        }
        {
            kvstore::KVStore store(path);
            Check(!store.Get("base") && store.Size() == 0, "merge crash revived deleted value");
            Check(store.Compact(), "could not finish merge");
            std::vector<fs::path> tables;
            for (const auto& entry : fs::recursive_directory_iterator(path)) {
                if (entry.path().extension() == ".sst") tables.push_back(entry.path());
            }
            Check(tables.size() == 1 && tables[0].filename() == "base-3.sst" &&
                  fs::file_size(tables[0]) < 4096, "merge retained obsolete tables");
            Check(store.Set("base", "after"), "write after merge failed");
        }
        kvstore::KVStore store(path);
        Check(store.Get("base") == "after" && store.Compact(), "merged manifest did not recover");
    }
}

void TestCheckpointAndRestore() {
    const auto path = Db("checkpoint-primary");
    const auto checkpoint = Db("checkpoint-copy");
    std::size_t shard = 0;
    fs::path table;
    {
        kvstore::KVStore store(path);
        Check(store.Set("base", "backup value"), "checkpoint seed failed");
        const auto wals = store.WalPaths();
        while (shard < wals.size() && fs::file_size(wals[shard]) == 0) ++shard;
        Check(shard < wals.size(), "checkpoint seed did not reach a shard");
        Check(store.Compact(), "checkpoint flush failed");
        table = fs::path(wals[shard]).parent_path() / "1.sst";
        Check(store.Set("different", "pending"), "checkpoint pending seed failed");
        store.Checkpoint(checkpoint);
        Check(store.Set("base", "after backup"), "primary update failed");
    }
    {
        const kvstore::KVStore copy(checkpoint);
        Check(copy.Get("base") == "backup value" && copy.Get("different") == "pending",
              "standalone checkpoint lost disk or WAL data");
    }
    fs::rename(table, table.string() + ".bad");
    Throws([&] { kvstore::KVStore strict(path); }, "strict open accepted missing shard data");
    const auto interrupt_restore = [&](Fault point) {
        const pid_t child = ::fork();
        Check(child >= 0, "fork failed");
        if (child == 0) {
            Arm(point);
            kvstore::KVStore::RestoreShard(path, checkpoint, shard);
            ::_exit(3);
        }
        int status = 0;
        Check(::waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 77,
              "restore crash point was not reached");
    };
    interrupt_restore(Fault::CrashRestoreBeforePublish);
    {
        kvstore::KVStore degraded(path, {}, std::nullopt, kvstore::OpenMode::Degraded);
        Check(degraded.UnavailableShards() == std::vector<std::size_t>{shard}, "wrong shard quarantined");
        bool unavailable = false;
        try { (void)degraded.Get("base"); }
        catch (const kvstore::ShardUnavailable&) { unavailable = true; }
        Check(unavailable && !degraded.Set("base", "discard"), "failed shard accepted a point operation");
        Check(degraded.Set("different", "healthy") && degraded.Get("different") == "healthy",
              "healthy shard did not serve operations");
        Throws([&] { (void)degraded.OpenSnapshot(); }, "snapshot accepted an incomplete database");
        Throws([&] { (void)degraded.Size(); }, "count accepted an incomplete database");
    }
    kvstore::KVStore::RestoreShard(path, checkpoint, shard);
    {
        kvstore::KVStore restored(path);
        Check(restored.Get("base") == "backup value" && restored.Get("different") == "healthy" &&
              restored.UnavailableShards().empty(), "restore changed the wrong shard or lost backup data");
        Check(restored.Set("base", "restored write"), "restored shard was not writable");
    }
    interrupt_restore(Fault::CrashRestoreAfterPublish);
    kvstore::KVStore restored(path);
    Check(restored.Get("base") == "backup value", "second restore did not replace the selected generation");
}

std::uint32_t TestCrc(std::string_view bytes) {
    std::uint32_t crc = 0xffffffffu;
    for (const auto byte : bytes) {
        crc ^= static_cast<unsigned char>(byte);
        for (int bit = 0; bit < 8; ++bit) crc = (crc >> 1) ^ ((crc & 1u) != 0 ? 0xedb88320u : 0u);
    }
    return ~crc;
}

void TestConsistentHashing() {
    using namespace kvstore::detail;
    constexpr auto count = kvstore::kNumShards;
    static_assert(count == 16);
    const std::string binary("\0\xff", 2);
    Check(ShardIndex(binary, Routing::Modulo, count) == 0 &&
          ShardIndex(binary, Routing::Rendezvous, count) == 2, "binary routing changed");
    std::array<std::size_t, count> distributed{};
    for (int i = 0; i < 8192; ++i) {
        const auto key = "hash-key-" + std::to_string(i);
        const auto before = ShardIndex(key, Routing::Rendezvous, count);
        const auto after = ShardIndex(key, Routing::Rendezvous, count + 1);
        Check(before == after || after == count, "adding a shard reassigned nearby keys");
        const auto smaller = ShardIndex(key, Routing::Rendezvous, count - 1);
        Check(before == smaller || before == count - 1, "removing a shard moved a surviving key");
        ++distributed[before];
    }
    for (auto size : distributed) Check(size > 300 && size < 700, "uneven hash distribution");

    const auto source = Db("old-modulo"), destination = Db("rehashed");
    { const kvstore::KVStore old(source); }
    const auto metadata = source + "/METADATA";
    auto bytes = Bytes(metadata);
    bytes.at(24) = static_cast<char>(Routing::Modulo);
    const auto crc = TestCrc(std::string_view(bytes).substr(0, bytes.size() - 4));
    for (unsigned i = 0; i < 4; ++i) bytes[bytes.size() - 4 + i] = static_cast<char>(crc >> (8 * i));
    std::ofstream(metadata, std::ios::binary | std::ios::trunc) << bytes;
    {
        kvstore::KVStore old(source);
        Check(old.SetWithTtl(binary, "bytes", 3600) && old.Set("other", "other value") &&
              old.Compact() && old.Delete("other"), "old routing could not write");
        const auto shard = fs::path(old.WalPaths().front()).parent_path();
        Check(fs::exists(shard / "1.sst"), "old routing moved a committed key");
    }
    kvstore::KVStore::Rehash(source, destination);
    kvstore::KVStore old(source), rehashed(destination);
    const auto collect_row = [&](const kvstore::KVStore& store) {
        auto snapshot = store.OpenSnapshot();
        kvstore::SnapshotRow row;
        Check(snapshot->Next(row) && row.key == binary && row.value == "bytes" &&
              row.expires_at && !snapshot->Next(row), "rehash copied an incorrect row");
        return row.expires_at;
    };
    Check(collect_row(old) == collect_row(rehashed), "rehash changed absolute expiry");
    const auto new_shard = fs::path(rehashed.WalPaths().at(2)).parent_path();
    Check(fs::exists(new_shard / "1.sst") && rehashed.Set(binary, "new"),
          "rehash did not route binary key to its new shard");
}

void TestConcurrentCompaction() {
    const auto path = Db("concurrent");
    constexpr int workers = 4;
    constexpr int keys = 30;
    std::vector<kvstore::KeyValue> before_restart;
    {
        kvstore::KVStore store(path);
        std::atomic<bool> start{false}, stop{false}, okay{true};
        std::vector<std::thread> writers;
        for (int w = 0; w < workers; ++w) writers.emplace_back([&, w] {
            while (!start.load()) std::this_thread::yield();
            for (int i = 0; i < keys; ++i) {
                const auto key = std::to_string(w) + ":" + std::to_string(i);
                if (!store.Set(key, "old") || !store.SetWithTtl(key, "new", 3600)) okay = false;
                if (i % 3 == 0 && !store.Delete(key)) okay = false;
                const auto hot = "hot-key-" + std::to_string(i % 4);
                if (!store.Set(hot, key)) okay = false;
                if (i % 5 == 0 && !store.Delete(hot)) okay = false;
            }
        });
        std::thread reader([&] {
            while (!stop.load()) { (void)store.Get("hot-key-0"); (void)store.Range("0:", "2:"); (void)store.Size(); }
        });
        std::thread compactor([&] {
            start = true;
            for (int i = 0; i < 8; ++i) if (!store.Compact()) okay = false;
        });
        for (auto& writer : writers) writer.join();
        compactor.join(); stop = true; reader.join();
        Check(okay, "concurrent operation failed");
        before_restart = store.Items();
    }
    kvstore::KVStore store(path);
    const auto recovered = store.Items();
    Check(recovered.size() == before_restart.size(), "concurrent recovery size mismatch");
    for (std::size_t i = 0; i < recovered.size(); ++i) {
        Check(recovered[i].key == before_restart[i].key &&
              recovered[i].value == before_restart[i].value, "same-key WAL/memory order diverged");
    }
    for (int w = 0; w < workers; ++w) for (int i = 0; i < keys; ++i) {
        const auto value = store.Get(std::to_string(w) + ":" + std::to_string(i));
        if (i % 3 == 0) Check(!value, "compaction resurrected deletion");
        else Check(value == "new", "compaction lost acknowledged update");
    }
}
void TestRetirementAndCommittedCorruption() {
    const auto path = Db("obsolete-wal");
    Seed(path);
    const auto old_wal = ActiveWal(path);
    const auto directory = fs::path(old_wal).parent_path();
    const auto table = (directory / "1.sst").string();
    {
        kvstore::KVStore store(path);
        Arm(Fault::Retire);
        Check(store.Compact(), "failed retirement invalidated a committed flush");
        Disarm();
        Check(fs::exists(old_wal), "retirement fault left no old WAL");
        Check(store.Delete("base"), "delete after failed retirement failed");
    }
    // An obsolete WAL must never be opened, even if its contents are unreadable.
    { std::ofstream out(old_wal, std::ios::binary | std::ios::trunc); out << "obsolete bytes"; }
    {
        kvstore::KVStore store(path);
        Check(!store.Get("base"), "stale WAL resurrected a key");
        Check(!fs::exists(old_wal), "recovery did not retry WAL retirement");
    }
    const auto saved = table + ".saved";
    fs::rename(table, saved);
    Throws([&] { kvstore::KVStore store(path); }, "missing committed table accepted");
    fs::rename(saved, table);
    auto data = Bytes(table);
    data[25] ^= 1;
    { std::ofstream out(table, std::ios::binary | std::ios::trunc); out << data; }
    Throws([&] { kvstore::KVStore store(path); }, "corrupt committed table accepted");
    Check(Bytes(table) == data, "strict open modified a corrupt committed table");
}

void TestBoundedFlush() {
    const auto path = Db("bounded");
    constexpr std::size_t budget = 16 * 1024;
    {
        kvstore::KVStore store(path, {}, budget);
        const auto initial = store.WalPaths();
        const std::string value(500, 'v');
        Check(store.Set("base", "older"), "bounded seed failed");
        Check(store.Set("base", value), "bounded overwrite failed");
        Arm(Fault::TempSync);
        Check(!store.Set("base", value), "pre-append flush fault ignored");
        Disarm();
        Check(store.WalPaths() == initial && store.Get("base") == value, "failed publication changed the active view");
        Check(store.SetWithTtl("base", value, 0), "expired newest write failed");
        Check(store.Compact(), "bounded flush failed");
        Check(store.Get("base") == std::nullopt, "newest expiry exposed an older table value");
        Check(store.Set("big", std::string(budget + 1, 'b')), "single above-budget record rejected");
        Check(store.WalBytes() == 0, "above-budget record retained in active WAL");
    }
    kvstore::KVStore reopened(path);
    Check(!reopened.Get("base") && reopened.Get("big")->size() == budget + 1, "bounded reopen mismatch");

    const auto crash = Db("oversized-replay");
    {
        kvstore::KVStore store(crash, {}, budget);
        Arm(Fault::TempSync);
        Check(!store.Set("big", std::string(budget + 1, 'b')), "oversized flush fault ignored");
        Disarm();
        CheckUnavailable(store);
    }
    kvstore::KVStore recovered(crash);
    Check(recovered.Get("big")->size() == budget + 1 && recovered.WalBytes() == 0,
          "durable oversized record did not recover into a table");
}

void LegacyRecord(std::ofstream& file, unsigned char type, std::string_view key,
                  std::string_view value, std::int64_t expiry = 0) {
    std::string bytes(1, static_cast<char>(type));
    const auto append_le = [&](std::uint64_t n, unsigned width) {
        for (unsigned i = 0; i < width; ++i) bytes.push_back(static_cast<char>((n >> (i * 8)) & 255u));
    };
    append_le(key.size(), 4);
    append_le(value.size(), 4);
    bytes.append(key);
    bytes.append(value);
    if (type == 3) append_le(static_cast<std::uint64_t>(expiry), 8);
    append_le(TestCrc(bytes), 4);
    file.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

void TestMigration() {
    const auto source = Db("legacy");
    const auto wal = source + ".wal";
    const std::string key("\0\xff", 2);
    const std::string value("v\0\x80", 3);
    const auto now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    {
        std::ofstream file(wal, std::ios::binary);
        LegacyRecord(file, 1, key, "older");
        LegacyRecord(file, 1, key, value);
        LegacyRecord(file, 1, "deleted", "visible");
        LegacyRecord(file, 2, "deleted", "");
        LegacyRecord(file, 1, "expired", "visible");
        LegacyRecord(file, 3, "expired", "expired", now - 5);
        LegacyRecord(file, 3, "future", "alive", now + 3600);
        LegacyRecord(file, 1, "", "");
        file.write("\1\2\3", 3); // Source's recoverable but unmodified tail.
    }
    const auto original = Bytes(wal);
    const auto verify = [&](const std::string& destination) {
        kvstore::KVStore store(destination);
        Check(store.Get(key) == value && store.Get("") == "" && store.Get("future") == "alive",
              "migrated live data mismatch");
        Check(!store.Get("deleted") && !store.Get("expired") && store.Size() == 3,
              "migrated deletion or absolute expiry mismatch");
        Check(store.Set(key, "routed"), "migrated destination is not writable");
        Check(fs::file_size(store.WalPaths().at(2)) != 0, "binary key routed to the wrong shard");
    };
    Throws([&] { kvstore::KVStore wrong_layout(source); }, "legacy WAL was silently bypassed");
    const auto locked = Db("locked-destination");
    const int fd = ::open((source + ".lock").c_str(), O_RDWR | O_CLOEXEC);
    Check(fd >= 0 && ::flock(fd, LOCK_EX | LOCK_NB) == 0, "could not lock migration source fixture");
    Throws([&] { kvstore::KVStore::MigrateLegacy(source, locked); }, "migration bypassed source lock");
    Check(::close(fd) == 0 && !fs::exists(locked), "locked source created a destination");

    const auto invalid = Db("unvalidated-destination");
    Arm(Fault::MigrationValidation);
    Throws([&] { kvstore::KVStore::MigrateLegacy(source, invalid, 8192); }, "validation fault ignored");
    Check(!fs::exists(invalid) && Bytes(wal) == original, "validation failure published or changed source");

    const auto corrupt = Db("corrupt-source");
    {
        auto bytes = original.substr(0, original.size() - 3);
        bytes.back() ^= 1;
        std::ofstream file(corrupt + ".wal", std::ios::binary);
        file << bytes;
    }
    Throws([&] { kvstore::KVStore::MigrateLegacy(corrupt, invalid); }, "legacy complete bad CRC accepted");
    Check(!fs::exists(invalid), "invalid source published a destination");

    for (auto crash : {Fault::CrashWrite, Fault::CrashMigrationBeforePublish, Fault::CrashMigrationAfterPublish}) {
        const auto destination = Db("migration-" + std::to_string(static_cast<int>(crash)));
        const pid_t child = ::fork();
        Check(child >= 0, "migration fork failed");
        if (child == 0) {
            Arm(crash);
            kvstore::KVStore::MigrateLegacy(source, destination, 8192);
            ::_exit(3);
        }
        int status = 0;
        Check(::waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 77,
              "migration crash point not reached");
        Check(Bytes(wal) == original, "migration changed source WAL");
        if (crash != Fault::CrashMigrationAfterPublish) {
            Check(!fs::exists(destination), "interrupted staging published a destination");
            kvstore::KVStore::MigrateLegacy(source, destination, 8192);
        }
        verify(destination);
        Throws([&] { kvstore::KVStore::MigrateLegacy(source, destination); }, "migration replaced an existing store");
    }
}

void TestCrashBoundaries() {
    for (auto f : {Fault::CrashWrite, Fault::CrashTableWrite, Fault::CrashTempSync, Fault::CrashRename,
                   Fault::CrashDirectorySync, Fault::CrashBeforeRetire, Fault::CrashAfterRetire}) {
        const auto path = Db("crash-" + std::to_string(static_cast<int>(f)));
        Seed(path);
        const pid_t child = ::fork();
        Check(child >= 0, "fork failed");
        if (child == 0) {
            kvstore::KVStore store(path);
            if (!store.Set("acknowledged", "value")) ::_exit(2);
            Arm(f);
            if (f == Fault::CrashWrite) (void)store.Set("inflight", "value");
            else (void)store.Compact();
            ::_exit(3);
        }
        int status = 0;
        Check(::waitpid(child, &status, 0) == child, "waitpid failed");
        Check(WIFEXITED(status) && WEXITSTATUS(status) == 77, "crash point not reached");
        kvstore::KVStore store(path);
        Check(store.Get("base") == "original", "crash lost base");
        Check(store.Get("acknowledged") == "value", "crash lost acknowledged write");
        Check(store.Set("after", "value"), "post-crash append failed");
    }
}
}
int main() {
    char directory[] = "/tmp/kvstore-failures-XXXXXX";
    if (!::mkdtemp(directory)) return 1;
    root = directory;
    try {
        TestRollback();
        TestRecoveryFailures();
        TestCorruptionAndTornRecords();
        TestFlushFailures();
        TestCreateFailuresAndMemoryFailure();
        TestEdges();
        TestPaths();
        TestSymlinkLock();
        TestModel();
        TestConcurrentCompaction();
        TestCrashBoundaries();
        TestRetirementAndCommittedCorruption();
        TestCorruptVisibilityRecord();
        TestSnapshot();
        TestMergeCrashBoundaries();
        TestCheckpointAndRestore();
        TestConsistentHashing();
        TestBoundedFlush();
        TestMigration();
        fs::remove_all(root);
        std::cout << "All failure, edge, concurrency, and crash-boundary tests passed\n";
    } catch (const std::exception& e) {
        Disarm();
        std::cerr << e.what() << " (artifacts: " << root << ")\n";
        return 1;
    }
}
