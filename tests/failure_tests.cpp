#include "kvstore.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdarg>
#include <cstdlib>
#include <filesystem>
#include <fstream>
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
    CrashWrite, CrashTempSync, CrashRename, CrashDirectorySync
};
std::atomic<Fault> fault{Fault::None};
std::atomic<int> phase{0};
std::atomic<int> wal_fd{-1};
std::atomic<int> temp_fd{-1};
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
void Tear(const std::string& path) {
    std::ofstream out(path + ".wal", std::ios::binary | std::ios::app);
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
    if ((fault == Fault::RecoveryOpen && wal && (flags & O_APPEND) == 0) ||
        (fault == Fault::DirectoryOpen && (flags & O_DIRECTORY) != 0)) {
        errno = EACCES;
        return -1;
    }
    const int fd = __real_open(path, flags, mode);
    if (fd >= 0 && wal) wal_fd = fd;
    if (fd >= 0 && name.ends_with(".wal.tmp")) {
        temp_fd = fd;
        if (fault == Fault::AllocationInCompact) fail_allocation = true;
    }
    if (fd >= 0 && wal && (flags & O_APPEND) != 0 &&
        fault == Fault::CreateDirectorySync) phase = 1;
    return fd;
}
extern "C" ssize_t __wrap_write(int fd, const void* data, size_t len) {
    const auto f = fault.load();
    if (fd == wal_fd) {
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
    if (fd == temp_fd && f == Fault::TempWrite) { errno = ENOSPC; return -1; }
    return __real_write(fd, data, len);
}
extern "C" ssize_t __wrap_read(int fd, void* data, size_t len) {
    if (fd == wal_fd && fault == Fault::ReadError) { errno = EIO; return -1; }
    if (fd == wal_fd && fault == Fault::ReadInterrupted && phase++ == 0) {
        errno = EINTR; return -1;
    }
    return __real_read(fd, data, len);
}
extern "C" int __wrap_ftruncate(int fd, off_t len) {
    if (fd == wal_fd && (fault == Fault::RollbackTruncate || fault == Fault::RecoveryTruncate)) {
        errno = EIO; return -1;
    }
    return __real_ftruncate(fd, len);
}
extern "C" int __wrap_fdatasync(int fd) {
    const auto f = fault.load();
    if (fd == wal_fd && (f == Fault::RollbackSync || f == Fault::CreateSync ||
                       (f == Fault::SyncOnce && phase++ == 0))) {
        errno = EIO; return -1;
    }
    if (fd == temp_fd && f == Fault::TempSync) { errno = EIO; return -1; }
    const int result = __real_fdatasync(fd);
    if (result == 0 && fd == wal_fd && f == Fault::AllocationAfterSync) fail_allocation = true;
    if (result == 0 && fd == temp_fd && f == Fault::CrashTempSync) ::_exit(77);
    return result;
}
extern "C" int __wrap_fsync(int fd) {
    struct stat info {};
    const bool directory = ::fstat(fd, &info) == 0 && S_ISDIR(info.st_mode);
    const auto f = fault.load();
    if ((fd == wal_fd && f == Fault::RecoverySync) ||
        (directory && (f == Fault::DirectorySync || (f == Fault::CreateDirectorySync && phase == 1)))) {
        errno = EIO; return -1;
    }
    const int result = __real_fsync(fd);
    if (result == 0 && directory && f == Fault::CrashDirectorySync) ::_exit(77);
    return result;
}
extern "C" int __wrap_rename(const char* from, const char* to) {
    if (fault == Fault::Rename) { errno = EIO; return -1; }
    const int result = __real_rename(from, to);
    if (result == 0 && std::string_view(from).ends_with(".wal.tmp") &&
        std::string_view(to).ends_with(".wal")) wal_fd = temp_fd.load();
    if (result == 0 && fault == Fault::CrashRename) ::_exit(77);
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
                kvstore::KVStore store(path);
                Check(store.Set("base", "original"), "seed failed");
                if (compact_first) Check(store.Compact(), "pre-failure compaction failed");
                Arm(f);
                Check(!store.Set("broken", "value"), "injected write unexpectedly succeeded");
                Disarm();
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
        Seed(path); Tear(path);
        const auto before = Bytes(path + ".wal");
        Arm(f);
        Throws([&] { kvstore::KVStore store(path); }, "recovery ignored I/O failure");
        if (f != Fault::RecoverySync) Check(Bytes(path + ".wal") == before, "failed recovery changed WAL");
        kvstore::KVStore store(path); // also verifies constructor released process lock
        Check(store.Get("base") == "original", "recovery lost seed");
        Check(store.Set("after", "value"), "recovery retry failed");
    }
    const auto path = Db("bad-file");
    fs::create_directory(path + ".wal");
    Throws([&] { kvstore::KVStore store(path); }, "directory accepted as WAL");
    fs::remove(path + ".wal");
    kvstore::KVStore retry(path);
    Check(retry.Set("key", "value"), "constructor leaked lock");
}
void TestCorruptionAndTornRecords() {
    const auto path = Db("middle-corruption");
    { kvstore::KVStore store(path); Check(store.Set("first", "one"), "write failed");
      Check(store.Set("second", "two"), "write failed"); }
    auto bytes = Bytes(path + ".wal");
    bytes[9] ^= 1;
    { std::ofstream out(path + ".wal", std::ios::binary | std::ios::trunc); out.write(bytes.data(), static_cast<std::streamsize>(bytes.size())); }
    Throws([&] { kvstore::KVStore store(path); }, "middle corruption silently truncated");
    Check(Bytes(path + ".wal") == bytes, "middle corruption was destructive");
    bytes[0] = 99;
    { std::ofstream out(path + ".wal", std::ios::binary | std::ios::trunc); out.write(bytes.data(), static_cast<std::streamsize>(bytes.size())); }
    Throws([&] { kvstore::KVStore store(path); }, "unknown header accepted");
    Check(Bytes(path + ".wal") == bytes, "unknown header was destructive");

    const auto source = Db("record-source");
    { kvstore::KVStore store(source); Check(store.SetWithTtl("ttl", "payload", 3600), "TTL write failed"); }
    const auto record = Bytes(source + ".wal");
    // Exercise every truncation boundary: header, payload, expiry, and checksum.
    for (std::size_t n = 1; n < record.size(); ++n) {
        const auto torn = Db("torn-" + std::to_string(n));
        Seed(torn);
        const auto prefix = fs::file_size(torn + ".wal");
        { std::ofstream out(torn + ".wal", std::ios::binary | std::ios::app);
          out.write(record.data(), static_cast<std::streamsize>(n)); }
        kvstore::KVStore store(torn);
        Check(store.Get("base") == "original", "torn record lost valid prefix");
        Check(!store.Get("ttl"), "torn record replayed");
        Check(fs::file_size(torn + ".wal") == prefix, "torn tail not repaired");
    }
}
void TestCompactionFailures() {
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
            Check(!fs::exists(path + ".wal.tmp"), "failed compaction leaked temp file");
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
        const auto wal = store.WalPath();
        Check(fs::path(wal).is_absolute(), "WAL path did not bind to absolute location");
        Check(store.Set("before", "value"), "initial relative write failed");
        fs::current_path(Db("cwd-second"));
        Check(store.Compact(), "compaction after chdir failed");
        Check(store.Set("after", "value"), "post-chdir write failed");
        Check(store.WalPath() == wal, "WAL location changed");
        Check(!fs::exists("relative.wal"), "compaction followed changed cwd");
    }
    kvstore::KVStore reopened(Db("cwd-first/relative"));
    Check(reopened.Get("before") == "value" && reopened.Get("after") == "value",
          "chdir redirected acknowledged writes away from original database");
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
void TestCrashBoundaries() {
    for (auto f : {Fault::CrashWrite, Fault::CrashTempSync, Fault::CrashRename, Fault::CrashDirectorySync}) {
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
        TestCompactionFailures();
        TestCreateFailuresAndMemoryFailure();
        TestEdges();
        TestPaths();
        TestModel();
        TestConcurrentCompaction();
        TestCrashBoundaries();
        fs::remove_all(root);
        std::cout << "All failure, edge, concurrency, and crash-boundary tests passed\n";
    } catch (const std::exception& e) {
        Disarm();
        std::cerr << e.what() << " (artifacts: " << root << ")\n";
        return 1;
    }
}
