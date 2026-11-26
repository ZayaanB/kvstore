#include "mapreduce.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <memory>
#include <queue>
#include <string>
#include <system_error>
#include <vector>

namespace kvstore::mapreduce {
namespace {
namespace fs = std::filesystem;

[[noreturn]] void IoError(const char* operation) {
    throw std::system_error(errno, std::generic_category(), operation);
}

void CheckStop(std::stop_token stop) {
    if (stop.stop_requested()) throw std::runtime_error("MapReduce job cancelled");
}

// A job stays failed even if application code catches an exception from a sink
// or the values iterator, whose underlying I/O may have advanced partway.
class CallbackErrors {
public:
    template<class Action>
    decltype(auto) Call(Action action) {
        Rethrow();
        try {
            return action();
        } catch (...) {
            error_ = std::current_exception();
            throw;
        }
    }

    void Rethrow() const {
        if (error_) std::rethrow_exception(error_);
    }

private:
    std::exception_ptr error_;
};

int Sync(int fd) {
    int result;
    do {
        result = ::fsync(fd);
    } while (result == -1 && errno == EINTR);
    return result;
}

class File {
public:
    explicit File(const fs::path& path, const char* mode) : file_(std::fopen(path.c_str(), mode)) {
        if (!file_) IoError("open shuffle run");
    }

    explicit File(int fd) : file_(::fdopen(fd, "wb")) {
        if (!file_) {
            const int error = errno;
            ::close(fd);
            throw std::system_error(error, std::generic_category(), "open staged result");
        }
    }

    void WriteBytes(const void* data, std::size_t size) {
        if (size != 0 && std::fwrite(data, 1, size, file_.get()) != size) {
            IoError("write MapReduce temporary file");
        }
    }

    // False means clean EOF before reading any part of the requested field.
    bool ReadBytes(void* data, std::size_t size) {
        if (size == 0) return true;
        const auto count = std::fread(data, 1, size, file_.get());
        if (count == size) return true;
        if (std::ferror(file_.get())) IoError("read shuffle run");
        if (count == 0) return false;
        throw std::runtime_error("truncated shuffle run");
    }

    void FlushAndSync() {
        if (std::fflush(file_.get()) != 0 || Sync(::fileno(file_.get())) != 0) {
            IoError("sync staged result");
        }
    }

    void Close() {
        if (std::fclose(file_.release()) != 0) IoError("close MapReduce temporary file");
    }

private:
    struct CloseFile {
        void operator()(std::FILE* file) const { std::fclose(file); }
    };
    std::unique_ptr<std::FILE, CloseFile> file_;
};

class Workspace {
public:
    explicit Workspace(const fs::path& parent)
        : path_((parent / ".kvstore-mapreduce-XXXXXX").string()) {
        if (!::mkdtemp(path_.data())) IoError("create MapReduce work directory");
    }

    ~Workspace() {
        std::error_code ignored;
        fs::remove_all(path_, ignored);
    }

    fs::path RunPath(std::size_t partition, std::size_t pass, std::size_t index) const {
        return fs::path(path_) / (std::to_string(partition) + "-" + std::to_string(pass) +
                                 "-" + std::to_string(index));
    }

private:
    std::string path_;
};

class Stage {
public:
    explicit Stage(const fs::path& parent)
        : path_((parent / ".kvstore-mapreduce-output-XXXXXX").string()) {}

    ~Stage() {
        if (exists_) ::unlink(path_.c_str());
    }

    int Create() {
        const int fd = ::mkstemp(path_.data());
        if (fd < 0) IoError("create staged result");
        exists_ = true;
        return fd;
    }

    void Publish(const fs::path& parent, const fs::path& destination, std::stop_token stop) {
        struct Directory {
            int fd;
            ~Directory() { if (fd >= 0) ::close(fd); }
        } directory{::open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC)};
        if (directory.fd < 0) IoError("open result directory");
        CheckStop(stop);
        if (::rename(path_.c_str(), destination.c_str()) != 0) IoError("rename staged result");
        exists_ = false;
        if (Sync(directory.fd) != 0) {
            throw PublicationUncertain("result renamed, but directory sync failed; check the destination before retrying");
        }
    }

private:
    std::string path_;
    bool exists_ = false;
};

class DiskBudget {
public:
    explicit DiskBudget(std::uint64_t limit) : limit_(limit) {}

    void Write(File& file, const void* data, std::size_t size) {
        if (size > limit_ - used_) throw std::runtime_error("MapReduce temporary disk budget exceeded");
        used_ += size;
        peak_ = std::max(peak_, used_);
        file.WriteBytes(data, size);
    }

    void RemoveRun(const fs::path& path) {
        const auto size = fs::file_size(path);
        if (!fs::remove(path)) throw std::runtime_error("shuffle run disappeared");
        used_ -= size;
    }

    std::uint64_t Peak() const { return peak_; }

private:
    std::uint64_t limit_;
    std::uint64_t used_ = 0;
    std::uint64_t peak_ = 0;
};

void WriteRecord(File& file, DiskBudget& disk, std::string_view key, std::string_view value) {
    // Temporary runs are private to one execution and use native byte order.
    const std::array<std::uint64_t, 2> lengths{key.size(), value.size()};
    disk.Write(file, lengths.data(), sizeof(lengths));
    disk.Write(file, key.data(), key.size());
    disk.Write(file, value.data(), value.size());
}

class RunReader {
public:
    RunReader(const fs::path& path, std::size_t record_limit)
        : file_(path, "rb"), record_limit_(record_limit) {}

    bool Advance() {
        std::array<std::uint64_t, 2> lengths{};
        if (!file_.ReadBytes(lengths.data(), sizeof(lengths))) return false;
        if (lengths[0] > record_limit_ || lengths[1] > record_limit_ - lengths[0]) {
            throw std::runtime_error("invalid shuffle record length");
        }
        key.resize(static_cast<std::size_t>(lengths[0]));
        value.resize(static_cast<std::size_t>(lengths[1]));
        if (!file_.ReadBytes(key.data(), key.size()) || !file_.ReadBytes(value.data(), value.size())) {
            throw std::runtime_error("truncated shuffle record");
        }
        return true;
    }

    std::string key;
    std::string value;

private:
    File file_;
    std::size_t record_limit_;
};

std::size_t Partition(std::string_view key, std::size_t count) {
    // FNV-1a over the emitted key's bytes.
    std::uint64_t hash = 14695981039346656037ull;
    for (const char byte : key) {
        hash ^= static_cast<unsigned char>(byte);
        hash *= 1099511628211ull;
    }
    return static_cast<std::size_t>(hash % count);
}

struct BufferedRecord {
    std::size_t partition;
    std::string_view key;
    std::string_view value;
};

class ShuffleBuffer {
public:
    ShuffleBuffer(std::size_t bytes, const Workspace& work, DiskBudget& disk,
                  std::vector<std::size_t>& counts, std::stop_token stop)
        : slots_(std::max(std::size_t{1}, bytes / sizeof(BufferedRecord) / 4)),
          capacity_(bytes - slots_ * sizeof(BufferedRecord)),
          record_limit_(bytes - sizeof(BufferedRecord)),
          records_(std::make_unique<BufferedRecord[]>(slots_)),
          payload_(std::make_unique<char[]>(capacity_)), work_(work), disk_(disk),
          counts_(counts), stop_(stop) {}

    void Add(std::string_view key, std::string_view value) {
        CheckStop(stop_);
        if (key.size() > record_limit_ || value.size() > record_limit_ - key.size()) {
            throw std::runtime_error("emitted record exceeds MapReduce shuffle limit");
        }
        const auto partition = Partition(key, counts_.size());
        const auto size = key.size() + value.size();
        if (count_ == slots_ || size > capacity_ - used_) Spill();
        if (size > capacity_) {
            // A record that fits the total budget can outgrow the buffer's payload region.
            File file(work_.RunPath(partition, 0, counts_[partition]), "wb");
            WriteRecord(file, disk_, key, value);
            file.Close();
            ++counts_[partition];
            return;
        }
        const auto stored_key = Copy(key);
        records_[count_++] = {partition, stored_key, Copy(value)};
    }

    void Spill() {
        std::sort(records_.get(), records_.get() + count_, [](const auto& a, const auto& b) {
            if (a.partition != b.partition) return a.partition < b.partition;
            return a.key < b.key;
        });
        std::size_t next = 0;
        while (next < count_) {
            CheckStop(stop_);
            const auto partition = records_[next].partition;
            File file(work_.RunPath(partition, 0, counts_[partition]), "wb");
            do {
                CheckStop(stop_);
                const auto& record = records_[next++];
                WriteRecord(file, disk_, record.key, record.value);
            } while (next < count_ && records_[next].partition == partition);
            file.Close();
            ++counts_[partition];
        }
        count_ = 0;
        used_ = 0;
    }

private:
    std::string_view Copy(std::string_view bytes) {
        char* start = payload_.get() + used_;
        if (!bytes.empty()) std::memcpy(start, bytes.data(), bytes.size());
        used_ += bytes.size();
        return {start, bytes.size()};
    }

    std::size_t slots_;
    std::size_t capacity_;
    std::size_t record_limit_;
    std::unique_ptr<BufferedRecord[]> records_;
    std::unique_ptr<char[]> payload_;
    const Workspace& work_;
    DiskBudget& disk_;
    std::vector<std::size_t>& counts_;
    std::stop_token stop_;
    std::size_t count_ = 0;
    std::size_t used_ = 0;
};

void MergeRuns(const Workspace& work, DiskBudget& disk, std::size_t partition,
               std::size_t pass, std::size_t first, std::size_t count,
               std::size_t output, std::size_t record_limit, std::stop_token stop) {
    const auto target = work.RunPath(partition, pass + 1, output);
    if (count == 1) {
        fs::rename(work.RunPath(partition, pass, first), target);
        return;
    }

    {
        std::vector<RunReader> readers;
        readers.reserve(count);
        auto later = [&readers](std::size_t left, std::size_t right) {
            return readers[left].key > readers[right].key;
        };
        std::priority_queue<std::size_t, std::vector<std::size_t>, decltype(later)> heap(later);
        for (std::size_t i = 0; i < count; ++i) {
            CheckStop(stop);
            readers.emplace_back(work.RunPath(partition, pass, first + i), record_limit);
            if (readers.back().Advance()) heap.push(i);
        }
        File file(target, "wb");
        while (!heap.empty()) {
            CheckStop(stop);
            const auto index = heap.top();
            heap.pop();
            auto& reader = readers[index];
            WriteRecord(file, disk, reader.key, reader.value);
            if (reader.Advance()) heap.push(index);
        }
        file.Close();
    }
    for (std::size_t i = 0; i < count; ++i) {
        disk.RemoveRun(work.RunPath(partition, pass, first + i));
    }
}

class GroupValues final : public Values {
public:
    GroupValues(RunReader& reader, bool& available, std::string_view key, std::stop_token stop,
                CallbackErrors& errors)
        : reader_(reader), available_(available), key_(key), stop_(stop), errors_(errors) {}

    bool Next(std::string_view& value) override {
        return errors_.Call([&] {
            CheckStop(stop_);
            if (consumed_) {
                available_ = reader_.Advance();
                consumed_ = false;
            }
            if (!available_ || reader_.key != key_) return false;
            value = reader_.value;
            consumed_ = true;
            return true;
        });
    }

private:
    RunReader& reader_;
    bool& available_;
    std::string_view key_;
    std::stop_token stop_;
    CallbackErrors& errors_;
    bool consumed_ = false;
};
}

Stats Run(SnapshotCursor& snapshot, const Mapper& mapper, const Reducer& reducer,
          const fs::path& destination, const Options& options, std::stop_token stop) {
    if (!destination.has_filename() || options.shuffle_bytes < sizeof(BufferedRecord) ||
        options.partitions == 0 || options.merge_fan_in < 2) {
        throw std::invalid_argument("invalid MapReduce destination or limits");
    }
    CheckStop(stop);
    const auto output = fs::absolute(destination);
    const auto parent = output.parent_path();
    const Workspace work(parent);
    DiskBudget disk(options.temporary_bytes);
    CallbackErrors errors;
    std::vector<std::size_t> runs(options.partitions);
    {
        ShuffleBuffer shuffle(options.shuffle_bytes, work, disk, runs, stop);
        const Emit emit = [&](std::string_view key, std::string_view value) {
            errors.Call([&] { shuffle.Add(key, value); });
        };
        std::string_view key;
        std::string_view value;
        while (true) {
            CheckStop(stop);
            if (!snapshot.Next(key, value)) break;
            mapper(key, value, emit);
            errors.Rethrow();
        }
        shuffle.Spill();
    }

    Stage stage(parent);
    File result(stage.Create());
    const Write write = [&](std::string_view bytes) {
        errors.Call([&] {
            CheckStop(stop);
            disk.Write(result, bytes.data(), bytes.size());
        });
    };
    Stats stats;
    const auto record_limit = options.shuffle_bytes - sizeof(BufferedRecord);
    for (std::size_t partition = 0; partition < runs.size(); ++partition) {
        CheckStop(stop);
        auto count = runs[partition];
        stats.spill_runs += count;
        if (count == 0) continue;
        std::size_t pass = 0;
        // Run names are indexed so the job holds no per-run list in memory.
        while (count > 1) {
            std::size_t outputs = 0;
            for (std::size_t first = 0; first < count;) {
                CheckStop(stop);
                const auto batch = std::min(options.merge_fan_in, count - first);
                MergeRuns(work, disk, partition, pass, first, batch, outputs++, record_limit, stop);
                first += batch;
            }
            count = outputs;
            ++pass;
        }
        stats.merge_passes = std::max(stats.merge_passes, pass);

        const auto path = work.RunPath(partition, pass, 0);
        {
            RunReader reader(path, record_limit);
            bool available = reader.Advance();
            while (available) {
                const auto key = reader.key;
                GroupValues values(reader, available, key, stop, errors);
                reducer(key, values, write);
                errors.Rethrow();
                std::string_view ignored;
                while (values.Next(ignored)) {}
            }
        }
        disk.RemoveRun(path);
    }
    CheckStop(stop);
    result.FlushAndSync();
    result.Close();
    stage.Publish(parent, output, stop);
    stats.peak_temporary_bytes = disk.Peak();
    return stats;
}

}
