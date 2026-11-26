#include "word_count.hpp"

#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

namespace {
namespace fs = std::filesystem;
namespace mr = kvstore::mapreduce;
using Counts = std::map<std::string, std::uint64_t>;
using Shards = std::vector<std::vector<std::string>>;

void Check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

class TempDirectory {
public:
    TempDirectory() : path_((fs::temp_directory_path() / "kvstore-mapreduce-test-XXXXXX").string()) {
        Check(::mkdtemp(path_.data()) != nullptr, "could not create test directory");
    }
    ~TempDirectory() {
        std::error_code ignored;
        fs::remove_all(path_, ignored);
    }

    fs::path Path() const { return path_; }

private:
    std::string path_;
};

class FixtureCursor final : public mr::SnapshotCursor {
public:
    explicit FixtureCursor(const Shards& shards) : shards_(shards) {}

    bool Next(std::string_view& key, std::string_view& value) override {
        if (fail_after == visited) throw std::runtime_error("fixture shard read failed");
        while (shard_ < shards_.size() && row_ == shards_[shard_].size()) {
            ++shard_;
            row_ = 0;
        }
        if (shard_ == shards_.size()) return false;

        key_ = "shard-" + std::to_string(shard_) + "-row-" + std::to_string(row_);
        key = key_;
        value = shards_[shard_][row_++];
        ++visited;
        return true;
    }

    std::size_t visited = 0;
    std::size_t fail_after = std::string::npos;

private:
    const Shards& shards_;
    std::size_t shard_ = 0;
    std::size_t row_ = 0;
    std::string key_;
};

std::string Read(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    Check(input.is_open(), "result file missing");
    std::string bytes(static_cast<std::size_t>(fs::file_size(path)), '\0');
    input.read(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    Check(static_cast<bool>(input), "could not read result");
    return bytes;
}

void CheckClean(const fs::path& result) {
    for (const auto& entry : fs::directory_iterator(result.parent_path())) {
        Check(entry.path() == result, "MapReduce temporary files leaked");
    }
}

void Seed(const fs::path& result) {
    std::ofstream output(result, std::ios::binary);
    output << "previous result\n";
    output.close();
    Check(static_cast<bool>(output), "could not seed previous result");
}

template<class Error = std::runtime_error, class Action>
void CheckFailure(const fs::path& result, Action action, std::string_view message) {
    Seed(result);
    bool caught = false;
    try {
        action();
    } catch (const Error& error) {
        caught = std::string_view(error.what()).find(message) != std::string_view::npos;
    }
    Check(caught, "expected MapReduce failure did not occur");
    Check(Read(result) == "previous result\n", "failure changed the published result");
    CheckClean(result);
}

Counts SerialReference(const Shards& shards) {
    const std::regex word("[A-Za-z0-9]+");
    Counts counts;
    for (const auto& shard : shards) {
        for (const auto& document : shard) {
            const std::sregex_iterator end;
            for (auto match = std::sregex_iterator(document.begin(), document.end(), word);
                 match != end; ++match) {
                auto token = match->str();
                std::transform(token.begin(), token.end(), token.begin(), [](char c) {
                    return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
                });
                ++counts[token];
            }
        }
    }
    return counts;
}

Counts ParseResult(const std::string& bytes) {
    std::istringstream input(bytes);
    Counts counts;
    std::string line;
    while (std::getline(input, line)) {
        const auto tab = line.find('\t');
        Check(tab != std::string::npos, "missing word count separator");
        auto [entry, inserted] = counts.emplace(line.substr(0, tab), std::stoull(line.substr(tab + 1)));
        Check(inserted, "word was reduced more than once");
    }
    return counts;
}

void RollingMerge(const fs::path& result) {
    Shards shards(4);
    for (std::size_t shard : {0u, 2u, 3u}) {
        for (std::size_t row = 0; row < 120; ++row) {
            shards[shard].push_back("hot Zebra HOT hot hot Hot doc" + std::to_string(row % 8));
        }
    }
    shards[2].emplace_back("a\xff" "B\0" "c", 5);
    shards[3].emplace_back(".,?!");

    const mr::Options tiny{512, 1u << 20, 3, 2};
    FixtureCursor fixture(shards);
    std::size_t mapped = 0;
    const auto stats = mr::Run(fixture, [&](auto key, auto value, const mr::Emit& emit) {
        ++mapped;
        mr::MapWords(key, value, emit);
    }, mr::ReduceWordCount, result, tiny);
    Check(fixture.visited == 362 && fixture.visited == mapped, "mapper did not visit each row once");
    Check(stats.spill_runs > 8 && stats.merge_passes >= 3, "fixture did not exercise multi-pass merging");
    Check(stats.peak_temporary_bytes <= tiny.temporary_bytes, "reported disk usage exceeded limit");
    const auto bytes = Read(result);
    const auto counts = ParseResult(bytes);
    Check(counts == SerialReference(shards), "word count differs from serial reference");
    Check(counts.at("hot") == 1800 && counts.at("hot") > tiny.shuffle_bytes,
          "hot group did not exceed the shuffle budget");
    CheckClean(result);

    FixtureCursor retry(shards);
    mr::Run(retry, mr::MapWords, mr::ReduceWordCount, result, mr::Options{8192, 1u << 20, 3, 4});
    Check(Read(result) == bytes, "result order changed with spill sizing and fan-in");
    CheckClean(result);
}

void EmptyInputAndEmissions(const fs::path& result) {
    for (const Shards& shards : {Shards{}, Shards{{"", " .\xff\t"}}}) {
        Seed(result);
        FixtureCursor fixture(shards);
        const auto stats = mr::Run(fixture, mr::MapWords, mr::ReduceWordCount, result);
        Check(Read(result).empty() && stats.spill_runs == 0 && stats.peak_temporary_bytes == 0,
              "empty job did not publish an empty result");
        CheckClean(result);
    }
}

void BytewiseGrouping(const fs::path& result) {
    const Shards shards{{"long"}, {"short"}};
    FixtureCursor fixture(shards);
    const std::string large(450, 'x');
    const std::string binary("a\0b", 3);
    std::vector<std::string> order;
    mr::Run(fixture, [&](auto, auto value, const mr::Emit& emit) {
        emit("", "");
        emit(binary, value == "long" ? std::string_view(large) : std::string_view("x"));
        emit("a", value);
    }, [&](auto key, mr::Values& values, const mr::Write&) {
        order.emplace_back(key);
        std::string_view value;
        Check(values.Next(value), "empty group");
        if (key.empty()) {
            Check(value.empty() && values.Next(value) && value.empty(), "empty record was lost");
            Check(!values.Next(value) && !values.Next(value), "group crossed a key boundary");
        } else if (key == binary) {
            const auto first = value.size();
            Check(values.Next(value) && value.size() + first == 451, "large or binary record was lost");
        }
        // The runner drains any unread values before the next reducer.
    }, result, mr::Options{512, 4096, 1, 2});
    Check(order == std::vector<std::string>{"", "a", binary}, "wrong bytewise group order");
    CheckClean(result);
}

void PreservesPublishedResult(const fs::path& result) {
    const Shards shards{{"one"}, {"two"}};
    const mr::Options tiny{128, 4096, 1, 2};

    CheckFailure(result, [&] {
        FixtureCursor fixture(shards);
        fixture.fail_after = 1;
        mr::Run(fixture, mr::MapWords, mr::ReduceWordCount, result, tiny);
    }, "fixture shard read failed");
    CheckFailure(result, [&] {
        FixtureCursor fixture(shards);
        mr::Run(fixture, [](auto, auto, const mr::Emit& emit) {
            emit("a", "1");
            emit("b", "1");
            throw std::runtime_error("mapper failed");
        }, mr::ReduceWordCount, result, tiny);
    }, "mapper failed");
    CheckFailure(result, [&] {
        FixtureCursor fixture(shards);
        mr::Run(fixture, mr::MapWords, [&](auto, mr::Values&, const mr::Write& write) {
            write("staged content");
            Check(Read(result) == "previous result\n", "result was published during reduction");
            throw std::runtime_error("reducer failed");
        }, result, tiny);
    }, "reducer failed");
    CheckFailure(result, [&] {
        FixtureCursor fixture(shards);
        std::stop_source stop;
        mr::Run(fixture, mr::MapWords, [&](auto, mr::Values&, const mr::Write& write) {
            write("staged content");
            stop.request_stop();
        }, result, tiny, stop.get_token());
    }, "cancelled");
    CheckFailure(result, [&] {
        FixtureCursor fixture(shards);
        const mr::Options limit{128, 15, 1, 2};
        mr::Run(fixture, mr::MapWords, mr::ReduceWordCount, result, limit);
    }, "temporary disk budget");
    CheckFailure(result, [&] {
        // Each record takes 20 bytes. Both spills fit; a concurrent merge output does not.
        FixtureCursor fixture(shards);
        const mr::Options limit{128, 70, 1, 2};
        mr::Run(fixture, mr::MapWords, mr::ReduceWordCount, result, limit);
    }, "temporary disk budget");
    CheckFailure(result, [&] {
        FixtureCursor fixture(shards);
        const mr::Options limit{128, 80, 1, 2};
        mr::Run(fixture, mr::MapWords, [](auto, mr::Values&, const mr::Write& write) {
            try {
                write(std::string(41, 'x'));
            } catch (const std::runtime_error&) {
                // A failed output write must abort even if the reducer catches it.
            }
        }, result, limit);
    }, "temporary disk budget");
    CheckFailure(result, [&] {
        FixtureCursor fixture(shards);
        mr::Run(fixture, [](auto, auto, const mr::Emit& emit) {
            emit(std::string(128, 'x'), "1");
        }, mr::ReduceWordCount, result, tiny);
    }, "record exceeds");
    CheckFailure<std::overflow_error>(result, [&] {
        FixtureCursor fixture(shards);
        mr::Run(fixture, [](auto, auto value, const mr::Emit& emit) {
            emit("x", value == "one" ? "18446744073709551615" : "1");
        }, mr::ReduceWordCount, result, tiny);
    }, "word count exceeds uint64_t");
}

#ifdef MAPREDUCE_WRAP_FSYNC
enum class SyncFailure { None, File, Directory };
SyncFailure fail_sync = SyncFailure::None;

void SyncFailureChecks(const fs::path& result) {
    const Shards shards{{"word"}};
    CheckFailure<std::system_error>(result, [&] {
        FixtureCursor fixture(shards);
        fail_sync = SyncFailure::File;
        mr::Run(fixture, mr::MapWords, mr::ReduceWordCount, result);
    }, "sync staged result");

    FixtureCursor fixture(shards);
    fail_sync = SyncFailure::Directory;
    bool uncertain = false;
    try {
        mr::Run(fixture, mr::MapWords, mr::ReduceWordCount, result);
    } catch (const mr::PublicationUncertain&) {
        uncertain = true;
    }
    Check(uncertain && Read(result) == "word\t1\n", "directory sync failure after rename was ambiguous");
    CheckClean(result);
}
#endif
}

#ifdef MAPREDUCE_WRAP_FSYNC
extern "C" int __real_fsync(int fd);
extern "C" int __wrap_fsync(int fd) {
    if (fail_sync != SyncFailure::None) {
        struct stat info {};
        if (::fstat(fd, &info) == 0 && (S_ISDIR(info.st_mode) == (fail_sync == SyncFailure::Directory))) {
            fail_sync = SyncFailure::None;
            errno = EIO;
            return -1;
        }
    }
    return __real_fsync(fd);
}
#endif

int main() {
    try {
        const TempDirectory root;
        const auto result = root.Path() / "result.tsv";
        RollingMerge(result);
        EmptyInputAndEmissions(result);
        BytewiseGrouping(result);
        PreservesPublishedResult(result);
#ifdef MAPREDUCE_WRAP_FSYNC
        SyncFailureChecks(result);
#endif
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
