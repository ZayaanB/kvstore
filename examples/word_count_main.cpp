#include "word_count.hpp"
#include "kvstore.hpp"

#include <fstream>
#include <iostream>
#include <string>

namespace {
namespace mr = kvstore::mapreduce;

class DatabaseCursor final : public mr::SnapshotCursor {
public:
    DatabaseCursor(const std::string& path, const std::filesystem::path& destination)
        : store_(path, {}, std::nullopt, kvstore::OpenMode::Existing) {
        namespace fs = std::filesystem;
        const auto database = fs::canonical(store_.DatabasePath());
        const auto output = fs::absolute(destination);
        const auto parent = fs::canonical(output.parent_path());
        const auto relative = parent.lexically_relative(database);
        if ((!relative.empty() && *relative.begin() != "..") ||
            parent / output.filename() == fs::path(database.string() + ".lock")) {
            throw std::invalid_argument("word count output must be outside the database");
        }
        snapshot_ = store_.OpenSnapshot();
    }

    bool Next(std::string_view& key, std::string_view& value) override {
        if (!snapshot_ || !snapshot_->Next(row_)) {
            snapshot_.reset();
            return false;
        }
        key = row_.key;
        value = row_.value;
        return true;
    }

private:
    kvstore::KVStore store_;
    std::unique_ptr<kvstore::KVStore::Snapshot> snapshot_;
    kvstore::SnapshotRow row_;
};

// Each immutable fixture file represents one shard of the same snapshot.
// Rows are key<TAB>document; only the first tab separates the two fields.
class FixtureCursor final : public mr::SnapshotCursor {
public:
    FixtureCursor(int count, char** paths) : count_(count), paths_(paths) {}

    bool Next(std::string_view& key, std::string_view& value) override {
        while (true) {
            if (!input_.is_open()) {
                if (index_ == count_) return false;
                input_.open(paths_[index_], std::ios::binary);
                if (!input_) Fail("cannot open");
            }
            if (std::getline(input_, row_)) {
                const auto split = row_.find('\t');
                if (split == std::string::npos) Fail("expected key<TAB>document in");
                const std::string_view row(row_);
                key = row.substr(0, split);
                value = row.substr(split + 1);
                return true;
            }
            if (input_.bad() || !input_.eof()) Fail("cannot read");
            input_.close();
            ++index_;
        }
    }

private:
    [[noreturn]] void Fail(const char* message) const {
        throw std::runtime_error(std::string(message) + " fixture shard " + paths_[index_]);
    }

    int count_;
    char** paths_;
    int index_ = 0;
    std::ifstream input_;
    std::string row_;
};
}

int main(int argc, char** argv) {
    const std::string_view mode = argc > 1 ? argv[1] : "";
    if (argc < 2 || mode == "--help" || (mode == "--database" && argc != 4)) {
        std::cerr << "Usage: kvstore_word_count OUTPUT [SHARD.tsv ...]\n"
                     "       kvstore_word_count --database DATABASE OUTPUT\n"
                     "Supply one immutable fixture file per shard with key<TAB>document rows.\n";
        return mode == "--help" ? 0 : 2;
    }

    try {
        if (mode == "--database") {
            DatabaseCursor snapshot(argv[2], argv[3]);
            mr::Run(snapshot, mr::MapWords, mr::ReduceWordCount, argv[3]);
        } else {
            FixtureCursor snapshot(argc - 2, argv + 2);
            mr::Run(snapshot, mr::MapWords, mr::ReduceWordCount, argv[1]);
        }
    } catch (const mr::PublicationUncertain& error) {
        std::cerr << error.what() << '\n';
        return 3;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
