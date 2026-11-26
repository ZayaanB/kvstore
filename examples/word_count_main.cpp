#include "word_count.hpp"

#include <fstream>
#include <iostream>
#include <string>

namespace {
namespace mr = kvstore::mapreduce;

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
    if (argc < 2 || (argc == 2 && std::string_view(argv[1]) == "--help")) {
        std::cerr << "Usage: kvstore_word_count OUTPUT [SHARD.tsv ...]\n"
                     "Supply one immutable fixture file per shard with key<TAB>document rows.\n";
        return argc < 2 ? 2 : 0;
    }

    try {
        FixtureCursor snapshot(argc - 2, argv + 2);
        mr::Run(snapshot, mr::MapWords, mr::ReduceWordCount, argv[1]);
    } catch (const mr::PublicationUncertain& error) {
        std::cerr << error.what() << '\n';
        return 3;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
