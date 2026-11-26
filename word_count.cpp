#include "word_count.hpp"

#include <charconv>
#include <limits>
#include <string>

namespace kvstore::mapreduce {
namespace {
bool IsWordByte(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
}
}

void MapWords(std::string_view, std::string_view value, const Emit& emit) {
    std::string word;
    for (const char c : value) {
        if (IsWordByte(c)) {
            word.push_back(c >= 'A' && c <= 'Z' ? static_cast<char>(c - 'A' + 'a') : c);
        } else if (!word.empty()) {
            emit(word, "1");
            word.clear();
        }
    }
    if (!word.empty()) emit(word, "1");
}

void ReduceWordCount(std::string_view word, Values& values, const Write& write) {
    std::uint64_t total = 0;
    std::string_view value;
    while (values.Next(value)) {
        if (value.empty()) throw std::runtime_error("invalid intermediate word count");
        const char* end = value.data() + value.size();
        std::uint64_t count = 0;
        const auto parsed = std::from_chars(value.data(), end, count);
        if (parsed.ec == std::errc::result_out_of_range ||
            count > std::numeric_limits<std::uint64_t>::max() - total) {
            throw std::overflow_error("word count exceeds uint64_t");
        }
        if (parsed.ec != std::errc{} || parsed.ptr != end) {
            throw std::runtime_error("invalid intermediate word count");
        }
        total += count;
    }
    write(word);
    write("\t");
    write(std::to_string(total));
    write("\n");
}

}
