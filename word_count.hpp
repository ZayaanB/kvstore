#pragma once

#include "mapreduce.hpp"

namespace kvstore::mapreduce {

// Words are runs of ASCII letters and digits. Uppercase folds to lowercase;
// every other byte, including non-ASCII and malformed UTF-8, is a separator.
void MapWords(std::string_view key, std::string_view value, const Emit& emit);

// Sums unsigned decimal counts with overflow checking and writes word<TAB>count<LF>.
void ReduceWordCount(std::string_view word, Values& values, const Write& write);

}
