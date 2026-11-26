#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace kvstore::detail {

enum class Routing : std::uint64_t { Modulo = 1, Rendezvous = 2 };

inline std::uint64_t KeyHash(std::string_view key) {
    std::uint64_t hash = 14695981039346656037ull;
    for (const auto byte : key) {
        hash ^= static_cast<unsigned char>(byte);
        hash *= 1099511628211ull;
    }
    return hash;
}

inline std::uint64_t ShardScore(std::uint64_t hash, std::size_t shard) {
    auto score = hash + (static_cast<std::uint64_t>(shard) + 1) * 0x9e3779b97f4a7c15ull;
    score = (score ^ (score >> 30)) * 0xbf58476d1ce4e5b9ull;
    score = (score ^ (score >> 27)) * 0x94d049bb133111ebull;
    return score ^ (score >> 31);
}

inline std::size_t ShardIndex(std::string_view key, Routing routing, std::size_t count) {
    const auto hash = KeyHash(key);
    if (routing == Routing::Modulo) return hash % count;
    std::size_t selected = 0;
    auto best = ShardScore(hash, selected);
    for (std::size_t shard = 1; shard < count; ++shard) {
        const auto score = ShardScore(hash, shard);
        if (score > best) {
            selected = shard;
            best = score;
        }
    }
    return selected;
}

}
