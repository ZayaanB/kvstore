#include "kvstore.hpp"
#include "metrics.hpp"
#include "metrics_http.hpp"

#include <charconv>
#include <cstdint>
#include <optional>
#include <string_view>

#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

    std::vector<std::string> SplitArgs(const std::string& line) {
        std::vector<std::string> args;
        std::istringstream iss(line);
        std::string tok;
        while (iss >> tok) {
            args.push_back(tok);
        }
        return args;
    }

    template<class Integer>
    void WriteNumber(Integer value) {
        for (std::size_t i = 0; i < sizeof(value); ++i) {
            std::cout.put(static_cast<char>((value >> (8 * i)) & 255u));
        }
    }

    void WriteSnapshot(const std::string& path) {
        const kvstore::KVStore store(path, {}, std::nullopt, kvstore::OpenMode::Existing);
        auto snapshot = store.OpenSnapshot();
        std::cout.exceptions(std::ios::failbit | std::ios::badbit);
        std::cout.write("KVSNAP01", 8);
        WriteNumber(static_cast<std::uint64_t>(snapshot->CapturedAt()));
        std::cout.flush();
        kvstore::SnapshotRow row;
        while (snapshot->Next(row)) {
            std::cout.put(1);
            WriteNumber(static_cast<std::uint32_t>(row.key.size()));
            WriteNumber(static_cast<std::uint32_t>(row.value.size()));
            WriteNumber(static_cast<std::uint64_t>(row.expires_at.value_or(0)));
            std::cout.write(row.key.data(), static_cast<std::streamsize>(row.key.size()));
            std::cout.write(row.value.data(), static_cast<std::streamsize>(row.value.size()));
        }
        std::cout.put(0);
        std::cout.flush();
    }

    void PrintHelp() {
        std::cout <<
            "commands:\n"
            "  set <key> <value>          set a key with no expiry\n"
            "  setttl <key> <ttl> <value> set a key that expires after ttl seconds\n"
            "  get <key>                  print a key's value, or (nil) if absent/expired\n"
            "  del <key>                  delete a key\n"
            "  size                       print the number of keys in the store\n"
            "  keys                       list every live, non-expired key (sorted)\n"
            "  scan <start> [end]         list keys in [start, end), end exclusive and optional\n"
            "  compact                    flush writes and merge sorted files\n"
            "  shards                     show unavailable shards\n"
            "  help                       show this message\n"
            "  exit                       quit\n";
    }

}

int main(int argc, char** argv) {
    const std::string_view command = argc > 1 ? argv[1] : "";
    if (command == "--snapshot" || command == "--checkpoint" || command == "--rehash" ||
        command == "--restore-shard" || command == "--migrate") {
        const int expected = command == "--snapshot" ? 3 : command == "--restore-shard" ? 5 : 4;
        if (argc != expected) {
            std::cerr << "usage: kvstore_cli --snapshot DATABASE\n"
                         "       kvstore_cli --checkpoint DATABASE DESTINATION\n"
                         "       kvstore_cli --restore-shard DATABASE CHECKPOINT SHARD\n"
                         "       kvstore_cli --rehash DATABASE DESTINATION\n"
                         "       kvstore_cli --migrate LEGACY_DATABASE DESTINATION\n";
            return 1;
        }
        try {
            if (command == "--snapshot") WriteSnapshot(argv[2]);
            else if (command == "--checkpoint") {
                const kvstore::KVStore store(argv[2], {}, std::nullopt, kvstore::OpenMode::Existing);
                store.Checkpoint(argv[3]);
                std::cout << "Checkpoint saved at " << argv[3] << '\n';
            } else if (command == "--restore-shard") {
                const std::string_view value = argv[4];
                std::size_t shard = 0;
                const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), shard);
                if (error != std::errc{} || end != value.data() + value.size()) {
                    throw std::invalid_argument("invalid shard number");
                }
                kvstore::KVStore::RestoreShard(argv[2], argv[3], shard);
                std::cout << "Restored shard " << shard << '\n';
            } else if (command == "--rehash") {
                kvstore::KVStore::Rehash(argv[2], argv[3]);
                std::cout << "Published rehashed database at " << argv[3] << '\n';
            } else {
                kvstore::KVStore::MigrateLegacy(argv[2], argv[3]);
                std::cout << "Published migrated database at " << argv[3] << '\n';
            }
            return 0;
        } catch (const std::exception& error) {
            std::cerr << command << ": " << error.what() << '\n';
            return 1;
        }
    }

    std::string db_path = "data/kvstore_cli.db";
    std::string metrics_address = "127.0.0.1";
    std::optional<std::uint16_t> metrics_port;
    bool has_path = false;
    auto mode = kvstore::OpenMode::Strict;
    constexpr auto usage = "usage: kvstore_cli [db_path] [--degraded] [--metrics-port PORT [--metrics-listen IPv4]]\n"
                           "       kvstore_cli --snapshot DATABASE\n"
                           "       kvstore_cli --checkpoint DATABASE DESTINATION\n"
                           "       kvstore_cli --restore-shard DATABASE CHECKPOINT SHARD\n"
                           "       kvstore_cli --rehash DATABASE DESTINATION\n"
                           "       kvstore_cli --migrate LEGACY_DATABASE DESTINATION\n";
    for (int i = 1; i < argc; ++i) {
        const std::string_view flag = argv[i];
        if (flag == "--help") {
            std::cout << usage;
            return 0;
        }
        if (flag == "--degraded") {
            mode = kvstore::OpenMode::Degraded;
        } else if (flag == "--metrics-port" && i + 1 < argc) {
            const std::string_view value = argv[++i];
            std::uint16_t port = 0;
            const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), port);
            if (error != std::errc{} || end != value.data() + value.size()) {
                std::cerr << usage;
                return 1;
            }
            metrics_port = port;
        } else if (flag == "--metrics-listen" && i + 1 < argc) {
            metrics_address = argv[++i];
        } else if (!has_path && !flag.starts_with("--")) {
            db_path = flag;
            has_path = true;
        } else {
            std::cerr << usage;
            return 1;
        }
    }

    std::shared_ptr<kvstore::Metrics> metrics;
    std::unique_ptr<kvstore::KVStore> store;
    try {
        if (metrics_port) metrics = std::make_shared<kvstore::Metrics>();
        store = std::make_unique<kvstore::KVStore>(db_path, metrics, std::nullopt, mode);
    } catch (const std::exception& e) {
        std::cerr << "failed to open store at '" << db_path << "': " << e.what() << "\n";
        return 1;
    }

    std::unique_ptr<kvstore::MetricsHttp> exporter;
    if (metrics) {
        try {
            exporter = std::make_unique<kvstore::MetricsHttp>(*metrics, *store, metrics_address, *metrics_port);
            std::cerr << "metrics listening on " << metrics_address << ':' << exporter->Port() << '\n';
        } catch (const std::exception& error) {
            std::cerr << "metrics exporter unavailable: " << error.what() << '\n';
        }
    }

    std::cout << "kvstore cli - db: " << db_path << "\n";
    std::cout << "type 'help' for commands, 'exit' to quit.\n";

    std::string line;
    while (true) {
        std::cout << "> ";
        if (!std::getline(std::cin, line)) {
            break; // eof on stdin
        }

        auto args = SplitArgs(line);
        if (args.empty()) {
            continue;
        }

        try {
            const std::string& cmd = args[0];

            if (cmd == "exit" || cmd == "quit") {
                break;
            } else if (cmd == "help") {
                PrintHelp();
            } else if (cmd == "set") {
                if (args.size() < 3) {
                    std::cout << "usage: set <key> <value>\n";
                    continue;
                }
                std::string value;
                for (std::size_t i = 2; i < args.size(); ++i) {
                    if (i > 2) value += ' ';
                    value += args[i];
                }
                bool ok = store->Set(args[1], value);
                std::cout << (ok ? "OK\n" : "ERROR: write failed\n");
            } else if (cmd == "setttl") {
                if (args.size() < 4) {
                    std::cout << "usage: setttl <key> <ttl_seconds> <value>\n";
                    continue;
                }
                std::int64_t ttl = 0;
                try {
                    std::size_t parsed = 0;
                    ttl = std::stoll(args[2], &parsed);
                    if (parsed != args[2].size()) throw std::invalid_argument("trailing TTL characters");
                } catch (const std::exception&) {
                    std::cout << "ERROR: ttl must be an integer number of seconds\n";
                    continue;
                }
                std::string value;
                for (std::size_t i = 3; i < args.size(); ++i) {
                    if (i > 3) value += ' ';
                    value += args[i];
                }
                bool ok = store->SetWithTtl(args[1], value, ttl);
                std::cout << (ok ? "OK\n" : "ERROR: write failed\n");
            } else if (cmd == "get") {
                if (args.size() != 2) {
                    std::cout << "usage: get <key>\n";
                    continue;
                }
                auto val = store->Get(args[1]);
                if (val.has_value()) {
                    std::cout << *val << "\n";
                } else {
                    std::cout << "(nil)\n";
                }
            } else if (cmd == "del") {
                if (args.size() != 2) {
                    std::cout << "usage: del <key>\n";
                    continue;
                }
                bool ok = store->Delete(args[1]);
                std::cout << (ok ? "OK\n" : "ERROR: write failed\n");
            } else if (cmd == "size") {
                std::cout << store->Size() << "\n";
            } else if (cmd == "keys") {
                auto items = store->Items();
                if (items.empty()) {
                    std::cout << "(empty)\n";
                }
                for (const auto& kv : items) {
                    std::cout << kv.key << "\n";
                }
            } else if (cmd == "scan") {
                if (args.size() < 2 || args.size() > 3) {
                    std::cout << "usage: scan <start> [end]\n";
                    continue;
                }
                std::string end = (args.size() == 3) ? args[2] : std::string();
                auto items = store->Range(args[1], end);
                if (items.empty()) {
                    std::cout << "(empty)\n";
                }
                for (const auto& kv : items) {
                    std::cout << kv.key << " = " << kv.value << "\n";
                }
            } else if (cmd == "compact") {
                bool ok = store->Compact();
                std::cout << (ok ? "OK\n" : "ERROR: compaction failed\n");
            } else if (cmd == "shards") {
                const auto failed = store->UnavailableShards();
                if (failed.empty()) std::cout << "All shards available\n";
                else {
                    std::cout << "Unavailable shards";
                    for (auto shard : failed) std::cout << ' ' << shard;
                    std::cout << '\n';
                }
            } else {
                std::cout << "unknown command '" << cmd << "', try 'help'\n";
            }
        } catch (const std::exception& e) {
            std::cout << "ERROR: " << e.what() << "\n";
        }
    }

    std::cout << "bye.\n";
    return 0;
}
