#include "kvstore.hpp"

#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

namespace {

// splits a line on whitespace, respecting nothing fancy - good enough for this cli.
std::vector<std::string> SplitArgs(const std::string& line) {
    std::vector<std::string> args;
    std::istringstream iss(line);
    std::string tok;
    while (iss >> tok) {
        args.push_back(tok);
    }
    return args;
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
        "  compact                    rewrite the log down to live, non-expired keys\n"
        "  help                       show this message\n"
        "  exit                       quit\n";
}

}

int main(int argc, char** argv) {
    std::string db_path = "data/kvstore_cli.db";
    if (argc > 1) {
        db_path = argv[1];
    }

    std::unique_ptr<kvstore::KVStore> store;
    try {
        store = std::make_unique<kvstore::KVStore>(db_path);
    } catch (const std::exception& e) {
        std::cerr << "failed to open store at '" << db_path << "': " << e.what() << "\n";
        return 1;
    }

    std::cout << "kvstore cli - db: " << db_path << "\n";
    std::cout << "type 'help' for commands, 'exit' to quit.\n";

    std::string line;
    while (true) {
        std::cout << "> ";
        if (!std::getline(std::cin, line)) {
            break; // eof on stdin.
        }

        auto args = SplitArgs(line);
        if (args.empty()) {
            continue;
        }

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
            // everything after the key is the value, joined back with spaces.
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
                ttl = std::stoll(args[2]);
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
        } else {
            std::cout << "unknown command '" << cmd << "', try 'help'\n";
        }
    }

    std::cout << "bye.\n";
    return 0;
}