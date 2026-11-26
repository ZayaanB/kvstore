#pragma once

#include <cstdint>
#include <memory>
#include <string>

namespace kvstore {

class Metrics;

// Used by the CLI only. The embedded engine never opens a network listener.
class MetricsHttp {
public:
    MetricsHttp(const Metrics& metrics, std::string wal_path, const std::string& address, std::uint16_t port);
    ~MetricsHttp();
    std::uint16_t Port() const;

private:
    struct Server;
    std::unique_ptr<Server> server_;
};

} // namespace kvstore
