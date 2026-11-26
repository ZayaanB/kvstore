#include "metrics_http.hpp"
#include "kvstore.hpp"
#include "metrics.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <filesystem>
#include <locale>
#include <sstream>
#include <string_view>
#include <system_error>
#include <thread>

namespace kvstore {
namespace {

namespace fs = std::filesystem;
using Clock = Metrics::Clock;

struct Socket {
    explicit Socket(int descriptor) : fd(descriptor) {}
    ~Socket() { if (fd >= 0) ::close(fd); }
    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;
    int fd;
};

[[noreturn]] void SocketError(const char* message) {
    throw std::system_error(errno, std::generic_category(), message);
}

bool ConfigureSocket(int fd) {
    return ::fcntl(fd, F_SETFD, FD_CLOEXEC) == 0 && ::fcntl(fd, F_SETFL, O_NONBLOCK) == 0;
}

bool Await(int fd, short events, Clock::time_point deadline) {
    for (;;) {
        const auto ms = std::chrono::ceil<std::chrono::milliseconds>(deadline - Clock::now());
        if (ms.count() <= 0) return false;
        pollfd request{fd, events, 0};
        const int result = ::poll(&request, 1, static_cast<int>(ms.count()));
        if (result >= 0) return result > 0 && (request.revents & events);
        if (errno != EINTR) return false;
    }
}

std::string ResourceMetrics(const KVStore& store, Clock::time_point deadline) {
    std::ostringstream out;
    out.imbue(std::locale::classic());
    rusage usage{};
    if (::getrusage(RUSAGE_SELF, &usage) == 0) {
        const auto seconds = [](timeval value) {
            return static_cast<double>(value.tv_sec) + static_cast<double>(value.tv_usec) * 1e-6;
        };
        out.precision(12);
        out << "# HELP kvstore_process_cpu_seconds_total Total user and system CPU time of the exporting process in seconds.\n"
               "# TYPE kvstore_process_cpu_seconds_total counter\n"
            << "kvstore_process_cpu_seconds_total " << seconds(usage.ru_utime) + seconds(usage.ru_stime) << '\n';
#if defined(__linux__) || defined(__APPLE__)
#ifdef __APPLE__
        constexpr std::uint64_t bytes_per_unit = 1;
#else
        constexpr std::uint64_t bytes_per_unit = 1024;
#endif
        out << "# HELP kvstore_process_peak_rss_bytes Lifetime peak resident set size of the exporting process in bytes.\n"
               "# TYPE kvstore_process_peak_rss_bytes gauge\n"
            << "kvstore_process_peak_rss_bytes " << static_cast<std::uint64_t>(usage.ru_maxrss) * bytes_per_unit << '\n';
#endif
    }
    std::error_code error;
    const fs::path database(store.DatabasePath());
    if (const auto bytes = store.TelemetryWalBytes()) {
        out << "# HELP kvstore_wal_file_bytes Committed logical bytes in active shard WALs.\n"
               "# TYPE kvstore_wal_file_bytes gauge\nkvstore_wal_file_bytes " << *bytes << '\n';
    }
    const auto space = fs::space(database, error);
    if (!error) {
        out << "# HELP kvstore_filesystem_available_bytes Bytes available to the CLI on the database filesystem.\n"
               "# TYPE kvstore_filesystem_available_bytes gauge\nkvstore_filesystem_available_bytes " << space.available << '\n'
            << "# HELP kvstore_filesystem_capacity_bytes Total capacity in bytes of the database filesystem.\n"
                   "# TYPE kvstore_filesystem_capacity_bytes gauge\nkvstore_filesystem_capacity_bytes " << space.capacity << '\n';
    }

    // Bound the scan as immutable tables accumulate. Omit an incomplete total.
    constexpr std::size_t max_entries = 4096;
    auto total = fs::file_size(store.DatabasePath() + ".lock", error);
    if (error) return out.str();
    fs::recursive_directory_iterator file(database, error), end;
    std::size_t entries = 0;
    while (!error && file != end && entries < max_entries && Clock::now() < deadline) {
        if (fs::is_regular_file(file->symlink_status(error))) total += file->file_size(error);
        if (!error) file.increment(error);
        ++entries;
    }
    if (!error && file == end) {
        out << "# HELP kvstore_database_file_bytes Combined logical length of database files in bytes.\n"
               "# TYPE kvstore_database_file_bytes gauge\nkvstore_database_file_bytes " << total << '\n';
    }
    return out.str();
}

void Serve(int fd, const Metrics& metrics, const KVStore& store) {
    using namespace std::chrono_literals;
    const auto deadline = Clock::now() + 500ms;
    std::array<char, 2048> input{};
    std::size_t received = 0;
    std::string_view header;
    while (received < input.size() && Await(fd, POLLIN, deadline)) {
        const auto n = ::recv(fd, input.data() + received, input.size() - received, 0);
        if (n == 0) return;
        if (n < 0) {
            if (errno == EAGAIN || errno == EINTR) continue;
            return;
        }
        received += static_cast<std::size_t>(n);
        header = {input.data(), received};
        if (header.find("\r\n\r\n") != std::string_view::npos) break;
    }
    if (header.find("\r\n\r\n") == std::string_view::npos) return;
    const auto first_line = header.substr(0, header.find("\r\n"));
    const bool found = first_line == "GET /metrics HTTP/1.1" || first_line == "GET /metrics HTTP/1.0";
    const auto body = found ? metrics.Prometheus() + ResourceMetrics(store, std::min(deadline, Clock::now() + 100ms))
                            : "Not found\n";
    std::string response = found ? "HTTP/1.1 200 OK\r\n" : "HTTP/1.1 404 Not Found\r\n";
    response += "Content-Type: text/plain; version=0.0.4; charset=utf-8\r\nConnection: close\r\nContent-Length: ";
    response += std::to_string(body.size()) + "\r\n\r\n" + body;
    std::string_view remaining(response);
    while (!remaining.empty() && Await(fd, POLLOUT, deadline)) {
        const auto sent = ::send(fd, remaining.data(), remaining.size(), MSG_NOSIGNAL);
        if (sent == 0) return;
        if (sent < 0) {
            if (errno == EAGAIN || errno == EINTR) continue;
            return;
        }
        remaining.remove_prefix(static_cast<std::size_t>(sent));
    }
}

} // namespace

struct MetricsHttp::Server {
    Server(const Metrics& recorder, const KVStore& database, const std::string& address, std::uint16_t port)
        : metrics(recorder), store(database), listener(::socket(AF_INET, SOCK_STREAM, 0)) {
        if (listener.fd < 0 || !ConfigureSocket(listener.fd)) SocketError("metrics socket");
        const int reuse = 1;
        if (::setsockopt(listener.fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) != 0) {
            SocketError("metrics socket options");
        }
        sockaddr_in endpoint{};
        endpoint.sin_family = AF_INET;
        endpoint.sin_port = htons(port);
        if (::inet_pton(AF_INET, address.c_str(), &endpoint.sin_addr) != 1) {
            throw std::invalid_argument("metrics address must be a numeric IPv4 address");
        }
        if (::bind(listener.fd, reinterpret_cast<const sockaddr*>(&endpoint), sizeof(endpoint)) != 0 ||
            ::listen(listener.fd, 8) != 0) {
            SocketError("metrics listen");
        }
        socklen_t length = sizeof(endpoint);
        if (::getsockname(listener.fd, reinterpret_cast<sockaddr*>(&endpoint), &length) != 0) {
            SocketError("metrics socket name");
        }
        bound_port = ntohs(endpoint.sin_port);
        thread = std::jthread([this](std::stop_token stop) {
            using namespace std::chrono_literals;
            while (!stop.stop_requested()) {
                if (!Await(listener.fd, POLLIN, Clock::now() + 100ms)) continue;
                const Socket client(::accept(listener.fd, nullptr, nullptr));
                if (client.fd < 0) {
                    // Avoid spinning if the process cannot allocate another descriptor.
                    std::this_thread::sleep_for(10ms);
                    continue;
                }
                if (!ConfigureSocket(client.fd)) continue;
                try {
                    Serve(client.fd, metrics, store);
                } catch (const std::exception&) {
                    // A failed scrape must not terminate the host.
                }
            }
        });
    }

    const Metrics& metrics;
    const KVStore& store;
    const Socket listener;
    std::uint16_t bound_port = 0;
    std::jthread thread;
};

MetricsHttp::MetricsHttp(const Metrics& metrics, const KVStore& store, const std::string& address, std::uint16_t port)
    : server_(std::make_unique<Server>(metrics, store, address, port)) {}

MetricsHttp::~MetricsHttp() = default;

std::uint16_t MetricsHttp::Port() const { return server_->bound_port; }

} // namespace kvstore
