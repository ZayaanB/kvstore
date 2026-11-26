"""Run the local KVStore monitoring stack and start a demo from the browser."""

import argparse
from contextlib import ExitStack, contextmanager, suppress
from http.client import HTTPConnection, HTTPException
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
from threading import Event, Lock, Thread
import time
import webbrowser


MONITORING = Path(__file__).resolve().parent
ROOT = MONITORING.parent
BUILD = ROOT / "build"
STATE = BUILD / "monitoring" / "local"
HOST = "127.0.0.1"
CONTROL, GRAFANA, PROMETHEUS, EXPORTER = 3001, 3000, 9090, 9108
ORIGIN = f"http://{HOST}:{CONTROL}"


def response(port, path):
    connection = HTTPConnection(HOST, port, timeout=0.5)
    try:
        connection.request("GET", path)
        result = connection.getresponse()
        return result.read(8192) if result.status == 200 else b""
    except (OSError, HTTPException):
        return b""
    finally:
        connection.close()


def require_free_port(port):
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            listener.bind((HOST, port))
        except OSError as error:
            raise RuntimeError(f"Port {port} is already in use.") from error


def executables():
    settings = STATE / "tools.json"
    previous = json.loads(settings.read_text()) if settings.exists() else {}
    prometheus_dir, grafana_dir = os.environ.get("PROMETHEUS_DIR"), os.environ.get("GRAFANA_DIR")
    prometheus = (
        Path(prometheus_dir) / "prometheus" if prometheus_dir else
        Path(shutil.which("prometheus") or previous.get("prometheus", "/usr/bin/prometheus"))
    ).resolve()
    grafana_home = Path(grafana_dir or previous.get("grafana_home", "/usr/share/grafana")).resolve()
    grafana = grafana_home / "bin/grafana"
    if grafana_dir is None and not grafana.is_file() and shutil.which("grafana"):
        grafana = Path(shutil.which("grafana")).resolve()
    if not os.access(prometheus, os.X_OK) or not prometheus.is_file():
        raise RuntimeError("Prometheus was not found. Set PROMETHEUS_DIR to its distribution directory for the first run.")
    if not os.access(grafana, os.X_OK) or not grafana.is_file() or not (grafana_home / "conf/defaults.ini").is_file():
        raise RuntimeError("Grafana was not found. Set GRAFANA_DIR to its distribution directory for the first run.")
    STATE.mkdir(parents=True, exist_ok=True)
    settings.write_text(json.dumps({"prometheus": str(prometheus), "grafana_home": str(grafana_home)}) + "\n")
    return prometheus, grafana, grafana_home


def spawn(command, name, **options):
    with (STATE / f"{name}.log").open("w") as log:
        return subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, text=True, **options)


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
    if process.stdin:
        with suppress(OSError):
            process.stdin.close()


@contextmanager
def monitoring_process(command, name, port, health, **options):
    process = spawn(command, name, **options)
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and process.poll() is None:
            if response(port, health):
                yield
                return
            time.sleep(0.2)
        raise RuntimeError(f"{name} did not become ready. See {STATE / (name + '.log')}.")
    finally:
        stop(process)


def start_monitoring(stack):
    prometheus, grafana, grafana_home = executables()
    config = STATE / "prometheus.yml"
    config.write_text((MONITORING / "prometheus.yml").read_text()
                      .replace("alerts.yml", str(MONITORING / "alerts.yml"))
                      .replace("kvstore:9108", f"{HOST}:{EXPORTER}"))
    stack.enter_context(monitoring_process(
        [prometheus, f"--config.file={config}", f"--storage.tsdb.path={STATE / 'tsdb'}",
         "--storage.tsdb.retention.time=30d", f"--web.listen-address={HOST}:{PROMETHEUS}"],
        "prometheus", PROMETHEUS, "/-/ready"
    ))
    stack.enter_context(monitoring_process(
        [grafana, "server", "--homepath", grafana_home], "grafana", GRAFANA, "/api/health",
        env=dict(os.environ, GF_SERVER_HTTP_ADDR=HOST, GF_SERVER_HTTP_PORT=str(GRAFANA),
                 GF_PATHS_DATA=str(STATE / "grafana"), GF_PATHS_LOGS=str(STATE / "grafana-logs"),
                 GF_PATHS_PLUGINS=str(STATE / "grafana-plugins"),
                 GF_PATHS_PROVISIONING=str(MONITORING / "grafana/provisioning"),
                 GF_PLUGINS_PREINSTALL_DISABLED="true", GF_PLUGINS_PUBLIC_KEY_RETRIEVAL_DISABLED="true",
                 GF_ANALYTICS_REPORTING_ENABLED="false", GF_ANALYTICS_CHECK_FOR_UPDATES="false",
                 GF_ANALYTICS_CHECK_FOR_PLUGIN_UPDATES="false",
                 KVSTORE_PROMETHEUS_URL=f"http://{HOST}:{PROMETHEUS}",
                 KVSTORE_DASHBOARDS_PATH=str(MONITORING / "grafana/dashboards"))
    ))


class Application:
    def __init__(self):
        self.process = None
        self.lock = Lock()
        self.stopping = Event()
        self.started = 0

    def status(self):
        if b"kvstore_operations_total" in response(EXPORTER, "/metrics"):
            return "connected"
        if self.process is None:
            return "stopped"
        if self.process.poll() is not None or time.monotonic() - self.started > 10:
            return "failed"
        return "starting"

    def start(self):
        with self.lock:
            state = self.status()
            if state in {"connected", "starting"} or self.stopping.is_set():
                return state
            if self.process is not None:
                stop(self.process)
            require_free_port(EXPORTER)
            self.started = time.monotonic()
            self.process = spawn(
                [BUILD / "kvstore_cli", STATE / "demo-store", "--metrics-port", str(EXPORTER)],
                "kvstore", stdin=subprocess.PIPE
            )
            Thread(target=self.demo, args=(self.process,), daemon=True).start()
            return "starting"

    def demo(self, process):
        try:
            process.stdin.write("".join(f"set demo-{key} initial\n" for key in range(8)))
            for iteration in range(30):
                if self.stopping.is_set():
                    return
                reads = "".join(f"get demo-{key}\n" for key in range(8))
                writes = "".join(f"set demo-{(iteration + key) % 8} sample-{iteration}\n" for key in range(2))
                process.stdin.write(reads + writes)
                process.stdin.flush()
                if self.stopping.wait(2):
                    return
        except (OSError, ValueError):
            # The child exited or the launcher closed its input.
            return

    def close(self):
        with self.lock:
            self.stopping.set()
            if self.process is not None:
                stop(self.process)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def reply(self, code, body, content_type="application/json"):
        if isinstance(body, dict):
            body = json.dumps(body).encode()
        with suppress(ConnectionError):
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Frame-Options", "DENY")
            self.end_headers()
            self.wfile.write(body)

    def do_GET(self):
        if self.headers.get("Host") != f"{HOST}:{CONTROL}":
            self.reply(403, {"error": "Use the local monitoring address."})
        elif self.path == "/":
            self.reply(200, (MONITORING / "index.html").read_bytes(), "text/html; charset=utf-8")
        elif self.path == "/api/status":
            self.reply(200, {"service": "kvstore-monitor", "app": self.server.app.status()})
        else:
            self.reply(404, {"error": "Page unavailable."})

    def do_POST(self):
        if self.headers.get("Host") != f"{HOST}:{CONTROL}" or self.headers.get("Origin") != ORIGIN:
            self.reply(403, {"error": "Start KVStore from the local monitoring page."})
        elif self.path != "/api/start":
            self.reply(404, {"error": "Page unavailable."})
        else:
            try:
                state = self.server.app.start()
            except (OSError, RuntimeError) as error:
                self.reply(503, {"error": str(error)})
                return
            self.reply(200, {"app": state})


def announce(open_browser):
    print(f"KVStore monitoring: {ORIGIN}", flush=True)
    if open_browser:
        Thread(target=webbrowser.open, args=(ORIGIN,), daemon=True).start()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--no-open", action="store_true", help="print the URL without opening a browser")
    args = parser.parse_args()
    if b'"service": "kvstore-monitor"' in response(CONTROL, "/api/status"):
        announce(False)
        if not args.no_open:
            webbrowser.open(ORIGIN)
        return

    def interrupt(*_):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupt)
    try:
        with ThreadingHTTPServer((HOST, CONTROL), Handler) as server, ExitStack() as stack:
            require_free_port(PROMETHEUS)
            require_free_port(GRAFANA)
            subprocess.run(["cmake", "-S", ROOT, "-B", BUILD, "-DCMAKE_BUILD_TYPE=Release"], check=True)
            subprocess.run(["cmake", "--build", BUILD, "--target", "kvstore_cli", "--parallel"], check=True)
            start_monitoring(stack)
            server.app = Application()
            stack.callback(server.app.close)
            announce(not args.no_open)
            server.serve_forever()
    except KeyboardInterrupt:
        return
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"monitor: {error}\n")


if __name__ == "__main__":
    main()
