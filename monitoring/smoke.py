#!/usr/bin/env python3
"""Verify a live KVStore CLI, Prometheus, and Grafana on loopback with local binaries."""

import argparse
import base64
import contextlib
import json
import os
import signal
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from urllib import error, parse, request


def port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def url(port_number):
    return f"http://127.0.0.1:{port_number}"


def http(endpoint, body=None, headers=None):
    payload = None if body is None else json.dumps(body).encode()
    with request.urlopen(request.Request(endpoint, payload, headers or {}), timeout=3) as response:
        return response.read().decode()


def until(check, description, seconds=45):
    limit = time.monotonic() + seconds
    failure = None
    while time.monotonic() < limit:
        try:
            if result := check():
                return result
        except (OSError, ValueError, KeyError, error.HTTPError) as exc:
            failure = exc
        time.sleep(0.5)
    raise RuntimeError(f"Timed out waiting for {description}: {failure}")


@contextlib.contextmanager
def process(command, output, *, env=None, stdin=None):
    with output.open("w", encoding="utf-8") as log:
        with subprocess.Popen(command, stdin=stdin, stdout=log, stderr=subprocess.STDOUT, text=True, env=env) as proc:
            try:
                yield proc
            finally:
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()


def save(path, data):
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def exercise(args, work):
    root = Path(__file__).resolve().parent
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    exporter, prometheus, grafana = (port() for _ in range(3))
    # Hold an unused port open without accepting connections for the outage.
    with socket.socket() as unreachable:
        unreachable.bind(("127.0.0.1", 0))
        broken = unreachable.getsockname()[1]
        config = work / "prometheus.yml"
        original = (root / "prometheus.yml").read_text(encoding="utf-8")
        original = original.replace("alerts.yml", str(root / "alerts.yml"))

        def configure(target):
            config.write_text(original.replace("kvstore:9108", f"127.0.0.1:{target}"), encoding="utf-8")

        def query(expression):
            endpoint = f"{url(prometheus)}/api/v1/query?" + parse.urlencode({"query": expression})
            return json.loads(http(endpoint))["data"]["result"]

        def alerts():
            return json.loads(http(f"{url(prometheus)}/api/v1/alerts"))["data"]["alerts"]

        def send(*commands):
            cli.stdin.write("\n".join(commands) + "\n")
            cli.stdin.flush()

        prometheus_cmd = [
            str(args.prometheus.resolve()), f"--config.file={config}", f"--storage.tsdb.path={work / 'tsdb'}",
            f"--web.listen-address=127.0.0.1:{prometheus}",
        ]
        grafana_cmd = [str(args.grafana.resolve() / "bin/grafana"), "server", "--homepath", str(args.grafana.resolve())]
        grafana_env = dict(os.environ, GF_SERVER_HTTP_ADDR="127.0.0.1", GF_SERVER_HTTP_PORT=str(grafana),
                           GF_PATHS_DATA=str(work / "grafana"), GF_PATHS_LOGS=str(work / "grafana-logs"),
                           GF_PATHS_PLUGINS=str(work / "grafana-plugins"),
                           GF_PLUGINS_PREINSTALL_DISABLED="true", GF_PLUGINS_PUBLIC_KEY_RETRIEVAL_DISABLED="true",
                           GF_PATHS_PROVISIONING=str(root / "grafana/provisioning"),
                           GF_ANALYTICS_REPORTING_ENABLED="false", GF_ANALYTICS_CHECK_FOR_UPDATES="false",
                           GF_ANALYTICS_CHECK_FOR_PLUGIN_UPDATES="false",
                           GF_SECURITY_ADMIN_USER="admin", GF_SECURITY_ADMIN_PASSWORD="admin",
                           KVSTORE_PROMETHEUS_URL=url(prometheus),
                           KVSTORE_DASHBOARDS_PATH=str(root / "grafana/dashboards"))
        auth = {"Authorization": "Basic " + base64.b64encode(b"admin:admin").decode(),
                "Content-Type": "application/json"}

        def grafana_query(expression):
            endpoint = f"{url(grafana)}/api/ds/query"
            body = {"from": "now-5m", "to": "now", "queries": [
                {"datasource": {"uid": "kvstore-prometheus", "type": "prometheus"}, "expr": expression,
                 "refId": "A", "instant": True, "range": False, "intervalMs": 5000}
            ]}
            return json.loads(http(endpoint, body, auth))["results"]["A"]

        def value_matches(result, compare):
            return any(compare(value) for frame in result.get("frames", [])
                       for value in frame.get("data", {}).get("values", [[], []])[-1] if value is not None)

        configure(exporter)
        with process([str(args.cli.resolve()), str(work / "store"), "--metrics-port", str(exporter)],
                     output / "cli.log", stdin=subprocess.PIPE) as cli, \
             process(prometheus_cmd, output / "prometheus.log") as prom, \
             process(grafana_cmd, output / "grafana.log", env=grafana_env):
            def scrape_has(metric):
                body = http(f"{url(exporter)}/metrics")
                return body if metric in body else None

            until(lambda: scrape_has('kvstore_operations_total{operation="recovery",outcome="ok"} 1'),
                  "CLI exporter")
            # A stalled scrape must not delay a database operation.
            with socket.create_connection(("127.0.0.1", exporter)) as slow:
                slow.sendall(b"GET /metrics")
                send("set live steady", "get absent")
                slow.settimeout(2)
                if slow.recv(16):
                    raise RuntimeError("Exporter answered an incomplete request")
            live = until(lambda: scrape_has('kvstore_operations_total{operation="get",outcome="miss"} 1'),
                         "miss counter")
            if "steady" in live:
                raise RuntimeError("A database value leaked into the metrics")
            until(lambda: query('up{job="kvstore"} == 1'), "healthy scrape")
            dashboard = until(lambda: json.loads(http(f"{url(grafana)}/api/dashboards/uid/kvstore-live", headers=auth)),
                              "provisioned Grafana dashboard")
            save(output / "dashboard.json", dashboard)

            panels = {panel["title"]: panel for panel in dashboard["dashboard"]["panels"]}

            def panel_expression(title, target=0):
                return panels[title]["targets"][target]["expr"].replace("$operation", "get").replace("$window", "30s")

            expressions = {
                "throughput": panel_expression("Completed data operations"),
                "latency": panel_expression("Operation latency", 1),
                "wal": panel_expression("WAL sync calls"),
            }
            samples = {}
            deadline = time.monotonic() + 40
            while len(samples) < len(expressions) and time.monotonic() < deadline:
                send(*(["set live steady", "get live", "get absent"] * 12), "compact")
                time.sleep(1)
                for name, expression in expressions.items():
                    result = grafana_query(expression)
                    if value_matches(result, lambda value: value > 0):
                        samples[name] = result
            if samples.keys() != expressions.keys():
                raise RuntimeError(f"Grafana load samples incomplete: {samples.keys()}")
            save(output / "grafana-load.json", samples)
            (output / "metrics.txt").write_text(http(f"{url(exporter)}/metrics"), encoding="utf-8")
            print("Grafana returned live throughput, p95, and WAL sync data", flush=True)

            # Suspend collection without stopping the CLI or Prometheus alert evaluation.
            configure(broken)
            prom.send_signal(signal.SIGHUP)
            timeout = 55
            firing = until(lambda: [entry for entry in alerts()
                                    if entry["labels"]["alertname"] == "KVStoreTelemetryMissing"
                                    and entry["state"] == "firing"], "missing telemetry alert", timeout)
            save(output / "firing-alert.json", firing)
            send("set outage still-available", "get outage", "setttl overflow 9223372036854775807 value")
            until(lambda: scrape_has('kvstore_operations_total{operation="set_ttl",outcome="error"} 1'),
                  "operation after collector outage")
            until(lambda: "still-available" in (output / "cli.log").read_text(encoding="utf-8"),
                  "successful read during collector outage")
            telemetry = panel_expression("Telemetry")
            missing = until(lambda: result if value_matches(result := grafana_query(telemetry), lambda value: value == 0)
                            else None, "Grafana missing telemetry", timeout)
            save(output / "grafana-missing.json", missing)
            print("Missing telemetry alert fired while the CLI continued to serve", flush=True)

            configure(exporter)
            prom.send_signal(signal.SIGHUP)
            until(lambda: query('up{job="kvstore"} == 1') and not any(
                entry["labels"]["alertname"] == "KVStoreTelemetryMissing" for entry in alerts()), "alert resolution")
            save(output / "resolved-alerts.json", alerts())
            restored = until(lambda: result if value_matches(result := grafana_query(telemetry), lambda value: value == 1)
                             else None, "Grafana restored telemetry")
            save(output / "grafana-restored.json", restored)
            send("exit")
            if cli.wait(timeout=5) != 0:
                raise RuntimeError("CLI did not exit successfully")
            print("Scraping recovered and the missing telemetry alert resolved", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cli", type=Path, required=True)
    parser.add_argument("--prometheus", type=Path, required=True, help="Path to the Prometheus executable")
    parser.add_argument("--grafana", type=Path, required=True, help="Grafana distribution directory")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="kvstore-monitoring-") as directory:
        exercise(args, Path(directory))


if __name__ == "__main__":
    main()
