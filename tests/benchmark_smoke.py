"""Exercise the benchmark artifact and compare reports with deterministic checks."""

import json
import math
import resource
import signal
import subprocess
import sys
import tempfile
from pathlib import Path


def run(directory, *args, preexec_fn=None):
    result = subprocess.run(
        [sys.argv[1], "--smoke", "--temp-root", str(directory), "--json", "-", *args],
        capture_output=True, text=True, preexec_fn=preexec_fn, check=False,
    )
    return result, json.loads(result.stdout)


def check_trial(trial, latency_enabled):
    elapsed = trial["duration_seconds"]
    assert elapsed > 0
    assert math.isclose(trial["completed"] / elapsed, trial["ops_per_second"])
    assert math.isclose(trial["attempted"] / elapsed, trial["attempted_ops_per_second"])
    for counts in trial["operations"].values():
        latency = counts["latency"]
        assert counts["attempted"] == counts["ok"] + counts["misses"] + counts["errors"]
        assert counts["completed"] == counts["ok"] + counts["misses"]
        assert math.isclose(counts["completed"] / elapsed, counts["ops_per_second"])
        assert latency["samples"] == (counts["attempted"] if latency_enabled else 0)
        if latency["samples"]:
            assert latency["p50_seconds"] <= latency["p95_seconds"] <= latency["p99_seconds"]
            assert latency["mean_seconds"] >= 0
        else:
            assert all(value is None for key, value in latency.items() if key != "samples")


def limit_wal_size():
    signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
    # Allow one complete 109-byte SET record during setup, then fail writes to that shard.
    resource.setrlimit(resource.RLIMIT_FSIZE, (109, 109))


with tempfile.TemporaryDirectory(prefix="kvstore-bench-test-") as root:
    root = Path(root)
    for mode in ("throughput", "latency"):
        result, data = run(root, "--mode", mode)
        assert result.returncode == 0, result.stderr
        assert data["schema_version"] == 1 and data["passed"]
        assert len(data["workloads"]) == 17
        assert sum(t["misses"] for w in data["workloads"] for t in w["trials"]) > 0
        for workload in data["workloads"]:
            trials = workload["trials"]
            summary = workload["summary"]
            assert summary["repetitions"] == summary["valid_trials"] == 2
            assert math.isclose(sum(t["completed"] for t in trials) / sum(t["duration_seconds"] for t in trials),
                                summary["ops_per_second"])
            for trial in trials:
                assert trial["passed"]
                check_trial(trial, mode == "latency")
                if workload["parameters"]["workload"] == "compaction":
                    assert trial["operations"]["compact"]["ok"] == 1
                    assert trial["maintenance_seconds"] > 0
            for name, counts in summary["operations"].items():
                assert counts["latency"]["samples"] == sum(t["operations"][name]["latency"]["samples"] for t in trials)
        assert not list(root.glob("kvstore-bench-*")), "trial databases should be cleaned up"
        (root / f"{mode}.json").write_text(result.stdout, encoding="utf-8")

    report = root / "report.html"
    command = [sys.executable, sys.argv[2], str(root / "throughput.json"), str(root / "latency.json"),
               "--output", str(report)]
    subprocess.run(command, check=True)
    assert "Telemetry mode differs" in report.read_text(encoding="utf-8")
    subprocess.run([*command, "--compare-modes"], check=True)
    content = report.read_text(encoding="utf-8")
    assert "Matched telemetry overhead comparison" in content
    assert "<svg" in content and "Candidate trials" in content and "Base samples" in content

    result, recorded = run(root, "--workload", "compaction", "--threads", "1", "--trials", "1",
                           "--engine-metrics", "on")
    assert result.returncode == 0 and recorded["engine_metrics"]
    for workload in recorded["workloads"]:
        resources = workload["trials"][0]["resources"]
        # Four appends and one next-WAL sync per shard.
        assert resources["wal_sync_count"] == 4 + 16
        assert resources["wal_sync_latency"]["samples"] == 4 + 16
        assert resources["compaction_bytes_processed"] == 32 * 109 + 16 * (8 + 52)
        assert resources["bytes_written"] == resources["compaction_bytes_processed"] + 4 * 109

    result, failed = run(root, "--workload", "write", "--keys", "1", "--threads", "1",
                         "--trials", "1", "--mode", "latency", "--engine-metrics", "on", preexec_fn=limit_wal_size)
    assert result.returncode == 1 and failed["passed"] is False, result.stderr
    for workload in failed["workloads"]:
        trial = workload["trials"][0]
        assert trial["attempted"] == trial["errors"] == 20
        assert trial["operations"]["set"]["latency"]["samples"] == 20
        assert trial["completed"] == 0 and trial["failure_rate"] == 1
        assert trial["resources"]["wal_sync_count"] == 20  # Rollback syncs only.
        assert trial["resources"]["failed_wal_sync_count"] == 0
        assert trial["passed"] is False and trial["error_messages"]
        assert workload["summary"]["ops_per_second"] is None
        check_trial(trial, True)
