#!/usr/bin/env python3
"""Compare two version-1 kvstore_bench JSON artifacts in a standalone HTML file."""

import argparse
import html
import json
from collections import defaultdict
from pathlib import Path

LATENCY = ("mean_seconds", "p50_seconds", "p95_seconds", "p99_seconds")
ENV_REVISION = {"revision", "source_state_at_configure"}
ENV_BUILD = {"shards", "shard_routing", "compiler", "build_type"}


def load(path):
    if path.is_dir():
        path /= "results.json"
    run = json.loads(path.read_text(encoding="utf-8"))
    if run["schema_version"] != 1:
        raise ValueError(f"{path} has an unsupported schema version")
    rows = {}
    for workload in run["workloads"]:
        key = json.dumps(workload["parameters"], sort_keys=True)
        if key in rows:
            raise ValueError(f"{path} contains duplicate workload parameters")
        rows[key] = workload
    return run, rows


def fmt(value, scale=1):
    if value is None:
        return "—"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f"{value * scale:,.4g}"
    return str(value)


def delta(left, right, allowed):
    if allowed and left and right is not None:
        return f"{(right / left - 1) * 100:+.2f}%"
    return "—"


def table(headers, rows):
    def tag(name, cells):
        return "<tr>" + "".join(f"<{name}>{html.escape(str(cell))}</{name}>" for cell in cells) + "</tr>"

    return ("<div class='scroll'><table><thead>" + tag("th", headers) + "</thead><tbody>"
            + "".join(tag("td", row) for row in rows) + "</tbody></table></div>")


def spread(summary):
    return " / ".join(fmt(summary.get(f"{name}_ops_per_second")) for name in ("min", "median", "max"))


def title(params):
    return f"{params['workload']} · {params['distribution']} · {params['threads']} worker(s)"


def trial_table(workload):
    if workload is None:
        return "<p>Missing workload.</p>"
    rows = []
    for trial in workload["trials"]:
        rows.append((trial["trial"], trial["passed"], fmt(trial["duration_seconds"]),
                     fmt(trial["ops_per_second"]), fmt(trial["attempted_ops_per_second"]),
                     trial["misses"], trial["failed_operations"],
                     fmt(trial["maintenance_seconds"], 1000), "; ".join(trial["error_messages"])))
    storage = []
    for trial in workload["trials"]:
        resource = trial["resources"]
        latency = resource.get("wal_sync_latency") or {}
        storage.append((trial["trial"], fmt(resource.get("wal_sync_count")),
                        fmt(resource.get("failed_wal_sync_count")), fmt(latency.get("mean_seconds"), 1e6),
                        fmt(resource.get("bytes_written")), fmt(resource.get("compaction_bytes_processed")),
                        fmt(resource.get("final_wal_bytes")), fmt(resource.get("filesystem_available_bytes")),
                        fmt(resource.get("measured_process_cpu_seconds")), fmt(resource.get("process_lifetime_peak_rss_bytes"))))
    operations = []
    for trial in workload["trials"]:
        for name, op in trial["operations"].items():
            if not op["attempted"]:
                continue
            latency = op["latency"]
            operations.append((trial["trial"], name, op["ok"], op["misses"], op["errors"],
                               fmt(op["ops_per_second"]), latency["samples"],
                               *(fmt(latency[key], 1e6) for key in LATENCY)))
    return (
        table(("Trial", "Passed", "Seconds", "Ops/s", "Attempted/s", "Misses", "Failures",
               "Maintenance (ms)", "Errors"), rows)
        + table(("Trial", "Operation", "OK", "Misses", "Errors", "Ops/s", "Samples", "Mean (µs)",
                 "p50 (µs)", "p95 (µs)", "p99 (µs)"), operations)
        + table(("Trial", "WAL syncs", "Sync failures", "Mean sync (µs)", "Bytes written",
                 "Compaction output (B)", "Final WAL (B)", "Filesystem available (B)", "Process CPU (s)",
                 "Process peak RSS (B)"), storage)
    )


def chart(points, xlabel, ylabel, label, connect=False):
    """Render a labeled linear scatter plot from (run, x, y, description) tuples."""
    width, height = 510, 235
    x_max = max((point[1] for point in points), default=0)
    y_max = max((point[2] for point in points), default=0)
    if not points:
        return ""
    left, right, top, bottom = 77, 485, 16, 181
    x_max, y_max = x_max or 1, y_max or 1
    coordinates = defaultdict(list)
    svg = [f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(label)}">']
    for tick in range(5):
        fraction = tick / 4
        x = left + (right - left) * fraction
        y = bottom - (bottom - top) * fraction
        svg.append(f'<path class="grid" d="M{x} {top}V{bottom} M{left} {y}H{right}"/>')
        svg.append(f'<text x="{x}" y="198" text-anchor="middle">{fmt(x_max * fraction)}</text>')
        svg.append(f'<text x="{left - 7}" y="{y + 4}" text-anchor="end">{fmt(y_max * fraction)}</text>')
    for run, x, y, detail in points:
        px = left + (right - left) * x / x_max
        py = bottom - (bottom - top) * y / y_max
        coordinates[run].append((px, py, detail))
    for run, values in coordinates.items():
        values.sort()
        if connect:
            path = " ".join(f"{x},{y}" for x, y, _ in values)
            svg.append(f'<polyline class="{run}" points="{path}"/>')
        for x, y, detail in values:
            svg.append(f'<circle class="{run}" cx="{x}" cy="{y}" r="4">'
                       f'<title>{html.escape(detail)}</title></circle>')
    svg.append(f'<text x="280" y="224" text-anchor="middle">{xlabel}</text>')
    svg.append(f'<text x="17" y="98" text-anchor="middle" transform="rotate(-90 17 98)">{ylabel}</text></svg>')
    return f"<figure><figcaption>{html.escape(label)}</figcaption>{''.join(svg)}</figure>"


def compare(baseline, candidate, allow_mode_change, allow_engine_change):
    before, before_rows = baseline
    after, after_rows = candidate
    env_before, env_after = before["environment"], after["environment"]
    fields = sorted(env_before.keys() | env_after.keys())
    env_rows = [(key, fmt(env_before.get(key)), fmt(env_after.get(key)),
                 "Changed" if env_before.get(key) != env_after.get(key) else "") for key in fields]
    changed = {key for key in fields if env_before.get(key) != env_after.get(key)}
    host_diff = changed - ENV_REVISION - ENV_BUILD
    mode_before, mode_after = before["telemetry_mode"], after["telemetry_mode"]
    engine_before, engine_after = before.get("engine_metrics", False), after.get("engine_metrics", False)
    engine_diff = engine_before != engine_after
    mode_diff = mode_before != mode_after
    mode_allowed = allow_mode_change and {mode_before, mode_after} == {"throughput", "latency"}
    run_reason = ("Host metadata differs" if host_diff else
                  "Build or shard configuration differs" if changed & ENV_BUILD else
                  "Both instrumentation settings differ" if mode_diff and engine_diff else
                  "Telemetry mode differs" if mode_diff and not mode_allowed else
                  "Engine metrics setting differs" if engine_diff and not allow_engine_change else
                  "Build metadata differs across telemetry modes"
                  if (mode_diff or engine_diff) and env_before != env_after else "")
    notice = (run_reason or ("Matched engine metrics overhead comparison" if engine_diff
                            else "Matched telemetry overhead comparison" if mode_diff
                            else "Matched-parameter baseline comparison"))
    overview = []
    details = []
    plots = defaultdict(list)
    tails = defaultdict(list)
    for key in sorted(before_rows.keys() | after_rows.keys()):
        left, right = before_rows.get(key), after_rows.get(key)
        params = (left or right)["parameters"]
        label = title(params)
        a = left["summary"] if left else {}
        b = right["summary"] if right else {}
        reason = ("Unmatched workload parameters" if not left or not right else
                  "Failed trial" if not a["passed"] or not b["passed"] else
                  "Repetition counts differ" if a["repetitions"] != b["repetitions"] else run_reason)
        comparable = not reason
        a_rate, b_rate = a.get("ops_per_second"), b.get("ops_per_second")
        overview.append((label, fmt(a_rate), fmt(b_rate), delta(a_rate, b_rate, comparable),
                         spread(a), spread(b), fmt(a.get("repetitions")), fmt(b.get("repetitions")),
                         reason or "Matched"))
        op_rows = []
        for name in sorted(a.get("operations", {}).keys() | b.get("operations", {}).keys()):
            one = a.get("operations", {}).get(name, {})
            two = b.get("operations", {}).get(name, {})
            if not one.get("attempted") and not two.get("attempted"):
                continue
            op_rows.append((name, fmt(one.get("ops_per_second")), fmt(two.get("ops_per_second")),
                            delta(one.get("ops_per_second"), two.get("ops_per_second"), comparable),
                            fmt(one.get("misses")), fmt(two.get("misses")),
                            fmt(one.get("errors")), fmt(two.get("errors")),
                            fmt(one.get("latency", {}).get("samples")), fmt(two.get("latency", {}).get("samples"))))
            for measure in LATENCY:
                x = one.get("latency", {}).get(measure)
                y = two.get("latency", {}).get(measure)
                op_rows.append((measure.removesuffix("_seconds") + " (µs)", fmt(x, 1e6),
                                fmt(y, 1e6), delta(x, y, comparable), "", "", "", "", "", ""))
        details.append(
            f"<details><summary>{html.escape(label)} · {html.escape(reason or 'Matched')}</summary>"
            f"<pre>{html.escape(json.dumps(params, indent=2))}</pre>"
            + table(("Operation / latency", "Baseline", "Candidate", "Δ", "Base misses", "Cand. misses",
                     "Base errors", "Cand. errors", "Base samples", "Cand. samples"), op_rows)
            + "<h3>Baseline trials</h3>" + trial_table(left)
            + "<h3>Candidate trials</h3>" + trial_table(right) + "</details>"
        )
        # Group by all workload parameters other than thread count.
        series = json.dumps({k: v for k, v in params.items() if k != "threads"}, sort_keys=True)
        series_label = f"{params['workload']} · {params['distribution']} · {params['keys']} keys"
        if comparable:
            for run, summary in (("baseline", a), ("candidate", b)):
                rate = summary["ops_per_second"]
                plots[(series_label, series)].append((run, params["threads"], rate, f"{run} {label}: {fmt(rate)} ops/s"))
                for name, op in summary["operations"].items():
                    p99 = op["latency"]["p99_seconds"]
                    if p99 is not None:
                        tails[(series_label, series, name)].append(
                            (run, rate, p99 * 1e6, f"{run} {label}: {fmt(p99, 1e6)} µs p99"))
    charts = [chart(points, "Data workers", "Workload ops/s", label, connect=True)
              for (label, _), points in plots.items()]
    charts += [chart(points, "Workload ops/s", "p99 (µs)", f"{label} · {op}")
               for (label, _, op), points in tails.items()]
    return (
        f"<p><strong>{html.escape(notice)}</strong></p>"
        + table(("Run", "Telemetry mode", "Engine metrics", "Passed"), [
            ("Baseline", mode_before, engine_before, before["passed"]),
            ("Candidate", mode_after, engine_after, after["passed"])])
        + "<h2>Workload throughput</h2><p>Average and spread in ops/s. Spread is min / median / max."
          " Δ is candidate relative to baseline.</p>"
        + table(("Workload", "Baseline avg", "Candidate avg", "Δ", "Baseline spread", "Candidate spread",
                 "Base trials", "Cand. trials", "Comparison"), overview)
        + ("<h2>Throughput and tail latency</h2><p><span class='baseline'>● Baseline</span> "
           "<span class='candidate'>● Candidate</span></p><div class='plots'>"
           + "".join(charts) + "</div>" if charts else "")
        + "<h2>Operations and trials</h2><p>Operation rates use the workload wall time. "
          "Latency percentiles pool samples from valid trials. Expand a workload to see individual trials.</p>"
        + "".join(details)
        + "<h2>Environment</h2>" + table(("Field", "Baseline", "Candidate", "Difference"), env_rows)
    )


HTML = """<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>KVStore benchmark comparison</title>
<style>
body{margin:2rem auto;padding:0 1rem;max-width:83rem;background:#f8fafc;color:#172438;font:15px system-ui,sans-serif}
h1{font-size:2rem} h2{margin-top:2rem} h3{font-size:1rem}
.scroll{overflow:auto;margin-bottom:1rem}
table{border-collapse:collapse;width:100%;background:white;font-variant-numeric:tabular-nums}
th,td{padding:.55rem;text-align:left;border-bottom:1px solid #dbe3ed}
th{background:#e8eef8;white-space:nowrap}
details{padding:.65rem .8rem;border:1px solid #dbe3ed;background:white;margin:.4rem 0}
summary{cursor:pointer;font-weight:600} pre{overflow:auto}
.plots{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,28rem),1fr));gap:1rem}
figure{margin:0;padding:.6rem;background:white;border:1px solid #dbe3ed}
figcaption{font-weight:600} svg{display:block;width:100%}
svg text{font-size:12px;fill:#34445d} .grid{stroke:#dbe3ed;fill:none}
.baseline{color:#2352a0} .candidate{color:#a13727}
circle{fill:currentColor} polyline{stroke:currentColor;fill:none;stroke-width:2}
</style>
<main><h1>KVStore benchmark comparison</h1>
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path, help="JSON file or directory containing results.json")
    parser.add_argument("candidate", type=Path, help="JSON file or directory containing results.json")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--compare-modes", action="store_true", help="Allow a throughput vs latency comparison")
    parser.add_argument("--compare-engine-metrics", action="store_true", help="Allow a comparison of engine metrics off vs on")
    args = parser.parse_args()
    try:
        content = compare(load(args.baseline), load(args.candidate), args.compare_modes, args.compare_engine_metrics)
        args.output.write_text(HTML + content + "</main></html>\n", encoding="utf-8")
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(1, f"compare.py: {error}\n")


if __name__ == "__main__":
    main()
