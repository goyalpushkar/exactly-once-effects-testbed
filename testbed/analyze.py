"""Summarize results/*.csv into Markdown tables (results/summary.md).

Usage: python -m testbed.analyze   (runs on the host too: standard library only)
"""

import csv
import os
import statistics
from collections import defaultdict

from_dir = os.environ.get("TB_RESULTS", "results")


def load(name):
    path = os.path.join(from_dir, name)
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return list(csv.DictReader(f))


def mean(values):
    values = [float(v) for v in values if v not in ("", "None")]
    return statistics.mean(values) if values else float("nan")


def group(rows, keys):
    out = defaultdict(list)
    for r in rows:
        out[tuple(r[k] for k in keys)].append(r)
    return out


def table(header, rows):
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    lines += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return "\n".join(lines)


def e1(rows):
    out = []
    for (variant, workers, jitter), rs in sorted(
        group(rows, ["variant", "workers", "jitter_ms"]).items(),
        key=lambda kv: (kv[0][0], int(kv[0][1]), int(kv[0][2])),
    ):
        per_1000 = [1000 * int(r["duplicate_effects"]) / int(r["orders_released"]) for r in rs]
        out.append([variant, workers, jitter, len(rs), f"{statistics.mean(per_1000):.0f}",
                    f"{max(per_1000):.0f}", sum(int(r["missed_orders"]) for r in rs),
                    sum(int(r["deadlocks"]) for r in rs)])
    return table(["variant", "workers", "arrival window (ms)", "trials",
                  "duplicate effects per 1,000 released (mean)", "max", "missed orders (total)",
                  "deadlocks (total)"], out)


def e2(rows):
    out = []
    for (scenario, variant), rs in group(rows, ["scenario", "variant"]).items():
        n = sum(int(r["messages"]) for r in rs)
        out.append([scenario, variant, len(rs),
                    f"{1000 * sum(int(r['duplicate_effects']) for r in rs) / n:.1f}",
                    f"{1000 * sum(int(r['dropped_work']) for r in rs) / n:.1f}",
                    f"{mean(r['deliveries'] for r in rs) / mean(r['messages'] for r in rs):.2f}",
                    sum(r["completed"] != "True" for r in rs)])
    return table(["scenario", "variant", "trials", "duplicates per 1,000 messages",
                  "dropped per 1,000 messages", "deliveries per message", "incomplete trials"], out)


def e3(rows):
    out = []
    for (scenario, variant), rs in group(rows, ["scenario", "variant"]).items():
        rel = sum(int(r["releases"]) for r in rs)
        out.append([scenario, variant, len(rs),
                    f"{1000 * sum(int(r['duplicate_lines']) for r in rs) / rel:.0f}",
                    f"{mean(r['split_keys'] for r in rs):.1f}",
                    sum(int(r["missing_releases"]) for r in rs),
                    f"{mean(r['latency_median_s'] for r in rs):.2f}",
                    f"{mean(r['latency_p95_s'] for r in rs):.2f}"])
    return table(["scenario", "variant", "trials", "duplicate lines per 1,000 releases",
                  "split keys per trial (of 30)", "missing releases (total)",
                  "merge latency median (s)", "p95 (s)"], out)


def e4(rows):
    out = []
    for (variant, workers, proc_s, p), rs in sorted(
        group(rows, ["variant", "workers", "proc_s", "p_lost_delete"]).items(),
        key=lambda kv: (kv[0][0], int(kv[0][1]), float(kv[0][2]), float(kv[0][3])),
    ):
        files = sum(int(r["files"]) for r in rs)
        per = lambda k: f"{1000 * sum(int(r[k]) for r in rs) / files:.0f}"
        out.append([variant, workers, proc_s, p, len(rs), per("duplicate_processing"), per("unprocessed"),
                    per("in_error_folder"), per("stranded_in_processing"), per("false_tickets"),
                    sum(int(r["errors"]) for r in rs)])
    return table(["variant", "workers", "processing (s)", "p lost delete", "trials",
                  "duplicate processing per 1,000 files", "unprocessed per 1,000", "in error folder per 1,000",
                  "stranded in processing per 1,000", "false tickets per 1,000", "errors"], out)


def main():
    parts = ["# Testbed results summary\n"]
    for title, fn, name in (("E1: release claim", e1, "e1_claim.csv"),
                            ("E2: message ledger", e2, "e2_ledger.csv"),
                            ("E3: merge barrier", e3, "e3_merge.csv"),
                            ("E4: file claim", e4, "e4_file_claim.csv")):
        rows = load(name)
        parts.append(f"## {title}\n\n" + (fn(rows) if rows else "_no results yet_") + "\n")
    text = "\n".join(parts)
    with open(os.path.join(from_dir, "summary.md"), "w") as f:
        f.write(text)
    print(text)


if __name__ == "__main__":
    main()
