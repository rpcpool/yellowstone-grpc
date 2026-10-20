#!/usr/bin/env python3
"""Prints one markdown table of probe latency across runs, from each run's report.json."""
import json
import sys
from pathlib import Path

KINDS = ("account", "transaction", "entry")


def main() -> None:
    runs = [Path(arg) for arg in sys.argv[1:]]
    print("| run | probe | " + " | ".join(f"{k} p50/p99/p99.9 (µs)" for k in KINDS) + " | entry >20ms |")
    print("|---|---|" + "---|" * len(KINDS) + "---|")
    for run in runs:
        report = json.loads((run / "report.json").read_text())
        rows = {(r["class"], r["compression"], r["kind"]): r for r in report["rows"]}
        for comp in ("none", "zstd"):
            cells = []
            for kind in KINDS:
                row = rows.get(("streamlab", comp, kind))
                cells.append(f"{row['p50_us']:.0f} / {row['p99_us']:.0f} / {row['p999_us']:.0f}" if row else "-")
            entry = rows.get(("streamlab", comp, "entry"))
            stalls = entry["stalls_over_20ms"] if entry else "-"
            print(f"| {run.name} | streamlab {comp} | " + " | ".join(cells) + f" | {stalls} |")


if __name__ == "__main__":
    main()
