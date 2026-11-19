"""Plot the four 64 KiB TLS cells from the paired actor comparison."""

import argparse
import json
from pathlib import Path
import statistics

import matplotlib.pyplot as plt


def load(root, variant, engine, workload, rate, quantile):
    paths = (root / variant).glob(f"*--{workload}--*--rate{rate}--pair*--{engine}.json")
    values = [json.loads(path.read_text())["latency_ns"][quantile] / 1e6 for path in paths]
    if len(values) != 3:
        raise ValueError("expected three independent repeats per engine and workload")
    return statistics.median(values), min(values), max(values)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw_directory", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    profiles = [
        ("rerun-handoff", "nabedra", "Nabedra handoff", "#596F83"),
        ("rerun-handoff", "valkey", "Valkey paired with handoff", "#A4B1BA"),
        ("inline", "nabedra", "Nabedra inline", "#125A64"),
        ("inline", "valkey", "Valkey paired with inline", "#D19241"),
    ]
    cases = [("get-hit", 400), ("get-hit", 800), ("set-px", 400), ("set-px", 800)]
    figure, panels = plt.subplots(1, 2, figsize=(10.5, 4.8), dpi=160, layout="constrained")
    for axis, quantile in zip(panels, ("p50", "p99")):
        for row, (variant, engine, label, color) in enumerate(profiles):
            for x, (workload, rate) in enumerate(cases):
                median, low, high = load(args.raw_directory, variant, engine, workload, rate, quantile)
                axis.errorbar(
                    x + (row - 1.5) * .18, median, yerr=[[median - low], [high - median]],
                    fmt="o", color=color, capsize=3, label=label if x == 0 else None,
                )
        axis.set_xticks(range(4), ["GET\n400/s", "GET\n800/s", "SET PX\n400/s", "SET PX\n800/s"])
        axis.set_title(f"{quantile} arrival-to-completion latency")
        axis.set_ylabel("milliseconds")
        axis.set_yscale("log")
        axis.axhline(1, color="#777777", linestyle=":", linewidth=1)
        axis.grid(axis="y", alpha=.26, which="both")
    panels[0].legend(loc="upper left", fontsize=8)
    figure.suptitle("64 KiB over warm TLS in Docker\nMedian and range of three runs per cell")
    figure.savefig(args.output)


if __name__ == "__main__":
    main()
