#!/usr/bin/env python3
"""Plot measured resource/pool benchmark repetitions. Requires matplotlib."""
import argparse
import json
from collections import defaultdict
from pathlib import Path
from statistics import mean


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    artifact = json.loads(args.artifact.read_text())
    runs = [run for run in artifact["runs"] if not run["warmup"]]
    if not runs:
        raise SystemExit("artifact contains no measured repetitions")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    groups = defaultdict(list)
    for run in runs:
        key = (run["case"], run["request_node_cap"], run["arrival_request_interval_us"])
        groups[key].append(run)
    if any(len(group) < 3 for group in groups.values()):
        raise SystemExit("each configuration needs at least three measured repetitions")
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    colors = ["#146B8C", "#C56A28", "#56873B"]
    cpu = axes[0, 0]
    for node, color in enumerate(colors):
        values = [mean(run["per_worker_compute_slot_occupancy"][node]
                       for run in groups[("B01", cap, 0)]) for cap in (1, 2, 3)]
        cpu.bar([cap + (node - 1) * .24 for cap in (1, 2, 3)], values,
                width=.23, color=color, label=f"Node {node}, {[1, 2, 4][node]} threads")
    cpu.set(xticks=[1, 2, 3], ylim=(0, 1), xlabel="Request compute-node cap",
            ylabel="Occupied compute slots / configured slots", title="B01 real executor occupancy")
    cpu.legend(frameon=False)

    throughput = axes[0, 1]
    intervals = [0, 250, 1000]
    for cap, color in zip((1, 2, 3), colors):
        means = [mean(run["completed_jobs_per_second"] for run in groups[("B02", cap, arrival)])
                 for arrival in intervals]
        throughput.plot(range(3), means, marker="o", color=color, label=f"Request cap {cap}")
        for index, arrival in enumerate(intervals):
            values = [run["completed_jobs_per_second"] for run in groups[("B02", cap, arrival)]]
            throughput.scatter([index] * len(values), values, color=color, alpha=.35, s=16)
    throughput.set(xticks=range(3), xticklabels=["Burst", "250 us", "1,000 us"],
                   xlabel="Interval between offered requests", ylabel="Completed partition jobs / second",
                   title="B02 mixed workload, measured repetitions")
    throughput.legend(frameon=False)

    latency = axes[1, 0]
    keys = sorted(groups)
    samples = [[value / 1e6 for run in groups[key] for value in run["job_completion_ns"]["raw"]]
               for key in keys]
    labels = [f"{case}\ncap {cap}\n{interval}us" for case, cap, interval in keys]
    latency.boxplot(samples, labels=labels, showfliers=False)
    latency.set(yscale="log", ylabel="Completion latency, milliseconds, log scale",
                title="Accepted job latency distributions, warmup excluded")
    latency.tick_params(axis="x", labelsize=8)

    usage = axes[1, 1]
    cpu_values = [run["process_cpu_core_equivalents"] for run in runs]
    quota = artifact["cpu_quota"]
    available = quota.get("quota_cores") or quota.get("available_parallelism")
    usage.plot(range(len(runs)), cpu_values, marker=".", color=colors[0], label="Measured process CPU cores")
    if available:
        usage.axhline(available, color="#777777", linestyle="--", label="Recorded quota/available cores")
    usage.set(xlabel="Measured repetition", ylabel="Process CPU seconds / wall seconds",
              title="Process CPU includes driver and control overhead")
    usage.legend(frameon=False)
    failed = sum(run["failed_jobs"] for run in runs)
    lost = sum(run["lost_identities"] for run in runs)
    duplicates = sum(run["duplicate_identities"] for run in runs)
    rejected = sum(run["rejected_requests"] for run in runs)
    peak_rss = max(run["process_peak_rss_bytes"] for run in runs) / 2**20
    fig.suptitle("MeshDB resource/pool microbenchmark\n"
                 f"Actual measured runs: {len(runs)} | failed jobs {failed} | rejected requests {rejected} | "
                 f"lost {lost} | duplicate {duplicates} | process RSS high-water {peak_rss:.1f} MiB", fontsize=13)
    for axis in axes.flat:
        axis.grid(axis="y", alpha=.2)
        axis.spines["top"].set_visible(False)
        axis.spines["right"].set_visible(False)
    fig.tight_layout(rect=[0, 0, 1, .91])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output)
    print(args.output)


if __name__ == "__main__":
    main()
