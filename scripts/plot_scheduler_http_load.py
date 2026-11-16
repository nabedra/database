#!/usr/bin/env python3.11
"""Plot completed HTTP load repetitions; requires matplotlib, excludes warm-up."""
import argparse
from collections import defaultdict
import json
import gzip
from pathlib import Path
from statistics import mean


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('artifact', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    encoded = args.artifact.read_bytes()
    evidence = json.loads(gzip.decompress(encoded) if args.artifact.suffix == '.gz' else encoded)
    if evidence['status'] != 'passed':
        raise SystemExit('refusing to plot an incomplete or failed run')
    groups = defaultdict(list)
    for benchmark in evidence.get('benchmarks', [evidence.get('benchmark')]):
        if benchmark is None:
            continue
        cap = benchmark.get('execution', {}).get('max_workers_per_request', 3)
        for run in benchmark['repetitions']:
            if not run['warmup']:
                groups[(cap, run['arrival_rate_per_second'])].append(run)
    if not groups or any(len(runs) < 3 for runs in groups.values()):
        raise SystemExit('each configuration requires at least three measured repetitions')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(3, 2, figsize=(14, 12))
    caps = sorted({cap for cap, _ in groups})
    rates = sorted({rate for _, rate in groups})
    positions = list(range(len(rates)))
    colors = ('#146B8C', '#C56A28', '#56873B')
    throughput, latency, occupancy, memory, cpu, network = axes.flat
    for cap, color in zip(caps, colors):
        repetitions = [groups[(cap, rate)] for rate in rates]
        throughput.plot(positions, [mean(run['throughput_per_second'] for run in runs) for runs in repetitions],
                        color=color, marker='o', label=f'Request cap {cap}')
        for x, runs in enumerate(repetitions):
            throughput.scatter([x] * len(runs), [run['throughput_per_second'] for run in runs], color=color, alpha=.4)
        cpu.plot(positions, [mean(sum(run['metrics']['cpu_seconds'].values()) / run['elapsed_seconds'] for run in runs)
                            for runs in repetitions], color=color, marker='o', label=f'Request cap {cap}')
        network.plot(positions, [mean(sum(run['metrics'][key] for key in (
                            'worker_transport_request_bytes', 'worker_transport_response_bytes',
                            'storage_transport_request_bytes', 'storage_transport_response_bytes')) / (1 << 20)
                            for run in runs) for runs in repetitions], color=color, marker='o', label=f'Request cap {cap}')
    throughput.set(title='Completed throughput and repetition variation', ylabel='Requests per second')
    throughput.legend(frameon=False)
    keys = sorted(groups)
    samples = [[(request['completed'] - request['sent']) * 1000
                for run in groups[key] for request in run['requests'] if request['accepted']] for key in keys]
    latency.boxplot(samples, showfliers=True)
    latency.set_xticks(list(range(1, len(keys) + 1)))
    latency.set_xticklabels([f'Cap {cap}\n{rate}/s' for cap, rate in keys])
    latency.set(title='Client completion latency distributions', ylabel='Milliseconds', yscale='log')
    highest_cap = max(caps)
    nodes = sorted({node for rate in rates for run in groups[(highest_cap, rate)]
                    for node in run['metrics']['peak_sampled_rss_bytes']})
    for node, color in zip(nodes, colors):
        label = 'Worker ' + str(int(node.replace('-', ''), 16))
        memory.plot(positions, [max(run['metrics']['peak_sampled_rss_bytes'].get(node, 0)
                                   for run in groups[(highest_cap, rate)]) / (1 << 20) for rate in rates],
                    marker='o', color=color, label=label)
        occupancy_values = []
        for rate in rates:
            observed = [run['metrics']['heartbeat_metrics'][node]['mean_sampled_slot_occupancy']
                        for run in groups[(highest_cap, rate)] if node in run['metrics'].get('heartbeat_metrics', {})]
            occupancy_values.append(mean(observed) if observed else float('nan'))
        occupancy.plot(positions, occupancy_values, marker='o', color=color, label=label)
    memory.set(title=f'Peak sampled worker RSS, request cap {highest_cap}', ylabel='MiB')
    memory.legend(frameon=False)
    occupancy.set(title=f'Heartbeat compute reservations, request cap {highest_cap}', ylabel='Mean sampled occupied slot fraction', ylim=(0, 1))
    occupancy.legend(frameon=False)
    cpu.set(title='Worker process CPU time / wall time', ylabel='CPU core equivalents')
    cpu.legend(frameon=False)
    network.set(title='Actual worker and object-store TCP bytes', ylabel='MiB per repetition, headers included')
    network.legend(frameon=False)
    for axis in (throughput, occupancy, memory, cpu, network):
        axis.set_xticks(positions)
        axis.set_xticklabels([str(rate) for rate in rates])
    for axis in axes.flat:
        axis.set_xlabel('Offered requests per second')
        axis.grid(axis='y', alpha=.2)
        axis.spines['top'].set_visible(False)
        axis.spines['right'].set_visible(False)
    accepted = sum(run['accepted'] for runs in groups.values() for run in runs)
    rejected = sum(run['rejections'] for runs in groups.values() for run in runs)
    missing = sum(run['missing'] for runs in groups.values() for run in runs)
    duplicates = sum(run['duplicate_terminal'] for runs in groups.values() for run in runs)
    fig.suptitle('Real API/worker HTTP workload with local conditional storage, warm-up excluded\n'
                 f'{accepted} accepted/completed | {rejected} rejected | {missing} missing | {duplicates} duplicate outcomes\n'
                 'Heartbeat and RSS samples are separate from exact resource/pool benchmark measurements', fontsize=12)
    fig.tight_layout(rect=(0, 0, 1, .91))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output, dpi=150)
    print(args.output)


if __name__ == '__main__':
    main()
