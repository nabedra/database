#!/usr/bin/env python3.11
"""Plot point-update latency and goodput from benchmark_oltp_workload results."""

import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
from statistics import median


COLORS = ('#9E491E', '#246488', '#337052', '#70528C', '#A14669', '#555B65')
MARKERS = ('o', 's', '^', 'D', 'P', 'v')


def measured(path):
    result = json.loads(path.read_text())
    expected = {
        (rate, repetition)
        for rate in result['settings']['rates']
        for repetition in range(1, result['settings']['repetitions'] + 1)
    }
    phases = [phase for phase in result['phases'] if phase['repetition'] > 0]
    actual = {(phase['rate'], phase['repetition']) for phase in phases}
    if not expected or actual != expected or len(phases) != len(expected) or result.get('error'):
        raise ValueError(f'{path} does not contain a complete measurement sweep')
    rates, totals = defaultdict(list), Counter()
    for phase in phases:
        rates[phase['rate']].append(phase)
        totals.update(phase['statuses'])
        totals['offered'] += phase['offered']
    return result['metadata']['engine'], totals, [rates[rate] for rate in sorted(rates)]


def plot_series(axis, rates, observations, color, marker, label):
    centers = [median(values) if values else float('nan') for values in observations]
    for rate, values in zip(rates, observations, strict=True):
        if len(values) > 1:
            axis.vlines(rate, min(values), max(values), color=color, alpha=.5, linewidth=1.5)
    line, = axis.plot(rates, centers, color=color, marker=marker, markersize=6,
                      linewidth=1.6, label=label, zorder=3)
    return line


def render(series, output):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    notes = []
    includes_zero_goodput = any(
        phase['succeeded_rps'] == 0 for _, _, groups in series for phases in groups for phase in phases
    )
    includes_missing_latency = False
    fig, (latency, goodput) = plt.subplots(1, 2, figsize=(11, 5.3 + .24 * len(series)), sharex=True)
    legend_rows = (len(series) + 3) // 4
    fig.subplots_adjust(left=.09, right=.975, top=.75 - .045 * (legend_rows - 1),
                        bottom=.24 + .025 * len(series), wspace=.28)
    fig.text(.09, .94, 'Replicated OLTP point updates', fontsize=19, weight='bold')
    fig.text(.09, .89, 'Points show medians across repetitions. Whiskers show the observed range. Warm-up excluded.',
             fontsize=10, color='#464D59')
    latency.axhline(10, color='#666D78', linewidth=1, linestyle='--', zorder=0)
    latency.text(.025, 10, '10 ms target', transform=latency.get_yaxis_transform(),
                 va='bottom', color='#545B66', fontsize=9)
    latency.set(title='Successful update latency', ylabel='p99 latency (ms)', yscale='log')
    goodput.set(title='Completed update throughput', ylabel='Successful updates per second',
                yscale='linear' if includes_zero_goodput else 'log')
    if includes_zero_goodput:
        goodput.set_yscale('symlog', linthresh=.1)
        goodput.set_ylim(bottom=-.015)

    handles, offered_rates = [], set()
    for index, (label, totals, groups) in enumerate(series):
        color, marker = COLORS[index % len(COLORS)], MARKERS[index % len(MARKERS)]
        rates = [median(phase['offered_rps'] for phase in phases) for phases in groups]
        tails = [[p99 for phase in phases if (p99 := phase['success_latency_ms']['p99']) is not None]
                 for phases in groups]
        completions = [[phase['succeeded_rps'] for phase in phases] for phases in groups]
        offered_rates.update(rates)
        handles.append(plot_series(latency, rates, tails, color, marker, label))
        plot_series(goodput, rates, completions, color, marker, label)
        empty = [rate for rate, p99 in zip(rates, tails, strict=True) if not p99]
        if empty:
            includes_missing_latency = True
            latency.scatter(empty, [.025 + .035 * index] * len(empty), marker='x', color=color,
                            transform=latency.get_xaxis_transform(), zorder=4)
        summary = f"{totals['succeeded']:,}/{totals['offered']:,} succeeded"
        details = (('rejected', 'rejected'), ('client_capacity', 'withheld by client limit'),
                   ('failed', 'failed'), ('unresolved', 'unresolved at timing cutoff'))
        counts = [summary, *(f'{totals[key]:,} {description}' for key, description in details if totals[key])]
        notes.append((color, f'{label}   ' + ', '.join(counts)))

    for axis in (latency, goodput):
        axis.set(xscale='log', xlabel='Offered updates per second')
        if len(offered_rates) <= 10:
            axis.set_xticks(sorted(offered_rates))
            axis.xaxis.set_major_formatter(FuncFormatter(lambda value, _: f'{value:g}'))
        axis.grid(which='major', axis='both', color='#D6DAE0', linewidth=.6)
        axis.set_axisbelow(True)
        axis.spines[['top', 'right']].set_visible(False)
        axis.spines[['bottom', 'left']].set_color('#A1A7B0')
    fig.legend(handles=handles, loc='upper left', bbox_to_anchor=(.083, .865), frameon=False,
               ncols=min(len(series), 4), handlelength=2, columnspacing=1.45)
    for index, (color, note) in enumerate(notes):
        fig.text(.09, .045 + .035 * (len(notes) - index), note, fontsize=9, color=color)
    method = 'Latency includes client scheduling delay. Throughput includes drain time.'
    if includes_missing_latency:
        method += ' × marks a rate with zero successes.'
    if includes_zero_goodput:
        method += ' Throughput is linear near zero.'
    fig.text(.09, .035, method, fontsize=8.5, color='#464D59')
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=160)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('results', nargs='+', type=Path)
    parser.add_argument('--label', action='append', help='series label, repeated in results-file order')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.label and len(args.label) != len(args.results):
        parser.error('supply one --label per results file')
    try:
        series = [measured(path) for path in args.results]
    except (OSError, ValueError, KeyError) as error:
        parser.error(str(error))
    if args.label:
        series = [(label, *item[1:]) for label, item in zip(args.label, series, strict=True)]
    elif len({item[0] for item in series}) < len(series):
        series = [(path.parent.name, *item[1:]) for path, item in zip(args.results, series, strict=True)]
    render(series, args.output)
    print(args.output)


if __name__ == '__main__':
    main()
