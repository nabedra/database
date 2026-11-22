#!/usr/bin/env python3
"""PERF1 (hardening): bucket perf folded stacks (perf-folded-<container>.txt) by stage.

usage: stacks.py <perf-out-dir> <frequency-hz> <record-seconds> <updates-per-second>

Each sample goes to the leaf-most frame that matches a stage pattern below; a sample whose
leaf is in the kernel is reported as kernel time under that stage. CPU ms per update is
samples / frequency / (record seconds x rate).
"""
import re
import sys
from collections import Counter
from pathlib import Path

STAGES = [  # (stage, pattern), matched on each frame from the leaf up; first stage that matches wins
    ('tls', r'rustls|aws_lc|ring::|tokio_rustls'),
    ('json', r'serde_json|serde::'),
    ('http', r'hyper::|h2::|axum::|http_body|oltp_runtime::http::'),
    ('routing/topology', r'application::direct::|TopologyCache|route_keys|::scan'),
    ('gateway rpc / transport', r'multiplex::|service_rpc::|typed_network::'),
    ('raft', r'openraft|oltp_replication::hosted|typed_log'),
    ('leader execution', r'oltp_runtime::direct::|mvcc::'),
    ('storage (nabedra-kv)', r'nabedra_kv'),
    ('native executor hops', r'oltp::pool::|NativeExecutor|native::'),
    ('tokio scheduling', r'tokio::'),
    ('allocator', r'^(malloc|free|realloc|calloc|_int_malloc|_int_free|cfree|__libc_)'),
]
KERNEL = re.compile(r'^[a-z_0-9.]+$')


def bucket(stack):
    frames = stack.split(';')
    kernel_leaf = bool(KERNEL.match(frames[-1])) and '::' not in frames[-1]
    for frame in reversed(frames):
        for stage, pattern in STAGES:
            if re.search(pattern, frame):
                return f'{stage} (kernel)' if kernel_leaf else stage
    return 'kernel (other)' if kernel_leaf else 'other'


def main(directory, frequency, seconds, rate):
    for path in sorted(Path(directory).glob('perf-folded-*.txt')):
        container = path.stem.removeprefix('perf-folded-')
        counts, total = Counter(), 0
        for line in path.open():
            stack, _, n = line.rstrip('\n').rpartition(' ')
            if not stack:
                continue
            counts[bucket(stack)] += int(n)
            total += int(n)
        updates = float(seconds) * float(rate)
        per_update = total / float(frequency) / updates * 1000 if updates else 0
        print(f'{container}: {total} samples, {total / float(frequency) / float(seconds):.3f} CPU, '
              f'{per_update:.2f} ms per update')
        for stage, n in counts.most_common():
            print(f'  {stage:32s} {100 * n / total:5.1f}%  {n / float(frequency) / updates * 1000 if updates else 0:6.3f} ms/update')


if __name__ == '__main__':
    main(*sys.argv[1:5])
