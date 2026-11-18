"""Shared, seeded open-loop workload for replicated OLTP point updates."""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import math
import platform
import random
import statistics
import time
from uuid import uuid4


STATUSES = {'succeeded', 'rejected', 'failed', 'unresolved', 'client_capacity'}
TABLE = 'benchmark_points'


def parse_rates(value):
    rates = [int(rate) for rate in value.split(',')]
    if not rates or min(rates) <= 0 or len(set(rates)) != len(rates):
        raise ValueError('rates must be distinct positive integers')
    return rates


def add_workload_args(parser):
    parser.add_argument('--rates', type=parse_rates, default='5,20,50,100,200',
                        help='comma-separated offered updates per second')
    parser.add_argument('--seconds', type=float, default=5)
    parser.add_argument('--warmup-seconds', type=float, default=3)
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--clients', type=int, default=32)
    parser.add_argument('--rows', type=int, default=2000)
    parser.add_argument('--seed', type=int, default=20261118)
    parser.add_argument('--timeout', type=float, default=10,
                        help='client socket or operation timeout in seconds')


def validate_args(args):
    if not all(math.isfinite(value) and value > 0 for value in
               (args.seconds, args.warmup_seconds, args.timeout)):
        raise ValueError('measurement, warmup and timeout durations must be finite and positive')
    if min(args.clients, args.repetitions, args.rows) < 1 or args.rows > 10_000:
        raise ValueError('clients and repetitions must be positive and rows must be 1–10000')
    counts = [int(rate * seconds) for rate in args.rates
              for seconds in (args.seconds, args.warmup_seconds)]
    if min(counts) < 1 or max(counts) > args.rows:
        raise ValueError('each phase must offer between 1 and --rows unique updates')


def distribution(values):
    values = sorted(values)
    if not values:
        return {'count': 0, 'p50': None, 'p90': None, 'p95': None, 'p99': None, 'max': None}
    return {'count': len(values), **{
        f'p{p}': values[math.ceil(p / 100 * len(values)) - 1] for p in (50, 90, 95, 99)
    }, 'max': values[-1]}


def observe(adapter, work):
    dispatched = time.monotonic_ns()
    result = adapter.update(work['key'], work['value'], work['request_id'])
    settled = time.monotonic_ns()
    if result['status'] not in STATUSES - {'client_capacity'}:
        raise ValueError(f"invalid update status {result['status']!r}")
    return work | result | {'dispatched_ns': dispatched, 'settled_ns': settled,
                            'service_time_ns': settled - dispatched}


def measure(executor, args, adapter, rate, seconds, phase_seed, first_value):
    count = int(rate * seconds)
    rng = random.Random(phase_seed)
    # Ordered uniform times are Poisson arrivals conditioned on the phase count.
    offsets = sorted(round(rng.random() * seconds * 1e9) for _ in range(count))
    keys = rng.sample(range(args.rows), count)
    work = [{'key': key, 'value': first_value + index, 'request_id': str(uuid4())}
            for index, key in enumerate(keys)]
    start = time.monotonic_ns()
    pending, samples = set(), []
    for operation, offset in zip(work, offsets, strict=True):
        due = start + offset
        operation['scheduled_ns'] = due
        sleep_ns = due - time.monotonic_ns()
        if sleep_ns > 0:
            time.sleep(sleep_ns / 1e9)
        finished = {future for future in pending if future.done()}
        samples.extend(future.result() for future in finished)
        pending -= finished
        if len(pending) < args.clients:
            pending.add(executor.submit(observe, adapter, operation))
        else:
            samples.append(operation | {'status': 'client_capacity', 'settled_ns': time.monotonic_ns()})
    samples.extend(future.result() for future in pending)
    samples.sort(key=lambda sample: sample['value'])
    end = start + round(seconds * 1e9)
    sleep_ns = end - time.monotonic_ns()
    if sleep_ns > 0:
        time.sleep(sleep_ns / 1e9)
    successful = [sample for sample in samples if sample['status'] == 'succeeded']
    terminal = [sample for sample in samples if sample['status'] in ('succeeded', 'failed')]
    drain_end = max(end, *(sample['settled_ns'] for sample in samples))
    elapsed = (drain_end - start) / 1e9
    status_counts = Counter(sample['status'] for sample in samples)
    timely_success = sum(sample['settled_ns'] <= end for sample in successful)
    return {
        'phase_seed': phase_seed, 'start_ns': start, 'offer_end_ns': end, 'drain_end_ns': drain_end,
        'offered': count, 'offered_rps': count / seconds, 'statuses': {s: status_counts[s] for s in sorted(STATUSES)},
        'admitted_or_completed': sum(sample.get('accepted', sample['status'] == 'succeeded') for sample in samples),
        'submitted': count - status_counts['client_capacity'],
        'succeeded_within_window': timely_success, 'succeeded_within_window_rps': timely_success / seconds,
        'succeeded_rps': len(successful) / elapsed, 'terminal_rps': len(terminal) / elapsed,
        'success_latency_ms': distribution((sample['settled_ns'] - sample['scheduled_ns']) / 1e6
                                           for sample in successful),
        'success_service_time_ms': distribution(sample['service_time_ns'] / 1e6 for sample in successful),
        'client_outcome_latency_ms': distribution((sample['settled_ns'] - sample['scheduled_ns']) / 1e6
                                                  for sample in samples if 'dispatched_ns' in sample),
        'client_dispatch_delay_ms': distribution((sample['dispatched_ns'] - sample['scheduled_ns']) / 1e6
                                                 for sample in samples if 'dispatched_ns' in sample),
    }, samples


def verify(adapter, before, samples, pending_values):
    required = dict(before)
    uncertain = {}
    carried = sum(len(values) for values in pending_values.values())
    for sample in samples:
        resolution = sample.get('reconciliation', {}).get('resolution')
        key, value = sample['key'], sample['value']
        if sample['status'] == 'succeeded' or resolution == 'succeeded':
            required[key] = value
        elif sample['status'] == 'unresolved' and resolution is None:
            uncertain[key] = value
    pending = carried + len(uncertain)
    if pending > len(before):
        raise RuntimeError('snapshot verification exceeded the unresolved value limit')
    for key, value in uncertain.items():
        pending_values.setdefault(key, set()).add(value)

    actual = adapter.read(range(len(before)))
    differences = []
    prior_values_observed = []
    for key in sorted(set(required) | set(actual)):
        value = actual.get(key)
        candidates = pending_values.get(key)
        if key in required and key in actual and candidates and value in candidates:
            # Values are unique across phases. Seeing one establishes this row's
            # next baseline while the original receipt remains unresolved.
            candidates.remove(value)
            if not candidates:
                del pending_values[key]
            if uncertain.get(key) != value:
                prior_values_observed.append({'key': key, 'value': value})
            pending -= 1
        elif key not in required or key not in actual or value != required[key]:
            differences.append({'key': key, 'expected': required.get(key), 'actual': value})
    digest = hashlib.sha256(json.dumps(sorted(actual.items()), separators=(',', ':')).encode()).hexdigest()
    return {'rows': len(actual), 'sha256': digest, 'mismatches': len(differences),
            'first_mismatches': differences[:10], 'unresolved_keys': len(uncertain),
            'unresolved_keys_with_new_value': sum(actual.get(key) == value for key, value in uncertain.items()),
            'unobserved_unresolved_values_before_phase': carried,
            'unobserved_unresolved_values_after_snapshot': pending,
            'prior_unresolved_values_observed': len(prior_values_observed),
            'first_prior_unresolved_values_observed': prior_values_observed[:10]}, actual


def phase_stats(adapter):
    if not hasattr(adapter, 'stage_stats'):
        return None
    try:
        return adapter.stage_stats()
    except (OSError, RuntimeError, ValueError) as error:
        return {'error': str(error)}


def run_workload(args, adapter, output, metadata):
    validate_args(args)
    output.mkdir(parents=True, exist_ok=True)
    results = {'metadata': metadata, 'started_utc': datetime.now(timezone.utc).isoformat(),
               'driver': {'platform': platform.platform(), 'python': platform.python_version(),
                          'clock': 'monotonic_ns'},
               'settings': {field: getattr(args, field) for field in ('rates', 'seconds', 'warmup_seconds',
                            'repetitions', 'clients', 'rows', 'seed', 'timeout')},
               'workload': 'existing bigint primary-key updates to a bigint value',
               'phases': [], 'correctness_verified': False}

    def save():
        staging = output / 'results.tmp'
        staging.write_text(json.dumps(results, indent=2) + '\n')
        staging.replace(output / 'results.json')

    next_value = 1
    pending_values = {}
    try:
        seed_details = adapter.seed(args.rows)
        if seed_details is not None:
            results['seed_details'] = seed_details
        initial, before = verify(adapter, dict.fromkeys(range(args.rows), 0), [], pending_values)
        results['initial_snapshot'] = initial
        if initial['mismatches']:
            raise RuntimeError(f'seed verification failed: {initial}')
        with ThreadPoolExecutor(max_workers=args.clients) as executor:
            for rate in args.rates:
                for rep in range(args.repetitions + 1):
                    label = f'{rate}-' + (f'rep{rep}' if rep else 'warmup')
                    phase_seed = random.Random(f'{args.seed}/{rate}/{rep}').getrandbits(64)
                    before_stats = phase_stats(adapter)
                    phase, samples = measure(executor, args, adapter, rate,
                                             args.seconds if rep else args.warmup_seconds, phase_seed, next_value)
                    phase.update(label=label, rate=rate, repetition=rep, samples=f'{label}.jsonl')
                    next_value += len(samples)
                    phase['stage_stats_before'] = before_stats
                    phase['stage_stats_after'] = phase_stats(adapter)
                    if hasattr(adapter, 'summarize_stage'):
                        phase['stage_summary'] = adapter.summarize_stage(before_stats, phase['stage_stats_after'])
                    unresolved = [sample for sample in samples if sample['status'] == 'unresolved']
                    if unresolved and hasattr(adapter, 'reconcile'):
                        adapter.reconcile(unresolved)
                    resolutions = Counter(sample.get('reconciliation', {}).get('resolution') for sample in unresolved)
                    phase.update(unresolved_after_reconciliation=resolutions[None],
                                 reconciled_successes=resolutions['succeeded'],
                                 reconciled_not_applied=resolutions['not_applied'])
                    with (output / phase['samples']).open('w') as stream:
                        for sample in samples:
                            stream.write(json.dumps(sample, separators=(',', ':')) + '\n')
                    results['phases'].append(phase)
                    phase['snapshot'], before = verify(adapter, before, samples, pending_values)
                    phase['protocol_failures'] = sum('protocol_error' in sample or 'unexpected_result' in sample
                                                     for sample in samples)
                    save()
                    if phase['snapshot']['mismatches']:
                        raise RuntimeError(f'{label} snapshot verification failed: {phase["snapshot"]}')
        results['correctness_verified'] = all(
            not phase['unresolved_after_reconciliation'] and not phase['protocol_failures']
            for phase in results['phases']
        )
        results['median_succeeded_rps'] = {
            str(rate): statistics.median(phase['succeeded_rps'] for phase in results['phases']
                                        if phase['rate'] == rate and phase['repetition'] > 0)
            for rate in args.rates
        }
        return results
    except Exception as error:
        results['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        save()
        adapter.close()
