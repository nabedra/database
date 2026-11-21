"""Shared, seeded open-loop workload for replicated OLTP point updates and point gets."""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from fractions import Fraction
import hashlib
import json
import math
import platform
import random
import statistics
import time
# Not through `time`: the unit tests replace that module's clocks.
from time import process_time
from time import time_ns as wall_ns
from uuid import uuid4


STATUSES = {'succeeded', 'rejected', 'failed', 'unresolved', 'client_capacity'}
TABLE = 'benchmark_points'
OPERATIONS = ('update', 'get', 'mix')


def parse_rates(value):
    rates = []
    for text in value.split(','):
        try:
            rate = int(text)
        except ValueError:
            rate = float(text)
            if not math.isfinite(rate):
                raise ValueError('rates must be finite')
            if rate.is_integer():
                rate = int(rate)
        rates.append(rate)
    if min(rates) <= 0 or len(set(rates)) != len(rates):
        raise ValueError('rates must be distinct and positive')
    return rates


def add_workload_args(parser):
    parser.add_argument('--rates', type=parse_rates, default='5,20,50,100,200',
                        help='comma-separated offered updates per second, including fractional rates')
    parser.add_argument('--seconds', type=float, default=5)
    parser.add_argument('--warmup-seconds', type=float, default=3)
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--clients', type=int, default=32)
    parser.add_argument('--rows', type=int, default=2000)
    parser.add_argument('--seed', type=int, default=20261118)
    parser.add_argument('--timeout', type=float, default=10,
                        help='client socket or operation timeout in seconds')
    parser.add_argument('--operation', choices=OPERATIONS, default='update',
                        help='point updates (default), point gets, or a read/write mix')
    parser.add_argument('--read-fraction', type=float, default=0.95,
                        help='share of gets in --operation mix (default 0.95)')
    parser.add_argument('--stop-p99-ms', type=float,
                        help='stop the sweep after a rate whose median successful p99 exceeds this')
    parser.add_argument('--stop-on-rejection', action='store_true',
                        help='stop the sweep after a rate with any explicit rejection')


def _offered_updates(rate, seconds):
    if isinstance(rate, int):
        try:
            return int(rate * seconds)
        except OverflowError as error:
            raise ValueError('the phase update count exceeds the supported range') from error
    # Multiply the normalized decimal strings to preserve counts such as 0.29 × 100.
    return int(Fraction(str(rate)) * Fraction(str(seconds)))


def validate_args(args):
    if not all(math.isfinite(value) and value > 0 for value in
               (args.seconds, args.warmup_seconds, args.timeout)):
        raise ValueError('measurement, warmup and timeout durations must be finite and positive')
    if min(args.clients, args.repetitions, args.rows) < 1 or args.rows > 10_000:
        raise ValueError('clients and repetitions must be positive and rows must be 1–10000')
    counts = [_offered_updates(rate, seconds) for rate in args.rates
              for seconds in (args.seconds, args.warmup_seconds)]
    if min(counts) < 1 or max(counts) > args.rows:
        raise ValueError('each phase must offer between 1 and --rows unique updates')
    if not 0 <= getattr(args, 'read_fraction', 0.95) <= 1:
        raise ValueError('--read-fraction must be within [0, 1]')


def distribution(values):
    values = sorted(values)
    if not values:
        return {'count': 0, 'p50': None, 'p90': None, 'p95': None, 'p99': None, 'max': None}
    return {'count': len(values), **{
        f'p{p}': values[math.ceil(p / 100 * len(values)) - 1] for p in (50, 90, 95, 99)
    }, 'max': values[-1]}


def observe(adapter, work):
    dispatched = time.monotonic_ns()
    if work.get('op', 'update') == 'get':
        result = adapter.get(work['key'], work['request_id'])
    else:
        result = adapter.update(work['key'], work['value'], work['request_id'])
    settled = time.monotonic_ns()
    if result['status'] not in STATUSES - {'client_capacity'}:
        raise ValueError(f"invalid {work.get('op', 'update')} status {result['status']!r}")
    return work | result | {'dispatched_ns': dispatched, 'settled_ns': settled,
                            'service_time_ns': settled - dispatched}


def measure(executor, args, adapter, rate, seconds, phase_seed, first_value):
    count = _offered_updates(rate, seconds)
    rng = random.Random(phase_seed)
    # Ordered uniform times are Poisson arrivals conditioned on the phase count.
    offsets = sorted(round(rng.random() * seconds * 1e9) for _ in range(count))
    keys = rng.sample(range(args.rows), count)
    operation_kind = getattr(args, 'operation', 'update')
    read_fraction = getattr(args, 'read_fraction', 0.95)
    # Keys stay distinct within a phase, so a get never races an update of its key
    # and must return the previous snapshot's value (or a still-unresolved one).
    ops = ['get' if operation_kind == 'get' or (operation_kind == 'mix' and rng.random() < read_fraction)
           else 'update' for _ in range(count)]
    work = [{'key': key, 'value': first_value + index, 'request_id': str(uuid4()), 'op': op}
            for index, (key, op) in enumerate(zip(keys, ops, strict=True))]
    start_wall = wall_ns()
    start = time.monotonic_ns()
    cpu_start = process_time()
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
    driver_cpu_seconds = process_time() - cpu_start
    successful = [sample for sample in samples if sample['status'] == 'succeeded']
    terminal = [sample for sample in samples if sample['status'] in ('succeeded', 'failed')]
    drain_end = max(end, *(sample['settled_ns'] for sample in samples))
    elapsed = (drain_end - start) / 1e9
    status_counts = Counter(sample['status'] for sample in samples)
    timely_success = sum(sample['settled_ns'] <= end for sample in successful)
    by_operation = {}
    for op in sorted({sample.get('op', 'update') for sample in samples}):
        mine = [sample for sample in samples if sample.get('op', 'update') == op]
        op_counts = Counter(sample['status'] for sample in mine)
        by_operation[op] = {
            'offered': len(mine), 'statuses': {s: op_counts[s] for s in sorted(STATUSES)},
            'succeeded_rps': op_counts['succeeded'] / elapsed,
            'success_latency_ms': distribution((sample['settled_ns'] - sample['scheduled_ns']) / 1e6
                                               for sample in mine if sample['status'] == 'succeeded'),
        }
    return {
        'phase_seed': phase_seed, 'start_ns': start, 'offer_end_ns': end, 'drain_end_ns': drain_end,
        'start_unix_ns': start_wall, 'drain_end_unix_ns': start_wall + (drain_end - start),
        'by_operation': by_operation,
        # CPU of the whole driver process (every client thread) over the phase.
        'driver_cpu_seconds': driver_cpu_seconds,
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
    read_mismatches = []
    for sample in samples:
        if sample.get('op', 'update') == 'get':
            if sample['status'] == 'succeeded':
                key, seen = sample['key'], sample.get('read_value')
                if seen != before.get(key) and seen not in pending_values.get(key, ()):
                    read_mismatches.append({'key': key, 'expected': before.get(key), 'actual': seen})
            continue
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
    return {'rows': len(actual), 'sha256': digest, 'mismatches': len(differences) + len(read_mismatches),
            'first_mismatches': differences[:10], 'read_mismatches': len(read_mismatches),
            'first_read_mismatches': read_mismatches[:10], 'unresolved_keys': len(uncertain),
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
               'settings': {field: getattr(args, field, None) for field in ('rates', 'seconds', 'warmup_seconds',
                            'repetitions', 'clients', 'rows', 'seed', 'timeout', 'operation', 'read_fraction',
                            'stop_p99_ms', 'stop_on_rejection')},
               'workload': {
                   'update': 'existing bigint primary-key updates to a bigint value',
                   'get': 'point gets of existing bigint primary keys',
                   'mix': 'point gets and updates of existing bigint primary keys (--read-fraction gets)',
               }[getattr(args, 'operation', None) or 'update'],
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
                if results.get('stopped_after_rate') is not None:
                    break
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
                stop_p99 = getattr(args, 'stop_p99_ms', None)
                if stop_p99 is None and not getattr(args, 'stop_on_rejection', False):
                    continue
                measured = [phase for phase in results['phases'] if phase['rate'] == rate and phase['repetition'] > 0]
                p99s = [phase['success_latency_ms']['p99'] for phase in measured]
                median_p99 = statistics.median(p99 if p99 is not None else math.inf for p99 in p99s)
                rejected = sum(phase['statuses']['rejected'] for phase in measured)
                if (stop_p99 is not None and median_p99 > stop_p99) or \
                        (getattr(args, 'stop_on_rejection', False) and rejected):
                    results['stopped_after_rate'] = rate
                    results['stop_reason'] = {'median_p99_ms': None if math.isinf(median_p99) else median_p99,
                                              'rejected': rejected}
        results['correctness_verified'] = all(
            not phase['unresolved_after_reconciliation'] and not phase['protocol_failures']
            for phase in results['phases']
        )
        results['median_succeeded_rps'] = {
            str(rate): statistics.median(phase['succeeded_rps'] for phase in results['phases']
                                        if phase['rate'] == rate and phase['repetition'] > 0)
            for rate in args.rates
            if any(phase['rate'] == rate and phase['repetition'] > 0 for phase in results['phases'])
        }
        return results
    except Exception as error:
        results['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        save()
        adapter.close()
