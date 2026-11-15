#!/usr/bin/env python3.11
"""Validate complementary pool and real HTTP benchmark evidence, then summarize."""
import argparse
from collections import defaultdict
import hashlib
import gzip
import json
from pathlib import Path
from statistics import mean


def load(path):
    encoded = path.read_bytes()
    decoded = gzip.decompress(encoded) if path.suffix == '.gz' else encoded
    return json.loads(decoded), dict(file=path.name, sha256=hashlib.sha256(encoded).hexdigest(), json_sha256=hashlib.sha256(decoded).hexdigest())


def measured_groups(runs, key):
    groups = defaultdict(list)
    for run in runs:
        groups[key(run)].append(run)
    for configuration, values in groups.items():
        assert len([run for run in values if run['warmup']]) >= 1, ('missing warmup', configuration)
        assert len([run for run in values if not run['warmup']]) >= 3, ('missing measured repetitions', configuration)
    return {configuration: [run for run in values if not run['warmup']] for configuration, values in groups.items()}


def summarize(pool_path, http_path, output):
    pool, pool_source = load(pool_path)
    http, http_source = load(http_path)
    assert http['status'] == 'passed', 'HTTP benchmark did not pass its oracle/conservation assertions'
    events = pool['work_conserving_assertions']
    assert events['feasible_idle_slots_after_processed_event'] == 0
    assert events['checked_release_events'] == 89 and events['settled_reservations'] == 96
    assert pool['cpu_quota']['available_parallelism'] >= 1
    pool_groups = measured_groups(pool['runs'], lambda run: (run['case'], run['request_node_cap'], run['arrival_request_interval_us']))
    assert {key[1] for key in pool_groups if key[0] == 'B01'} == {1, 2, 3}
    assert {key[1] for key in pool_groups if key[0] == 'B02'} == {1, 2, 3}
    assert len({key[2] for key in pool_groups if key[0] == 'B02'}) >= 3
    pool_summary = []
    for (case, cap, interval), runs in sorted(pool_groups.items()):
        for run in runs:
            assert run['failed_jobs'] == run['lost_identities'] == run['duplicate_identities'] == 0, run
            assert run['completed_jobs'] == run['accepted_jobs'] == run['succeeded_jobs'] > 0, run
            assert len(run['per_worker_compute_slot_occupancy']) == len(run['per_worker_peak_charged_bytes']) == 3
            assert all(0 <= fraction <= 1.001 for fraction in run['per_worker_compute_slot_occupancy'])
            assert run['queue_wait_ns']['samples'] == run['job_completion_ns']['samples'] == run['completed_jobs']
            assert run['process_cpu_seconds'] > 0 and run['process_peak_rss_bytes'] > 0
        pool_summary.append(dict(case=case, cap=cap, arrival_interval_us=interval, repetitions=len(runs),
            completed_jobs=sum(run['completed_jobs'] for run in runs),
            mean_jobs_per_second=mean(run['completed_jobs_per_second'] for run in runs),
            mean_process_cpu_core_equivalents=mean(run['process_cpu_core_equivalents'] for run in runs),
            mean_slot_occupancy=[mean(run['per_worker_compute_slot_occupancy'][node] for run in runs) for node in range(3)]))
    http_summary = []
    http_caps, http_rates = set(), set()
    for benchmark in http['benchmarks']:
        cap = benchmark['execution']['max_workers_per_request']
        http_caps.add(cap)
        groups = measured_groups(benchmark['repetitions'], lambda run: run['arrival_rate_per_second'])
        http_rates.update(groups)
        for rate, runs in sorted(groups.items()):
            for run in runs:
                assert run['missing'] == run['duplicate_terminal'] == 0
                assert run['completed'] == run['accepted'] > 0
                assert run['submitted'] == run['accepted'] + run['rejections']
                accepted = [request for request in run['requests'] if request['accepted']]
                assert len(accepted) == run['accepted']
                assert len({request['request_id'] for request in run['requests']}) == run['submitted']
                assert all(request['receipt_samples'][-1]['state'] == 'succeeded' for request in accepted)
                assert all(request['oracle_result_sha256'] == request['terminal_result_sha256'] for request in accepted)
                assert all(request['queue_wait_lower_seconds'] <= request['queue_wait_upper_seconds'] for request in accepted)
                metrics = run['metrics']
                assert len(metrics['heartbeat_metrics']) == len(metrics['peak_sampled_rss_bytes']) == 3
                assert all(sample['samples'] > 0 and sample['peak_charged_bytes'] > 0 for sample in metrics['heartbeat_metrics'].values())
                assert metrics['worker_transport_request_bytes'] > 0 and metrics['storage_transport_response_bytes'] > 0
                assert metrics['partition_count'] > 0
                assert metrics['replica_payloads']['per_worker']
                assert 'observed_catalog_cas_conflicts' in metrics and 'retried_partition_attempts' in metrics
            http_summary.append(dict(cap=cap, offered_rate_per_second=rate, repetitions=len(runs),
                accepted=sum(run['accepted'] for run in runs), completed=sum(run['completed'] for run in runs),
                rejected=sum(run['rejections'] for run in runs),
                mean_requests_per_second=mean(run['throughput_per_second'] for run in runs),
                mean_p95_completion_seconds=mean(run['latency_seconds']['p95'] for run in runs),
                mean_p95_queue_lower_seconds=mean(run['queue_wait_lower_seconds']['p95'] for run in runs),
                mean_p95_queue_upper_seconds=mean(run['queue_wait_upper_seconds']['p95'] for run in runs)))
    assert http_caps == {1, 2, 3} and len(http_rates) >= 3
    summary = dict(status='passed', cases=['B01', 'B02'], pool_artifact=pool_source, http_artifact=http_source,
        build=http['build'], machine=http['machine'], source_tree_sha256=http['source_tree_sha256'],
        binary_sha256=http['binary_sha256'], work_conserving_assertions=events,
        pool_measurements=pool_summary, http_measurements=http_summary,
        scopes=dict(pool='exact logical scheduler release events, actual WorkerPool occupied time, logical charged peaks and scheduler queue wait',
                    http='real API/worker/object-store TCP, serial result oracles, accepted-ID conservation, client latency and receipt queue-wait bounds',
                    sampled='persisted heartbeat CPU/byte reservations and OS RSS/CPU, with configured 250ms sample/publication intervals',
                    queue_payloads='actual successful signed Record wire request/pin/result bytes; excludes allocator metadata and unobserved local transitions'),
        limitations=['unoptimized dev profile, not release throughput',
                     'local conditional SQLite storage, not AWS S3 or a cloud provider benchmark',
                     'heartbeat samples do not prove continuous HTTP compute occupancy',
                     'HTTP server queue wait is bounded by receipt observations; exact scheduler wait is measured in the pool benchmark'])
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, indent=2) + '\n')
    print(output)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pool', type=Path, required=True)
    parser.add_argument('--http', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    summarize(args.pool, args.http, args.output)
