#!/usr/bin/env python3.11
"""ST01/ST02/ST05 live ORM, pinned snapshots, and two CPU PyTorch ranks."""
import argparse
from contextlib import ExitStack, contextmanager
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile

from acceptance_scheduler import Cluster
from nabedra import ConflictError, Database, NotFoundError, StreamUnsupportedError, col

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'server/tests'))
from oltp_orm_contract import exercise, rejected
from oltp_process import Server as OltpServer


TABLE = 'stream_training'
SCHEMA = {'id': int, 'feature': float, 'label': int, 'enabled': bool}
NUM_ROWS = 129
BATCH_ROWS = 17


def example(id_):
    return {'id': id_, 'feature': id_ / 4, 'label': id_ % 5, 'enabled': id_ % 3 != 0}


def training_query(table, minimum):
    return table.where((col('id') >= minimum) & (col('enabled') == True)).select(
        'id', 'feature', 'label',
    )


def rows(query, **options):
    with query.stream(**options) as batches:
        if options.get('format') == 'arrow':
            return [row for batch in batches for row in batch.to_pylist()]
        return [row for batch in batches for row in batch]


@contextmanager
def cluster_started(binary, directory, workers=2):
    directory.mkdir()
    cluster = Cluster(binary, directory)
    try:
        for node in range(1, workers + 1):
            cluster.start_worker(node, 1)
        yield cluster.api_door.url, cluster.auth['data_token']
    finally:
        cluster.close()


def snapshot_isolation(db):
    schema = {'id': int, 'text': str}
    old = db.create_table('stream_version', schema, key='id')
    original = [{'id': id_, 'text': f'original-{id_}-é'} for id_ in range(7)]
    assert old.insert_many(reversed(original)) == len(original)
    query = old.query().order_by('id').select('text')
    expected = [{'text': row['text']} for row in original]
    with query.stream_snapshot(shards=1) as lease:
        with query.stream(batch_rows=3, prefetch_batches=0, snapshot=lease.id) as stream:
            assert next(stream) == expected[:3]
            assert old.update(4, {'text': 'new-value'})
            assert rows(query, batch_rows=3)[4] == {'text': 'new-value'}
            old.drop()
            fresh = db.create_table('stream_version', schema, key='id')
            assert fresh.id != old.id
            fresh.insert({'id': 30, 'text': 'replacement'})
            remaining = list(stream)
            assert list(map(len, remaining)) == [3, 1]
            assert [row for batch in remaining for row in batch] == expected[3:]
        assert rows(query, batch_rows=3, snapshot=lease.id) == expected
        rejected(ConflictError, lambda: rows(fresh.query().select('text'), snapshot=lease.id))
    rejected(NotFoundError, lambda: rows(query))
    assert rows(fresh.query().select('text')) == [{'text': 'replacement'}]
    fresh.drop()
    return {'pinned_rows': len(expected), 'tail': list(map(len, remaining))}


def query_batch(query, snapshot_id, rank, drop_last):
    import numpy as np
    import torch
    from torch.utils.data import DataLoader, IterableDataset

    class Batches(IterableDataset):
        def __init__(self, stream):
            super().__init__()
            self.stream = stream

        def __iter__(self):
            for batch in self.stream:
                yield tuple(torch.from_numpy(np.array(column.to_numpy(), copy=True))
                            for column in batch.columns)

    delivered, sizes = [], []
    with query.stream(batch_rows=BATCH_ROWS, format='arrow', drop_last=drop_last,
                      snapshot=snapshot_id, shard=rank) as stream:
        for ids, feature, labels in DataLoader(Batches(stream), batch_size=None, num_workers=0):
            assert ids.ndim == feature.ndim == labels.ndim == 1
            assert ids.dtype == torch.int64 and labels.dtype == torch.int64
            assert feature.dtype == torch.float64 and feature.device.type == 'cpu'
            assert torch.equal(ids.to(feature.dtype) / 4.0, feature)
            assert torch.equal(ids % 5, labels)
            delivered.extend(ids.tolist())
            sizes.append(ids.shape[0])
    return delivered, sizes


def training_rank(url):
    import torch
    import torch.distributed as dist
    from datetime import timedelta

    torch.set_num_threads(1)
    dist.init_process_group('gloo', timeout=timedelta(seconds=35))
    rank, ranks = dist.get_rank(), dist.get_world_size()
    try:
        assert ranks == 2
        db = Database(url, os.environ['NABEDRA_TEST_STREAM_TOKEN'], timeout=25)
        table = db.table(TABLE)
        reports = []
        for minimum in (44, 90):
            query = training_query(table, minimum)
            with ExitStack() as stack:
                snapshot = [None]
                oracle = None
                if rank == 0:
                    oracle = [row['id'] for row in rows(query, batch_rows=BATCH_ROWS)]
                    snapshot[0] = stack.enter_context(query.stream_snapshot(shards=ranks)).id
                dist.broadcast_object_list(snapshot, src=0)
                first = query_batch(query, snapshot[0], rank, False)
                assert query_batch(query, snapshot[0], rank, False) == first
                full = query_batch(query, snapshot[0], rank, True)
                kept = len(first[0]) // BATCH_ROWS * BATCH_ROWS
                assert full[0] == first[0][:kept] and all(n == BATCH_ROWS for n in full[1])
                outputs = [None] * ranks
                dist.all_gather_object(outputs, first)
                if rank == 0:
                    (left, left_batches), (right, right_batches) = outputs
                    assert left + right == oracle
                    assert set(left).isdisjoint(right)
                    expected = [id_ for id_ in range(minimum, NUM_ROWS) if example(id_)['enabled']]
                    assert oracle == expected
                    assert left == [id_ for id_ in expected if id_ < NUM_ROWS // ranks]
                    assert right == [id_ for id_ in expected if id_ >= NUM_ROWS // ranks]
                    if minimum == 44:
                        assert 0 < len(left) < len(right)
                    else:
                        assert not left and right
                    reports.append({'minimum': minimum, 'rows_per_rank': [len(left), len(right)],
                                    'batch_sizes': [left_batches, right_batches]})
                dist.barrier()
        if rank == 0:
            print(json.dumps({'cpu_gloo_ranks': ranks, 'rank_cases': reports}), flush=True)
    finally:
        dist.destroy_process_group()


def launch_gloo(url, token):
    args = [
        sys.executable, '-m', 'torch.distributed.run', '--standalone', '--nnodes=1',
        '--nproc-per-node=2', '--max-restarts=0', str(Path(__file__).resolve()),
        '--rank-url', url,
    ]
    with subprocess.Popen(
        args, stdout=subprocess.PIPE, text=True, start_new_session=True,
        env={**os.environ, 'NABEDRA_TEST_STREAM_TOKEN': token, 'OMP_NUM_THREADS': '1'},
    ) as process:
        try:
            output, _ = process.communicate(timeout=150)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            raise AssertionError('CPU rank subprocesses failed to settle') from None
        assert process.returncode == 0, process.returncode
        return json.loads(output.strip().splitlines()[-1])


def unsupported(url, token, database=None):
    db = Database(url, token, database=database, timeout=5)
    table = db.create_table('stream_unsupported', {'id': int}, key='id')
    try:
        for mode in ('olap', 'oltp'):
            query = Database(url, token, database=database, timeout=5, mode=mode).table(table.name).query()
            rejected(StreamUnsupportedError, lambda: rows(query))
    finally:
        table.drop()


def open_multiple_shards(query):
    with query.stream_snapshot(shards=2):
        pass


def run(args, directory):
    evidence = {}
    if args.olap_binary:
        with cluster_started(args.olap_binary, directory / 'olap') as (url, token):
            evidence['olap_unchanged_orm'] = exercise(url, token, None)
            db = Database(url, token, mode='oltp', timeout=25)
            evidence['st01_snapshot_isolation'] = snapshot_isolation(db)
            table = db.create_table(TABLE, SCHEMA, key='id')
            assert table.insert_many(example(id_) for id_ in reversed(range(NUM_ROWS))) == NUM_ROWS
            evidence['st05_pytorch_cpu'] = launch_gloo(url, token)
            evidence['st05_multishard_limit'] = rejected(
                StreamUnsupportedError, lambda: open_multiple_shards(training_query(table, 0).limit(1)),
            )
            table.drop()
    if args.oltp_binary:
        db_dir = directory / 'oltp'
        db_dir.mkdir()
        server = OltpServer(str(args.oltp_binary), str(db_dir))
        try:
            server.start()
            evidence['oltp_unchanged_orm'] = exercise(server.url, 'test-token', 'test')
            unsupported(server.url, 'test-token', database='test')
            evidence['oltp_stream_rejected'] = True
        finally:
            server.stop()
    if args.old_olap_binary:
        with cluster_started(args.old_olap_binary, directory / 'old-olap', workers=1) as (url, token):
            unsupported(url, token)
            evidence['old_olap_stream_rejected'] = True
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--olap-binary', type=Path)
    parser.add_argument('--oltp-binary', type=Path)
    parser.add_argument('--old-olap-binary', type=Path)
    parser.add_argument('--rank-url')
    args = parser.parse_args()
    if args.rank_url:
        training_rank(args.rank_url)
        return
    for name in ('olap_binary', 'oltp_binary', 'old_olap_binary'):
        if getattr(args, name):
            setattr(args, name, getattr(args, name).resolve(strict=True))
    if not (args.olap_binary or args.oltp_binary or args.old_olap_binary):
        parser.error('Supply at least one binary to test.')
    with tempfile.TemporaryDirectory(prefix='olap-stream-sdk-') as directory:
        evidence = run(args, Path(directory))
    print(json.dumps(evidence, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
