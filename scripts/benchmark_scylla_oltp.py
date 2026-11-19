#!/usr/bin/env python3
"""Run the common OLTP point update workload against a fresh ScyllaDB cluster."""

import argparse
import json
import logging
import time
from importlib.metadata import version
from pathlib import Path

from cassandra import ConsistencyLevel, DriverException, Unavailable
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, NoHostAvailable
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import DCAwareRoundRobinPolicy, FallthroughRetryPolicy, TokenAwarePolicy
from cassandra.query import SimpleStatement

from benchmark_oltp_workload import TABLE, add_workload_args, run_workload, validate_args


KEYSPACE = "oltp_comparison"
DATABASE = f"{KEYSPACE}.{TABLE}"
CGROUP = Path("/sys/fs/cgroup")


def connect_cluster(args):
    deadline = time.monotonic() + args.startup_timeout
    while True:
        profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
            consistency_level=ConsistencyLevel.QUORUM,
            retry_policy=FallthroughRetryPolicy(),
            request_timeout=args.timeout,
        )
        cluster = Cluster(
            contact_points=args.hosts, protocol_version=4, connect_timeout=3,
            control_connection_timeout=3, execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        )
        try:
            session = cluster.connect(wait_for_all_pools=True)
            hosts = cluster.metadata.all_hosts()
            if len(hosts) == len(args.hosts) and all(host.is_up for host in hosts):
                identify = SimpleStatement(
                    "SELECT data_center, release_version FROM system.local",
                    consistency_level=ConsistencyLevel.ONE,
                )
                infos = [session.execute(identify, host=host).one() for host in hosts]
                if all(infos):
                    return cluster, session, hosts, infos
            last_error = f"discovered {len(hosts)} peer(s); waiting for {len(args.hosts)} reachable nodes"
        except (DriverException, NoHostAvailable) as error:
            last_error = error
        except BaseException:
            cluster.shutdown()
            raise
        cluster.shutdown()
        if time.monotonic() >= deadline:
            raise RuntimeError(f"ScyllaDB did not become ready: {last_error}")
        time.sleep(1)


class Scylla:
    def __init__(self, cluster, session, datacenter):
        self.cluster = cluster
        self.session = session
        self.datacenter = datacenter

    def seed(self, rows):
        self.session.execute(
            f"CREATE KEYSPACE {KEYSPACE} WITH replication = "
            f"{{'class': 'NetworkTopologyStrategy', '{self.datacenter}': 3}} "
            "AND durable_writes = true"
        )
        self.session.execute(f"CREATE TABLE {DATABASE} (id bigint PRIMARY KEY, value bigint)")
        insert = self.session.prepare(f"INSERT INTO {DATABASE} (id, value) VALUES (?, ?)")
        execute_concurrent_with_args(
            self.session, insert, ((key, 0) for key in range(rows)), concurrency=32,
        )
        self.point_update = self.session.prepare(f"UPDATE {DATABASE} SET value = ? WHERE id = ?")

    def update(self, key, value, _request_id):
        try:
            self.session.execute(self.point_update, (value, key))
        except Unavailable as error:
            return {"status": "rejected", "error": f"{type(error).__name__}: {error}"}
        except (DriverException, NoHostAvailable) as error:
            # A client or replica timeout can occur after the write has taken effect.
            return {"status": "unresolved", "error": f"{type(error).__name__}: {error}"}
        return {"status": "succeeded"}

    def read(self, _keys):
        values = {}
        for row in self.session.execute(f"SELECT id, value FROM {DATABASE}"):
            if row.id in values:
                raise RuntimeError(f"duplicate key in ScyllaDB snapshot: {row.id}")
            values[row.id] = row.value
        return values

    def stage_stats(self):
        if not (CGROUP / "cpu.stat").is_file():
            return {"driver_cgroup": "cgroup v2 statistics unavailable"}
        stats = dict(line.split() for line in (CGROUP / "cpu.stat").read_text().splitlines())
        return {"driver_cgroup": {
            "cpu_usage_usec": int(stats["usage_usec"]),
            "cpu_throttled_usec": int(stats["throttled_usec"]),
            "cpu_periods": int(stats["nr_periods"]),
            "cpu_periods_throttled": int(stats["nr_throttled"]),
            "memory_current_bytes": int((CGROUP / "memory.current").read_text()),
            **({"memory_peak_bytes": int((CGROUP / "memory.peak").read_text())}
               if (CGROUP / "memory.peak").is_file() else {}),
        }}

    def close(self):
        self.cluster.shutdown()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    add_workload_args(parser)
    parser.add_argument("--hosts", nargs="+", default=["scylla1", "scylla2", "scylla3"])
    parser.add_argument("--startup-timeout", type=int, default=240)
    parser.add_argument("--wait-only", action="store_true")
    parser.add_argument("--output", type=Path, default=Path("/output"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.CRITICAL if args.wait_only else logging.ERROR)
    if not args.wait_only:
        if len(set(args.hosts)) != 3 or len(args.hosts) != 3:
            parser.error("measurement requires three distinct ScyllaDB nodes")
        try:
            validate_args(args)
        except ValueError as error:
            parser.error(str(error))

    cluster, session, hosts, infos = connect_cluster(args)
    try:
        if args.wait_only:
            return 0
        datacenters = {info.data_center for info in infos}
        if len(datacenters) != 1:
            raise RuntimeError(f"expected one datacenter: {infos}")
        build = SimpleStatement(
            "SELECT version FROM system.versions WHERE key = 'local'",
            consistency_level=ConsistencyLevel.ONE,
        )
        versions = {session.execute(build, host=host).one().version for host in hosts}
        if len(versions) != 1:
            raise RuntimeError(f"expected one ScyllaDB version: {versions}")
        config = SimpleStatement(
            "SELECT value FROM system.config WHERE name = 'commitlog_sync'",
            consistency_level=ConsistencyLevel.ONE,
        )
        node_modes = {
            host.address: json.loads(session.execute(config, host=host).one().value)
            for host in hosts
        }
        if set(node_modes.values()) != {"batch"}:
            raise RuntimeError(f"commitlog sync mode mismatch: {node_modes}")
        adapter = Scylla(cluster, session, datacenters.pop())
        metadata = {
            "engine": "ScyllaDB",
            "engine_version": versions.pop(),
            "engine_version_source": "system.versions",
            "cassandra_compatibility_versions": sorted({info.release_version for info in infos}),
            "driver_version": version("scylla-driver"),
            "cql_protocol_version": 4,
            "replication_factor": 3,
            "consistency": "QUORUM",
            "commitlog_sync_by_node": node_modes,
            "durable_writes": True,
            "measurement": "persistent native CQL, prepared UPDATE, no application retries",
        }
        result = run_workload(args, adapter, args.output, metadata)
        return 0 if result["correctness_verified"] else 1
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
