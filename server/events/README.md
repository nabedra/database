# Nabedra Events

Nabedra Events is a replicated event log. Each topic is split into
partitions, and each partition is a durable log. For every subscription, the
brokers also store one checkpoint per partition. Events runs on its own
brokers and replicates its data separately from the OLAP and OLTP engines.

A producer picks the partition for each event. To retry a publish safely, it
keeps the event's UUID, partition and payload and sends the same values again.
A consumer claims one partition of a subscription at a time and acknowledges
events in order, up to the last one it has processed.

The [Python client and OLAP sink guide](../../orm/python/docs/events.md) covers
the client side, and the [design and acceptance plan](../../plans/events.md)
explains the design.

## Run in Docker

From the repository root, build the broker image:

```sh
docker build --build-arg BUILD_JOBS=2 \
  -t nabedra-events:local -f server/events/deploy/Dockerfile server
```

Then generate a Compose fixture and start it. This one has three brokers, one
volume per broker, and a combined limit of two CPUs and 1536 MiB of memory.
Lower the totals if Docker is running another test. Use `--brokers 1` for a
single broker. To test promoting topics from replication factor 1 (RF1) to RF3,
start with three or more brokers and `--metadata-voters 1`.

```sh
fixture_dir=$(mktemp -d)/cluster
python3 server/events/deploy/fixture.py "$fixture_dir" \
  --brokers 3 --cpus 2 --memory-mib 1536
docker compose -f "$fixture_dir/compose.json" up -d
curl --fail --retry 10 --retry-connrefused http://127.0.0.1:18080/readyz
```

The brokers listen on loopback ports 18080, 18081 and 18082, and talk to each
other over the fixture's Docker network. The fixture uses plain HTTP and keeps
the credentials it generates in its own directory. To restart on the same
disks, run `docker compose stop` and then `up -d` with the same Compose file.
A plain `down` keeps the named volumes too.

## Issue grants and create topics

Clients authenticate with grants. To issue one, pass a broker config and a
JSON grant to `nabedra-events issue-grant --config FILE --grant FILE`. Keep
expirations short. They are in epoch milliseconds. A publish grant has the
scope `{"operation":"publish","topic":"orders"}`, and a read grant's scope
includes `"operation":"read","topic":"orders","subscription":"analytics"`.
Give each sink its own read grant and its own database credentials.

For the Docker fixture, start with an operator grant that is valid for one
hour:

```sh
python3 - "$fixture_dir/operator.json" <<'PY'
import json
import sys
import time

with open(sys.argv[1], "w", encoding="utf-8") as output:
    json.dump({
        "tenant": "local",
        "subject": "operator",
        "scope": {"operation": "operator"},
        "expires_at_ms": int(time.time() * 1000) + 3_600_000,
    }, output)
PY

operator_token=$(docker compose -f "$fixture_dir/compose.json" run --rm --no-deps \
  -v "$fixture_dir/operator.json:/etc/nabedra-events/grant.json:ro" broker-1 \
  issue-grant --config /etc/nabedra-events/broker.json --grant /etc/nabedra-events/grant.json)

admin() {
  printf 'Authorization: Bearer %s\n' "$operator_token" |
    curl --silent --show-error --fail-with-body -H @- "$@"
}
```

Administrative changes are operations, each identified by a UUID you choose.
Pick the UUID before you submit, and keep the body so that a retry sends
exactly the same request:

```sh
base=http://127.0.0.1:18080
operation_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
admin "$base/v1/admin/operations" -H 'Content-Type: application/json' \
  --data "{\"operation_id\":\"$operation_id\",\"action\":{\"type\":\"create_topic\",\"topic\":\"orders\",\"partitions\":8,\"replication_factor\":3,\"retention_seconds\":86400}}"
admin "$base/v1/admin/operations/$operation_id"
```

Wait until the receipt shows `"state":"completed"` before you make the next
topology change. On a single broker, use replication factor 1.

A cluster can host up to 4,095 data partitions plus one metadata group. A
request that would go over that limit fails with `partition_limit`, and the
broker doesn't record it as a pending operation.

To add a subscription, submit a new operation whose action is
`{"type":"create_subscription","topic":"orders","subscription":"analytics","start":"earliest"}`.
With `"start":"latest"` instead, the subscription begins at the current end of
each partition. Subscriptions are independent, so to replay a topic you create
another one.

If a checkpoint falls behind retention, the broker returns `checkpoint_expired` with
the earliest offset that is still available. Check which events were deleted,
then run `reset_subscription` with the topic, partition and subscription to
resume from the oldest retained event.

## Inspect and change placement

Every administrative read and write needs an operator grant. To see where a
topic's partitions live, read `/v1/admin/topics/{topic}`. To compare Raft state
across brokers, read `/v1/admin/groups/{group}`. Under the topic path,
`/partitions/{partition}` shows a partition's tail, and
`/partitions/{partition}/subscriptions/{subscription}` shows a subscription's
checkpoint.

Submit the actions below as operations, the same way as `create_topic`:

| Operation | Action fields |
| --- | --- |
| Register a running, empty broker | `type: add_broker`, `broker: {id, zone, peer_url}` |
| Add partition IDs | `type: add_partitions`, `topic`, `partitions` as the new total |
| Promote a topic from RF1 to RF3 | `type: set_replication_factor`, `topic`, `replication_factor: 3` |
| Change metadata voters | `type: move_metadata`, `voters` as broker IDs |
| Move a partition | `type: move_partition`, `topic`, `partition`, `voters` as broker IDs |
| Redistribute a topic | `type: rebalance`, `topic` |
| Drain a broker | `type: drain`, `broker_id` |

To promote from RF1, bring up three eligible brokers in different zones. Move
the metadata group to all three as voters, promote each topic, and check that
the replicas have caught up before you retire any of the original brokers.

When you drain a broker, keep it running until the operation completes and the
committed membership of every group no longer includes it. After adding
partitions, producers that retry an earlier publish must keep using the
partition ID they first chose.

For production provisioning, credentials, backups, disk replacement, recovery
and monitoring, see the [AWS runbook](deploy/aws/README.md).
