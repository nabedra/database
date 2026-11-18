# Events and subscriptions

The `nabedra.events` package connects to an Events broker with its own URL,
bearer token and TLS trust settings. A producer publishes to a topic and
partition. Each named subscription maintains its own checkpoint for that
partition. An operator creates topics and subscriptions before use.

```python
from uuid import uuid4

from nabedra.events import Events

producer = Events("https://events.example", token=publish_token)
event_id = uuid4()
event = producer.publish(
    "orders", partition=7, event_id=event_id, data={"order_id": "A-42"},
)
assert event.event_id == event_id
```

Save the partition, UUID and payload together before sending an event that
needs retry. Within retention, identical retries return their original
receipts. Changed data for the same UUID and partition conflicts.
`publish_batch` accepts up to 100 `PublishRecord` objects or dictionaries with
`event_id` and `data` fields. It returns receipts in input order from one atomic
partition write. Data uses finite JSON values and supports up to 64 nested
containers. The encoded request is limited to 64 KiB per record, including
the UUID, and 1 MiB per batch, including the batch envelope. Batch receipts
also have a 1 MiB limit.
`client.partitions("orders")` yields partition IDs and fetches discovery pages
as needed.

An `EventsUnknownOutcomeError` means the write may have committed. For a
publish, retry the same partition, UUID and data. Other `EventsError` instances
include a code and may carry the HTTP status, `Retry-After` duration in seconds,
or earliest available offset. If a claim still has an unknown outcome after
the client's retry, the exception carries its `claim_id`. Supply
`claim(claim_id=error.claim_id)` to continue recovering that claim.

```python
consumer = Events("https://events.example", token=read_token)
subscription = consumer.subscription("orders", "fulfillment", partition=7)
with subscription.claim() as lease:
    batch = lease.poll(limit=100)
    for event in batch.events:
        process_order(
            event.data,
            idempotency_key=(event.topic_id, event.partition, event.event_id),
        )
    if batch.events:
        lease.ack(batch, through=batch.events[-1].offset)
```

A claim reserves one subscription partition until its lease expires or is
released. Call `lease.renew()` during long processing and use
`lease.expires_at` to see the deadline. Acknowledging a processed prefix leaves
the remainder available for redelivery. Event offsets and checkpoints are
Python integers. The context manager releases the claim, and an identical
ack can be retried during the same lease if the response is lost.
`checkpoint_expired` requires an operator to choose a new checkpoint.

The client verifies HTTPS using system trust by default. `ca_file` adds a
private CA. `allow_insecure=True` enables HTTP for local fixtures.

## Write a subscription to OLAP

Create a destination using the normal OLAP API and record the returned UUID.

```python
from nabedra import Database

db = Database("https://olap.example", token=database_token, mode="olap", database="analytics")
table = db.create_table(
    "order_events",
    {"event_key": str, "partition": int, "offset": str, "published_at": str, "data": str},
    key="event_key",
)
print(table.id)
```

Use that UUID in a sink configuration. The range includes `start` and excludes
`stop`.

```json
{
  "events": {
    "url": "https://events.example",
    "token_env": "ORDER_EVENTS_READ_TOKEN",
    "topic": "orders",
    "subscription": "analytics"
  },
  "olap": {
    "url": "https://olap.example",
    "token_env": "ORDER_EVENTS_OLAP_TOKEN",
    "database": "analytics",
    "table": "order_events",
    "table_id": "fbb50b2a-a35f-43c5-9e17-eac70d974b12"
  },
  "partitions": {"start": 0, "stop": 8},
  "workers": 4,
  "max_in_flight": 2,
  "batch_size": 100
}
```

```sh
python -m nabedra.events.sink --config orders-to-olap.json
```

Set `olap.database` when the destination is an application gateway that
requires a database selector. For a direct OLAP endpoint, omit it. The direct
endpoint sets the storage scope from its connection configuration and token.

Each endpoint also accepts optional `ca_file`, `timeout` in seconds, and
`allow_insecure` for local HTTP fixtures. The sink uses at most 32 workers
over up to 10,000 partitions. Optional top-level `poll_interval`,
`receipt_wait`, and `renew_interval` are seconds and default to 0.5, 2, and
5. The sink renews earlier if its lease expiry is near. A standby sink can
use the same config and acquire available claims.

The sink acknowledges after the database confirms the insert. It keeps
observing a known queued receipt across lease changes. After an ambiguous
submit, expired receipt, conflict or database-wide shutdown, it reads the
destination keys and resubmits missing rows with a fresh request identity.
The primary key combines the topic UUID, partition and event UUID. The
offset column stores a decimal string, and the data column stores canonical
JSON. A replaced table UUID or a different row for the same key pauses the
partition. Stop its sinks before changing the destination table, then start
them with the new destination and the intended subscription checkpoint.

Keep unacknowledged deliveries inside Events retention, including during
OLAP outages. The sink inherits the database's existing table capacity
limits. Size the destination for its ingest rate and retention.
