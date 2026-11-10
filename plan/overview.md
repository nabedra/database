# Overall design

Keep the API simple and require production fault tolerance from the first release.
These are interface plans, not an implemented or production-validated runtime.

```text
Application -> stable HTTPS API -> coordinators -> workers -> S3 / GCS
                                      |             |
                                      +-- HA metadata database
                                      +-- one elected scaling controller
```

## Deployment

Run at least two coordinators and two workers across failure zones. Coordinators
share a managed/operated HA PostgreSQL catalog with synchronous replication,
automatic failover and fencing of the former primary. Store schemas, ownership,
committed object references, request outcomes and lifecycle progress there.
Provision enough synchronous standbys to retain the required durability after the
supported failure, and promote only a standby containing acknowledged commits.
Use [PostgreSQL HA](https://www.postgresql.org/docs/current/warm-standby.html)
and its [failover requirements](https://www.postgresql.org/docs/current/warm-standby-failover.html)
rather than implementing a consensus protocol inside MeshDB.

Workers use shared durable object storage; local disks are disposable caches.
Any coordinator handles a request. One elected coordinator per cluster reconciles
worker membership and scaling. A single coordinator is a development configuration.

## Startup and requests

1. Start coordinator replicas with the same metadata database and provider handlers.
2. Use `Cloud(api_url=..., token=...)` to create the cluster and add workers.
3. Connect `DB(api_url=cluster.endpoint(), token=...)` to the stable API URL.
4. A coordinator authenticates each request and selects a partition owner or executor.
5. A write uploads immutable objects, then transactionally publishes their references
   and request outcome in the catalog before acknowledgment. Reads use a committed
   metadata snapshot. Reconnecting to another coordinator preserves correctness.

Worker loss triggers ownership recovery from committed storage, even if autoscaling
is disabled. Ownership generations reject commits from an old or partitioned worker.
Coordinator loss reroutes API traffic and another replica resumes reconciliation.
Metadata unavailability stops writes and lifecycle changes; it never permits split brain.

## Scaling

Manual calls and CPU/RAM policies use the same durable `add`/`remove` operations.
Register callbacks on every `Server`; only the elected controller dispatches them.
Its database lease and increasing epoch fence stale state changes. Provider calls
use recorded intents and stable event IDs because external side effects can repeat.

- Add a worker, wait for readiness, then assign partitions; its cache warms on demand.
- Remove by draining work and handing off ownership before stopping the resource.
- Average fresh CPU/RAM over the policy window. Add above either configured target;
  remove below 80% of all configured targets. Respect bounds, cooldown and disruption
  budgets. Missing metrics, an outage or an upgrade blocks scale-in.

Start with identical workers and `min_nodes=2`. Enforce per-cluster capacity/tenant
limits and one in-flight scaling operation; keep coordinator capacity independent.
In Kubernetes, the elected controller manages worker Pods. Do not also give HPA
ownership of that count; infrastructure node scaling remains separate.

## Release requirements

Define availability, latency, throughput, RPO and RTO for the intended workload.
Before production, verify coordinator/worker/metadata failover and network partitions,
no lost acknowledged writes under the supported failure model, retry deduplication,
rolling upgrades, and restoration of metadata plus referenced objects. Require TLS,
internal mTLS, scoped/rotatable credentials, tenant isolation, audit logs and alerts.
Load-test catalog commits, hot partitions and recovery at the stated capacity;
adding workers alone does not establish enterprise-scale performance.
The initial write latency includes durable object upload plus a synchronous catalog
commit. Validate that against OLTP targets before choosing any faster durable write path.

Build table operations, scans and safe membership first. Cross-partition transactions,
analytics/ML and streaming are later interfaces and must report unsupported until
implemented. HA, durable commits, fencing and recovery are first-release requirements.

| Module | Owns |
| --- | --- |
| [data](data.md) | Client API, tables, queries and formats |
| [server](server.md) | API/worker processes, auth and request routing |
| [engine](engine.md) | Catalog transactions, ownership and query execution |
| [storage](storage.md) | Immutable objects, encoding, cache and recovery snapshots |
| [cloud](cloud.md) | Lifecycle reconciliation and autoscaling policy |
| [stream](stream.md) | Streaming and pipelines after the core path |
