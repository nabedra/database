# Cloud module

Manage workers through the coordinator API. Register provider callbacks on
`Server` as shown in [server.md](server.md); the same events handle manual and
policy-driven scaling. See [overview.md](overview.md) for the complete flow.

```python
from typing import Any, Literal

class Cloud:
  def __init__(self, api_url: str, token: str): ...
  def create(self, name: str, storage: S3 | GCS, mode: str = "byoc") -> Cluster: ...
  def cluster(self, name: str) -> Cluster: ...
  def clusters(self) -> list[Cluster]: ...
  def delete(self, name: str) -> None: ...

class Cluster:
  name: str
  nodes: list[Node]
  storage: StorageManager
  access: Access
  monitor: Monitor
  backups: Backups
  usage: Usage

  def add(self, count: int = 1, **options: Any) -> Job: ...
  def remove(self, node_id: str | None = None) -> Job: ...
  def autoscale(self, *, min_nodes: int = 2,
          max_nodes: int | None = None,
          target_cpu: float | None = None,
          target_ram: float | None = None,
          window: str = "2m", cooldown: str = "60s",
          enabled: bool = True) -> None: ...
  def invite(self, max_joins: int | None = None,
         expires_in: int | None = None) -> Invitation: ...
  def endpoint(self) -> str: ...
  def drain(self, node_id: str) -> None: ...
  def status(self) -> dict: ...

class NodeEvent:
  id: str             # Stable across retries of this operation.
  cluster: str
  action: Literal["add", "remove"]
  reason: str           # "manual", "autoscale", "recovery", "delete".
  epoch: int            # Controller epoch; event ID stays stable on takeover.
  node_id: str           # Reserved before provisioning.
  invite_token: str | None     # Present for add; bound to this node.
  resource_id: str | None      # Present for remove of a provisioned node.
  options: dict[str, Any]      # Options supplied to add; retained for remove.

class Invitation:
  token: str
  expires_at: str | None
  max_joins: int | None

  def revoke(self) -> None: ...

class Access:
  def create_token(self, name: str, permissions: list[str]) -> str: ...
  def revoke_token(self, token_id: str) -> None: ...
  def grant(self, principal: str, resource: str, permissions: list[str]) -> None: ...
  def revoke(self, principal: str, resource: str) -> None: ...
  def audit(self) -> Query: ...

class Monitor:
  def metrics(self, name: str, since: str = "1h") -> Query: ...
  def logs(self, node: str | None = None) -> Query: ...
  def traces(self, operation: str | None = None) -> Query: ...
  def alert(self, name: str, metric: str,
        above: float, notify: str) -> None: ...

class Backups:
  def create(self, name: str | None = None) -> str: ...
  def list(self) -> list[str]: ...
  def restore(self, snapshot_id: str) -> Job: ...
  def restore_at(self, timestamp: str) -> Job: ...

class Usage:
  storage_bytes: int
  compute_seconds: float
  egress_bytes: int
  queries: int

  def report(self, since: str = "30d") -> dict: ...
  def estimate_cost(self) -> float: ...
```

## Usage

```python
from meshdb import Cloud, S3

cloud = Cloud(api_url="https://db.example.com", token="secret")
cluster = cloud.create("prod", storage=S3(bucket="data", region="eu-west-1"))
cluster.add(count=3).result()
cluster.autoscale(min_nodes=2, max_nodes=10, target_cpu=0.70, target_ram=0.80)

# Manual calls use the same handlers; remove drains before stopping the machine.
cluster.remove().result()
cluster.autoscale(enabled=False)
```

## Lifecycle

- `create` stores cluster metadata and storage configuration. `endpoint()` returns
  its stable data API URL. The deployment provides the coordinator's HTTPS address.
- `add(count=n)` requires `n > 0`. Persist one event per worker with a reserved ID
  and expiring, single-join invitation. The handler returns its resource ID; the
  job completes after all workers join and become ready.
- `remove(node_id)` selects that worker, or an eligible one when omitted. Stop new
  assignments, finish work and hand off ownership before invoking the handler.
  Failed draining never stops the machine. Coordinator replicas are excluded.
  Respect failure-zone placement and the cluster disruption budget, normally one worker.
- Events are delivered at least once. Handlers deduplicate by `event.id` and treat
  an already stopped resource as success. Persist resource IDs and progress; retry
  the same operation after takeover. Persist immutable provisioning/termination intent
  before provider calls. Never reuse worker IDs; deletion targets the original resource
  identity, including Pod UID where applicable. Failed joins trigger a remove event.
- `invite` also permits externally started workers. Their remove handler resolves
  the machine from `node_id` when no resource ID is recorded. `delete` stops all
  workers through this flow before removing metadata; it preserves object storage.

## Policy

- `min_nodes >= 1`; `max_nodes`, if set, must be at least `min_nodes`. Targets are
  fractions in `(0, 1]`; enabling requires at least one. Production starts at two
  workers across zones; one is a development setting. A call replaces the policy;
  `enabled=False` pauses it. Manual operations respect active bounds.
- Use identical worker sizes. Average fresh CPU/RAM samples over `window`; RAM
  excludes reclaimable cache. Missing samples prevent utilization-based scaling.
  Outages, recovery and rolling upgrades prevent scale-in; bound scale-out by quotas.
- Above either configured target, call `add()` once. When all configured metrics
  stay below 80% of their targets for the window, call `remove()` once.
- Reconcile the minimum immediately. Count pending additions, allow one lifecycle
  operation at a time, then wait `cooldown` and a full stable measurement window.
  Failed/pending provisioning must be reconciled before another scaling decision.
- One elected controller per cluster runs the loop. Claim a database lease with a
  monotonically increasing epoch; renew it and compare lease/epoch transactionally
  on every state change. A stale controller cannot authorize new operations.
  External calls may still finish after lease loss, so stable intents and provider
  deduplication are required. Takeover reconciles observed resources before retrying.
- Worker failure recovery runs independently of the policy's enabled flag. Replace
  lost capacity and fence old owners. Only one system owns the desired worker count.
