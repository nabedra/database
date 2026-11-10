
# Engine Module

Execute requests on workers and manage partition ownership through the coordinator.
The [server module](server.md) owns process lifecycle, the API and routing.

Coordinators share the HA catalog; workers receive committed metadata snapshots.
`Request` and `Response` are shared API/internal envelopes. Each partition has one
mutable owner, fenced by generation. Cross-partition transactions below are later
interfaces; durable recovery and owner failover are required from the first release.

```python
class Node:
  id: str
  endpoint: str
  cpu: float
  memory: int
  status: str

class Request:
  id: str  # Scoped to the verified principal and cluster.
  operation: str
  payload: dict
  timeout: float | None

class Response:
  data: Any
  status: str
  error: str | None

class Scheduler:
  def submit(self, operation: str, payload: dict) -> Task: ...
  def assign(self, task: Task) -> Node: ...
  def cancel(self, task_id: str) -> None: ...
  def rebalance(self) -> None: ...

class Task:
  id: str
  status: str
  assigned_node: str | None

  def result(self) -> Any: ...
  def cancel(self) -> None: ...

class Partition:
  id: str
  leader: str
  generation: int
  replicas: list[str]
  range: tuple[Any, Any]

  def move(self, node_id: str) -> None: ...
  def replicate(self, node_id: str) -> None: ...
  def split(self) -> tuple[Partition, Partition]: ...

class Catalog:
  def tables(self) -> list[str]: ...
  def streams(self) -> list[str]: ...
  def partitions(self, table: str) -> list[Partition]: ...
  def locate(self, table: str, key: Any) -> Partition: ...
  def update(self, resource: str, metadata: dict) -> None: ...
  def commit(self, partition: str, node_id: str, generation: int,
             expected_version: int, blocks: list[str],
             request_id: str, principal_id: str, payload_hash: str,
             result: Any = None) -> int: ...

class QueryEngine:
  def plan(self, query: Query) -> Plan: ...
  def optimize(self, plan: Plan) -> Plan: ...
  def execute(self, plan: Plan) -> Iterator[Row]: ...
  def explain(self, plan: Plan) -> str: ...

class Plan:
  operators: list[Any]
  partitions: list[str]
  estimated_cost: float

class TransactionCoordinator:
  def begin(self) -> str: ...
  def prepare(self, tx_id: str) -> bool: ...
  def commit(self, tx_id: str) -> None: ...
  def rollback(self, tx_id: str) -> None: ...
  def recover(self, tx_id: str) -> None: ...
```

## Execution

Keyed operations run on the partition owner. Scans select a worker executor;
`QueryEngine` uses `Scheduler` for parallel tasks and combines results.

Workers upload immutable objects before calling `Catalog.commit`. In a single
PostgreSQL transaction, authorize the principal and first check for a recorded
request outcome. Matching IDs/payloads return it even if ownership has since changed.
Otherwise validate the current owner, generation and expected data version, then
publish references and store the result together. Reject stale owners, conflicting
versions and reused IDs with different payloads.

Ownership changes lock the same catalog record and increment its generation.
A failed worker is replaced by assigning a ready worker its last committed snapshot.
Old workers cannot commit afterward. Reads use authoritative committed snapshots;
read-after-write cannot return a version older than the acknowledged commit.

Use [PostgreSQL transaction/locking semantics](https://www.postgresql.org/docs/current/explicit-locking.html).
Require synchronous HA durability for acknowledged catalog commits and fail closed
when that guarantee is unavailable. A local worker disk is never the only copy of
acknowledged data. Bound/batch commits without weakening acknowledgment semantics.

Initially provide atomic single-partition operations and committed snapshot scans.
Reject cross-partition transactions until their isolation, recovery and failure
semantics are implemented and tested. Warm replicas are an optional recovery-speed
optimization; shared durable objects plus HA metadata supply the recovery baseline.
