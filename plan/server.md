# Server module

Run interchangeable coordinator replicas and identical workers. Coordinators serve
the API and route requests; one elected replica reconciles membership and scaling.
See [overview.md](overview.md) for the deployment and [engine.md](engine.md) for execution.

```python
from collections.abc import Callable
from typing import Literal

class Server:
  catalog: Catalog

  def __init__(self, *, metadata_url: str, token: str): ...
  def on(self, event: Literal["add", "remove"],
         handler: Callable[[NodeEvent], str | None]) -> None: ...
  def serve(self, host: str = "0.0.0.0", port: int = 8000) -> None: ...
  def route(self, request: Request) -> Node: ...

class Worker:
  id: str

  def join(self, invitation: str) -> None: ...
  def serve(self, host: str = "0.0.0.0", port: int = 8000) -> None: ...
  def heartbeat(self) -> None: ...
  def drain(self) -> None: ...
  def leave(self) -> None: ...
```

- Replicas share the HA PostgreSQL catalog from [overview.md](overview.md).
  Keep transaction/job/cursor IDs and deduplication state there, not in API sessions.
  Coordinators hold no authoritative local state and can be replaced independently.
- Register the same callbacks on each replica before election eligibility. Only the
  elected controller dispatches lifecycle events. SDK processes do not host handlers.
- `POST /v1/operations` validates explicit operations, authorizes the principal and
  propagates request IDs/deadlines to authenticated workers. Keyed operations route
  to their owner; scans select an engine executor. Reject stale routing before work.
- Require TLS, internal mTLS and scoped/rotatable identities. `token` is bootstrap
  configuration; production clients use access tokens. Bound queues, connection pools,
  payloads and tenant concurrency. Reject overload rather than exhaust the process.
- Writes use catalog transactions and durable request outcomes. Replay the recorded
  result for the same principal, cluster, request ID and payload; reject payload changes.
  Retain outcomes for the advertised retry window. Lost responses are not rollbacks.
- Readiness requires access to authoritative metadata and compatible schema versions.
  Remove draining replicas from service, finish requests and transfer reconciliation.
  If metadata is unavailable, fail requests safely; never commit from cached ownership.

## Usage

```python
import os
from meshdb import Server
from my_infrastructure import start_machine, stop_machine

server = Server(metadata_url=os.environ["MESHDB_METADATA_URL"], token="secret")
server.on("add", lambda event: start_machine(
  invitation=event.invite_token, request_id=event.id, **event.options
))
server.on("remove", lambda event: stop_machine(
  event.resource_id, request_id=event.id
))
server.serve()  # Expose this coordinator as https://db.example.com.
```

Each machine started by `start_machine` runs `Worker.join(invitation)` followed
by `Worker.serve()`. Heartbeats report membership and CPU/RAM utilization.
API traffic can reach any ready coordinator. Losing one replica must not lose
committed requests or pending scaling operations.
