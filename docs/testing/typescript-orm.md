# TypeScript ORM verification

The [implementation plan](../../plans/typescript-orm.md) uses the Python package
at `1418bad` as the reference. The verification ran on macOS 26.7.3 x86-64
with Node.js 22.13.1, TypeScript 5.9.3, and Python 3.12.1.

Strict compilation checked inferred keys, updates, filters, projections, and
stream batches. Runtime checks compared the Python and TypeScript package
exports and imported all 29 TypeScript package entry points. The differential
test compared all 13 database operation envelopes, including integer and
float tokens, against the Python client.

| Cases | Executed assertions |
| --- | --- |
| O01, T06 | Both clients passed the 13-operation contract against the local OLTP binary. The TypeScript client round-tripped both signed int64 limits, verified atomic batch rejection and stale table handles, and checked results after 1, 100, and 1,000 row writes. Each client used a fresh store. |
| Q04–Q06, T04 | The client reconnected to the same receipt identity, recovered a settled result, rejected a conflicting replay, and surfaced an unknown write outcome after the peer lost its response. The client sent the ambiguous write once. |
| U01, R04 | The supervisor started and reaped an actual OLTP server. A cold start produced a fresh run ID and retained the committed row and committed receipt. |
| A01, A02 | Local fixtures tested fixed-cluster rejection, callback admission and authorization, retained resource ownership, replay, and stop permits. Provider fixtures exercised Docker, EC2, EKS, and Kubernetes APIs with local doubles. |
| U02 | Local peers exercised duplicate JSON fields, strict integer and float tokens, bounds, malformed replies, redirects, verified TLS, and explicit deadlines. A progressing response stayed open past the socket inactivity timeout. |
| ST02–ST05 | Loopback fixtures tested row and Arrow validation, truncated-frame retry with stable identity, early cleanup, and typed lease errors. A real OLAP API and three workers returned disjoint rows from a shared snapshot and correctly sized Arrow batches. |
| GW01–GW03, SL06 | Loopback fixtures tested gateway middleware order, immutable context, bad requests, uncertain responses, and capacity retention while middleware settled. |
| Events and sink | Local peers tested precision, publishing, replay, lease ownership, renewal, checkpoint order, and reconciliation of a lost sink acknowledgment. |
| Packaging and CLI | `npm pack` installed into a fresh strict TypeScript consumer with optional dependencies omitted. The SDK, deployment entry points, native Arrow types, and installed CLI loaded. Local CLI fixtures checked grouped arguments and protected configuration. |

The TypeScript suite reported 44 passes and no skips with both binary paths and
Python configured. Python's ORM suite reported 188 passes and one skip. One
local OLTP run measured `insertMany` at 1.17 s for 1 row, 1.53 s for 100 rows,
and 5.44 s for 1,000 rows. These elapsed times include execution by the real
server on the test host.

The [scheduler contract](scheduler-contract.md) defines cluster-wide fault
cases separately from the tested client assertions. Live AWS, EKS, Docker
provisioning, and an external Events broker remain unverified.
