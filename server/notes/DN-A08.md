# DN-A08 control-root reservation for the first safe stop

Design only, accepted by the parent on 2026-11-11. Correct the reservation in
accepted [A05](DN-A05.md) and [shared contracts](../../plans/distributed-contracts.md)
without changing A07 authorization/drain evidence, identities, retention or the
public maximum of 1024. This is an arithmetic and pure-transition contract.

## Decision and arithmetic

Use a **32 MiB encoded control-root cap**, distinct from the unchanged 16 MiB job
CONTROL/PART_META cap. Reserve one additional 4 KiB intent and one intent/count
slot for each managed node that may still need its first provider stop. Derive
credits from retained records; add no persisted counter or schema field.

All units are binary. Let `N` count every retained node and `I` count every
retained intent, including completed/failed records. Let `C` count unconsumed
first-stop credits as defined below. Use checked arithmetic throughout:

```text
L = 32 * 1024 * 1024 = 33,554,432 bytes
F = 256 * 1024
B(N,I)   = F + 8192*N + 4096*I
A(N,I,C) = F + 8192*N + 4096*(I+C)
```

The old first population has `N=1024,I=1024`: `B=12.25 MiB`. Its initial
1024 stops produce `I=2048`, so `B=16.25 MiB`, exceeding A05's 16 MiB.
Increasing the cap alone would leave count exhaustion possible. Credits cover
both constraints. Under the count inequalities below, the largest `A` is
`24.25 MiB`, at `N=2048,I+C=2048`. Thus 32 MiB covers all permitted count
combinations and leaves a simple explicit cap. 16.25 MiB covers only the first
fleet example; 24.25 MiB is the arithmetic minimum for all count combinations.

## Credit identity and release

A managed node has one credit from its atomic node/start allocation, including
while resource/incarnation are unknown. Starting, ready, draining, lost,
unregistered, overdue and ambiguous launches all retain it. External nodes have
no credit and receive no provider stop. A credit is absent exactly when either:

* Any retained stop intent matches that node and its immutable launch identity.
  The first stop's allocation consumes the credit permanently, regardless of its
  later phase or outcome. Cross-record validation must check its exact resource
  and any incarnation binding. A failed `not_invoked` stop satisfies the
  first-record reservation but proves nothing about resource termination.
* The retained terminal launch outcome proves no resource can still appear:
  `failed/not_invoked` with definite pre-entry evidence and no earlier ambiguous
  invocation or outstanding authorized entry, or a terminal `terminated`
  outcome supported by authoritative exact-resource proof. A matching terminal
  stop with `terminated` also proves termination and already satisfies the first
  condition. Retain every record and binding used for this conclusion.

These are historical facts established by transitions, not claims inferred from
an arbitrary phase string. For deriving `C` from a validated root, the terminal
outcome is the persisted witness of the previously checked evidence. A05's
schema does not itself encode or verify provider truth. `Node.state=stopped`,
lost membership, deadline expiry, inventory absence or drain-idle proof alone
cannot release a credit. A completed `resource_known` start still needs one.
Ambiguous exhausted launches remain reconciling and keep theirs, so late resource
recovery requires no new reservation. Never terminalize an ambiguous launch as
`not_invoked` to obtain space.

Terminal never-invoked/terminated evidence is irreversible for that launch.
Neither enrollment nor a late callback can resurrect it or erase the witness.
An observation contradicting that proof must fail closed as a contract violation,
not revive the identity or manufacture a credit. Ordinary late effects remain
recoverable under the retained ambiguous launch, as A05/A07 require.

## Loaded roots and candidate checks

Every decoded root and every encoded candidate must satisfy the strict A05/A07
schema, record-local bounds, sorted uniqueness and cross-record bindings, plus:

```text
N <= 2048
I <= 3072                       # all starts and stops together
N + I <= 4096
B(N,I) <= L
actual bounded root encoding <= L
```

Keep Node 8192 bytes, Intent 4096 bytes, Heartbeat 8192 bytes and Template
64 KiB. The fixed 256 KiB must cover the template, maximum-width global/scalar
fields, root keys/brackets and all array separators up to 4096 records. C04a2b
must establish that bound, rather than reserve only currently encoded bytes.
Per-record bounds must allow all permitted mutable-field growth, including JSON
escaping, retired incarnations, resource recovery, session/drain/delivery data,
maximum u64 decimal widths and terminal errors. Root writers must use bounded
encoding; root readers must bound input while streaming before parsing.

NEW membership, a new managed node/start pair, and any additional stop identity
that does not consume a credit must also satisfy these **admission-only** checks
on the entire post-transition candidate:

```text
N' <= 2048
I' + C' <= 3072
N' + I' + C' <= 4096
A(N',I',C') <= L
```

Allocate a managed node and its retained start atomically, costing one node,
one start and one credit: 16 KiB, three combined slots, two intent slots.
External membership costs 8 KiB and one node/combined slot. A separately
allocated start for an existing node is forbidden; each managed launch keeps
its original start. Policy acceptance reserves no resources and may report
provisioning blocked when actual allocation fails.

Converting a credit into the first stop requires `C>=1`, the exact eligible
node/launch/resource binding and A05's transition eligibility. In one candidate:

```text
N' = N; I' = I+1; C' = C-1
I'+C' = I+C
N'+I'+C' = N+I+C
A(N',I',C') = A(N,I,C)
```

Check the loaded-root invariants and bounded encoding of that exact candidate.
Do not demand another free admission slot, a credit after insertion, or a
successful projected public status response. The stop intent conveys no provider
authority until the existing exact drain/claim/authorization rules allow it.
Beginning drainage itself adds no record and preserves `C`; concurrent drain
requests cannot consume its reserved slot. A CAS loser reloads and recomputes;
if the stop already exists it adopts/retries that identity rather than inserts
another. No credit transfer between nodes is allowed.

Renewal, registration into an admitted node, resource recovery, drain updates,
claims/retries and terminal updates allocate no identities. They perform loaded
root/candidate checks but no new admission check. They preserve the maximum
record reservations, so field growth cannot consume another node's credit.
For these transitions `C'<=C`; first-stop conversion is the only no-new-admission
transition that increases `I`, by exactly the credits consumed. Terminalizing a
start may reduce `C` atomically with its irreversible witness; terminalizing or
failing an already retained stop leaves `C` unchanged. Never recompute credits
as "nodes without an incomplete stop". That would restore a credit on stop
completion/failure and could deadlock at a limit.

The pure transition layer must preserve these relations, unrelated history,
owner/epoch checks and exact loaded-token CAS. It checks a complete candidate
before persistence or planned effects. Admission inequalities are not decoder
phase requirements: a structurally valid loaded root that lacks admission room
must still decode and allow retained-record progress. The first-stop guarantee
applies to roots admitted and evolved under this contract. It is inductive from
empty initialization; decoding arbitrary imported data cannot retroactively grant
missing reservations. Counter exhaustion, invalid evidence and unavailable
storage remain independent reasons progress may fail.

## Fleet examples and finite history

| Retained fleet state | N | I | C | Actual-record budget B | Admission budget A |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1024 starting, retained starts | 1024 | 1024 | 1024 | 12.25 MiB | 16.25 MiB |
| 1024 ready, completed starts retained | 1024 | 1024 | 1024 | 12.25 MiB | 16.25 MiB |
| All draining, first k stops inserted | 1024 | 1024+k | 1024-k | 12.25 MiB + 4 KiB*k | 16.25 MiB |
| 1024 stopping, all first stops inserted | 1024 | 2048 | 0 | 16.25 MiB | 16.25 MiB |

For every `0<=k<=1024`, `I+C=2048` and `N+I+C=3072`; all initial stops fit,
even when every drain begins concurrently. Stops still wait for safe evidence.
Positive completion never deletes a start, stop or node.

Replacement uses fresh node/launch identities with full admission checks. With
the first fleet retained and no other history, at most 341 simultaneous additional
managed launches needing first stops fit: combined reservation is
`3072+3*m<=4096`, intent reservation is
`2048+2*m<=3072`, and bytes are `16.25 MiB+16 KiB*m`. Existing first stops still
fit throughout that overlap. The unchanged history limits do not promise an
entire second 1024-node population after retaining the first fleet.

Retries and backoff reuse A05's stable intent, launch and resource identities;
an ambiguous or retained failed stop never restores first-stop credit. A05's
terminal-state retry eligibility remains unchanged. If recovery requires a new
stop identity after a definite failure, it is an ordinary additional allocation
subject to all admission checks while preserving other nodes' credits. It may
be safely refused. This proposal guarantees one first stop record per admitted
eligible node, not successful provider termination, unlimited churn, unlimited
stop identities or resets of max_attempts. No TTL/GC/archive, catalog-history
reuse, node-ID reuse or public count increase is introduced.

## Adoption and next ownership boundary

Replace A05's complete paragraph beginning "Use a 16 MiB encoded control-root
limit" and ending "Renewals and recovery of admitted identities must retain
their reserved room" with this note's Decision, Credit and Loaded-root/candidate
rules. Also qualify A05's retention paragraph sentence "Until that exists,
hitting record/byte limits safely blocks new allocations" to exclude conversion
of already reserved first-stop credits and retained-record growth. Its public
status advice "Ordinary allocations should check their projected status too"
applies to new admissions, never mandatory recovery or credit conversion.

In shared contracts, replace the sentence starting "The control root is
bounded to 16 MiB" and ending "public status stays at 1 MiB" with:

> The control root is bounded to 32 MiB with A08's maximum-field growth and
> byte/count reservation for each eligible managed node's first future stop.
> New admissions preserve those reservations; first-stop conversion and updates
> of retained identities use the reserved room. Public status stays at 1 MiB.

Qualify the following "metadata exhaustion refuses new allocations" clause as
"metadata exhaustion refuses unreserved allocations". Retention, late-effect
reconciliation and safe-stop authority otherwise keep their accepted wording.
The rubric's C04a2b requirement already names subsequent safe-stop capacity.
Parent owns all adoption edits; this note changes no authoritative file.

The implemented local helper caps objects at 80 MiB in
`scripts/local_store.py:24,176`; `HttpStore` caps PUT at 80 MiB in
`server/src/storage/http.rs:31,250`. Its GET returns a provider-neutral stream,
not a 16 MiB metadata buffer. Thus 32 MiB fits those implemented byte limits by
source inspection. This proves no throughput, deadline or backend conformance
result. Later control loaders must read through EOF with a 32 MiB bound and
later root codecs must use this separate constant. Do not raise job framing,
public bodies/status, node/intent/heartbeat bounds or generic backend limits.
S3 metadata support remains C01d, with no new claim here.

C04a2b should own only the new root codec, cross-record credit derivation and
checked budget/candidate helpers with focused root tests, plus parent-assigned
exports. Reuse C04a1 member and C04a2a template/timing codecs. No runtime,
controller loop, provider call, storage integration, Cargo/dependency or Python
guard work belongs to that slice. Later pure transitions enforce the atomic and
monotonic relationships above; codecs alone cannot prove historical evidence.

Focused future behavioral assertions, not results of this design task:

1. A 1024-start root retains its budget through readiness and all 1024 first
   stop insertions; beginning all drains before insertion gives the same bound.
2. At a saturated augmented count, unreserved membership/stop allocation fails
   without mutation while credit conversion and maximum permitted field growth
   still encode and reload, including renewal and late registration/recovery.
3. A lost/ambiguous start keeps its credit; authoritative never-invoked or
   terminated resolution releases it atomically without deleting its identity.
4. Stop completion and definite stop failure never restore credit; repeated
   delivery keeps identity/backoff, and a new stop identity can be refused while
   every other admitted node's first-stop slot remains available.
5. Competing first-stop proposals converge on one retained intent after CAS
   reload; replacement admission cannot spend that node's reserved slot.

Read-only evidence: ancestor and repository AGENTS searches found no applicable
AGENTS.md; inspected shared contracts, accepted A05/A07, `cluster/member.rs`,
DN-C04a1, C04 rubric/ownership entries and local/HTTP source limits. No tests or
runtime proof claimed. Only this new note was written. No delegates, other
edits, branches/worktrees, commits/pushes, .env reads, cloud calls, Cargo,
builds/tests or dependency changes occurred. The parent checked the fleet and
maximum-count arithmetic and adopted the 32 MiB cap and admission contract.
No new persisted field is needed. Actual codec/transition and concurrency tests
remain required; design adoption supplies no runtime evidence.
