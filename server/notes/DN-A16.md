# DN-A16 membership implementation handoff

Design only, adopted by the parent after independent review and a fresh passing
re-check of the drain, failure-invalidation and root-order corrections. Refine
accepted C03 into four sequential Sol-high assignments on
[the shared contract](../../plans/distributed-contracts.md), [the rubric and
testing policy](../../plans/distributed-nodes.md), and adopted
[A05](DN-A05.md), [A07](DN-A07.md), [A08](DN-A08.md), [A10](DN-A10.md),
[A11](DN-A11.md) and [A15](DN-A15.md). This note supplies no implementation or
test guarantee. It adds no public SDK method, provider interface, persistent
field, event framework or controller runtime. Identical workers retain separate
operation/partition capacity. C05 owns provisioning, C06 owns positive drain
proof and termination, R03/R04 own authenticated transport and admission, and
G01/G02 retain routing/status integration.

## Existing code and dependencies

`cluster/member.rs` already defines Node, Session, Drain, Intent and Heartbeat
with bounded codecs. Session is `{id,verifier}`; Heartbeat has no wall timestamp.
Its positive sequence and occupancy bounds do not establish history, authority
or agreement with the member's capacities. `cluster/state.rs` checks sorted
nodes/intents, launch/resource/incarnation bindings and retired identities.
`Control::capacity()` derives permanent first-stop credits;
`check_new_admission()` adds the admission-only inequalities. Retained progress
uses bounded validation without requiring new admission room. Keep these helpers
and their separate purposes.

`configuration::{Template,Timing}` supplies exact template fingerprinting and
validated timings. `startup_config::Bootstrap` already contains cluster/node,
nullable request ID, endpoint, enrollment token and Template. Parsing it proves
neither enrollment eligibility nor successful initialization. Target-local paths
may differ from the persisted outgoing template. Compare the retained start's
fingerprint to the persisted validated template, not to a hash of rewritten
worker paths. Enrollment has no caller-supplied template fingerprint field.

`store::LoadedControl` owns an immutable validated root and private backend
token. `load_control(deadline)` bounds the complete read. `PreparedOwnership`
consumes a captured base and one Match; its pure constructor changes only the
controller tuple. `controller::{OwnershipObservation,OwnershipGuard}` already
provides complete-root observation, checked certainty and final `authorize`.
Reuse these boundaries. Reviewed C04a1/a2, C04b2 and C04c3 notes describe their
limited evidence; they do not establish a running membership service.

Storage already provides `ObjectKey::heartbeat(incarnation)`, bounded
`Storage::read(key, MAX_HEARTBEAT_BYTES)`, `put`, `WriteCondition::{Create,Match}`
and provider-neutral ObjectStore. A heartbeat loader must additionally require a
nonempty representation token and run Heartbeat::decode. Reuse these APIs;
there is no heartbeat HTTP route, backend LIST authority or replacement store.

A15/A17's checked EnrollmentProof and its narrow claim/verifier accessors are
implemented and independently reviewed in R03a2b1. C03a consumes only that
reviewed possession boundary; it does not depend on the later provisional
eligibility implementation under review. C03 must not reimplement signing,
proof verification, hashing domains or replay. A proof establishes public-key
possession only. The enrollment bearer check, fresh durable authority and final
admission remain route responsibilities. A11's
generation-2 Config/worker semantic fingerprint is now implemented and reviewed
in C08c1. Preserve config opaquely here and use its generation-2 synthetic
fixtures. C08c2 compatible HTTP attachment is implemented and independently
reviewed; S3 composition and runtime attachment remain later work. Invent no migration.
The parent serializes Cargo and Rust source ownership. These assignments
receive no edit/build slot until the parent releases and assigns it.

## Typed boundary and responsibility

All new modules below are crate-private. Their records have private fields and
redacted Debug where needed. Use existing StorageError categories; no new
public errors or methods are needed. The following names are proposed C03
interfaces, not claims that they exist today.

| File owner | Callable interface | Meaning |
| --- | --- | --- |
| C03a, `cluster/membership.rs` | `pub(crate) registration_plan(root: &Control, owner: Owner, proof: &EnrollmentProof, proposal: Uuid) -> Result<RegistrationPlan, StorageError>` | Pure exact retry or validated registration candidate. Read validated claims through A15's narrow accessors. |
| C03a, same file | `pub(crate) ready_candidate(root: &Control, owner: Owner, evidence: &ReadinessInput, proposal: Uuid) -> Result<MembershipCandidate, StorageError>` | Pure starting/registered to ready/verified calculation. Input facts are supplied by the later authenticated probe owner. |
| C03a, same file | `pub(crate) loss_candidate(root: &Control, owner: Owner, evidence: &ExpiryInput, proposal: Uuid) -> Result<MembershipCandidate, StorageError>` | Pure exact-incarnation lost candidate, never a stop or idle proof. C03d produces time evidence. |
| C03b, `cluster/store/membership.rs` | `pub(crate) PreparedMembership::prepare(base: LoadedControl, candidate: MembershipCandidate) -> Result<Self, StorageError>`; `submit(self, &Storage<B>, deadline: Instant) -> Result<OwnershipOutcome, StorageError>` | Consumed single-write metadata submission, using the existing four outcome categories. Historical outcome naming grants no authority. |
| C03c, `cluster/heartbeat.rs` | `pub(crate) HeartbeatWriter::new(binding: HeartbeatBinding) -> Result<Self, StorageError>`; `pub(crate) publish(&mut self, &Storage<B>, sample: HeartbeatSample, deadline: Instant) -> Result<HeartbeatWriteResult, StorageError>` | One owned per-boot writer, one write per invocation, with bounded reload before another attempt after uncertainty. |
| C03d, `cluster/heartbeat/observation.rs` | `pub(crate) Observations::new(controller_boot: Uuid, now: Instant) -> Self`; all methods below are also `pub(crate)` | Synchronous local evidence, bounded by retained membership. |

`RegistrationPlan` is `Existing(Node)` or `Change(MembershipCandidate)`.
Existing is a copy of the exact current binding, not a fresh session or a
readiness grant. MembershipCandidate is non-Clone and carries the complete
base OwnershipObservation, expected Owner and validated candidate Control.
Only membership.rs constructs it. A `pub(super) into_parts(self)` exposes those
three parts to the store child through its cluster parent visibility; no
caller-supplied arbitrary bytes or token access is added. Preparation compares
the captured base's complete observation and exact held controller tuple before
encoding. The base/candidate ownership tuple is unchanged by membership.

ReadinessInput and ExpiryInput are crate-private plain records with
crate-private fields in membership.rs. They are
explicit caller assertions, not cryptographic certificates or reusable
authorization tickets. ReadinessInput contains cluster/node/incarnation/session,
endpoint, verifier, resource ID, original challenge and verified echoed challenge,
initialized outcome, and probe/root-observation association. Its later caller
constructs it only after A15 reply signature/content/authority checks and local
initialized-worker/storage/catalog checks. ExpiryInput contains the exact
binding, observation generation, controller boot/grace anchor, optional last
accepted sequence, last-advance anchor and sample Instant. A missing-heartbeat
case uses the first tracked/grace anchor rather than a fictitious sequence.
C03d validates and produces this time evidence. Neither record performs I/O
or claims that supplied facts were authenticated. Runtime final checks remain
mandatory even when the pure function accepts them.

HeartbeatBinding is a crate-private record with crate-private fields for the
exact cluster/node/incarnation, session ID/verifier, operation slots, partition
slots and queue size. The session fields are local binding evidence and add no
heartbeat JSON fields. A changed durable binding cannot inherit observations.
HeartbeatSample has crate-private fields for running/queued operation/partition
counts, drain ID and accepting. It has no caller-selected sequence or identity.
HeartbeatWriteResult distinguishes acknowledged, exact read-back, definite
conflict and unknown; it includes no session secret. Adopted [A21](DN-A21.md)
adds a consuming acknowledged-result extraction yielding opaque non-Clone
AcknowledgedHeartbeat with read-only exact binding/sample/positive-sequence
access. Only a timely acknowledged write mints it; exact read-back/Unknown does
not. It proves the write, never task settlement or idle. LoadedHeartbeat remains
opaque with private decoded record/token; A21 grants cluster-visible bounded
load_heartbeat and borrowed binding/heartbeat access for C03d, with no token
accessor. No public heartbeat facade or general publication/state-machine
abstraction is needed. C03d methods take explicit `now: Instant` and `expiry: Duration`:
`reconcile_members(&mut self, root: &Control, now) -> Result<(), StorageError>`,
`observe(&mut self, binding: &HeartbeatBinding, heartbeat: &Heartbeat, now)
-> Result<(), StorageError>`,
`read_failed(&mut self, binding: &HeartbeatBinding, now) -> Result<(), StorageError>`,
`fresh(&mut self, binding: &HeartbeatBinding, now,
expiry) -> Result<bool, StorageError>` and
`expiry_input(&mut self, binding: &HeartbeatBinding, now, expiry)
-> Result<Option<ExpiryInput>, StorageError>`. These are concrete operations,
not an extensible event dispatcher. `read_failed` records a failed, missing or
corrupt heartbeat read. Every operation that checks time persists any detected
continuity invalidation; neither freshness nor expiry checks are observationally
read-only after such a failure.

The later invocation owner creates one absolute monotonic deadline before the
first root read. It also bounds fresh recomputation attempts, at most one fresh
recovery proposal per invocation. Parsing, root cloning, bounded encoding,
fingerprinting and OwnershipObservation::new run outside short locks on the
existing bounded coordinator CPU/I/O executor as finite commands. Do not move
them onto Tokio polling threads or add unbounded spawn_blocking work. Pure here
means synchronous and effect-free, not cheap enough for an admission lock.

At admission capture the guard generation. Outside its mutex load the complete
root, prepare its OwnershipObservation and candidate, and validate the route
evidence. Observe that root under the short mutex. Immediately before first
polling a membership submission, call `guard.authorize(generation, observation,
fresh_sample)` and require its Owner to equal the candidate Owner. Check the
original deadline there as well. A newer high-water root, changed controller
tuple, closed certainty or expired budget rejects submission. Exact retry
responses need this same final authority boundary, though they perform no PUT.
There is no mutex across storage or HTTP awaits. A race after the final check
is fenced by the captured token; a lease lapse alone does not revoke a write
already submitted.

The later async runtime registers and retains submitted work before polling it.
It keeps task/admission ownership through preparation, storage completion,
cancellation and settlement, and prevents an abandoned request from starting an
untracked second attempt. Results are observed through A10, never passed to
OwnershipGuard::finish as though they were acquisition/renewal evidence.
Acknowledged registration is durable metadata, not new controller authority.
Unchanged-controller observations preserve installed certainty and its original
deadline; they never restore or extend it. An obsolete request's error must not
unconditionally invalidate a newer recovered guard. Current authority/time or
integrity failures close the current permission through the existing rules.

## 1. C03a, pure registration and readiness/loss candidates

Own new `server/src/cluster/membership.rs` and its private
`server/src/cluster/membership/tests.rs`. Parent owns only its module declaration
in `cluster/mod.rs`. Prerequisites are reviewed control/member codecs, A10 and
A15 R03a1/a2, including narrow proof-claim accessors. This mini implements no
listener, readiness client, timer, task registry, heartbeat read or runtime loop.

Every change validates the complete base, checks held owner/epoch, rejects the
base change UUID, uses checked root revision+1 and the caller's fresh proposal
UUID, and validates the whole result. It preserves controller, config, template,
timing, policy/revision, scheduler, admin head/sequence, all unrelated members
and all intents. Reject any new/current incarnation that appears in permanently
retained retired evidence. Pure acceptance cannot establish local certainty.

Managed first registration binds exact cluster/node/request launch, retained
start and persisted template/capacities. The node must be unregistered starting
or draining. Allow in_progress or reconciling only with retained dispatched
delivery evidence, or completed/resource_known. Reject pending, failed,
unrecognized/quarantined and terminal identities. Deadline expiry never
invalidates an otherwise eligible first registration or exact retry. Resource
recovery may follow registration. Do not complete a start, attach a guessed
resource, erase a delivery or allocate a replacement here.

Starting registration remains starting/registered; draining remains
draining/registered with the same drain. Exact same-incarnation retries recover
starting, ready or draining only when launch, session ID, A15-computed verifier,
endpoint and all capacities match. Return Existing without root/counter changes,
even when revision is exhausted. Changed values conflict. Ready retries preserve
verified, and draining retries preserve drain evidence. Never revive managed
lost/stopped, retired boots or a failed terminal launch. Per-boot incarnation,
session and signing key are generated once by the later worker startup owner
and retained across lost replies/API failover. No bearer session secret is
stored or returned. Enrollment uses A07's distinct API reply, not ReplyAuth.

External registration requires request_id null and external mode. New admission
creates resource `external:<node_id>` and checks the complete candidate's
`check_new_admission()` plus the required projected status bound through the
later bounded status encoder. Until that encoder is available, the parent must
sequence this admission gate before enabling the route; do not silently omit
it or implement a second public status format here. Replacement under a retained
stable node ID requires old lost/stopped durable state, fresh incarnation/session,
and room to append the old incarnation to sorted, permanently retained retired
evidence. At 16 refuse replacement. Reset verification/drain, require readiness
again and preserve the retired set. This is retained-record growth, not a new
node allocation. Neither external path creates provider intents or autoscaling.

Ready requires known exact resource, unchanged starting/registered binding and
a successful initialized authenticated execution challenge for that incarnation.
The pure candidate compares all supplied association fields and changes only
registration/state. TCP reachability, endpoint syntax, heartbeat or an
initialized:false reply cannot substitute. A ready exact probe may be treated
as unchanged by the later caller; draining/lost/stopped cannot become ready.
Expiry compares the exact registered incarnation and evidence binding. It
changes starting/ready members to lost and preserves registration/session/resource
and retained launch/stop evidence. Stale draining members remain draining and
unavailable for routing, preserving their binding and drain evidence for
continued authenticated C06 reconciliation. A loss candidate refuses to change
a draining member. Staleness never marks stopped, confirms idle, creates a stop
intent or proves termination. An unregistered ambiguous launch is not expired
by readiness deadline. Runtime excludes expired nodes immediately even before CAS.

Acceptance tests are a few private grouped cases using reviewed root fixtures:

- First registration and exact retry after simulated reply loss, including a
  late reconciling dispatch, completed resource_known and draining first/retry.
  Isolate pending, missing dispatch, wrong launch/template/capacity and failed
  start as separate negative causes with otherwise valid roots.
- Exact retry preserves ready verification/drain/session/endpoint; changed
  session/key/endpoint fails. Managed terminal and retired external identity
  rejection, external durable-terminal replacement, sorted retirement and
  exhaustion each leave the original root unchanged on failure.
- Readiness succeeds only with exact initialized challenge/resource/binding;
  independently change challenge, incarnation or known resource. A heartbeat
  supplies no ReadinessInput. Drain/terminal phase rejects a valid probe.
- Expire an otherwise eligible starting/ready registration, then use otherwise
  valid expiry evidence for a draining registration and require preservation of
  its phase/binding/drain fields for continued authenticated reconciliation.
- Preserve a nonempty admin chain, independent policy revision, scheduler and
  unrelated nodes/intents byte-for-byte through representative changes. At
  saturated admission bounds, new external allocation fails while retained
  registration/recovery still encodes with unchanged first-stop credits.
  Checked revision exhaustion rejects changes while an exact retry succeeds.

## 2. C03b, consumed membership root submission

Own new `server/src/cluster/store/membership.rs` and private
`membership/tests.rs`; only the private declaration/reexports and shared Match
helper extraction in `server/src/cluster/store.rs`, with affected assertions in
`store/tests.rs`, are additionally owned. Depends on C03a and reviewed C04b2.
No controller.rs changes, runtime integration, admin-history scan or Cargo
dependency change belongs here.

Factor the existing PreparedOwnership::submit single-Match/read-back body into
one private store.rs helper taking the owned base, validated candidate, bounded
bytes and original deadline. PreparedOwnership and PreparedMembership consume
that same helper; leave existing ownership preparation restrictions intact.
The helper remains private to store and its children. Reuse OwnershipOutcome's
Acknowledged, ObservedExact, Conflict and Unknown rather than another public
facade. Membership prepare must check its candidate/base association and owner
again; it cannot call ownership_candidate unchanged because that constructor
deliberately preserves every member. This is the precise required extension.

At most one `Match(captured_token)` and one reconciliation load_control occur
under the original deadline. Only timely nonempty-token durable success is
Acknowledged. Exact complete candidate and proposal UUID read-back can yield
ObservedExact after possible application. A definite rejected Match stays
Conflict. Later/different roots, missing/corrupt/failed reads and equal UUID
with unequal contents remain Unknown after possible application. No error,
cancellation, timeout or lower/later read proves absence. No automatic resend.

The caller may recover an exact durable registration binding through C03a even
after unrelated root updates. That proves the binding now exists, not that this
particular ambiguous proposal won. If absent, only an explicit fresh-root,
fresh-proposal owner-checked CAS within the remaining budget can establish a
new result and fence the old token. Observing absence alone cannot release the
old work or authorize a conflicting new session. Local certainty and current
eligibility must still hold before acknowledging enrollment or using readiness.

Private acceptance tests reuse `server/tests/support/storage.rs` Fake,
ReadGate, put_faults and the store's before/after-application gate pattern:

- Two registrations from one token have one winner. Reload the loser and prove
  identical claims recover Existing, while different claims conflict.
- Lose a committed registration reply; exact-current metadata confirms it.
  Interleave an unrelated root update and require Unknown for the original
  proposal while exact-binding retry still recovers the session unchanged.
- Hold a membership write before application, commit takeover or drain from
  its token, then release it. It loses without overwriting the new phase/owner.
  Gate readiness against drain/replacement in the same manner.
- Missing/corrupt/failed reconciliation and a pending delayed write preserve
  uncertainty, with one PUT/one reconciliation bound and no hidden resubmission.
  Exercise only the affected existing ownership helper assertions after the
  extraction, including exact candidate equality and unrelated-state preservation.

## 3. C03c, bounded per-incarnation heartbeat publication

Own new `server/src/cluster/heartbeat.rs` and private
`server/src/cluster/heartbeat/tests.rs`. Parent owns its cluster module line.
Depends on C03b's settled uncertainty semantics and existing storage/member
codecs. It owns only heartbeat loading/writer state and one-call I/O. It does
not generate boot keys, start a periodic task, inspect provider inventory or
implement execution counters/admission. R04 later supplies bounded memory-only
occupancy samples while control work remains independent of full compute slots.

Writer fields retain binding, exact loaded token, highest attempted sequence,
last confirmed sample and whether reconciliation is required. It is non-Clone
and single-owner. Validate exact capacities and identities as well as the codec
occupancy limits before I/O. Allocate sequences internally with checked addition,
starting at 1 for an absent fresh-incarnation key. Caller samples cannot choose
or reset the sequence. No wrap or saturation is allowed. On exhaustion return a
distinct internal exhausted result; the runtime closes new admission and
requires a fresh boot. Retained work follows the ordinary drain/cleanup rules.

First publication uses Create; after a complete valid read or acknowledged
write use Match of that representation's exact nonempty token. A conflict or
possible application closes the writer's direct-write path. Before any later
write, perform a bounded complete reload/reconcile under that invocation's
remaining original deadline. Compare the exact pending sample where available.
Keep the highest attempted sequence even if read-back is missing or lower.
An explicit subsequent proposal must be higher than both observed and attempted
sequences. Its Create after definite current absence, or fresh-token Match,
can fence the prior condition only by actually winning; until then the old
attempt may still apply. Never resend pending bytes/condition automatically or
treat a lower read/timeout as rollback. Mismatched/corrupt records fail closed.

Each publish invocation attempts at most one write and at most one reload;
it returns conflict/unknown without retry loops when its budget is exhausted.
The later runtime owns a bounded serialized writer task, fresh durable
eligibility checks, shutdown/cancellation settlement and retained task handles.
It must not instantiate a second writer for a possibly submitted boot attempt.
Under A21, the proof-heartbeat publication is separately registered against its
barrier sample/observer before I/O and retained through acknowledgement, cleanup
and required supervisor reaping. AcknowledgedHeartbeat alone cannot release it.
R04c's final locked drain check requires that ownership to settle; the observer
exception excludes no pending heartbeat publication. C03c tests cover receipt
issuance/refusal; R04c owns the causal post-acknowledgement/pre-reaping gates.
Heartbeats can report drainage but cannot open local admission or revive a
durably lost/stopped binding. No file or large serialization work is added to
an admission lock or Tokio polling thread.

Small private tests assert Create then exact-token Match and exact capacities;
independently invalid occupancy/identity/capacity rejects before PUT. Gate a
lost acknowledgment before and after application, return a lower or missing
read while the old write is pending, and prove no sequence reset or automatic
second PUT. A fresh higher-sequence conditional proposal must resolve its race
by token competition. Test definite conflict, failed reconciliation, exhausted
deadline and u64 exhaustion through an explicit private fixture. Settle all
gated writes before checking final objects and counters. No codec matrix repeats.

## 4. C03d, local advancement observations and stale candidates

Adopted A21 adds the opaque DrainHeartbeat composition described in its receipt
table: a current ordered root, exact binding, complete LoadedHeartbeat and
mutable continuity/time checks. Its read-only evidence grants no independent
idle authority and exposes no raw constructor; C06b must combine it with the
exact authenticated drain reply and current round. Existing failure, high-water
and takeover-grace rules below remain mandatory.

Own new `server/src/cluster/heartbeat/observation.rs` and its private
`observation/tests.rs`; only its private declaration and narrow crate-private
reexports in heartbeat.rs are additionally owned. Depends on C03a/c and A10's
takeover rules. It constructs C03a ExpiryInput through its crate-private
constructor. No storage polling loop, router, autoscaler, readiness HTTP client
or drain protocol is implemented in this mini.

Keep at most one observation entry per retained current incarnation, bounded
by Control's retained membership. Before any observation-map mutation,
reconcile_members enforces A10's monotonically ordered complete-root observation
boundary. Prepare OwnershipObservation::new from that exact validated root
outside short locks; retain the accepted revision and complete observation.
Reject older roots without mutation. Equal revisions require identical complete
observations; disagreement fails with sanitized InvalidCatalog, invalidates
current continuity/freshness, and retains the accepted root and sequence
evidence. A rejected root cannot prune entries, install an old binding or reset
sequence evidence. The later runtime also applies the existing A10 guard
boundary before authorizing effects; observation-map ordering grants no authority.
After that ordering check, reconcile_members removes obsolete bindings
and adds new eligible ones using explicit sample time. Membership removal,
incarnation/session/verifier/capacity changes invalidate prior observations.
Controller restart/takeover creates a new local generation, clears load and
stabilization evidence, and starts a full expiry grace interval. Newly tracked
bindings likewise get a full interval. Routine unchanged-controller root
updates do not reset grace or last advancement. Clock/sample errors detected by
any method, including fresh and expiry_input, persistently invalidate continuity
and captured expiry evidence. They never erase sequence evidence to accept a
regression. No later normal sample may silently reuse invalidated evidence.
Reestablished monotonic continuity receives a full grace interval before loss;
fresh routing/shrink additionally requires a valid sequence advancement. Repeated
or lower sequences cannot restore freshness.

Accept a heartbeat only for exact current binding/capacities, valid metrics and
strictly advancing positive sequence. Equal sequence does not change its
last-advance Instant or metrics; regression and mismatched records reject.
Record sample time when that advancement is accepted, not GET start time,
worker time or last successful GET. Bound the caller's GET/body/decode to its
read deadline before observation. Freshness requires valid metrics and elapsed
time strictly less than expiry since last advancement. The read owner calls
read_failed for a failed, missing or corrupt result before consulting freshness.
It invalidates metrics and captured expiry evidence, advances the checked local
observation generation, and retains the sequence high-water and existing time
anchors. Ordinary read failures do not refresh last advancement or repeatedly
restart expiry grace. Subsequent time-valid expiry may still produce newly
bound loss evidence once the original intervals elapse. Fresh routing/shrink
remains denied until a valid higher sequence reestablishes evidence with full
grace. No read failure or recovery establishes safe idle.

expiry_input is available only after both the controller/tracking grace interval
and the last-advance expiry interval have elapsed, using checked monotonic
arithmetic. It binds sequence, observation generation and exact incarnation.
Before submitting loss, the runtime rechecks that generation/sequence/anchors
still match the live entry and that no newer sequence was observed while the
candidate was prepared. A new advancement invalidates the old candidate even
when the durable root token is unchanged. Resource, phase and controller
changes also require recomputation. Retain this responsibility through final
guard authorization and submission; pure time records cannot certify it.

Readiness/probe orchestration and routers require fresh observations in addition
to their durable phase/authenticated initialization checks. Fresh heartbeat is
necessary evidence for routing, never sufficient readiness proof. Missing or
stale data resets local continuous-low-load evidence and cannot authorize
autoscale shrink. A stale drain candidate can only request continued C06
reconciliation; it cannot set ack_seq/confirmed_idle, infer termination or issue
a provider stop. Expiry changes starting/ready members to lost under C03a while
stale draining members remain draining and unavailable for routing. Unresolved
launches keep consuming capacity. Neither absence nor readiness deadline permits
replacement. Draining registrations stay draining solely for authenticated
drainage.

Private tests use separate explicit sequence/time samples. Show advancement at
t0, repeated GETs of that sequence near expiry, and expiry at the original
deadline. Independently vary regression, capacity and incarnation while keeping
other facts valid. Restart/takeover with an old high sequence grants a full
grace interval yet grants no readiness from that grace alone. A newer sequence
invalidates an already captured ExpiryInput; unrelated root progress does not
refresh it. Independently test a failed/missing/corrupt read immediately denying
freshness, a repeated sequence failing to restore it, and a higher sequence
restoring evidence with full grace. Exercise time regression through both fresh
and expiry_input: later normal samples cannot reuse invalidated evidence, and
recovery preserves the sequence high-water. Reconcile R2, delayed R1, then R2;
R1 must not mutate/prune/reset, and repeating R2's heartbeat must not count as a
new advance. An equal-revision changed root must fail integrity without erasing
sequence evidence. Test valid pruning/replacement, bounded entry count and
duration overflow without another fake-clock framework. Couple one expiry
record to C03a loss_candidate and verify retained intents, credits, drain,
controller and admin fields survive. Do not duplicate C06 drain-proof tests.

## Evidence discipline and remaining parent work

Each mini owns only the files listed above and its corresponding new
`server/notes/DN-C03a.md`, `DN-C03b.md`, `DN-C03c.md` or `DN-C03d.md` delivery
note. Private APIs are tested in those modules' private tests, never by expanding
public visibility for `server/tests`. Reuse reviewed codecs/Fake/helpers and
existing test-only crate alias. Parent serializes shared declarations and every
Cargo slot, then runs its specification/quality review workflow. Implementers
use focused causal regressions and affected checks, not repeated broad suites.

Use gates/events and explicit Instants. Watchdogs bound failed observations;
retain any failure until all gates are released, tasks settled or aborted and
joined, and owned pools/resources cleaned up, then assert it. Sleeps and
scheduler timing do not prove races. These in-process tests can establish
candidate, token and local-time behavior only. Later E03/E04 real-process/TCP
acceptance must independently cover worker initialization/enrollment with lost
replies, two API owners/takeover, reserved readiness/heartbeat/drain progress at
full compute capacity, stale boot rejection and actual transport cancellation.
Reuse the planned local conditional service and acceptance harness.

No new protocol/schema decision is requested. Parent reconciliation is limited
to sequencing the four file owners, A15 proof accessors, the projected status
admission gate and C08c fixture generation. Preserve A05's existing C06 ordering:
create/claim a stop only after durable authenticated drain proof. This matters
because state.rs binds retained stop incarnation to member incarnation. Eligible
late unregistered draining enrollment must precede that proof/stop allocation;
it retains its first-stop credit meanwhile. Do not rewrite a dispatched stop's
target or weaken the codec to accommodate a different ordering.

Later small runtime assignments still own bearer authentication and distinct
enrollment replies, initialization/challenge I/O, fresh observation admission,
retained async writer/submission ownership, periodic reads, routing and controller
renewal supervision. C05/C06 retain inventory/replacement and positive drain
proof. None of that wiring belongs in a pure-record mini. Only this new note
is written here; verification is a scoped whitespace check on this file.
