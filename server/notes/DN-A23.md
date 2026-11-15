# DN-A23 bounded autoscaling handoff

Adopted design handoff, 2026-11-11, after independent review, parent corrections
and a fresh design re-check. The parent explicitly adopts all five interpretations
below and grants the listed narrow file ownership for sequential assignments.
This establishes no C07 implementation or runtime behavior. The original design
used gpt-6.1-sol with high reasoning, without delegation, and changed only this
note. R04c2 was unfinished during design and was excluded from inspection; it is
now delivered and under separate review. Shared modified/untracked work must
remain intact.

## Basis and prerequisite status

The current rubric leaves F09..F14, aggregate C03..C07, T07 and E03/E04 open.
Component completion does not close those requirements. Read the orchestration
and testing policy, relevant distributed contracts and A05 controller/autoscale
rules, adopted A08 accounting, A10 guard, A16 observations, A19 provisioning and
A21 drainage. Inspected completed cluster interfaces and private test seams.
No applicable ancestor or repository AGENTS.md was found. Applied unslop.

C03c/d, C04b2c, C04c2, C05b/c/d and C06a are reviewed component prerequisites
according to the parent rubric. Their delivery notes supply reported execution
evidence only; nothing was rerun here. Preserve C03d's concrete drain-ID correction,
C04b2c's pending-operation clock correction, and both C05d parent corrections:
independent positive recoveries need new successor-root inventory, and collision
checks scan bounded records directly without the redundant nested node scan.
C06b/c/d, R04c2/c3, configured issuance and runtime composition remain incomplete.
These three minis need no unfinished R04 bodies. They must not fabricate successful
idle, callback, routing or provider behavior to bypass those dependencies.

Existing interfaces determine the split:

- `state.rs` defines Policy, Scheduler and Control. Limits are 2048 nodes,
  3072 intents, 4096 combined records and 32 MiB encoded Control. Managed autoscale
  requires `1 <= min_nodes <= desired <= max_nodes <= 1024`. Fixed zero is valid;
  external mode has no autoscale.
- `Timing` already validates heartbeat/expiry and lifecycle timings. Defaults are
  heartbeat 5000 ms, expiry 15000 ms, stabilization 60000 ms and cooldown 30000 ms.
  No new public knob or record field is needed.
- `heartbeat::Observations` privately retains complete OwnershipObservation,
  boot/generation, exact Node/binding, sequence, acceptance Instant and metrics.
  Its existing public-to-crate queries expose freshness, expiry and DrainHeartbeat,
  not a fleet metric/continuity certificate. The first sequence is only baseline.
- `controller.rs` supplies Owner, OwnershipSample, OwnershipObservation and
  OwnershipGuard. A full observation hashes the bounded complete root. Its
  generation is not an autoscale continuity counter.
- `admin::admin_candidate` preserves identical-policy revision semantics, advances
  admin history even for identical submissions, and preserves Scheduler. Controller
  target changes must never call it or write a ControlDecision.
- `provision::managed_capacity` charges each retained managed node/launch once,
  including pending, ambiguous, lost, draining and phase-only stopped members.
  Only its exact retained irreversible witnesses release the charge. Node/start
  cross-record validation binds that accounting. Positive inventory recovers that
  same launch; unknown or contradictory inventory blocks later allocation.
- `drain::begin_drain` and `clear_idle` construct metadata only. DrainBinding owns
  an exact private Node. Consuming membership/provision/drain adapters capture the
  whole base and Owner and call `store::submit_control`, one original-token Match
  with at most one readback under the original absolute deadline.

## Adopted parent reconciliation

The parent adopts these five explicit refinements of A05/A16 after the corrected
design passed independent re-check. No public API, configuration or schema changes.

1. Define grow eligibility as current Ready, Verified, accepting members with exact
   registered bindings. Every such member must have fresh accepted progressing
   heartbeats; missing/stale metrics cannot remove a member from this set. Require a
   nonempty eligible fleet. An empty managed fleet retains desired at least min;
   C05 fills that persisted deficit. Do not synthesize a queue observation or grow
   vacuously. Hold growth until every charged managed member is eligible,
   `managed_capacity == desired`, and no unresolved intent or drain remains.
   Thus one queued observation can add one target unit, but a subsequent queued
   observation cannot add another while its launch is pending, starting or
   unresolved. Lost/stopped overlap also blocks growth until positively resolved.
   This is a conservative startup interpretation, not replacement permission.
2. For shrink, require `managed_capacity == desired` and every nonterminated
   member, including external members in a managed cluster, to be Ready/Verified
   with fresh accepting zero observations throughout the interval. Require a
   managed drain target and no unresolved intent or active drain. This prevents
   repeated target reductions before a previous physical removal finishes. A
   smaller physical fleet is a C05 deficit, not a reason to shrink the target.
3. Preserve autoscale elapsed continuity across a strictly increasing same-boot,
   same-owner/epoch Held lease extension only through the explicit renewal handoff.
   Before ordinary renewal begins, require a successful fresh guard check for the
   existing Owner. Successful begin suspends autoscale eligibility, retains the
   low anchor provisionally and records the post-begin authorization generation.
   During that pending operation, ordinary Unavailable with unchanged generation
   does not reset continuity. Any guard invalidation or generation change, failed
   begin, failed/ambiguous/obsolete completion, time failure or pending-operation
   expiry immediately calls autoscale_failed, even without a decision poll.
   Resume only after the matching finish returns Ok(true), a fresh guard check
   succeeds for the same Owner/epoch, and progressing loaded heartbeats restore
   eligibility before their retained freshness deadlines. A lease-extension root
   alone cannot resume continuity. Suspension grants no authority. Existing C03d
   freshness still clears on controller tuple change; its expiry/drain receipts
   remain unchanged. Takeover, release and actual certainty loss reset low time.
4. Scope continuous low load to accepted heartbeat observations. It cannot prove
   no work occurred between published or accepted samples. An identical repeat
   neither refreshes evidence nor resets an otherwise valid low interval. It
   cannot establish or restore evidence after baseline, invalidation or expiry;
   a later strict advancement is required. Contradictory equal-sequence or lower
   observations invalidate continuity. Accepted nonzero samples reset immediately, even if
   later zero samples arrive before the next decision poll. Do not require
   contiguous sequence numbers; the writer can skip uncertain sequence attempts.
5. Treat exact-root authenticated-drain feasibility here as structural eligibility
   to attempt A21's authenticated protocol, never completed idle proof. A live
   worker can fail after target persistence; C06 then holds safe drainage without
   stop permission. Requiring an actual confirmed drain before changing desired
   would conflict with the stipulated persist-target-before-lifecycle ordering.

No essential policy decision remains. The parent grants the narrow file ownership
below, serialized by assignment. Fixed overrides, changed policy history, physical
termination witnesses and original A10 time anchors remain as accepted.

## Mini 1: closed metrics and local continuity

Owner edits only `cluster/heartbeat/observation.rs`, its private `tests.rs`, and
narrow reexports in `heartbeat.rs`. Parent grants two read-only cluster-private
OwnershipSample getters in `controller.rs`, `monotonic() -> Instant` and
`unix_ms() -> u64`, with no guard-body change. All paths below are under
`server/src/`. Parent serializes these edits after the current Rust owner releases.

Add these concrete private types and methods, using existing tokio Instant,
OwnershipSample, Owner, LoadedHeartbeat, Control and StorageError:

```rust
Observations::autoscale_observe(
    &mut self, root: &Control, loaded: LoadedHeartbeat,
    sample: Result<OwnershipSample, StorageError>,
) -> Result<(), StorageError>;
Observations::autoscale_failed(
    &mut self, sample: Result<OwnershipSample, StorageError>,
) -> Result<(), StorageError>;
Observations::autoscale_snapshot(
    &mut self, root: &Control, owner: Owner,
    sample: Result<OwnershipSample, StorageError>,
) -> Result<AutoscaleSnapshot, StorageError>;
Observations::matches_autoscale(
    &mut self, snapshot: &AutoscaleSnapshot,
    sample: Result<OwnershipSample, StorageError>,
) -> Result<bool, StorageError>;
```

AutoscaleSnapshot has private fields, no raw constructor, Clone or Serde, and
value-free Debug. It retains the exact complete root observation, boot, Owner,
current observation generation, separate checked autoscale invalidation serial,
paired sample, exact bounded member/heartbeat associations and optional local
low anchor. Borrowed cluster-private getters supply those facts to mini 2 and
selection only. They expose no token or mutable metrics. A private consuming
`into_parts` is unnecessary. Snapshot creation itself grants no live authority.

The loaded-only ingestion path reconciles the ordered full root and accepts the
opaque load through the existing sequence/metric checks before recording autoscale
facts. Refactor the common internal observation routine if needed without changing
C03d semantics. Calls to raw `observe(&Heartbeat, ...)` must invalidate autoscale
continuity rather than mint verified autoscale facts. Existing read_failed,
invalidations, root reconciliation and time checks must feed the autoscale reset
logic even when no snapshot is being requested. Successful drain receipt queries
must not bypass that logic. Do not certify arbitrary metric arrays or booleans.

Missing/corrupt/failed heartbeat reads call existing binding-specific read_failed
and autoscale invalidation before any decision. Failed/missing/corrupt root reads
and failed paired time conversion call autoscale_failed even without a binding;
later runtime must also invalidate OwnershipGuard where A10 requires it. The
failure API always clears low evidence, even when its sample is Err. It never
creates a replacement timestamp. Stale old roots conflict without replacing the
accepted root or changing C03d maps; the autoscale attempt fails and its low
interval resets. Equal-revision divergent roots invalidate it with InvalidCatalog.
Malformed roots cannot retain a usable old autoscale snapshot.

The later renewal owner records the original guard boot, Owner, stamp and
post-begin generation in its existing single pending ownership operation. It
applies rule 3 before requesting snapshots, live matches or submission;
none is eligible while renewal is suspended. Loaded heartbeat ingestion and
deadline/time checks continue during suspension. A normal pending check's
Unavailable is distinguished by that still-pending operation and unchanged
generation, not by the error category alone. Completion errors and obsolete
stamps invalidate low evidence before any recovery can request another snapshot.
The matching acknowledged finish and fresh successful check close the suspension;
new progressing loads must also satisfy the original retained freshness deadlines.
Ordinary roots and new lease timestamps do not discharge this handoff. Existing
guard methods and authorization_generation suffice; add no guard-body changes,
caller success boolean, second renewal loop or authority-producing observation API.
This is an explicit later coordinator obligation, exercised now in component
composition tests with the actual guard operations.

Use one private bounded autoscale state within Observations, not an event journal
or another tracker supplied by callers. Keep at most one per-member sequence and
last verified acceptance/deadline, one exact membership/policy identity, one paired
low anchor and one checked invalidation serial. Nodes are bounded by MAX_NODES;
terminal lookup uses at most MAX_INTENTS entries. No per-read accumulation. Copies
of old/new membership and one outstanding bounded snapshot are finite; later
coordinator admission permits one outstanding autoscale proposal per boot.

Before replacing any acceptance timestamp, check every previous required member's
old expiry against the incoming Instant. A sample at or beyond that deadline
resets the interval, even if no poll ran during the gap. Check this on ingestion,
reconciliation and snapshot/match. First baseline grants no zero/fresh evidence;
a strict higher sequence can start evidence. Any of four nonzero counts,
accepting=false, missing required binding, bad capacity, lower or
contradictory sequence, failed read, freshness gap or time failure clears the
fleet anchor immediately. Sequence high-water and original C03d grace/advance
anchors survive their existing invalidation rules. An identical repeat preserves
an existing valid interval only until its original freshness deadline; it cannot
restart low using cached zero counts. All required members must recover via new accepted
advancements before a new fleet interval starts.

Start the fleet anchor at the paired sample where the last required member first
has valid fresh zero evidence. Store its Unix value only for diagnostics and use
its original monotonic Instant for elapsed stabilization. On subsequent samples,
compare signed wall elapsed with monotonic elapsed from that original anchor,
using nanoseconds and A10's inclusive 2000 ms drift tolerance. Never slide the
anchor forward on reads, metadata updates or renewal. Backward monotonic or Unix
time resets and fails closed. Conversion/addition/counter exhaustion also closes
proof. A clock that returns to normal cannot restore the old interval. This local
check supplements the live guard; it never renews it or changes its original time
anchor, including while a renewal is pending.

Continuity identity includes cluster/config/Template/Timing/provisioning, boot,
owner/epoch/Held state, whole effective Policy including revision and desired,
admin_seq/head, exact full Nodes and terminal-witness classification. Any change
to those resets. Comparing admin history conservatively resets even on an
identical-policy submission without altering its public revision semantics.
Ordinary newer root revision/change ID, own low_since/cooldown housekeeping,
unchanged membership and unrelated completed-intent progress preserve elapsed
continuity. Scheduler failures/backoff changes do not erase observed zero time;
they block target changes as below. An unresolved-intent/drain transition resets.
Renewal uses rule 3's explicit exception only. Never key continuity solely on
desired/bounds, complete-root digest or observation generation. Every newer root
still invalidates an issued snapshot; a new snapshot can retain the elapsed anchor
while binding the new full observation. Recompute rather than rebind old receipts.

Mini 1 causal checks use actual Control/Heartbeat codecs, complete load_heartbeat
and crate::test_storage::Fake in embedded private tests. Establish baseline then
strict advancement independently. Feed zero, busy, zero between snapshot calls
and prove lost continuity; vary failed, missing, corrupt, stale, lower,
time regression and root-integrity cases against a healthy positive control.
Repeated identical reads before the next progressing publication preserve the
original low anchor but never advance its expiry. After a real invalidation or
expiry, those same repeats cannot restore proof. Exercise a read cadence faster
than heartbeat publication so an ordinary duplicate read cannot prevent stabilization.
Prove a missed freshness deadline resets even when the next load advances.
Takeover/process replacement cannot inherit low_since; ordinary unrelated root
progress and successful renewal can reach stabilization without moving the original
anchor. Compose actual guard begin/check/finish with observations across repeated
ordinary renewals: pending Unavailable cannot erase healthy elapsed time or permit
an eligible snapshot, and only matching acknowledged completion plus fresh guard
check and progressing loads resumes. Separate failed, ambiguous, expired and
superseded renewal controls must reset before recovery. A same-owner extension
root without this completion cannot restore continuity. Keep original C03d
expiry/drain tests rather than duplicate their matrix.

## Mini 2: pure decisions and bounded candidates

Owner creates `cluster/autoscale.rs` and `cluster/autoscale/tests.rs`. Parent owns
its one declaration in `cluster/mod.rs`. Grant only a narrow structural selector
and tests in `cluster/drain.rs` and `cluster/drain/tests.rs`:

```rust
drain::select_autoscale_drain(
    root: &Control, owner: Owner, metrics: &AutoscaleSnapshot,
) -> Result<Option<DrainBinding>, StorageError>;
DrainBinding::member(&self) -> &Node;
autoscale::autoscale_candidate(
    root: &Control, owner: Owner, metrics: AutoscaleSnapshot, proposal: Uuid,
) -> Result<AutoscalePlan, StorageError>;
```

AutoscalePlan is `Unchanged` or `Change(AutoscaleCandidate)`. Candidate privately
retains full base observation, exact Owner, consumed snapshot and expected Control;
only autoscale constructs it. Its consuming `pub(super) into_parts` gives mini 3
those four values. Plans/candidates are must-use, non-Clone, non-Serde, with
value-free Debug. No live-authority name, generic transaction closure or unchecked
Control constructor is added. Use the snapshot sample for all now values; callers
cannot inject an unrelated Unix decision time. Mini 2 validates base, exact
observation/owner/boot and snapshot associations again. Later caller must still
run matches_autoscale and live authorization immediately before submission.

The selector is production code owned by drain, not a successful test boolean.
It selects the lowest node_id among managed Ready/Verified members with concrete
resource, incarnation, endpoint and session/verifier, no drain, and exact fresh
accepting zero heartbeat in this snapshot. Require the launch's unused A08
first-stop credit, with no retained stop for that launch. A failed stop with a
consumed credit cannot become a successful selector control; later ordinary
additional-stop admission is outside this slice. It checks the complete base and held
owner. Its exact Node binding is a structural C06a target hint only. The autoscale
candidate does not mutate that node or retain a reusable drain permission.
After target confirmation C06 selects/revalidates against its new current root,
then uses begin_drain with fresh IDs and eventually C06b's actual authentication.
Unknown endpoint liveness, R04 configuration and barrier/settlement remain live
proof obligations. Never issue confirmed_idle, stop or termination from this hint.

Derive terminal exclusions using the exact witness rules of managed_capacity,
never NodeState::Stopped or missing observations. For other members, no terminal
witness means they remain required. Reuse managed_capacity for the physical count;
use a bounded local set for the same exact witness classification in continuity.
No ready-plus-intent sum, inventory guess, or decrement for idle is permitted.
Unresolved means Pending/InProgress/Reconciling for either action, including
stalled/exhausted starts. A phase Draining or retained active drain blocks shrink.
Completed history alone is not an unresolved intent. Failed stop/not_invoked
releases neither physical capacity nor first-stop credit.

Apply the reconciled growth/shrink eligibility above. Grow by exactly one if any
eligible operation OR partition queue is nonzero. Running work alone does not
trigger growth. Shrink by exactly one only if every required member has all four
counts zero for the interval, no unresolved intent/drain, physical count equals
desired and selector returns a binding. Empty, starting, unregistered, lost,
phase-only stopped and gapped fleets cannot qualify as idle by omission.

Time boundaries are exact: fresh requires `now < advance + expiry`; a gap begins
at equality. Stabilization passes at `now >= low_anchor + stabilization`.
Cooldown/backoff permit target change at equality with their persisted Unix
millisecond deadlines. Both block before equality. Use checked Instant additions,
checked u64 target/revision/time arithmetic and no saturation/wrap. Respect max
and min before increment/decrement. Target changes reset local low evidence on
their observed successor, set low_since_ms=null, and set cooldown_until_ms to
checked `sample.unix_ms + timing.cooldown_ms`. Preserve the existing greater
cooldown if necessary; never reduce it. Keep backoff_until_ms, failures, attempts,
retry history and stalled intents exactly intact. Metadata target persistence
precedes all lifecycle work.

low_since_ms is diagnostic only. When managed autoscale has a valid local low
anchor but no eligible target change, persist that anchor's Unix value if it
differs. When evidence is absent/reset, clear a nonnull diagnostic. Do not adopt
an old persisted value on restart/takeover. Fixed/external roots can only clear
stale diagnostics, never change desired. On invalid root/time input return an
error and clear local proof; a later valid exact root can propose the diagnostic
clear. No diagnostic write is required before that safe read. Low tracking can
continue through cooldown/backoff, so it need not restart when those expire.

A true no-op returns Unchanged before proposal/revision growth checks, after base
and proof validation. Diagnostic change and target change each use one ordinary
bounded root mutation, with distinct proposal, checked root revision+1 and complete
encoding. Reject nil/reused current change/admin-head proposal IDs; caller must
supply globally fresh UUIDs since old proposals are not all retained. Exhaustion
fails without partially changing root or emitting lifecycle work. Preserve every
field except revision/change_id, desired on a target change, and the documented
low_since/cooldown fields. In particular preserve policy.revision, bounds, mode,
admin_seq/head and immutable history, controller, config, Template/Timing, nodes,
intents/resources, delivery/error history and derived first-stop credits. Use
retained validation, not check_new_admission, for these updates.

Mini 2 checks reach stabilization at equality with independent all-zero controls
and refuse just before it. Vary operation versus partition queue for growth and
running versus queued work for shrink. Use real allocations to show ready plus
retained start counts once and pending/startup/ambiguous/physical overlap prevents
a second desired increase. Exercise min/max, fixed zero, external mode, exact
cooldown/backoff boundaries, checked exhaustion and no-op at exhausted revision.
Verify structural drain target selection/refusal independently. Compare whole
roots with expected allowed differences, including saturated A08 retained roots,
credits, scheduler failures and attempts. No codec matrix or integration-test
visibility widening is needed.

## Mini 3: consumed submission and component composition

Owner creates `cluster/store/autoscale.rs` and its private `tests.rs`. Parent owns
only the child declaration/reexport in `cluster/store.rs`. Adapter owns:

```rust
PreparedAutoscale::prepare(base: LoadedControl, candidate: AutoscaleCandidate)
    -> Result<PreparedAutoscale, StorageError>;
PreparedAutoscale::submit<B: ObjectStore>(
    self, storage: &Storage<B>, original_deadline: Instant,
) -> Result<OwnershipOutcome, StorageError>;
PreparedAutoscale::snapshot(&self) -> &AutoscaleSnapshot;
PreparedAutoscale::observation(&self) -> &OwnershipObservation;
PreparedAutoscale::owner(&self) -> Owner;
```

Follow PreparedMembership/PreparedDrain exactly: consume the original base and
candidate, compare complete OwnershipObservation, held Owner and unchanged
controller tuple, bounded-encode, privately retain the original token/bytes and
snapshot through settlement. Submit directly through unchanged submit_control.
The borrowed getters let the real caller perform final checks after consuming
preparation, without recovering a token, cloning proof or accessing private fields.
Do not add a second CAS/reconciliation algorithm, result authority, automatic
resend, public facade or callback. Unchanged creates no Prepared value or I/O.
Snapshot consumption prevents reuse, but cannot replace the later live match.

Caller preflight before polling submission is matches_autoscale on the live
tracker, fresh complete-root ordering, and guard.authorize with the captured
authorization_generation, exact observation and fresh paired sample. Require the
returned Owner to match candidate Owner and preserve the original absolute
budget. No await may intervene between final local validation and submission
entry except the owned submission itself. A remote race is still fenced by the
captured backend token. A later heartbeat can invalidate local evidence but does
not retroactively cancel an entered CAS; subsequent live drain proof is mandatory.

Acknowledged and whole-root ObservedExact confirm metadata only, never install
ownership certainty. Feed roots through guard.observe, not ownership finish as
renewal evidence. A later root after possible application remains Unknown even
if desired happens to match; no history search guesses success. Unknown, failed
read or actual lost certainty closes local low proof. Ordinary pending renewal
uses rule 3's suspension handoff and permits no submission. Conflict requires fresh root and
new evidence/proposal, never rebinding the old consumed snapshot/token. Supervisor
must retain the pending submission through cancellation until settled; dropping
its future does not establish rollback. No lifecycle action is released by these
minis, including after a diagnostic-only acknowledgment.

Component tests compose actual Observations, codecs, admin_candidate,
PreparedAutoscale and existing Fake helpers inside the crate. Gate two controllers
on the exact captured token and show at most one root wins. Independently race a
fixed override/changed bounds against a valid old growth or shrink candidate and
assert preserved admin history and target. Test applied-before-lost-reply exact
confirmation versus a genuinely later-root Unknown and missing/corrupt readback.
At retained admission saturation, metadata still commits with unchanged credits,
node/intent fields and failure history. Reuse the reviewed shared CAS matrix;
these tests demonstrate autoscale association/composition only. Every gate wait
has a finite watchdog; release gates and settle/join owners before assertions,
including failure paths. No sleeps as causal evidence or detached tasks.

## Sequential gates and later runtime obligations

Implement minis 1, 2 and 3 serially after parent adoption and R04c2 release, each
with separate new specification review then quality review. Parent owns shared
declarations and the narrow cross-file grants above. Keep tests in the existing
embedded private modules using crate::test_storage; do not expose pub(crate) APIs
to integration tests, duplicate Fake or add another runner. Future validation uses
the rubric's serialized existing target, locked/offline/no-default scoped commands
and finite test owners. This design assignment ran none of those commands.

Later runtime must own bounded complete root/heartbeat reads and report every
failed/missing/corrupt result, sample paired time outside locks, observe every
accepted load through the closed path and keep one supervised submission slot.
It must connect every renewal begin, invalidation, completion and pending expiry
to the suspension/reset handoff, even when no autoscale decision poll runs.
All finite encoding/hash/cloning/scans run on the coordinator executor outside
locks, never Tokio admission paths. Bound scans by nodes plus intents using sets
or binary lookup, at most O((N+I) log(N+I)) beyond existing bounded codec work;
do not introduce per-node nested full inventory scans. Snapshot storage is O(N+I)
within the validated limits, with no unbounded history. Errors use sanitized
Conflict, InvalidCatalog, Unavailable and LimitExceeded; Debug never emits root
bodies, tokens, verifiers, resource IDs or provider diagnostics. Checked local
serial exhaustion permanently closes that tracker rather than recycling proof.

Before enabling C05 allocation, later composition still needs the complete fresh
authorized inventory and actual <=1 MiB status-envelope admission gate. Before any
callback/effect it needs current live OwnershipGuard, original-deadline supervised
submission, fresh root ordering and independent callback-entry authorization.
C06 still needs actual authenticated drain round/settled heartbeat evidence,
reserved first-stop conversion, proof continuation and positive exact-resource
termination. Accepted stop callbacks, stopped labels, heartbeat loss and inventory
absence independently establish none of those facts. External workers receive no
provider termination. Persisted backoff/attempts survive policy and process changes.

Controller loops, public routes, callbacks, R04 settlement, startup attachment,
physical reconciliation and E03/E04 real-process acceptance are later work.
No source inspection or Fake test proves provider truth, runtime scaling,
distributed fencing, performance or RSS. The existing acceptance harness alone
will exercise real startup lag, competing APIs, restarts and safe drainage.

Only source/document inspection and creation of this note occurred. Validation
for this assignment is limited to this file's whitespace and final newline.
No code/test/build/Cargo, network/cloud, diagnostic probe, dependency/download,
secret/profile/.env, branch/worktree/commit/push or target operation was performed.
