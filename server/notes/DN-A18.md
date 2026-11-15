# DN-A18 sequence, replay and consuming proof handoff

Adopted design after independent reconciliation and the API cluster-access
correction below. Refine [A15 mini 3](DN-A15.md)
into three sequential assignments using the actual authentication implementation.
[Distributed contracts](../../plans/distributed-contracts.md),
[R03/R04 and testing requirements](../../plans/distributed-nodes.md), adopted
[A07](DN-A07.md), corrected A15 and [A17](DN-A17.md) remain authoritative.
Preserve [A06](DN-A06.md), [A09](DN-A09.md), [A10](DN-A10.md) and
[A16](DN-A16.md) ownership and evidence requirements.

R03a1/a2a/a2b1/a2b2 have completed both reviews. R03a2b3 has specification
PASS and a fresh quality PASS after the parent corrected its coordinator-key
test gap with a separate key and a passing focused test. C03a has delivered
and passed specification review; its quality review remains separate. This
design neither reads nor depends on that implementation. The parent must
serialize each assignment's Rust/Cargo access before implementation.

The three assignments implement sequence/replay mechanics and consuming memory
handoffs. They cannot complete aggregate R03a3's final authenticated admission,
R04, HTTP integration or distributed acceptance. No bool standing for capacity,
permit, barrier, initialization or accepted work may turn these helpers into a
claimed final admission. No new protocol, persistence schema, public SDK,
configurable policy, authentication framework or transaction callback is needed.

## Actual boundaries and small required changes

`auth.rs` declares private wire, credentials and verify modules and reexports
concrete crate-private types. Wire DTO fields and DecodeRecord remain private
to wire. Auth/Sender/Receiver are checked Clone records; proofs and
RequestContext are non-Clone, non-Serialize and non-Deserialize. Existing
sign_request methods check role/key, and the API key also checks cluster. A
worker key has no boot identity attached to it. Do not redo those primitives.

`verify.rs` owns private RequestAssociation and ReplyAssociation Arcs.
CheckedContent owns the exact original Box<[u8]> and parsed ExpectedContent.
EligibleRequest owns the matching request Arc, complete OwnershipObservation,
ReceiverIdentity copy and PendingLocalChecks. Content makes no root observation.
`matches_content` compares Arc identity, including for identical canonical Auth
verified twice. ReceiverIdentity captures cluster/node/incarnation/session/key;
`same_boot` compares all five. It does not prove enrollment or initialization.

RequestContext::for_send currently consumes checked Auth and original bytes,
validates content and captures a durable target verifier and complete root
observation. Its request, expected-content and target Arcs identify one outgoing
invocation. verify_reply borrows that context. Each call creates a new reply Arc
retaining those same three associations. EligibleReply and CheckedReplyContent
retain that reply Arc, and eligibility compares all three context associations.
Borrowed verification alone therefore allows multiple valid reply candidates;
it does not implement one-time acceptance.

OwnershipObservation::new encodes/validates the complete Control and records
revision, change UUID, SHA256 digest and complete controller tuple. It implements
Clone and full Eq; revision is private in cluster/controller.rs. Add only
`pub(crate) fn revision(&self) -> u64` there in assignment 2. Equality must still
compare the complete observation. A revision/change UUID pair alone is weaker.
Reuse this type, without an arbitrary digest constructor or another root hash.

The visibility obstacle is concrete. auth/replay.rs is a sibling of verify and
cannot destructure its private proof fields. Assignment 2 adds a minimal
`pub(super)` consuming helper and opaque combined value in verify.rs. Assignment
3 adds the corresponding consuming context method there. Keep all proof fields
private. Do not make RequestAssociation, ReplyAssociation, ExpectedContent or
TargetBinding accessible to replay, or export unchecked record constructors.
No new auth facade is needed.

`payload_stream::drive` still reads the first CONTROL and passes raw bytes to
Command::BeginRequest/BeginResult, which parse again and may create semantic
scratch. receive_request/result already require PayloadLease. PayloadOwners
admission is supervised payload ownership, not authenticated route admission.
WorkerPool::submit awaits reserve_owned on a per-worker channel of capacity one;
accepted Job::receive is separate, and dropping it does not cancel work. Waiting
submit callers are not bounded by that channel. These facts prevent treating
the pool or PayloadLease as a ready-made synchronous final reservation.

## 1. R03a3a, one checked sender counter

Own new `server/src/node/auth/sequence.rs` and private `sequence/tests.rs`.
Own only its module/reexports in auth.rs and the following small wire method.
Assignment 1 additionally owns only `pub(super) fn cluster_id(&self) -> Uuid`
on ApiSigningKey in credentials.rs. It returns the existing retained cluster.
No other credentials changes or temporary signing records are permitted.
Membership, payload, listener and Cargo remain unowned.

Use one `SenderSequence` with private fixed cluster, complete checked Sender
and last-issued u64 initialized to zero. It has no Clone/Serde/reset/setter and
value-free Debug. The concrete crate-private interfaces return auth::Error:

```text
SenderSequence::for_worker(cluster: Uuid, sender: &Sender,
                          key: &WorkerSigningKey) -> Result<Self, Error>
SenderSequence::for_api(cluster: Uuid, sender: &Sender,
                       key: &ApiSigningKey) -> Result<Self, Error>
SenderSequence::stamp(&mut self, auth: Auth) -> Result<Auth, Error>
```

Constructors require the matching role and key, retaining the complete checked
sender, including worker process/node/incarnation/session/key or API process/key.
for_api also compares the supplied cluster with key.cluster_id(); it rejects
a mismatched cluster, role or public key before constructing the counter.
They assert local binding, not membership or a singleton boot lifecycle.
stamp first compares cluster and complete sender to the fixed values. It uses
checked_add(1), fails Exhausted without mutation on overflow, replaces only the
checked Auth sequence, and returns the stamped checked record. Wrong binding
fails Invalid without spending a sequence. Once issued, a sequence is spent
even if context construction, signing, connection or later work fails.

Wire adds only `pub(super) fn with_sequence(self, sequence: u64)
-> Result<Auth, Error>` on Auth. Reject zero; move the record and replace its
Number field. All other validated fields remain unchanged. This does not expose
raw fields or Deserialize. Auth::decode remains the current send-template entry
point; this task need not invent a builder or manufacture temporary signatures.
The existing canonical writer checks the resulting encoded bound before send.

One monotonically increasing counter across ALL routes and ALL targets is
stronger than A07's per-complete-sender/key/target-session requirement. Gaps and
out-of-order arrival are permitted; the latter can reject legitimate requests.
No target map, per-route counter, extra allocator class or automatic reset is
needed. Issue u64::MAX once, then refuse every further stamp. Never wrap,
saturate, recycle or decrement. Do not use a response counter; ReplyAuth echoes
request_sequence and has no independent response sequence.

The later boot owner constructs exactly one counter after fixing its identity
and retains it through every route and enrollment retry. Worker boot fixes a
fresh incarnation/session/key; losing that memory requires replacement rules,
not reuse of an old incarnation with a counter reset. Each API boot has a fresh
process UUID even though replicas/restarts derive the same API key. API failover
cannot inherit another boot's process UUID with a zero counter. Shutdown stops
new stamps and settles accepted work before dropping the owner.

Put this counter inside the actual boot sender owner and use its one short
mutex for concurrent stamps. The private API prevents accidental field resets
and wrong-identity stamping; it CANNOT prevent constructing two allocators for
the same Sender, cloning a stamped Auth, signing it twice, or reconstructing a
context from the same record. There is no process-wide singleton registry here.
The later boot wiring and focused integration review must prove one constructor
and one shared owner across operations/jobs/readiness/open/close/drain. Private
unit tests cannot claim that wiring already exists.

Construct RequestContext::for_send from the returned Auth and exact outgoing
initial bytes. Sign context.auth() and send context.initial_bytes() with those
same canonical claims. Never stamp a different record after retaining context.
No hidden resend uses a new sequence after uncertain mutation. Retain original
operation UUID/exact bytes and C02 reconciliation. Explicit retries require the
route's existing permission; job retries retain existing logical/attempt rules.

Focused tests use checked Auth and the real RequestContext/parser. Alternate
routes and target sessions on the same owner and assert distinct increasing
sequences and unchanged identities/content. Reject changed cluster/sender/key
with a subsequent correct stamp proving no advance. A private test-only initial
last value reaches MAX-1 without billions of calls: MAX succeeds once, then
repeated attempts fail Exhausted. No production setter or reset exists. Reuse
existing key/signature vectors; do not rerun the crypto or full codec matrix.

## 2. R03a3b, retained receiver replay and consuming request transfer

Depends on assignment 1 and reviewed b3. Own new `auth/replay.rs`, private
`replay/tests.rs`, additive verify.rs consuming definitions and private
`verify/consume_tests.rs`, narrow auth.rs declarations/reexports, and ONLY the
revision getter above in cluster/controller.rs. No controller guard behavior,
membership, content/eligibility algorithms, payload or runtime edits.

The crate-private interfaces are:

```text
ReplayState::new(local: ReceiverIdentity) -> Self
ReplayState::observe(&mut self, root: &OwnershipObservation) -> Result<(), Error>
ReplayState::consume(&mut self, eligible: EligibleRequest,
                     content: CheckedContent) -> Result<ReplayedRequest, Error>
ReplayedRequest::into_handoff(self) -> RequestHandoff
```

ReplayState owns the complete immutable receiver boot/cluster/session/key and
the monotonic complete-root high-water. No constructor from three receiver
UUIDs, default instance, Clone, reload/reset, prune or removal API. Own two
bounded collections of fixed-size identity/highest-positive-sequence entries.
Worker keys include role, process, node, incarnation, session and raw key bytes;
API keys include process UUID and raw key bytes. Cluster and receiver boot are
fixed by the enclosing state. Limits are exactly 2048 worker identities and
128 API identities, including entries for subsequently retired/ineligible
senders. Use bounded Vec entries and linear lookup, reserving both fixed
capacities at construction. This avoids requiring Hash on DTOs and bounds the
short-lock lookup. No body, root encoding or secret goes in an entry.

Existing identities may advance at full capacity. Reject a new identity with
Exhausted before push, without eviction or a sequence change. Reject sequence
zero as Invalid and sequence <= retained high-water as Replay. The checked wire
boundary already excludes zero, but consume must not rely on an unchecked raw
test constructor. A high-water at MAX stays MAX. A later authorization failure,
retirement or key change never removes that evidence. New keys are distinct
identities subject to the same lifetime limit. No TTL, LRU or rollback is allowed.

observe rejects older revisions without replacing its high-water. Equal
revision requires full Eq, including complete-root digest/controller. Divergence
returns Invalid and sets a sticky integrity-failed flag; subsequent consumes
fail closed for this boot. Retain the original observation and all sequences.
A strictly newer root advances the observation but cannot clear that flag.
There is no public reset to recover from inconsistent storage. First/newer
observations are supplied from fresh validated roots outside the lock; observe
cannot certify freshness. Observation advancement is distinct from replay
advancement and may occur even when a request is refused.

consume requires an installed, healthy observation, matching complete eligible
observation, and eligible.local().same_boot against the fixed receiver. Require
Auth cluster and exact receiver to match that binding. Check Arc association,
positive sequence, identity lookup and available identity-entry room before any
replay mutation. A proof for another root/local boot or another verification
invocation leaves every sequence untouched. Content has no independent root to
compare; it must match the eligible request Arc supplying that sole observation.

Inside verify.rs add `pub(super) fn combine_request(eligible: EligibleRequest,
content: CheckedContent) -> Result<CombinedRequest, Error>`. CombinedRequest has
private fields holding those two original proofs and only borrowed auth,
observation, local, pending and typed content accessors needed by replay/caller.
It checks Arc::ptr_eq before constructing the value. Replay uses this helper
before mutation and retains it in ReplayedRequest, whose only constructor is
inside replay.rs after the successful high-water update. CombinedRequest is
visible only within auth through a narrow verify reexport. It proves association,
not replay or local admission; it is never reexported through auth.rs.

Combination also performs the remaining pure cross-proof claims: require the
parsed content alternative to match the closed route/pending alternative and,
for Drain, checked content.drain_id to equal the durable drain_id retained by
PendingLocalChecks::Drain. Existing operation/job hash and identity checks need
no reparse. Open/Close permit IDs stay attached to checked content for the later
live mapping comparison; no local permit/barrier/capacity claim is inferred here.
Thus a consistently signed but wrong drain body cannot spend a sequence merely
because header eligibility and content checking separately succeeded.

To make the ownership transfer concrete, define RequestHandoff in verify.rs
with private retained binding, observation, local identity, pending predicates,
original Box<[u8]> and a closed owned RequestBody. Its variants are
Operation(Box<Request>), Job(Box<JobEnvelope>), Readiness(Readiness),
Open(Dependency), Close(Dependency), Drain(Drain). CombinedRequest's auth-private
consuming conversion moves these existing parsed values and original bytes,
and retains the original request Arc with its Auth/canonical bytes. No reparse,
rehash, reserialization, Arc unwrap assumption or disk owner is involved.
ReplayedRequest::into_handoff uses that conversion exactly once. The handoff
has borrowed binding/bytes/pending getters and a consuming into_parts returning
that opaque binding, original bytes, owned body, observation/local/pending.
Binding has read-only Auth/canonical access, no raw constructor. It is not a
detached signature, authority or accepted-work certificate.

All combined/replayed/handoff/binding types remain non-Clone/non-Serde with
value-free Debug. This is a checked memory transfer, explicitly named Replayed,
not final AcceptedAuth. It grants no capacity, permit, initialization or execution.
No generic closure such as consume_then(callback) and no boolean local-check
argument is permitted. Do not add a standalone accepted-proof constructor.

ReplayState has no internal mutex. The later receiver admission owner embeds
it alongside barrier/permit/capacity/accepted-owner state behind ONE short mutex.
observe and consume must use that mutex. A replay-only lock followed by another
lock is invalid. An optional would_reject lookup is advisory only; omit it until
a caller needs it. It must never reserve, insert or advance an entry.

Focused causal tests use real signed requests, check_request_content,
check_request_authority and round-tripped generation-2 roots. Tests can prepare
all hashing/parsing/proofs outside std::sync::Mutex<ReplayState>. Two gated
threads consume independently verified equal-sequence candidates at that same
shared mutex: exactly one advances and the other returns Replay. This is real
shared replay serialization, not evidence for an as-yet absent R04 owner.
Use channels/barriers, join both threads and retain failures through teardown;
no sleeps. Force 12 to consume before 11 and assert 11 rejects. Mix invocation
Arcs despite identical UUIDs/bytes; vary receiver and admitted root independently,
with valid baselines, and prove a matching attempt can still use that sequence.
Also combine a real signed drain request with a body ID different from the
durable pending drain ID, then admit the corrected same-sequence pair to prove
that the pure claim failure spent nothing.

Fill both actual limits using checked synthetic identities with one reusable
eligible root where appropriate. Refuse identity limit+1, advance an existing
identity, then reject its old sequence. Verify no eviction and unchanged counts.
Worker fixtures may vary checked key/identity bindings with valid gen2 roots;
do not forge EligibleRequest fields to bypass authority. Retire/invalidate a
seen sender through a later real root, retain its high-water, and show rejection
plus unrelated existing progress. Exercise MAX acceptance and lower/equal
rejection. Reject older roots and equal-revision roots differing in complete
contents despite identical revision/change UUID. Drop a replayed handoff, then
show the same sequence still rejects. These are replay tests, not cancellation,
permit, capacity or final-admission tests. Preserve existing negative proof
trait checks and add move/trait guards for the new types.

## 3. R03a3c, consume the one original reply context

Depends on assignments 1/2. Own additive verify.rs definitions, private
`verify/reply_handoff_tests.rs` and narrow auth.rs reexports only. Preserve
verify_reply's borrowed signature and existing content/eligibility behavior.
No reply replay map, response sequence or membership API changes.

Add the concrete crate-private consuming method:

```text
RequestContext::into_reply_handoff(self, eligible: EligibleReply,
    content: CheckedReplyContent, current: &OwnershipObservation)
    -> Result<ReplyHandoff, Error>
```

Require exact reply Arc equality between eligibility and content, then exact
request/expected/target Arc equality against self. Require the eligible complete
observation to equal current. Do not require current to equal the older target
observation captured at send; legitimate unrelated root advancement is allowed.
All failures occur before any runtime acceptance, with static Invalid. Moving
self consumes the one context on success or failure. A failed final handoff
ends that attempt; it does not return a reusable context or trigger a retry.

ReplyHandoff privately owns the original RequestContext, eligible observation,
PendingReplyChecks, reply association, original reply Box<[u8]> and parsed reply
content. Provide borrowed context/auth/status/bytes/pending/typed claim access
and a consuming into_parts for the semantic owner. The closed owned ReplyBody
mirrors existing ReplyContent exactly: opaque Operation(Box<RawValue>),
Job(Box<ResultEnvelope>), Readiness, Open, Close, Drain, Rejection and
UnattributedRejection. Preserve remote ProtocolError as data and keep its
unattributed classification. Retain request/expected/target associations in the
opaque context binding returned by into_parts, rather than returning a reusable
RequestContext. No Clone/Serde, context extraction or unchecked constructor.
Move original content, with no reparsing or raw spool/file/path transfer.

Several verify_reply calls can borrow the context before finalization, and the
same reply may be verified into several distinct reply Arcs. Only one candidate
can consume that actual context. Another context built with identical UUIDs is
a different invocation and fails association. Copying an on-wire response
between two deliberately reconstructed contexts with the SAME sender/target/
sequence is not cryptographically distinguishable by ReplyAuth. A07's temporal
sequence rule and the one boot sender owner prohibit creating those duplicate
outgoing invocations. Do not claim Arc identity repairs sequence reuse on wire.

The final reply owner must hold its short local operation/context admission
mutex, reconcile monotonic current-root observations and verify the actual
live local sending boot before calling this method. It must require the exact
retained accepted operation/digest or dependency/admission/permit/job/attempt
for AcceptedOperation/AcceptedJob. Small replies require the matching live
challenge/permit/drain context. Reserve the continuation/result-delivery owner
synchronously in that same critical section, with no fallible acquisition
after consuming context. Pure handoff checks alone establish none of those
runtime predicates. No fake accepted-work flag belongs in this assignment.

Focused tests prepare real contexts, signed replies, checked content and
eligibility on real gen2 roots. Combine two independently verified reply Arcs
for one context and reject; combine with a separate identical-ID context and
reject; change current root while keeping valid proofs and reject without
producing a handoff. A valid complete combination transfers exact original
bytes and typed data once, preserving full job expectations and false small
observations. Use compile-fail documentation or the project's existing negative
trait technique to pin the non-Clone context/handoff boundary; do not pretend a
runtime test can call a moved value twice. No full crypto/codec rerun is needed.

## Final receiver transaction and remaining integration evidence

The later R04 owner performs this ordered transaction, under its ONE receiver
admission mutex, only after fresh GET, root encoding, signature, hash and parse
work have completed outside it:

1. Observe the complete root monotonically, then require proof/root/local boot
   equality. A newer admitted root cannot be replaced by a delayed older one.
2. Match the closed PendingLocalChecks and checked content against live state.
   Operations require open barrier and operation capacity. Probe requires the
   same initialized live endpoint/storage boot and exact challenge. Open requires
   open barrier, retained live operation binding, and exact existing OPEN retry
   or permit room; closing cannot reopen. Job requires that exact pre-barrier
   open permit plus partition capacity, with no operation-slot acquisition.
   Close compares its retained binding if present; absence only observes closed.
   Drain compares checked drain_id with BOTH durable and current local barrier.
3. Complete all fallible claims, bounded route-capacity/queue/permit acquisition
   and accepted-owner registry room checks before replay consume. Prepare any
   needed memory-only reservation in this section, releasing it if consume fails.
   Nothing may await or run hash/parse/root encoding in the section.
4. Consume replay, then synchronously and infallibly install the reserved
   accepted request/permit owner before releasing the mutex. No unregistered
   accepted interval and no fallible capacity acquisition may follow consume.

A failed signature/content/claim/capacity/permit attempt leaves replay sequences
untouched. Once consumed, cancellation, supervisor setup failure or uncertain
effects never roll them back. A setup failure must retain/settle the registered
owner and spent sequence. If existing PayloadOwners APIs cannot satisfy that
ordering, refine their reservation/registration in a separate bounded R04 slice;
do not merely call fallible try_admit after consume. This note assigns no full
listener/admission/worker module to any implementer.

Later integrated tests must use actual permit/barrier/capacity and accepted-owner
implementations to prove claim/capacity failures preserve sequence, equal-sequence
one-winner reservation, pre-barrier job admission versus drain/close, cancellation
after acceptance, registry/setup failure and queued-start/result rechecks. They
must gate real state transitions and measure ownership, not pass bool arguments
to a simulated consume. A07 permits a revocation after the fresh linearizable
GET to race with an otherwise admitted request; replay is not database fencing.

Transport admission precedes body reads and validation submission. The A15
prelude supervisor retains that bounded admission while submit is pending,
while accepted coordinator-pool commands run and through cancellation/settlement.
Signature/root/content/context work remains finite coordinator CPU/I/O work,
separate from partition execution and responsive readiness/heartbeat/drain work.
Do not abandon a pending submission or accepted Job::receive on cancellation.
No new unbounded spawn_blocking or caller queue is authorized.

For jobs, that supervisor reads one CONTROL using the SAME FrameReader and
retains the advanced reader. After final reservation it transfers the original
checked CONTROL and typed JobEnvelope/ResultEnvelope once into registered
payload ownership. A later small payload-command change must accept the checked
typed value rather than calling BeginRequest/BeginResult's raw parser again.
The reader must keep actual HTTP body errors, reject trailers and reach real
body EOF; never rebuild it from leftover bytes or mistake first CONTROL for
complete framed acceptance. DATA/PART_META grammar, lengths, row/file validation,
STREAM_END, output cleanup, supervisor reaping, result/publication and durable
outcome semantics remain their existing owners' obligations.

Readiness/drain signed observations remain provisional. C03 must match actual
initialized challenge/resource/binding evidence and fresh owner-authorized CAS;
C06 must match the live barrier, accepted work, heartbeat advancement, delivery,
publication and cleanup before idle/termination authority. A10 guard generation,
owner/intent checks and exact-token CAS remain additional controller duties.
Do not import unfinished C03a types or equate a small signed body with that proof.

## Dependency order and evidence limits

The parent adopted this design after b3 quality review and the fresh design
re-check, then schedules R03a3a, R03a3b and R03a3c in order with separate
specification/quality review.
Each assignment gets only its listed new files and additive ownership above.
Shared auth.rs/verify.rs ownership is sequential. The controller revision getter
and ApiSigningKey cluster getter are the only added read-only accessors, and
the wire sequence replacement is the sole checked-record mutation. Credentials
ownership is limited to that cluster getter; its key and signing logic remain
unchanged. No Cargo/dependency change is required.

After those helpers, the parent still owes narrow boot-sender wiring, actual
receiver/context reservation transactions, the supervised prelude and checked
payload transfer, HTTP framing/TLS/deadlines, queue-start/result checks and C03/C06
live evidence. Preserve R03a3/R04 aggregate checkboxes until those integrations
have real evidence. There is no essential unresolved protocol choice: the
single global sender counter is a compatible strengthening of A07, and consuming
the retained context uses ReplyAuth's existing request_sequence/scope binding.

This task inspected applicable guidance, contracts/rubric, the adopted notes and
completed delivery notes, and actual wire/credentials/verify/content/eligibility,
complete observation, frame/payload owner and WorkerPool seams. No applicable
AGENTS.md was found in repository discovery or checked ancestors. Only this new
note was written, followed by its own whitespace/final-newline check. No source,
test, Cargo, plan or existing note changes, builds/tests/probes, secrets/.env,
network/cloud, branches/worktrees/commits or delegation occurred. Shared changes
were preserved. This design supplies no implementation, test or runtime guarantee.

## Parent correction after design review

The first design review found that assignment 1 could not check the API key's
cluster binding through its permitted interfaces. Its existing cluster field
is private, and sign_request requires a complete Auth. The parent added only
the read-only cluster getter to assignment 1's ownership and made constructor
rejection explicit. The dependency/ownership summary now includes it. This is
a design correction, not a production edit or test result. Fresh independent
reconciliation passed without essential remaining findings, and the parent
adopts the corrected handoff. Runtime and implementation claims remain deferred.
