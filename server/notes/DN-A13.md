# DN-A13 administration history and policy publication

Design handoff only, 2026-11-11. Persist exact administration outcomes and policy
changes through the separate A05 control chain. Follow [shared contracts](../../plans/distributed-contracts.md),
the C04/G02 and testing sections of [the rubric](../../plans/distributed-nodes.md),
accepted [A05](DN-A05.md), [A08](DN-A08.md), [A10](DN-A10.md) and
[A11](DN-A11.md). Inspected C04a3/C04b1/C04b2b/C04b2c notes and actual codecs,
store, typed keys, catalog history/publication and Fake fixtures. No applicable
ancestor or repository AGENTS.md was found. Applied plan-creation and unslop
within the requested note-only scope.

Parent adoption after independent re-check: accepted as C04c1 bounded history
lookup, C04c2 pure policy/terminal decisions, and C04c3 consumed publication.
Replay occurs before preparation, with only one optional reconciliation scan
under the original deadline. This adoption establishes no implemented behavior.

Current `state::Config` still accepts generation 1. C08c owns the adopted
generation-2/`worker_semantic_fingerprint` amendment and synthetic fixtures.
These assignments clone the complete validated configuration opaquely, without
schema edits, migration, path opens or fingerprint reinterpretation. C04b2c and
its parent pending-renewal clock correction have passed independent review.
Neither guard metadata nor read-back alone establishes controller authority.

## Contract and private boundaries

Any attached, authenticated API replica may accept policy through exact-token
CAS, including one without scaling-controller ownership or an active bridge.
A05 explicitly separates G02 policy acceptance from owner-checked controller
mutations. Preserve the complete controller tuple. Policy acceptance grants no
callback, lifecycle or database-publication authority. Later runtime observes
new roots through A10, preserving existing certainty only under its unchanged
controller and original-deadline rules. No ownership guard call is an admin
admission requirement.

G02 must authorize before replay lookup and retain the exact authenticated
request envelope, at most 1 MiB. UUID plus original bytes binds identity, including
whitespace and escaped spelling. Only strict scale/autoscale requests enter this
ledger. Persist deterministic terminal successes/errors, including identical
policy and unsupported external autoscale. Status, auth/framing, overload,
storage and unknown outcomes never enter it. A same-ID/different-body replay
conflicts without appending a second outcome or disclosing the stored body.
No TTL, GC, cache eviction or identity forgetting is added.

Proposed APIs below are `pub(crate)` only where used across modules. Fields
remain private; owning modules may use private helpers. Use existing
`StorageError` categories and sanitized diagnostics. Debug must omit tokens,
bodies, configuration, paths and provider diagnostics.

```rust
// cluster/admin_history.rs
#[derive(Clone, Copy)]
struct AdminHistoryBudget { /* private absolute deadline; fixed hard caps */ }
impl AdminHistoryBudget {
    fn new(caller_deadline: Instant) -> Result<Self, StorageError>;
    fn deadline(&self) -> Instant;
}
enum AdminLookup { Found(ControlDecision), IdentityConflict, Absent }
impl<B: ObjectStore> Storage<B> {
    async fn lookup_admin(
        &self, base: &LoadedControl, request_id: Uuid, exact_request: &[u8],
        budget: AdminHistoryBudget,
    ) -> Result<AdminLookup, StorageError>;
}

// cluster/admin.rs. Pure, strict request parsing and deterministic response.
fn admin_candidate(
    root: &Control, request_id: Uuid, exact_request: &[u8], proposal_id: Uuid,
) -> Result<(Control, ControlDecision), StorageError>;

// cluster/store/admin.rs, a private child of store.rs with access to its fields.
struct PreparedAdmin { /* base, candidate, decision, both bounded encodings */ }
impl PreparedAdmin {
    fn prepare(
        base: LoadedControl, request_id: Uuid, exact_request: &[u8],
        proposal_id: Uuid,
    ) -> Result<Self, StorageError>;
    async fn publish<B: ObjectStore>(
        self, storage: &Storage<B>, budget: AdminHistoryBudget,
    ) -> Result<AdminPublicationOutcome, StorageError>;
}
enum AdminPublicationOutcome {
    Terminal(ControlDecision), IdentityConflict, NeedsRecompute, Unknown,
}
```

`LoadedControl` currently exposes only `root()`. Keep its root/token fields and
construction private to `store` and its descendants. Assignment 3 owns the child
module declaration and crate-private reexports in `store.rs`; it needs no token
getter, mutable root accessor or general CAS API. `PreparedAdmin` has no Clone,
setters or caller-supplied serialized candidate. It consumes one captured base.
The caller supplies one fresh proposal UUID, different from the base change UUID
and current head. This writer may use it for both decision ID and change UUID;
history validation must never require those values to coincide.

## Three sequential assignments

The parent assigns each mini to GPT-6.1 Sol/high, serializes shared file/Cargo
ownership, and obtains separate specification then quality review. Children do
not delegate and receive Cargo ownership only through an explicit parent handoff.
No mini owns routes, Python,
controller loops, dependencies, a public controller facade or a generic framework.

1. **Bounded administration lookup.** Own new
   `server/src/cluster/admin_history.rs` and
   `server/src/cluster/admin_history/tests.rs`, plus only its crate-private module
   declaration in `server/src/cluster/mod.rs`. Reuse `Storage::read`,
   `ObjectKey::control_decision`, `ControlDecision::decode` and the existing
   `LoadedControl::root()`. Do not change catalog history or codecs.

   Search only the supplied root's reachable chain. Validate head sequence equals
   root `admin_seq`, cluster identity, fetched key equals record UUID, and head
   published revision is at most root revision. Each parent sequence decrements
   exactly one; its published revision is at most the child's base revision.
   The codec checks each record's positive base and checked published=base+1,
   parent null iff sequence 1, and request/response correlation. Root revisions
   can skip across renewals/member writes. Never require head==change_id,
   root revision==head publication revision, or consecutive root revisions.
   A validated matching prefix permits Found or IdentityConflict immediately.
   Only null at expected sequence zero permits Absent, including an initial
   root with zero/null. Missing referenced records, malformed links, wrong
   cluster/UUID/body integrity, unavailable/truncated reads and exhausted budgets
   return errors. Strict sequence decrease bounds cycles without a visited set.

   Fix caps at 4096 fetched records and 32 MiB aggregate encoded decision bytes
   per lookup. Exclude the root from that aggregate: each root GET independently
   has `MAX_CONTROL_BYTES`=32 MiB through EOF. The caller loads its initial root
   under the same absolute deadline. `new` captures
   `min(caller_deadline, now + 5 seconds)` with checked time arithmetic; reusing
   the budget never refreshes this deadline. Check it before/after I/O and
   decoding. Each decision read uses `min(MAX_CONTROL_DECISION_BYTES, remaining)`;
   bound while streaming and probe at most one excess byte without retaining it.
   Use checked counters and retain one decoded record, not a chain/body vector.
   Root-plus-history therefore permits up to 64 MiB encoded input per lookup,
   plus bounded excess probes; it is not a claim about peak decoded heap size.

   Group tests around valid interleaved root revisions and exact replay/body
   conflict, malformed/incomplete ancestry, and record/byte/deadline exhaustion.
   Include a valid zero terminus and matching prefix with an older unavailable
   suffix. Assert no decision/root PUT occurs for lookup. Exercise actual root
   and decision decoders and EOF readers, not manually trusted DTO substitutes.

2. **Pure policy and terminal decision.** After lookup delivery/review, own new
   `server/src/cluster/admin.rs`, `server/src/cluster/admin/tests.rs`, and only
   its crate-private module declaration in `server/src/cluster/mod.rs`.
   `admin_candidate` is preparation, with no I/O or authority claim; its caller
   must first obtain complete absence. Parse the original envelope directly with
   existing strict object techniques, including duplicate rejection, exact u64
   counts, required fields and UUID correlation. Do not parse through a Value
   map. Bound input before allocations. Use `ControlDecision::encode` on the
   complete generated record and `Control::encode` on the candidate.

   Fixed scale sets mode=fixed and desired=min=max=target in 0..1024. Autoscale
   requires 1<=min<=max<=1024, replaces bounds and clamps the current desired.
   Compare all four effective policy fields before changing policy revision.
   Identical fields preserve it; an effective change checked-increments it.
   External fixed scale persists the operator goal without provider effects;
   valid external autoscale durably returns unsupported_capability/400 and
   leaves policy unchanged. Typed out-of-bounds requests durably return
   validation_error/400. Use fixed sanitized messages and exact correlated
   `{request_id,data}` or `{request_id,error}` response envelopes under 1 MiB.
   Errors have `outcome:"not_applied"`. The existing codec's terminal allowlist
   also permits conflict/409 and limit_exceeded/413, but does not justify
   inventing policy conflicts or treating storage/budget failures as durable.

   Every publishable outcome advances root revision and admin sequence by checked
   +1, sets fresh change UUID/head, and records base/published root revisions,
   cluster, parent, exact request and precomputed response. Counter exhaustion
   that prevents recording is a sanitized failure without mutation. Clone the
   entire base and change only policy, root revision/change UUID and admin head/
   sequence. Preserve scheduler, controller, configuration, template, timing,
   nodes, intents and every A08 credit. Do not call `check_new_admission`, reserve
   lifecycle identities or require projected status to fit.

   Group tests around fixed/auto transitions with clamping and external behavior,
   identical-policy/deterministic-error decisions with independent revisions,
   and preservation/overflow. Seed real retained managed launches and credits,
   nonzero scheduler/owner fields and a nonempty chain; encode/decode each final
   candidate and decision. A codec-valid root lacking new-admission room must
   still accept policy without changing its capacity. Avoid a second codec CRUD
   matrix; representative malformed input suffices at this new parser boundary.

3. **Consumed immutable Create and root Match.** After both reviews, own new
   `server/src/cluster/store/admin.rs` and
   `server/src/cluster/store/admin/tests.rs`, plus only its private module
   declaration/crate-private reexports in `server/src/cluster/store.rs`.
   The authorized caller searches the captured base before preparation. Found
   returns the original terminal response; IdentityConflict returns without
   writes. Only complete Absent permits preparation, so replay precedes candidate
   encoding and counter arithmetic, including exhausted counters. Preparation
   invokes assignment 2 and captures that same immutable base/token and bounded
   bytes. Publish does not repeat this lookup. The initial lookup and any
   reconciliation lookup are the only two scans, sharing the original budget
   created before initial root loading. Incomplete lookup permits no preparation,
   Create or Match.

   Attempt at most one immutable Create at the typed decision key. A nonempty
   timely acknowledgement confirms it; Conflict, Ambiguous or empty reply needs
   one bounded GET comparing the entire original encoded decision bytes. Never
   accept decoded-equivalent replacement bytes or resend Create. Confirmation
   failure prevents root submission. Its independent read cap is
   `MAX_CONTROL_DECISION_BYTES`; it is not part of a history scan's 32 MiB.
   Then attempt one Match at `ObjectKey::control()` with the captured token and
   exact prepared root. No unconditional overwrite or blind retry is allowed.

   Timely nonempty acknowledgement returns Terminal. After root Conflict or a
   possible application, load at most one fresh root and search its reachable
   chain under the original deadline. This attempt has at most two scans, each
   with its own count/byte allowance, and one reconciliation root GET. Found
   returns the original terminal record even after later renewal/member/admin
   writes. A different body yields IdentityConflict. Complete absence after
   definite CAS rejection returns NeedsRecompute; the caller must reload/search
   before deriving another candidate. All incomplete reconciliation, including
   root disappearance/corruption, and complete absence after possible application
   return Unknown. Definite pre-application adapter errors retain their existing
   categories. Errors before this attempt's root submission prove nothing about
   an earlier same-request attempt. Cancellation/drop never proves rollback.

   No automatic recomputation, recovery CAS, request-ID replacement or uncertain
   mutation resend belongs inside publish. An orphan decision proves nothing.
   A separately caller-controlled fresh lookup/candidate CAS may resolve an
   unknown request using the same exact request UUID/bytes and a fresh proposal
   UUID. Only a confirmed successful CAS or reachable terminal outcome resolves
   it. If the earlier PUT is still pending, a successful fresh CAS consumes its
   token and fences it; if the earlier PUT wins first, fresh CAS loses and must
   search again. Wall-clock expiry and guard state cannot fence that PUT.

   Group causal tests around competing policy writes, immutable confirmation,
   applied-before-lost-root-reply, and late original PUT fencing. Gate both
   contenders after capturing the same base; assert one root Match winner and
   loser lookup before fresh recomputation. Lose a reply after application,
   commit a renewal/member update and another admin decision before releasing
   reconciliation, then prove exact historical replay. Hold an original Match
   before application, observe complete absence/Unknown, explicitly publish a
   fresh same-request proposal, then release the original and prove token
   rejection and one reachable outcome. Also cover the original winning first.
   Assert incomplete history causes zero candidate writes, different-body reuse
   causes zero writes, and identical policy has a durable decision across reload.
   Count reads to prove one initial scan and at most one reconciliation scan.
   Replay from a valid root with exhausted publication counters must still return
   the original decision before candidate encoding or counter arithmetic.
   For immutable confirmation, inject a lost Create reply and then exact bytes
   versus a codec-valid byte-different record; only the exact case may Match.

## Evidence and handoff limits

Use embedded private module tests. Reuse `server/tests/support/storage.rs` via
the existing test-only crate alias/import technique. Small local wrappers may
add before-application and after-application/before-reply gates, exact byte/token
capture and GET/PUT counts as in `cluster/store/tests.rs` and
`server/tests/publication.rs`. Preserve actual Fake CAS and codec paths. Never
hold its mutex across await. Bound every task, join success, and abort then join
on timeout/cancellation. Sleeps and detached tasks do not establish ordering.
Use a few representative grouped cases with meaningful behavioral red before
implementation; report compile-only failures separately. Focused tests and
required scoped lint/format checks wait for the parent's Cargo slot. Do not
repeat unrelated passing suites or broaden visibility for integration tests.

No essential parent contract decision remains: external fixed goals and
replica-independent authenticated policy acceptance are explicit in A05/shared
contracts. The parent still owns task labels, serialized scheduling, G02
authorization/routes/status/waits, retained request supervision, A10 runtime
observation, and eventual actual TCP/process acceptance. Fakes establish only
bounded storage/policy behavior. This note changes no plan or other note and
provides no implementation, passing-test, real-cluster or authority guarantee.
Only `server/notes/DN-A13.md` was written; no Cargo/build/test/probe, delegation,
branch/worktree/commit, secret read or cloud call was performed.

Independent design review found a redundant publish-time lookup after the
already required initial absence scan. With reconciliation that prescribed three
scans instead of two. The parent removed the repeated lookup, made the original
budget start before root loading explicit, and retained replay before candidate
encoding/counter arithmetic. Read-count and exhausted-counter replay assertions
now make that ordering explicit. Fresh design re-check passed without essential
remaining findings and the parent adopted the contract. No implementation or
test result follows from this correction or adoption.
