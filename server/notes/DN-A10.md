# DN-A10 bounded control CAS and controller ownership

Design handoff only, 2026-11-11. This note establishes no implemented or tested
guarantees. It follows [shared contracts](../../plans/distributed-contracts.md),
[A05](DN-A05.md), accepted [A07](DN-A07.md)/[A08](DN-A08.md), and completed
[C04a2b](DN-C04a2b.md)/[C04a3](DN-C04a3.md). Actual `state`, `configuration`
and `member` records remain the DTOs. No schema, SDK, attachment, lifecycle,
admin-history or remote-payload redesign belongs here.

Parent status: accepted after independent design review and two corrective
re-checks. Final re-check found no essential clarification remaining. Explicit
shared proposal identity, stamp-scoped errors and unchanged-controller
observation rules below supersede the original proposal in this note.

## Dependency and scope

C04b1 supplies `LoadedControl` with private validated root/nonempty opaque token,
`root()` and sanitized Debug, plus `Storage<B>::load_control(deadline)` and
`initialize_control(initial, deadline)`. Initialization permits at most one
Create and bounded observed-root reconciliation. Returning a competitor's root
does not establish compatible attachment or controller ownership. C04b2 starts
after that slice is delivered and reviewed. It must use its delivered internals,
not depend on the current unfinished implementation or replace its public API.

Later startup verifies cluster/name, semantic configuration, provisioning and
managed bridge availability before attempting ownership. A process gets a fresh
random owner UUID on each boot; restart never reconstructs local authority from
the persisted owner/epoch. External mode can own control metadata but grants no
provider callback capability. Database publication has separate authority.

## Small programmer interface

Use one new `cluster/controller.rs` for pure ownership rules and local certainty.
Keep token access and conditional I/O inside existing `cluster/store.rs`.
The following are internal Rust interfaces, `pub(crate)` where cross-module
access is needed. Focused unit tests can live beside private code. No broad
public controller object or background task framework is needed.

```rust
// controller.rs. A pair identifies an owner; it grants no authority by itself.
struct Owner { process_id: Uuid, epoch: u64 }
enum OwnershipChange { Acquire { process_id: Uuid }, Renew(Owner), Release(Owner) }

fn ownership_candidate(
    root: &Control, change: OwnershipChange, unix_ms: u64,
    change_id: Uuid,
) -> Result<Control, StorageError>;

// store.rs. Private fields, no Clone, consumed by submit.
struct PreparedOwnership { /* loaded base, validated candidate and encoded bytes */ }
impl PreparedOwnership {
    fn prepare(
        base: LoadedControl, change: OwnershipChange, unix_ms: u64,
        proposal_id: Uuid,
    ) -> Result<Self, StorageError>;
    async fn submit<B: ObjectStore>(self, storage: &Storage<B>, deadline: Instant)
        -> Result<OwnershipOutcome, StorageError>;
}
enum OwnershipOutcome {
    Acknowledged(LoadedControl),
    ObservedExact(LoadedControl),
    Conflict { current: Option<LoadedControl> },
    Unknown { current: Option<LoadedControl> },
}
```

The caller selects one fresh random proposal UUID before `begin` and supplies
that same UUID explicitly to both `begin` and `prepare`. Preparation uses it as
the change UUID without replacement, calls the pure constructor, and
bounded-encodes the complete candidate before I/O. Both boundaries reject reuse
of the base change UUID; the caller is responsible for generating a fresh UUID
per new proposal. The prepared value owns the exact validated
loaded root and its captured token. No setter, token accessor, arbitrary public
candidate constructor or caller-supplied serialized bytes are added. Only this
store module can make a new loaded value from candidate plus returned token.
Debug prints at most revision/change UUID and outcome category, never tokens,
configuration fingerprints, templates, bodies or raw provider diagnostics.

Reuse `StorageError`: invalid transition/owner mismatch is `Conflict`, malformed
candidate is `InvalidCatalog`, exhausted checked arithmetic is `LimitExceeded`,
and definite availability failures are `Unavailable`. Map existing
`ControlError` variants to their existing storage equivalents. These are
internal errors, not new HTTP codes or provider error text. Keep uncertainty in
the outcome enum once a Match may have applied.

## Ownership transitions

Every candidate clones the validated base, changes root revision with checked
`+1`, uses the new change UUID, and changes only `controller`. Preserve exactly
version, cluster ID/name, config including generation/fingerprints, template,
timing, provisioning, policy including its revision, scheduler, admin head/seq,
nodes and intents. Validate and bounded-encode the final root. Ownership changes
allocate no members/intents and need no new-admission check. Existing A08 credit
accounting and all retained reservations remain unchanged.

| Change | Required base | Controller result |
| --- | --- | --- |
| Acquire | Released, or held with `unix_ms >= lease_until_ms`; the acquiring boot UUID differs from any held owner | Checked epoch+1, acquiring owner, held, checked `unix_ms + lease_ms` |
| Renew | Held with exact supplied process UUID and epoch | Same owner/epoch, checked `unix_ms + lease_ms`, strictly greater than the old deadline |
| Release | Held with exact supplied process UUID and epoch | Released, null owner/deadline, same epoch |

The eligibility comparison is the accepted persisted Unix-time rule. The
two-second discontinuity tolerance below is a local detector, not a global
clock-skew bound or extra expiry-fencing guarantee. A process recovering its
own uncertain acquisition uses an owner-checked renewal, not Acquire or an
epoch reset. A renewal can re-establish certainty after a local lapse only if
its exact-token CAS still wins. If the current millisecond would not advance
the deadline, reject that proposal; a later explicit proposal can use a new
sample. Never manufacture a farther deadline by saturating arithmetic.

Takeover resets local inventory/load/stabilization evidence in the later
runtime; it preserves durable scheduler/backoff, attempts and all identities.
Pure checks prove neither callback entry nor provider truth/drain safety.
Later admin writes preserve the entire loaded controller, and later lifecycle
writes must use the same exact loaded-token and owner/epoch checks. Do not
extend this ownership-only constructor with a generic event framework now.

## One submission and exact outcomes

`submit` checks the absolute monotonic deadline before polling storage, then
sends at most one `Match(captured_token)` to the canonical control key. It
never uses Create or unconditional replacement. The same absolute deadline
covers write and reconciliation. There is at most one fresh `load_control`
after Conflict, Ambiguous, timeout after possible submission, or an empty
success token. There is no automatic recomputation/resend inside this API.

| Outcome | Evidence and permitted use |
| --- | --- |
| Acknowledged | Match returned durable success with a nonempty token. Candidate plus token is the loaded result. This is the only outcome eligible to install local certainty, subject to the local checks below. |
| ObservedExact | A possibly applied Match lacked a valid acknowledgment, but the one fresh read has the exact proposed change UUID and entire candidate root. This confirms that metadata transition occurred. It never installs live controller authority. |
| Conflict | Backend definitively rejected this Match without applying it. Include a valid fresh root if available. Reconciliation failure does not make this rejected proposal applied or ambiguous. |
| Unknown | Submission may have applied and exact-current confirmation failed. Include a valid later/different root if obtained. Its revision, owner/epoch or similar deadline cannot prove this earlier proposal happened. |

An exact UUID with different candidate contents is an integrity failure and
cannot confirm the transition. Fail closed, report sanitized `InvalidCatalog`
before submission or retain Unknown after possible submission. A failed or
missing reconciliation read after a possible write stays Unknown, even for
NotFound, decoding failure or deadline exhaustion. Definite adapter failures
that guarantee no application may return their existing StorageError; they
still invalidate local certainty. Pre-submission errors establish only that
this proposal was not submitted.

There is no reachable ownership history analogous to admin/catalog history.
Reuse publication's consumed candidate, token capture and deadline pattern,
but do not copy its terminal-history search or head==change_id invariant.
A later root cannot resolve an earlier acquisition, renewal or release by
inference. An exact-current release can confirm released metadata, while local
authority remains revoked. An uncertain release that still observes held state
does not authorize continued use of the old lease.

After ambiguous acquisition/renewal, observe a fresh held root matching the boot
owner and epoch, prepare a new renewal with a fresh UUID/token, and require its
Acknowledged outcome before dispatch. If another owner has won, stop. If the
fresh root is released, a new explicit acquisition is a separate proposal.
The caller may make at most one such fresh recovery proposal per bounded
recovery invocation under its original deadline; another conflict/uncertainty
returns to supervision without dispatch. This is never a resend of the old
bytes/token. A successful subsequent CAS consumes the current token and fences
an earlier still-pending CAS. Until then that pending operation can still win.
Expiry and dropping a future alone do not fence it.

## Local certainty and acknowledgment ordering

Add one memory-only `OwnershipGuard` in `controller.rs`. Its private state is
boot owner UUID, checked generation counter, high-water validated root identity
including complete-content equality, one optional in-flight generation, and
optional certainty carrying the complete held controller tuple, paired
wall/monotonic sample and monotonic deadline. Prepare content identities and
any bounded encoding/digest work outside the short guard lock; revision and
change UUID alone are insufficient. No backend token, credentials or persistence
belongs in this guard. `Owner` and generation stamps are plain values, not
reusable authorization tickets.

Keep the guard's operations synchronous under a short caller-owned mutex:

```text
begin(base, change, proposal_id, sample) -> generation stamp or StorageError
finish(stamp, Result<OwnershipOutcome, StorageError>, sample) -> Result<bool, StorageError>
observe(root) -> Result<(), StorageError>
check(sample) -> Result<Owner, StorageError>
invalidate() -> Result<(), StorageError>
```

`sample` is a small private pair of `tokio::time::Instant` and checked Unix
milliseconds derived from `SystemTime`. Use explicit samples in pure tests;
no new clock trait or timer service is required. `begin` first processes the
validated base through the observation rules below, then rejects a still-active
ownership operation, advances generation, clears certainty and captures the
start sample before preparation/submission. Thus release revokes local
authority before I/O and an abandoned future cannot leave dispatch open.
An older base returns Conflict without changing newer guard state; an equal
revision requires equal change UUID and complete root contents. Bind the active
stamp to this base's complete identity, requested transition, original paired
sample, explicit proposal UUID, expected result owner/epoch and complete
expected candidate identity. Compute bounded candidate-identity work outside
the guard lock using the pure constructor and these exact inputs. Preparation
uses that same UUID and the original sample's Unix milliseconds without
replacement; do not substitute a later wall sample while retaining an earlier
certainty interval.
Normal renewal also briefly closes local authorization; this small slice does
not promise uninterrupted availability during renewal. The caller prepares
and submits outside the lock, then calls `finish` inside it. A preparation
failure after a successful begin must complete that same stamp with its error,
not leave the operation slot occupied.

`finish` can install only Acknowledged acquisition/renewal, with the exact active
stamp, same boot owner/expected epoch, candidate identity and no newer
observation/invalidation. After checking stamp identity first, an acknowledgment
is eligible only if its complete root equals the bound expected candidate.
A mismatched proposal UUID or any other differing candidate contents closes
and releases only that active operation and returns sanitized InvalidCatalog.
Every active terminal completion releases its slot,
including preparation/submission errors and cancellation. It never installs for
release, ObservedExact, Conflict, Unknown or error. Check stamp identity before
processing an operation's result or time sample: an obsolete finish returns
Ok(false) and cannot clear certainty, release a newer slot, advance generation
or overwrite newer state, even when its metadata write really succeeded or its
late error arrives after recovery. Route operation cancellation through this
same stamp-scoped path. Reserve unconditional `invalidate` for current
process-wide invalidations; it clears certainty and the slot and advances
generation, so recovery can start while old remote work may still be pending.
Counter exhaustion permanently closes this guard for its process lifetime;
it cannot reset to zero and reuse stamps.

`observe` returns Conflict for an older observation without modifying newer
state. Equal revisions require equal change UUIDs and complete root contents;
disagreement invalidates authority and returns sanitized InvalidCatalog. Apply
the same identity checks in begin and finish. A newer validated root advances
the high-water observation. Preserve already-installed certainty only when its
complete controller tuple, including owner, epoch, held state and Unix deadline,
is unchanged. Preserve its original time anchor and monotonic deadline:
observation never installs, extends or restores certainty. A changed controller
tuple clears certainty. Every newer observation invalidates any pending
ownership completion stamp and releases its slot, even when that tuple is
unchanged. Same-tuple observations with no pending ownership operation do not
advance the ownership generation. Thus a delivery claim or unrelated admin or
member update can preserve existing authority without an extra ownership CAS.
`finish` compares against the high-water observation before recording its own
acknowledged root, so its normal revision advance does not invalidate itself.

For acquisition/renewal certainty, compute `start_monotonic.checked_add(lease)`
before I/O and retain that deadline, never acknowledgment time plus lease.
Elapsed I/O therefore consumes the certainty interval. At finish/check require
monotonic time strictly before that deadline and wall time before the candidate
Unix deadline. Check signed wall elapsed against monotonic elapsed from the
paired start sample. An absolute discrepancy greater than 2000 ms invalidates
certainty; exactly 2000 ms is within the accepted detector tolerance. Check
against the retained anchor rather than resetting it on each read, so drift
cannot accumulate unnoticed between reads. Backward/unrepresentable monotonic
elapsed, pre-epoch/unrepresentable wall time, duration/conversion/addition
overflow, or any exhausted revision/epoch/generation fails closed. A fresh
recovery CAS needs a stable before/after sample pair before it can install.
Guard time/deadline/discontinuity failures return Unavailable; checked numeric
or Instant-capacity exhaustion returns LimitExceeded. Errors remain sanitized.

Conflict, uncertain I/O or cancellation of the current ownership operation
leave its local permission closed through stamp-scoped completion. Their late
arrival for an obsolete stamp cannot affect recovered authority. Current
process-wide restart, clock discontinuity, local expiry and checked-arithmetic
failure close certainty and invalidate current authorization stamps and local
inventory/load/stabilization evidence. They do not erase durable owners,
intents, retry counts or reservations. Restart creates a new guard and boot
owner. `check` also invalidates on time failure, rather than returning a stale
owner for a caller to reuse. A later integrity error after a possibly applied
write cannot turn its Unknown outcome into proof of nonapplication.

The runtime must retain/supervise submitted work, finish operation errors and
cancellation with their original stamp, schedule renewals independently of
callback waits, and stop admission when certainty closes. No mutex spans
awaited storage or callback HTTP. Later A04 authorization captures generation,
performs its fresh root/intent read and command validation outside the lock,
then rechecks generation, certainty and that the read root still equals the
guard's high-water root under the short lock before replying. Each fresh
observation uses `observe`; a superseded read cannot authorize and no late
read/completion may reinstall authority. This is a check at the authorization
boundary, not a claim of cancelling effects already entered afterward.
Later lifecycle completion CAS checks exact owner/epoch
and token. These obligations remain runtime work; this guard does not cancel
already entered provider effects, fence wall-clock expiry precisely, or prove
worker drainage. A07 session verification and A04 command/intent checks remain
additional requirements.

## Sequential implementation assignments

Each mini has finite ownership below and follows delivery/review of its
dependency. Preserve shared edits. The parent serializes export lines and
`store.rs` access after C04b1; none of these assignments owns startup/routes,
plans/contracts, dependencies, Python, lifecycle modules or target cleanup.
Tests directly invoking crate-private ownership, prepared-submission or guard
APIs live in their owning modules' private `#[cfg(test)]` tests. Reuse the
existing CAS fake through narrowly scoped test-support imports. Integration
tests remain for behavior reachable through existing public interfaces; do not
add a public controller facade or SDK API solely for tests. Each mini also owns
its corresponding DN-C04b2a/b/c completion note.

1. **C04b2a pure ownership candidates.** Depends on reviewed C04b1 and completed
   root codecs. Own new `server/src/cluster/controller.rs` with Owner,
   OwnershipChange and ownership_candidate, its embedded unit tests, and only
   the module declaration in `server/src/cluster/mod.rs`. Cover released/expired
   acquisition, exact-owner renewal/release and strict deadline advance. Assert
   all non-controller fields remain equal except root revision/change UUID,
   including a nonempty admin chain and independent policy revision. Root/epoch
   and deadline overflow must leave the base unchanged with sanitized failure.
   Reuse codec fixtures where practical; no duplicate JSON rejection matrix.

2. **C04b2b consumed conditional submission.** Depends on a and delivered C04b1.
   Own only additions in `server/src/cluster/store.rs` and its private
   `server/src/cluster/store/tests.rs`. Implement PreparedOwnership and the four
   outcomes above. Import the existing in-process CAS Fake and reuse its race,
   put_faults, lost-response and read-gate support. Add a small test-local wrapper
   only for bounded one-shot gates immediately before Match application and
   after application/before acknowledgment, taking no fake-state mutex across
   an await. Competing
   acquisitions must have one winner; a held stale renewal or completion-shaped
   owner check released after takeover must fail its token/owner check. Lose
   acquisition, renewal and release replies after application. Exact-current
   reads may confirm metadata, later roots must yield Unknown, and no outcome
   may cause an automatic second PUT. A separate explicit fresh renewal must
   be required for authority recovery. Count PUTs/GETs and prove the one-read
   bound on reconciliation failure. This test is metadata evidence; no real
   lifecycle completion implementation is claimed.

3. **C04b2c local certainty and causal ordering.** Depends on a/b. Own additions
   only in `server/src/cluster/controller.rs`, its private tests, and the existing
   private store test module for combined gated cases. Implement the guard
   without a controller loop. Test loss of acquisition/renewal/release
   acknowledgment leaves check closed; ObservedExact cannot reopen it; a fresh
   successful owner-checked renewal can. Gate an old acknowledgment, install or
   observe a newer takeover/recovery, then release the old acknowledgment and
   require no restoration or clearing of newer state. Also invalidate while a
   write is pending and finish after a newer generation starts, including late
   errors and cancellation after newer recovery. Prove an unchanged-controller
   claim/admin/member observation preserves the original certainty deadline,
   changed controller contents revoke it, superseded authorization reads reject,
   and equal-revision/equal-UUID but different contents fail integrity checks.
   Preparation failure must release its own stamp's slot. Explicit paired
   times cover forward/backward jumps over two seconds, the exact tolerance,
   certainty expiration during I/O, checked timestamp/Instant arithmetic and
   generation exhaustion. Require renewed certainty to use pre-I/O time. Use
   gates/explicit samples, not scheduling sleeps or a new fake clock framework.
   Prepare two otherwise identical ownership changes with different proposal
   UUIDs and prove the acknowledgment of one cannot satisfy the other's stamp.

Implementers run only focused changed ownership/CAS tests and required targeted
format/lint checks after they obtain the parent's Cargo slot. Existing codec,
history and publication tests provide reusable patterns, not a reason to rerun
every suite. Eventual E03/E04 real-process acceptance must separately exercise
two APIs over the local conditional backend, actual authorization while callback
I/O is blocked, takeover, lost replies and stale completions. In-process fake
tests do not establish process/network/provider or drain safety.

## Parent reconciliation and evidence

Parent corrections after independent design review preserve installed certainty
through newer roots with an unchanged complete controller tuple, scope late
errors/cancellation to their original stamp, require complete observation
identity and explicit errors, and move private-API tests into owning modules.
The original invalidate-on-every-new-root rule is rejected. Renewal itself
still closes local certainty while in flight; observing a claim or unrelated
admin/member update cannot extend or recreate authority. Fresh independent
design re-check precedes adoption. Startup supervision, authorization integration
and actual-process acceptance remain explicitly later ownership.

The first re-check closed those four findings but required explicit proposal
identity binding. The parent updated begin/prepare to share one caller-selected
fresh UUID, bound the expected complete candidate to the active stamp and added
the wrong-proposal acknowledgment assertion above. Another fresh re-check
precedes adoption; no implementation or test result is inferred from this edit.

Read-only inspection covered applicable ancestor/path guidance, shared
contracts, A05's rootCAS/clock/admin rules and C04b1 split, accepted A07/A08,
actual root/configuration/member APIs, C04a2b/C04a3 delivery notes, storage
history/publication/errors and the existing CAS fake, plus A04/Python's private
authorization boundary. No applicable AGENTS.md was found. Only this new note
was written. No production/tests, plans/contracts, secrets, remote services,
delegation, Cargo/build/test commands or repository-history operations were
used. The tests above are assignments, not results.
