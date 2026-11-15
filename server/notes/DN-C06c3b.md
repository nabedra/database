# DN-C06c3b conservative stop outcomes and consumed retry

Implemented only the remaining component scope of adopted A21 assignment 3.
C06c3a's reviewed initial claim and joint confirmation remain the baseline.
The parent assigns a NEW specification review, fixes/recheck, then a NEW full
quality review and owns rubric decisions. No aggregate C06 or runtime acceptance
is claimed. No rubric/contracts/design/other-note changes were made.

## Ownership and snapshots

Exactly six owned paths changed:

- `server/src/cluster/provision/stop.rs`
- `server/src/cluster/provision/stop/tests.rs`
- `server/src/cluster/provision.rs`, stop reexports only
- `server/src/cluster/provision/delivery.rs`, failure matrix extraction only
- `server/src/cluster/provision/reconcile.rs`, inventory_failure visibility only
- This new note

Exact pre-edit copies of all five existing files, with repository-relative paths,
are at `/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/dn-c06c3b-originals-mybwh2pi`. `final-changes.diff` and `comparisons.txt` there record
final comparisons. The note had no predecessor. No applicable AGENTS.md was
found in repository/ancestor discovery. Applied TDD, entropy-reducer and unslop.
Read the orchestration policy and c3a/c3b grants, distributed contracts, A21 common
rules/assignment 3, A19/A10/A08/A04 and reviewed C05c/d, C06b/c1/c2/c3a notes.
Implementation follows actual callback, delivery, reconciliation, proof, guard,
heartbeat, PreparedProvision and Fake APIs.

Shared main and existing tracked/untracked work were preserved. No delegation,
branch, worktree, commit, push, network/cloud/paid operation, secret/.env/profile
access, dependency/download/Cargo change, new target or target cleanup occurred.
Existing `server/target` was used. No callback/proof/allocator/store/controller/
heartbeat/member/state/auth/runtime or existing delivery/reconcile test edits.

All thirteen original stop test bodies and assertions are byte-identical.
Only three original fixture helpers changed, as shown in the snapshot diff:
`round`, `positive`, and `fresh_confirmed` derive the actual held owner from the
root instead of hardcoding owner 90/epoch 1. They use `proof_sample` and
`round_offset` for a successor-owner round after the real takeover CAS at paired
sample 20000. Heartbeat observations start in that same paired-time domain and
use the new owner's process identity. Epoch-1 fixtures retain exactly their
original samples and owner. This parameterization permits fresh original signed
requests, replies, heartbeat publications and closed proofs for the new owner;
it does not rebind old evidence or modify proof fields. All new tests/helpers
are appended after the original test file, apart from those three helpers.

Snapshot checks preserve the entire c3a claim-owner/confirmation implementation
from its imports through the claim function preamble byte for byte. Initial
eligibility/history checks remain intact. Only actual claim construction is
shared with retry. provision.rs differs only in stop reexports. Reversing the
single `fn inventory_failure` to `pub(super) fn inventory_failure` substitution
reconstructs reconcile.rs exactly. delivery.rs differs only in extraction of the
existing accepted failure-code/outcome matrix to provision-private
`failure_is_definite(code: &str, outcome: &DeliveryOutcome)` and its replacement
call in complete_start. Start's caller-specific predicate and error precedence
remain intact. Returning D && N instead of D at that call preserves its existing
D && N && caller_conditions expression. All other delivery algorithms are
unchanged, including base/finish/prepare_claim/failure_candidate.

## Concrete private APIs

All functions take `root: &Control`, `owner: Owner` and existing error categories:

```text
complete_stop(root, owner, &ReplyContext, CallbackReply, now_ms, proposal)
    -> Result<ProvisionCandidate, StorageError>
uncertain_stop(root, owner, &ReplyContext, now_ms, proposal)
    -> Result<ProvisionCandidate, StorageError>
reconcile_stop(root, owner, intent_id, InventoryEvidence, now_ms, proposal)
    -> Result<StopReconcilePlan, StorageError>
retry_stop(root, owner, intent_id, ConfirmedDrain, InventoryEvidence,
           invocation_id, now_ms, proposal) -> Result<StopClaim, StorageError>
```

StopReconcilePlan is Change { candidate, classification } or Unchanged(class).
StopInventoryClass is Retryable, Quarantined, TerminalConflict or
NeedsTermination. These types have no Clone/Serde, and Debug exposes no values.
Retryable class describes the inventory only; it grants no command or proof and
does not bypass separate state, time, identity, history or drain eligibility.
NeedsTermination is a closed refusal here. C06d must obtain its required fresh
exact evidence before constructing an irreversible candidate.

Completion validates the whole base and held owner through delivery::base,
then uses reviewed ReplyContext::matches_delivery for the current in_progress
action, intent, node, original launch, incarnation, resource, invocation, owner,
epoch and digest. Further retained checks require an exact managed registered
draining node/session, original start association and consistent history.
The context observation remains provenance and need not equal the claim root.
Stale contexts after takeover, reconciliation or a new invocation refuse.

Stop/null sets only state Reconciling, with outcome still None. It preserves
resource, member phase/session/drain, delivery, attempts, prior error/backoff,
deadlines, physical capacity, spent credit and unrelated fields. Uncertainty
uses the unchanged failure_candidate and checked capped arithmetic, retaining
identity/delivery while recording callback_uncertain and backoffs. A correlated
accepted definite denial terminalizes only the exact current first attempt with
no prior error/ambiguity. identity_conflict, callback_in_progress, unknown and
uncorrelated preparse errors cannot prove no effect. Later attempts remain
Reconciling. Start-only unregistered/unknown-resource/retired-boot conditions
are not imported into known-resource stop. Invalid manually constructed failure
code/outcome pairs return InvalidCatalog, using the same matrix as start.
No failed/not_invoked stop releases physical capacity or restores credit.

Reconciliation consumes exact current-root/current-owner complete inventory
through unchanged classify_inventory/InventoryClass. Unknown identities,
association/resource conflicts and contradictions fail closed. The stop caller
also applies the reviewed unselected-record rule: an unrelated record must be
already bound or eligible for its own live recovery. It does not duplicate the
shared collision scanner. Quarantine/degradation uses exactly inventory_failure
with inventory_quarantined or inventory_terminal_conflict. Invalid/partial
inventory has no usable InventoryEvidence; inventory_failed_candidate remains
the separate later failure path.

A clean old-owner in_progress stop can be adopted into Reconciling while all
old delivery, attempts, backoffs and history remain unchanged. Same-owner
in_progress does not become a retry shortcut. Adoption emits neither command nor
drain proof. The changed root requires new inventory and a new authenticated
round. A unique selected exact terminated record returns NeedsTermination,
never a terminal candidate or retry. Absence can permit an idempotent retry of
the same known resource but proves no termination.

Retry consumes both a fresh complete exact-root inventory and ConfirmedDrain.
Only Reconciling with retained delivery, positive attempts, no outcome, exact
registered draining target and consistent history is eligible. Both persisted
backoffs must have elapsed. Shared prepare_claim checks attempts+1 and max;
exhaustion leaves the caller's root unchanged for later reconciliation. Initial
claim_stop remains Pending/attempts=0 only. Failed stops are never revived.

Initial and retry branches share construct_claim only after distinct eligibility.
For retry it clones the prospective root and changes only retained delivery
invocation/owner/epoch before StopCommand reconstruction; the old digest and all
history remain until the shared preparation replaces delivery. The candidate
captures the ORIGINAL base observation. There is no Pending reset, duplicate
encoder, raw claim constructor, widened proof field or generic transaction API.
Only state/attempts/delivery and root revision/change change on retry.

The original PendingStopClaim continuation and c3a PendingStop::confirm still
check one actual complete-root outcome for both command and proof before either
is released. Ack/ObservedExact release original command/context and successor
proof together. Unknown/Conflict release neither. No production I/O, callback
client, controller loop, listener or new writer is introduced.

## Personally executed tests and limits

Ten appended groups extend the real allocation/claim fixture with actual
Stop/null and correlated error decoding, PreparedProvision publication,
uncertainty/retry, fresh signed drain/reply and published heartbeat evidence,
real owner guard acquisition/takeover, adoption, new-owner re-probe, exact joint
confirmation and final fresh guard authorization. They check complete-root
restoration/equality, capacity/credit, history and identity preservation,
independent inventory/context/proof staleness, selected termination refusal,
unknown association/resource and collision quarantine, an unrelated recoverable
launch, both backoffs/max/overflow, codec-valid changed bindings and opaque types.
Manual CallbackReply construction tests invalid categories only, not authentication.
The shared delay/saturation matrix is reused through existing delivery tests.

The new completion gate holds an already prepared original-token submission
before submit, performs an actual takeover CAS, releases the task and observes
Conflict. It bounds gate entry and settlement, aborts AND joins on watchdog
expiry, consumes the release channel and reloads before assertions. It does not
claim to gate inside the provider's Match. The existing thirteen c3a tests retain
their actual Fake reconciliation gate and cleanup unchanged. No detached tasks
or sleep-based causal overlap is used. Original proof time/submission limits are
retained; new paired-time samples are fixture inputs, not wall-clock/provider
truth. c3a's actual clock-expiry regression is preserved and rerun.

Every Cargo used exactly:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

All tests used the command template below, replacing FILTER with the recorded
filter. Every command ran sequentially through actual exit before any next
Cargo, Rust mutation or formatting:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib FILTER -- --test-threads=2
```

Runs in order, with logs under `/tmp/dn-c06c3b-`:

1. stop filter, red.log, exit 101: compiling behavioral red at the rejection-only
   complete_stop stub. Thirteen original groups passed, Stop/null completion
   failed with Conflict. This preceded production completion behavior.
2. stop filter, green1.log, exit 101: compile-only moved DeliveryOutcome after
   extraction. Corrected the shared helper to borrow the non-Clone outcome.
3. stop filter, green2.log, exit 0: 17 passed.
4. stop filter, expanded.log, exit 101: 19 passed, one new fixture assertion used
   revision MAX-1 although that increment is representable. Corrected to MAX.
5. stop filter, final-stop.log, exit 101: compile-only ambiguous Registration
   import in the newly appended unrelated-launch fixture. Qualified the type.
6. stop filter, classification-red.log, exit 101: 21 passed, two fixture failures.
   InProgress/attempts=0 is codec-invalid, so that case was removed. The new
   unregistered unrelated node incorrectly retained an endpoint; cleared it.
7. stop filter, classification-red2.log, exit 101: 22 passed, one compiling
   behavioral red. Unbound unrelated terminated inventory incorrectly returned
   Unchanged instead of quarantine. Added the caller's unselected-record check.
8. stop filter, stop-green.log, exit 0: all 23 passed, tests 1.77s.
9. cluster::provision::delivery, delivery.log, exit 0: 15 passed, tests 4.01s.
10. cluster::provision::reconcile, reconcile.log, exit 0: 13 passed, tests 2.06s.
11. cluster::callback, callback.log, exit 0: 10 passed, tests 0.34s.
12. cluster::drain::proof, proof.log, exit 0: 21 passed, tests 0.56s.
13. Strict Clippy below, clippy.log, exit 0, 25.34s.
14. stop filter, stop-capacity.log, exit 0: 23 passed, tests 1.76s, after adding
    an explicit managed_capacity equality assertion for denial outcomes. This
    test-only addition supplements the existing nodes/allocation-capacity/credit
    assertions; production code did not change.
15. Strict Clippy repeated for that test addition, clippy-final.log, exit 0,
    24.77s, no diagnostics.

The two identified behavioral reds establish only those behaviors. Other new
coverage has green-only provenance; compile/fixture failures are not behavioral
reds, and no assertion-by-assertion red claim is made. These are separate runs,
not a combined final suite. No broad/default/S3/runtime suite was run.

With the same environment, both recorded strict Clippy runs passed:

```sh
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

No Rust changed after the final stop-capacity and Clippy runs. The earlier
affected regressions cover identical production code; they were not repeated
for the final assertion-only addition.
Scoped formatting and final formatting check used only the five owned Rust
paths with `rustfmt --edition 2024 --config skip_children=true` and `--check`.
Direct byte checks cover all six owned files, including this untracked note,
requiring no trailing whitespace and exactly one final newline. Scoped
`git diff --check` and snapshot comparisons pass. Git tracked diff alone does
not cover these untracked source files.

## Remaining obligations and release

Provider quarantine/termination semantics here are component-model assumptions,
not verified real inventory truth. C06d owns unique exact positive termination;
no absence, callback acceptance/failure, phase or timeout proves it. Another stop
ID after terminal no-effect would require ordinary admission outside these APIs.
Later integration must supervise/cancel/join original submissions and deliveries,
retain original budgets, obtain fresh authenticated rounds and authorized
inventory for each new root, consume check_live and independently authorize the
current guard generation/root immediately before dispatch. Callback entry needs
its independent fresh authorization and provider idempotency. Runtime loops,
HTTP/provider acceptance and whole-C06 acceptance remain open.

Rust implementation and Cargo/build ownership are explicitly RELEASED to the
parent. All initiated Cargo/formatter/runner processes exited. Every initiated
gated task settled or was joined, and all gates were released/removed. No owned
process or detached task remains. NEW specification review and NEW full quality
review remain required.
