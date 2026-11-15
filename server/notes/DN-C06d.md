# DN-C06d positive exact termination reconciliation

Implemented adopted A21 assignment 4 only. The latest user direction prioritizes
integrated functionality and supersedes additional per-mini review steps. No
rubric, aggregate decision or runtime acceptance is claimed here.

## Scope and preservation

Exactly four owned files:

- New server/src/cluster/provision/termination.rs
- New server/src/cluster/provision/termination/tests.rs
- server/src/cluster/provision.rs, private declaration/reexports only
- This new note

Original provision.rs and the exact final diff/comparison are outside the repo at
/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/dn-c06d-originals-wlmg41fl.
The entire original provision.rs remains a byte-identical prefix. Nonexcluded
baseline files are compared by digest. The only observed nonowned difference was
plans/distributed-nodes.md, updated concurrently during the parent priority shift.
This implementation made no rubric edit.
No applicable AGENTS.md was found. Applied TDD, entropy-reducer and unslop; read
the current orchestration/grant, contracts, A21 common/assignment 4, A19/A08/A10,
relevant A04 and reviewed C05d/C06a/b/c notes and actual supporting APIs.

Shared work was preserved. No delegation, branch/worktree, commit/push,
network/cloud/paid action, dependency/Cargo change, download, new target or target
cleanup occurred. Existing server/target was used. No existing tests or callback,
proof, stop, delivery, reconciliation, store, controller, heartbeat, member,
state, auth or runtime source was changed.

Scope incident: the initial broad preservation hash sweep unintentionally read
.env bytes to compute a digest, contrary to the assignment. No contents were
printed or copied; the digest was removed from the external baseline and .env
was excluded from subsequent comparisons. No further .env access occurred.

## API and behavior

Cluster-private consuming API:

```text
reconcile_termination(&Control, Owner, node_id, InventoryEvidence, now_ms, proposal)
    -> Result<TerminationPlan, StorageError>
TerminationPlan = Change(ProvisionCandidate) | Unchanged(TerminationClass)
TerminationClass = Absent | Pending | Quarantined | TerminalConflict | AlreadyTerminated
```

Plans have redacted Debug, no Clone/Serde and no raw proof constructor. Inventory
comes only through existing InventoryCommand/decode_reply. The unchanged common
classify_inventory validates the entire current root/held owner association and
complete inventory, including resource collisions. The new child indexes retained
intents once, checks resource/history/delivery consistency, and refuses multiple
stops, no-effect START, repeated delivery invocations, impossible delivery epochs
or same-epoch owners and inconsistent attempt evidence. Typed validity is not
provider truth.

A unique exact terminated resource terminalizes only its retained launch. Unknown
resource binding and termination are atomic, including Lost and late unregistered
Draining nodes. An exact retained stop becomes Completed/Terminated; otherwise the
original START becomes Failed/Terminated. Node becomes Stopped. Resource,
incarnation, registration/session/drain, delivery/attempt/error/backoff and unrelated
state are preserved except the explicit terminal fields and revision/change ID.
No stop, replacement, resource_known outcome or incarnation rewrite is created.
Managed physical capacity resolves without resurrecting first-stop credits.

Exact terminal replay precedes proposal/revision growth, including MAX/repeated
proposal. Changed candidates use delivery::base/finish and bounded full encoding.
Quarantine uses unchanged inventory_failure and fixed inventory_quarantined or
inventory_terminal_conflict codes with checked backoff. Pending START degradation
is global only. Irreversible witnesses are never overwritten by failure.
Absence, active/stopping records, callback acceptance/no-effect STOP, deadlines
and phase-only Stopped do not prove physical termination.

Two eligible independent unbound terminal resources permit sequential selected-only
progress, requiring newly decoded successor-associated inventory for the second.
Unknown/conflicting records still refuse. Concrete ambiguous retained history is
two stop IDs for the same launch/resource/incarnation, even if one previously
failed/not_invoked. This API fails closed rather than inventing a stop-selection
protocol; later ordinary admission remains outside its scope.

Candidates use existing PreparedProvision/submit_control with the captured base,
one Match and the existing bounded readback semantics. There is no token access,
new writer, resend, callback/provider action or local certainty installation.

## Executed validation

Every Cargo used existing server/target and exactly this environment:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Every test used:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib FILTER -- --test-threads=2
```

All commands awaited actual exit before the next Cargo, source mutation or
formatter. Logs are /tmp/dn-c06d-NAME.log. Execution order:

| NAME | FILTER / operation | Result |
| --- | --- | --- |
| red | cluster::provision::termination | exit 101, compiling behavioral red: legitimate unique terminated evidence returned Pending |
| green1 | same | exit 101, 7 passed, 2 fixture failures: foreign-cluster decode refusal and invalid external shape |
| green2 | same | exit 101, 10 passed, external shape still lacked external provisioning mode |
| green3 | same | exit 0, 12 passed |
| final | same | exit 0, 13 passed |
| reconcile | cluster::provision::reconcile | exit 0, 13 passed |
| stop | cluster::provision::stop | exit 0, 23 passed |
| capacity | cluster::provision::tests | exit 0, 7 passed |
| store | cluster::store::provision | exit 0, 4 passed |
| clippy | strict command below | exit 0 |
| delivery-red | cluster::provision::termination | exit 101, 13 passed, compiling behavioral red: reused retained invocation still terminalized |
| delivery-green | same | exit 0, 14 passed |
| clippy-final | strict command below | exit 0, no diagnostics, 25.01s |

```sh
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

The final focused run covers the delivery-consistency correction. Earlier affected
regressions cover unchanged sibling implementations; no blanket repeat or combined
final suite is claimed. Other additions have green-only provenance. Fixture/decode
failures are not behavioral reds. Provisional unused warnings in the initial stub
were resolved; no compilation or Clippy error was used as behavioral evidence.

Tests use actual generation-2 codecs and InventoryCommand/decode_reply. Positive
submit/reload paths compare the retained whole root, physical capacity and credits.
A bounded gate holds the original prepared submission before submit while an actual
takeover or late-resource-recovery CAS wins. It then releases and settles the
original task, reloads and reclassifies with fresh evidence. This is not a gate
inside provider Match. Watchdog expiry retains cleanup ownership and aborts AND
joins before assertions. The real original StartCommand reply context is also
used to reject a late different callback resource after terminal commit.

Most fixtures are memory-only. The last callback-context test uses the existing
owned material fixture only to construct/confirm a real start command; no worker,
provider or network probe runs. Stop/null, not_invoked, phase and deadline controls
are component shapes, not executed provider/callback or stale-heartbeat evidence.
Foreign-cluster/partial/duplicate replies refuse at decode; external mode refuses
InventoryCommand construction. Those are earlier boundaries, not transition-branch
coverage. No provider truth, authenticated live inventory or runtime behavior is
established by these fixtures.

Final owned formatting uses rustfmt --edition 2024 --config skip_children=true
and --check on the three owned Rust files. Direct whitespace/exact-one-final-newline
checks include all four owned files, plus scoped git diff --check and snapshot
comparison. No original test file changed.

## Release and integration obligations

Provider truth/idempotency, independent callback-entry authorization, fresh final
OwnershipGuard generation/root/time authorization, bounded coordinator execution,
original submission/delivery ownership and runtime/API/worker integration remain
required. A new root or owner requires fresh inventory. Component reconciliation
does not close whole C06 or establish real provider termination.

Rust implementation and Cargo/build ownership are explicitly RELEASED. Every
initiated Cargo, serial runner, formatter and fixture process exited. Every gated
async task settled or was abort-and-joined; no owned process or task remains.
