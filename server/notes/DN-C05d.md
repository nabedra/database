# DN-C05d consumed inventory recovery and same-identity retry

Implemented only adopted A19 mini 4, including the parent's narrow shared claim/backoff extraction. This is component metadata planning and consumed publication evidence. Allocation is not enabled, and aggregate C05/F10/F12/F13 and distributed acceptance remain open. Independent specification review, then fresh quality review, belong to the parent.

Applied test-driven-development and unslop. No applicable AGENTS.md was found. Inspected the distributed contracts and rubric orchestration/testing/C05/F10/F12/F13, A19 common/mini4 and helper clarification, adopted A05/A08/A10/A04 including the completed-cache removal override, relevant A07/A16/A21, completed C05a/b/c and C03b, C04 store/ownership and codec notes, DN-test-lints, and actual completed callback/attachment/store/Fake interfaces. Historical pending C03b/c/d statements in A19 were treated as superseded. Unfinished R04c1 admission/auth/initialization implementation was not inspected.

## Files and snapshots

New files are `server/src/cluster/provision/reconcile.rs`, its private `reconcile/tests.rs`, and this note. `provision.rs` changes only its child declaration and narrow reexports. `delivery.rs` shares the existing base/final candidate checks, finite claim construction and checked backoff, with a separate closed reconciling eligibility branch. `delivery/tests.rs` appends one preservation group. Every original delivery test body is a byte-identical prefix of the final file. Confirmation, completion and conservative first-attempt no-effect algorithms remain unchanged; completion uses the extracted equivalent scheduler arithmetic.

Every existing owned file was copied before editing to `/tmp/meshdb-c05d-before-h4i4pdn8/`, retaining its repository-relative path:

- `server/src/cluster/provision.rs`
- `server/src/cluster/provision/delivery.rs`
- `server/src/cluster/provision/delivery/tests.rs`

The three new files did not exist. Shared tracked and untracked edits were preserved. No branch, worktree, commit, push, delegation, dependency/Cargo/schema/runtime/Python edit, secret/profile/.env read, network/cloud/paid action, download, target cleanup or new target was used. All Cargo work used existing `server/target`.

## APIs and transition rules

Cluster-private consuming operations are:

```rust
reconcile_start(&Control, Owner, intent_id: Uuid, InventoryEvidence,
                now_ms: u64, proposal: Uuid)
    -> Result<ReconcilePlan, StorageError>
retry_start(&Control, Owner, intent_id: Uuid, InventoryEvidence,
            invocation_id: Uuid, &AttachedConfig, now_ms: u64, proposal: Uuid)
    -> Result<StartClaim, StorageError>
inventory_failed_candidate(&Control, Owner, now_ms: u64, proposal: Uuid)
    -> Result<ProvisionCandidate, StorageError>
```

`ReconcilePlan` is `Change { candidate, classification }` or `Unchanged(classification)`. `ReconcileClass` distinguishes Recovered, Absent, Quarantined, TerminalConflict, AlreadyKnown and NeedsTerminationReconciliation. The last carries an opaque exact cluster/node/original-request/resource binding with consuming `into_parts()`. Plans and bindings are non-Clone, have redacted Debug, private representation and no Serde or unchecked constructors. Existing candidate and StartClaim privacy remains intact.

Provision-private `classify_inventory(root, owner, &InventoryEvidence)` checks the original complete observation and held exact owner/epoch, then scans every bounded record. It rejects foreign/unknown identities, contradictory resources, resource reuse, multiple resources for one launch including terminated records, and active evidence against irreversible witnesses. The one-start classification permits unrelated resources already bound on their node/start or exact nonterminated positives for eligible retained unresolved launches. Only the selected launch is recovered in that round. It never silently selects a matching target while ignoring unknown resources. Existing strict decoding rejects malformed/duplicate snapshots before evidence exists.

Exact nonterminated positive evidence recovers both resource bindings on the same retained start and completes ResourceKnown, clearing only its error. It preserves node phase, registration, session/verifier, drain, identities, attempts, delivery, deadline, retry history, scheduler, capacity and stop credits. Late and registered/unregistered draining launches remain recoverable. Lost/stopped, retired and failed launches cannot revive.

A prior owner's unfinished in_progress delivery first produces a reconciling candidate, preserving the entire original delivery/history/backoff. Its classification describes the observed snapshot; even Recovered on this adoption step does not mean that the candidate completed ResourceKnown. Submit that candidate, load the actual new root and obtain NEW inventory for recovery or retry. No old snapshot is rebound, and retry refuses in_progress. Quarantine/terminal contradiction instead produces sanitized failure/backoff while preserving terminal witnesses and resource bindings.

Absence changes no capacity, terminal witness, identity or credit. Positive terminated records produce only NeedsTerminationReconciliation with their exact binding. They never complete ResourceKnown, mark stopped or authorize stop. Inventory failure has no snapshot input and changes only checked scheduler failures/backoff. Quarantine errors are fixed `inventory_quarantined` or `inventory_terminal_conflict`; no provider diagnostics enter persistence.

Retry consumes exact current complete absence, refuses unknown/conflicting resources and requires a reconciling dispatched unresolved start, elapsed intent AND scheduler backoffs, attempts below max, a fresh delivery invocation, starting/unregistered phase and no resource/retired boot/terminal witness. It increments attempts once and preserves the original identities/template/history. Exhaustion returns LimitExceeded without mutation; later positive inventory remains usable. Initial `claim_start` remains pending-only.

The shared constructor stages only the prospective delivery identity in a complete reconciling root before StartCommand reconstruction. It retains the old digest until the codec reconstructs and hashes the exact new bytes, then replaces the delivery with that digest and sets in_progress/attempts. It never resets a root to Pending. The candidate captures the ORIGINAL complete observation, not the transient reconstruction root. PendingStart still consumes whole-root Ack/ObservedExact confirmation once; Unknown/Conflict release no dispatch bytes.

Every mutation validates the complete original root, owner, proposal, checked revision and bounded final encoding. Recovery/failure use ordinary validation, with no new-admission or public-status gate. Config, Template, Timing, controller, independent policy/admin fields, unrelated records, cooldown and low_since are preserved. Backoff uses the existing capped exponential algorithm with at most 64 doublings, checked failure/time arithmetic and maximum with retained deadlines. Success, takeover and admin changes do not reset history. Existing InvalidCatalog/Conflict/LimitExceeded categories are reused.

## Personally executed checks and red provenance

Every Cargo invocation used this exact environment and flags, sequentially through actual process exit:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::provision::reconcile::tests -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::provision:: -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::provision::reconcile::tests::known_unrelated_resource -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::callback::tests::inventory_ -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::store::provision::tests -- --test-threads=2
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

The first reconciliation run compiled against rejection-only placeholders and failed both positive groups with unexpected Conflict. The expanded seven-group placeholder run compiled, failed six positive groups with Conflict and passed the refusal-only group. These are behavioral reds. Initial implementation passed six groups but failed valid retry because command reconstruction still saw the old delivery invocation. After staging the prospective delivery identity, all 27 provisioning tests passed. Expanded draining/unrelated/saturated/confirmation and extraction checks then passed all 32 provisioning tests, with three new unused-result warnings subsequently corrected.

An attempted focused node-only-known-resource regression first had a test-edit macro syntax error, which is compile-only provenance. After fixing that edit, it compiled and failed behaviorally with Quarantined instead of Recovered. The classifier was corrected to recognize an exact resource retained on either the node or its start. Later additions and strengthened assertions otherwise have green-only provenance; no independent red is claimed for every assertion or reused algorithm.

Final focused runs passed 11 reconciliation groups, two existing inventory codec groups and four existing provision-store groups. The last complete provisioning run passed all 32 groups before the node-only-known-resource correction; the final reconciliation run includes that corrected case. Existing delivery groups and their new preservation group had already passed. No broad suite or default/S3 build was run.

The first strict library/test Clippy run failed on unread ReconcilePlan fields awaiting runtime callers and its intentionally unequal concrete variant sizes. Item-scoped dead_code and large_enum_variant allowances document the pending supervisor and preserve the adopted inline ProvisionCandidate API. No behavioral code changed for those diagnostics. The final strict Clippy result is recorded at handoff.

The tests use actual Control/Template codecs, InventoryCommand/decode_reply, reviewed attachment fixtures and Fake, with real PreparedProvision submission for recovery and retry confirmation. They cover complete scans, independent wrong request/unknown/collision/terminal roots, terminated versus absence, late draining recovery, original-delivery adoption, old inventory after takeover/admin advancement, both backoffs/max/phase restrictions, fresh delivery/hash and retained retry errors, later-attempt denial ambiguity, saturated retained recovery/failure, exact Ack and Unknown command withholding. Existing callback groups supply duplicate rejection and stop-null distinction; existing delivery groups retain their causal takeover/CAS/confirmation checks. New tests create no detached tasks or sleeps. Existing gated regressions release gates and join their owned task before assertions.

## Remaining caller obligations

The caller must retain consumed inventory, candidate, pending command, async submission and effect ownership through cancellation; use PreparedProvision and only exact whole-root confirmation; generate fresh proposal/invocation UUIDs; obtain inventory authorized for the actual current root after each advance; and perform final live OwnershipGuard and independent fresh callback-entry authorization. Metadata owner pairs, codec-valid inventory and confirmed commands grant no live authority or provider-truth certificate. Finite clone/hash/encoding work belongs on the existing bounded coordinator executor at integration.

Actual projected public status encoding of the complete envelope at <=1 MiB, live guard integration, fresh inventory authorization, retained delivery/submission supervision, C06 positive drain/termination, periodic/runtime/network/provider truth and aggregate C05 remain open. Stop-null, heartbeat loss, expired deadlines and absence never establish termination. No allocation-enabled or distributed acceptance claim is made.

## Final tooling and release

Final strict `clippy --lib --tests -- -D warnings` passed without diagnostics, exit 0, in 19.27 seconds. Owned formatting and direct whitespace checks passed, including untracked files and exactly one final newline. The scoped commands were:

```sh
rustfmt --edition 2024 --config skip_children=true server/src/cluster/provision.rs server/src/cluster/provision/delivery.rs server/src/cluster/provision/delivery/tests.rs server/src/cluster/provision/reconcile.rs server/src/cluster/provision/reconcile/tests.rs
rustfmt --check --edition 2024 --config skip_children=true server/src/cluster/provision.rs server/src/cluster/provision/delivery.rs server/src/cluster/provision/delivery/tests.rs server/src/cluster/provision/reconcile.rs server/src/cluster/provision/reconcile/tests.rs
git diff --check -- server/src/cluster/provision.rs server/src/cluster/provision/delivery.rs server/src/cluster/provision/delivery/tests.rs server/src/cluster/provision/reconcile.rs server/src/cluster/provision/reconcile/tests.rs server/notes/DN-C05d.md
```

A direct Python byte check covered those six files for trailing spaces/tabs and exactly one final newline, and verified the original delivery test file remains a byte-identical prefix. Snapshot diffs confirm provision.rs contains only declaration/reexports and delivery.rs contains only the finite claim/backoff/candidate helper changes described above.

Rust implementation and Cargo/build ownership are RELEASED, with no owned process running. Parent assigns fresh specification review followed by fresh quality review. This handoff remains component-only and grants no runtime allocation, callback, stop or distributed authority.

## Parent specification corrections: independent recovery and bounded scans

Independent specification review found two P2 gaps. Two valid dispatched unresolved
launches, each with an exact positive inventory record, quarantined one another
because the unrelated resource was not yet persisted. The collision predicates
also repeated a full node scan inside each other-node and other-intent check.

The parent changed only reconcile.rs, its tests and this note. Complete inventory
contradiction checks still run first. A shared finite recoverable-start predicate
now permits exact nonterminated positives for eligible retained unresolved launches
to coexist; the candidate changes only the selected launch. An unrelated launch
still needs its own new inventory round against the committed successor root.
Known node-only bindings remain accepted. Unknown identities, collisions, invalid
states and irreversible contradictions retain their refusals; no raw evidence,
termination, admission or authority interface was added.

Collision checks now scan each other-node and other-intent array directly. The
redundant nested node lookups were removed. The one-start unrelated-record check
also uses separate bounded identity lookups. Resource scans are therefore bounded
by records times nodes plus intents, rather than a further node-count multiplier.
Test-only thread-local counters observe collision predicate visits; they change
no production behavior and measure neither elapsed performance nor process RSS.

Pre-correction copies, retaining repository-relative paths, are at
`/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/meshdb-c05d-recovery-scan-kdulio3o/`.
The existing unrelated-resource test now expects recovery for its eligible second
unresolved launch, while preserving known-binding, duplicate-terminated and retired
controls. New causal tests establish:

- Two actual allocated/dispatched retained launches recover sequentially through
  Fake-backed PreparedProvision acknowledgements. Each whole expected root changes
  only its selected resource/completion fields and revision/change ID. Old inventory
  fails after the first commit; the second recovery requires newly decoded inventory.
  Attempts, delivery, scheduler, capacity and first-stop credits stay retained.
- Actual complete 32-record and 1024-record validated inventories satisfy the
  collision-visit bound `records * (nodes + intents)`, with a positive observer
  assertion. The smaller case runs first so the failing-before execution does not
  perform the old fleet-sized repeated scan. This is algorithmic count evidence,
  not a wall-clock benchmark or a full distributed fleet test.

All commands used the original four required environment variables, existing
target, locked/offline/no-default flags and serial actual process completion:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::provision::reconcile::tests::parent_regression_ -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::provision::reconcile::tests -- --test-threads=2
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

Both new tests failed behaviorally before correction (30.30s build, 0.06s tests,
exit 101): Quarantined instead of Recovered, and 65,536 collision visits exceeding
2,048 for 32 records. After correction all thirteen reconciliation groups passed
(30.14s build, 2.08s tests), including the 1024-record case. Strict library/test
Clippy passed in 24.15s. Owned rustfmt, scoped git diff --check and direct
whitespace/single-final-newline checks passed. Prior inventory/store/delivery
results are reused; no later combined provisioning rerun is claimed.

The parent releases Rust/Cargo with no process running. A NEW specification
re-check must close both findings before independent quality review. Component
recovery supplies no provider truth, live authorization or aggregate runtime result.
