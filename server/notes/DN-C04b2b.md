# DN-C04b2b consumed ownership submission

Implemented only the second accepted A10 mini. Read the shared distributed
contracts, accepted A10 including FINAL proposal UUID binding and outcomes,
A05/A08 preservation and reservation rules, C04b1/C04b2a delivery notes, and
actual store/controller/state/configuration, storage/publication and shared Fake
code. No applicable AGENTS.md was found in ancestor or repository searches.
Applied test-driven-development and unslop. Existing shared-main work was
preserved. No delegate, branch/worktree, commit/push, dependency/download,
.env/real-secret read, cloud call, target deletion or default AWS build occurred.

## Delivered API

Crate-private `PreparedOwnership::prepare(base: LoadedControl,
change: OwnershipChange, unix_ms: u64, proposal_id: Uuid)
-> Result<Self, StorageError>` calls the reviewed pure constructor with exactly
that caller UUID/time, then bounded-encodes the complete validated candidate
before I/O. It owns the exact base/token, candidate and bytes with private fields,
no Clone, setters, token accessor or arbitrary serialized-candidate constructor.
Rust consumption prevents submitting the same prepared value twice.

Consumed `PreparedOwnership::submit<B: ObjectStore>(self,
storage: &Storage<B>, deadline: Instant)
-> Result<OwnershipOutcome, StorageError>` checks the deadline before polling
storage and attempts at most one canonical-key Match with the captured token.
There is no Create, unconditional overwrite, recomputation, resend, replacement
UUID, ownership-history traversal or publication-head invariant.

`OwnershipOutcome` has `Acknowledged(LoadedControl)`,
`ObservedExact(LoadedControl)`, `Conflict { current: Option<LoadedControl> }`
and `Unknown { current: Option<LoadedControl> }`. Only a timely durable response
with a nonempty token yields Acknowledged. Conflict remains definite rejection
even if reconciliation fails or reads bytes identical to the candidate. After a
possibly applied write, only the exact proposal UUID and entire candidate root
can yield ObservedExact. Different/later roots, equal UUID with different
contents, corrupt/missing/failed/expired reads and empty read tokens retain
Unknown. A validated different root may be included. Definite backend errors
that guarantee no application propagate their existing static StorageError.

At most one load_control reconciliation follows Conflict, Ambiguous, empty
success token or write timeout. It shares the original absolute deadline. An
already exhausted deadline polls neither PUT nor reconciliation GET. Dropping
or cancelling a possibly submitted future cannot prove rollback. Debug includes
only outcome category and loaded/prepared revision/change UUID. It excludes
opaque tokens, configuration, template/body and provider details.

Existing load_control/initialize_control behavior remains compatible. Ownership
preserves the entire validated configuration supported by the current codec,
including its generation and fingerprints. A11 is adopted, but C08c owns the
future generation-2/worker-semantic-field codec and fixture amendment. This
mini neither implements that update nor interprets configuration fingerprints.

## Files and authorized boundary exceptions

- `src/cluster/store.rs` adds the private prepared value/outcomes/submission.
- `src/cluster/store/tests.rs` is the new private test module.
- `src/cluster/mod.rs` removes the obsolete controller module-wide allowance.
- This note records delivery.
- Parent explicitly authorized only the private test-only
  `#[cfg(test)] extern crate self as meshdb_server;` addition to `src/lib.rs`.
  The shared fixture imports meshdb_server::storage, which otherwise cannot
  resolve inside library unit tests. The alias enables an unchanged path import
  of tests/support/storage.rs, with no public facade or fixture edit.
- Parent explicitly authorized only a temporary enum-level `#[allow(dead_code)]`
  and C04b2c removal comment on OwnershipChange in `src/cluster/controller.rs`.
  No controller implementation or tests changed. Actual production callers now
  use its constructor, but do not construct variants until later integration.

New pending PreparedOwnership and OwnershipOutcome items/impl have narrowly
scoped dead_code allowances, with a C04b2c removal obligation. There is no blanket
module allowance or public exposure for lint purposes. Both authorized exception
files are included in owned formatting/whitespace checks.

## Executed red and green evidence

All Cargo commands ran sequentially in the existing target with this exact
prefix and flags, without default features or online resolution:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::store::tests
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::controller
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test control_store
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --test control_store -- -D warnings
```

The first private test compile exited 101 on the fixture crate-name import.
After the authorized alias, the behavioral red run compiled against a temporary
rejection-only preparation/submission stub. All ten groups failed, exit 101,
with unexpected Conflict for successful preparation and LimitExceeded cases.
Two event-gated groups also hit bounded watchdogs because preparation failed
before their events. No actual submission implementation preceded this red run.
Replacing the stub produced ten green groups in 0.48 seconds. Test strengthening
added complete nonempty retained-state preservation, exact-current definite
conflict via Fake race injection, and abort-on-drop task ownership. The final
private run again passed all ten groups in 0.48 seconds, exit 0.

Pure-candidate regression passed all five groups in 4.28 seconds. Existing
public control-store regression passed all nine groups in 1.45 seconds. That
library compile exposed unused OwnershipChange variants after removing the
module allowance. The parent authorized the narrower enum allowance. Final
strict library/targeted-test Clippy passed, exit 0, in 9.77 seconds.

Private groups cover captured token/condition/key and acknowledged body/token;
exact retained-field preservation including nodes/intents/config/template,
policy/admin revisions and scheduler; local invalid/overflow/UUID/owner
rejection before any I/O; two acquisitions with one CAS winner; a one-shot gate
immediately before Match application holding renewal while takeover wins;
post-application/pre-ack gates losing acquire/renew/release responses; exact
current confirmation versus later-root uncertainty; a separate explicit fresh
owner-checked successful recovery renewal; empty acknowledgments; malformed
exact-ID bodies, validated exact-ID/different contents and failed/missing/corrupt
reads; definite rejection/failure classification; and deadline/cancellation
uncertainty. Fake race, lost-response, put faults and read gates are reused.
There is no mutex held across await. Event gates establish polling/application;
no sleep is used as proof. Tasks have bounded watchdogs with abort/join on
expiry, abort-on-drop ownership, and explicit abort/join for cancellation.
PUT/GET counts establish the single-write and reconciliation bounds. Fake GET
counts increment after its read gate, so the stalled-read case additionally
observes the entered event to prove the request was polled.

Owned checks, including untracked files:

```sh
rustfmt --edition 2024 --config skip_children=true --check server/src/cluster/store.rs server/src/cluster/store/tests.rs server/src/cluster/mod.rs server/src/cluster/controller.rs server/src/lib.rs
git diff --check -- server/src/cluster/store.rs server/src/cluster/store/tests.rs server/src/cluster/mod.rs server/src/cluster/controller.rs server/src/lib.rs server/notes/DN-C04b2b.md
```

A scoped Python byte check also verifies the same six files end in newline and
contain no trailing spaces/tabs. No broad suite or unrelated sender regression
was run. The parent owns that pending sender regression after Cargo release.

## Integration obligations and limits

The future caller selects a fresh UUID before guard.begin and supplies that
same identity/time to preparation. Acknowledged is only eligible for future
guard checks; it is not live authority by itself. ObservedExact never installs
certainty. After uncertain acquire/renew, recovery requires a distinct explicit
owner-checked renewal with fresh observation, UUID and successful timely CAS.
No hidden renewal occurs here. The caller owns cancellation uncertainty.

C04b2c owns local certainty, stamps/generations, clock rules and combined guard
tests. No controller loop, callback authorization, lifecycle completion,
startup/runtime wiring, real TCP, cloud/provider safety or distributed authority
guarantee is supplied. These tests prove in-process metadata behavior only.
Independent specification review followed by quality review remains parent-owned.
Cargo/build ownership is explicitly released at delivery.
