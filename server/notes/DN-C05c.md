# DN-C05c pure start claim and exact delivery outcomes

Implemented adopted A19 mini3, including the parent's pulled-forward A21 start-context association. This is private metadata construction and consumed publication evidence only. It establishes no runtime or aggregate C05/F10/F12/F13 guarantee.

## Files and ownership

New `server/src/cluster/provision/delivery.rs` and private `delivery/tests.rs` implement and test the slice. `provision.rs` changes only the child declaration, cluster-private reexports and removal of the obsolete `into_parts` allowance. Its allocator, capacity and identity helper bodies are unchanged. `callback.rs` retains the validated start target and exact serialized-command digest privately, adds read-only `ReplyContext::intent_id()` and `matches_delivery(root, owner, intent_id)`, and narrows obsolete allowances. Its wire parsing, inventory behavior, original-byte validation/hashing and redaction remain intact. `callback/tests.rs` adds one focused association group. This note is new. No other source, test, dependency, schema or plan file was edited.

Pre-edit snapshots are outside the repository at `/tmp/meshdb-DN-C05c-before/{provision.rs,callback.rs,callback-tests.rs}`. Snapshot comparison confirmed the authorized provision delta and preservation of the pre-existing callback test prefix. Existing modified/untracked work was preserved. No applicable AGENTS.md was found in ancestor/path discovery. The TDD and unslop skills were applied. No further delegation, branch/worktree, commit/push, dependency download, network/cloud/paid call, real secret/profile/.env access or target cleanup occurred. Synthetic material uses the existing attachment Fixture; storage uses `crate::test_storage::Fake`.

## Private interfaces and invariants

All new delivery interfaces have cluster visibility, matching the existing callback records without widening them. `claim_start(root, owner, intent_id, invocation_id, attached, now_ms, proposal)` accepts only an undispatched pending managed start. It validates the complete base, exact held owner, current eligibility, retained identity collisions, retry/global backoffs, checked attempts and max attempts. It reconstructs the StartCommand from persisted Template and retained enrollment before hashing. It advances revision/change identity once and changes only start state, attempts and exact delivery metadata.

Opaque must-use, non-Clone `StartClaim::into_parts()` consumes into `ProvisionCandidate` plus `PendingStart`. The latter owns the entire expected candidate and original command. Consuming `confirm(&OwnershipOutcome)` accepts only whole-root Acknowledged/ObservedExact and returns `ConfirmedStart`; Unknown, Conflict and UUID-only matches release nothing. Consuming `ConfirmedStart::into_parts()` yields the exact original bytes and ReplyContext once. These values carry no live authority.

`complete_start` and `uncertain_start` return a consumed ProvisionCandidate, using callback-owned validation of current owner/action/intent/launch/invocation/epoch/digest and in_progress state. Provision reads no private context fields. The original context observation remains provenance, rather than an equality requirement against the claim successor. Positive resource completion attaches node and start together, rejects collisions/terminal contradictions and preserves phase, registration/session/verifier/drain and scheduler history. It never makes a member ready. A registered draining current delivery can succeed late; obsolete deliveries cannot complete.

Uncertain evidence retains stable IDs, last delivery and attempts, moves to reconciling, and records fixed sanitized error text. Intent/global delay uses the configured cap with at most 64 bounded doublings and no intermediate overflow. Failures and both timestamps are checked; existing greater retry/backoff, cooldown and low_since remain. Success does not reset global failures. Only an exact initial in_progress first attempt with definite correlated pre-entry denial, no resource, registered/retired boot or prior uncertainty can terminalize failed/not_invoked. Identity conflict and callback-in-progress never provide that proof. Later-attempt, registered/resource-bearing and prior-ambiguity denials stay reconciling. Terminal contradictions fail closed.

Retained updates use full bounded validation/encoding, without new-admission gating. The existing private ProvisionCandidate constructor and PreparedProvision remain the sole candidate/publication path. No second writer, retry loop, callback HTTP/client/cache, authorization facade or production material accessor was added. Small private base/finish/failure helpers stay local; C06's Start/Stop extraction is deferred.

## Personal execution evidence

Every Cargo invocation used the following environment and existing `server/target`, with no overlap. Each process was awaited through its actual exit before the next Cargo command.

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Commands below used `cargo ... --manifest-path server/Cargo.toml --locked --offline --no-default-features`.

- Behavioral red: `test ... --lib cluster::provision::delivery::tests::pending_start_can_be_claimed` compiled and failed the expected claim-is-ok assertion against the rejecting stub. Subsequent initial delivery run passed eight groups.
- Compile-only correction: the expanded tests initially failed to compile because `assert_ne!` required Debug on opaque OwnershipObservation. The assertion became a boolean comparison; no observation Debug was added. Initial visibility warnings were corrected by narrowing delivery APIs to cluster visibility. These were not behavioral red results.
- Additional behavioral red: `test ... --lib cluster::provision::delivery::tests::retired_boot_prevents_false_no_effect_terminalization` compiled and failed with Failed versus expected Reconciling. Adding the retained-retired-boot exclusion corrected it.
- Final `test ... --lib cluster::provision::delivery::tests`: **13 passed**. Coverage includes exact command/hash/root confirmation and resource completion, independent valid wrong-owner/epoch/invocation/digest/launch/resource/phase controls, Unknown/Conflict withholding, definite denial versus prior uncertainty/registered/resource/later-attempt/identity-conflict/in-progress controls, late draining registration preservation, bounded cap and checked counters/timestamps, unchanged input on errors, sanitized uncorrelated failure and full-admission retained-credit progress. Remaining cases were green-only additions, not individually observed red.
- `test ... --lib cluster::callback::tests`: **7 passed**, including the new start-context association group and unchanged Python-compatible literal-byte/hash, relocated/deleted material, strict parser, inventory and redaction regressions.
- `test ... --lib cluster::store::provision::tests`: **4 passed**. These reuse the existing preparation, same-token competition, lost-response exact/unknown and takeover tests instead of copying the CAS matrix.
- Final `clippy ... --lib --tests -- -D warnings`: **passed**. The first Clippy run identified inventory's unused production payload reader after removal of the old enum-wide allowance; the remaining allowance is on that variant only, pending C05d.

The new bounded takeover gate wraps the actual Fake Match. It records entry, publishes the takeover using the exact captured token, releases the gate, joins/settles the owned task and removes synthetic scratch before assertions. The stale completion returns Conflict with the takeover root; assertions require exactly two PUTs, two GETs, the original completion token, unchanged takeover bytes and a changed backend token. This is metadata CAS evidence, not live callback-entry fencing.

Owned `rustfmt --edition 2024 --config skip_children=true` and its check cover only the five Rust files above. Direct trailing-whitespace/final-newline checks include all six owned files, including untracked files. No whole-suite run, default/S3 build or repeated passing suite without a concrete change was used. Prior prerequisite completion/review claims came from the parent and existing notes; they are reported evidence, distinct from the personal source inspection and commands recorded here. The concurrent reviewers' unfinished work was not inspected.

## Deferred boundaries and handoff

C05d owns fresh complete inventory classification and same-identity retry. C06 owns Stop construction/shared extraction, drain proof and termination. Fresh guard authorization, independent callback-entry authorization, actual projected-status/fresh-inventory allocation gates, supervised submit/delivery, provider truth and runtime composition remain explicitly deferred. No caller-supplied boolean, local confirmation or successful Fake test substitutes for them.

Rust implementation and Cargo/build ownership are RELEASED after completed checks, with no process left running. The parent assigns a NEW specification review, then an independent quality review. No rubric completion or aggregate distributed guarantee is claimed here.
