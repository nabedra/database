# DN-C04a3 administration decision codec

Implemented the pure A05 administration-decision codec, including the accepted parent codec reconciliation. Ownership is limited to new `src/storage/control_decision.rs`, `tests/control_decision.rs`, this note, and two module/export lines in `src/storage.rs`. Existing shared-main changes, including C04a2b, are preserved. No dependencies, runtime wiring or Python code changed.

## API and errors

`storage::ControlDecision` exposes the exact A05 fields and `decode(&[u8])`, `validate()` and `encode()`. Body fields remain owned UTF-8 strings, byte for byte. Encoding revalidates constructed candidates. Debug exposes only decision ID/admin sequence. Errors reuse static `StorageError::InvalidCatalog` for invalid shape/semantics and `StorageError::LimitExceeded` for byte excess. Parser details and body contents never enter returned errors or Debug.

`MAX_CONTROL_BODY_BYTES` is independently 1 MiB. `MAX_CONTROL_DECISION_BYTES` is 4 MiB + 64 KiB. Decode checks metadata length before parsing. Each original body is checked by UTF-8 byte length before parsing and parsed directly, preserving duplicate-field evidence. Typed serde objects reject unknown/duplicate fields, sequence encodings and non-u64 integer tokens. UUID strings must match lowercase hyphenated formatting. Terminal code/outcome fields deserialize as strings, avoiding serde's map-shaped unit-enum acceptance.

Both valid original JSON strings may double when quoted into the outer record: quotes/backslashes and permitted whitespace controls need escaping. Thus two 1 MiB bodies need up to 4 MiB plus bounded fixed metadata. The encoder writes through a capped writer rather than serializing an unbounded vector and checking afterward. Fixed-width IDs and integers keep metadata far below the 64 KiB allowance. `validate()` checks the local facts and body limits; those imply the record cap. `encode()` additionally enforces that cap while writing.

Success policy data uses the existing strict `Policy` deserializer directly on original response bytes and then validates its fields through the existing codec. It never sends the raw response or raw policy whitespace through `Policy::decode`'s 4 KiB input budget. Re-encoding the five scalar fields for Policy validation is small and does not replace either retained original string.

## Parent semantic choice

The exact required `parent` field is null precisely when `admin_seq == 1`. Later sequences require a canonical UUID different from the decision ID. Base/published root revisions are positive, with checked `base + 1 == published`. Policy revision is independently positive; identical-policy outcomes may retain a revision while root/admin revisions advance.

Only strict scale/autoscale envelopes enter history. Typed counts outside policy bounds permit only `validation_error`/400. Malformed wire does not become a policy-rejection decision. A success must match the requested fixed target or autoscale bounds and satisfy SDK policy invariants. Errors require correlated non-null UUIDs, string code/message/outcome, `not_applied`, and the exact allowlist/status pairing: validation_error/400, unsupported_capability/400, conflict/409, limit_exceeded/413.

Producing the correct deterministic outcome and sanitized message remains the caller's job. This codec cannot infer capability, authorization, external provisioning mode, publication, replay reachability, or historical evidence. Later publication/history work must validate parent sequence/cluster, interleaved root ordering, loaded-root/head relationships, atomic root/policy/history publication and ambiguous-CAS reconciliation. It must not copy catalog head==change_id rules or derive policy revision from root/admin sequence. No history lookup, root CAS, transitions/routes, status encoder, configuration loading or authority checks are implemented here.

## TDD and verification

Initial red preceded implementation. The focused test command exited 101 with E0432 because `ControlDecision`, `MAX_CONTROL_BODY_BYTES` and `MAX_CONTROL_DECISION_BYTES` were absent. The initial five grouped tests then passed after implementation. Expanded coverage also passed, including scale zero/1024, autoscale success, independent/same-policy revision, all terminal pairs, typed policy bounds rejection, nested duplicates/unknown fields/object-only shapes, canonical identities, integer spelling, mismatched policies/status/outcomes, excluded operation/error classes, required fields, parent/root overflow, constructed encode checks and sanitized diagnostics.

The body-limit fixture stores both original bodies at exactly 1 MiB, including escaped operation spelling, whitespace and escaped response-message quotes. Its outer encoding exceeds 2 MiB; both decoded strings compare exactly. Each body's limit+1 and metadata cap+1 reject with LimitExceeded. A success policy containing more than 4 KiB of internal whitespace remains accepted within its envelope budget.

Commands from the workspace root, using existing `server/target`:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test control_decision
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --test control_decision -- -D warnings
rustfmt --edition 2024 --config skip_children=true --check server/src/storage/control_decision.rs server/tests/control_decision.rs server/src/storage.rs
git diff --check -- server/src/storage.rs server/src/storage/control_decision.rs server/tests/control_decision.rs server/notes/DN-C04a3.md
```

Final focused tests: 5 passed, 0 failed. Targeted strict Clippy: exit 0. Owned rustfmt/whitespace checks: exit 0. No default AWS build, fetch, full suite, acceptance harness or existing decision-test run. Exports alone do not justify rerunning the catalog decision suite. Cargo ownership is released on delivery. Independent spec/quality reviews remain parent follow-ups.

Independent specification review subsequently passed without actionable findings.
It inspected the exact A05/parent contract, raw-body parsing, strict schemas,
terminal validation, independent sequences and all five actual test groups,
including exact near-limit bodies and the larger raw policy whitespace case.
The reviewer reused reported checks without tests, builds, probes or edits.
Independent quality review follows. Publication, reachable history, authorization
and runtime behavior remain outside this codec's evidence.

## Parent fixture correction after quality review

Quality review found two negative fixtures whose rejection could come from an
unrelated check. The malformed autoscale payloads now retain an otherwise valid
matching autoscale success. The self-parent case now starts from a successfully
decoded sequence-two record with a different parent, then changes only that
parent to its own decision ID. The sequence-one/non-null-parent checks remain
separate. No production implementation changed.

Owned rustfmt and whitespace checks passed after these edits. Once R01b2b3a
released Cargo, the parent ran the same focused control_decision command above:
all five groups passed in 2.71 seconds. The targeted library/control_decision
Clippy command then passed in 4.82 seconds with warnings denied. No production
change or new failing-before claim accompanies this test-only correction. A
fresh independent quality re-check remains required before checking the slice.

The fresh independent quality re-check passed. It verified the matching autoscale
response and successfully decoded sequence-two baseline, retained separate
sequence-one coverage, and found no scoped regression. It reused the parent
checks without rerunning tests/builds or changing files. Both independent review
stages are complete; this evidence remains limited to the codec and its tests.
