# DN-R04c3 incomplete implementation attempt

DN-R04c3 / adopted DN-A22 mini 3 is NOT implemented. This attempt did not
establish a genuine external interface or ownership blocker. It failed to
complete the registered publication lifecycle, generation integration and
required causal test matrix. No specification or quality review is requested.

All edits to the five existing owned Rust files were restored byte-for-byte
from repository-relative pre-edit snapshots at:

`/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/meshdb-r04c3-orn4zdma/`

These snapshots contain admission.rs, payload_owner.rs, mod.rs,
payload_owner/installation_tests.rs and payload_owner/tests.rs under
server/src/node. The incomplete new drain_observation.rs was removed. Only
this note remains as a new repository artifact from this attempt. Shared
modified/untracked source and Cargo files were preserved.

Applied TDD and unslop instructions, without delegation. No branch/worktree,
commit/push, dependency edit, download, new target, target cleanup, network/cloud
operation or secret/profile/.env access occurred. Builds used server/target.

## Observed evidence, before restoration

Every Cargo command used:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib drain_proof_exact_observer_baseline -- --nocapture
```

Three invocations ran sequentially; each process exited before the next started.

1. Exit 101. A new private composition test using signed_booted, a real accepted
   signed Drain control and fixture teardown failed to compile because
   PayloadRegistry::drain_work_locked did not exist. This is missing-API compile
   red, not an observed behavioral assertion failure.
2. Exit 0. The same test passed after adding a classifier. Build 51.74 seconds;
   one test passed in 0.02 seconds. This proves only that temporary baseline.
3. Exit 101. The subsequent incomplete publication integration failed compilation
   because work_generation was duplicated in one registry initializer and
   missing in another. These initializer errors were corrected locally, but no
   subsequent build verified the correction before restoration.

The temporary classifier, test and publication integration have all been
removed. The passing temporary test is not evidence for the restored source.
No affected-suite regression, strict Clippy or final implementation verification
was completed. Scoped edition-2024 rustfmt ran on the temporary edits before
restoration. A direct byte comparison after restoration confirmed all five
existing owned files match their snapshots exactly.

## Work still required

The entire adopted mini remains open. In particular, a future implementation
must retain the sole writer and storage through supervisor panic, cancellation,
uncertain provider work and reaping; count all required outside-lock transitions;
implement checked work generation separately from allocation generation; sample
without allocation under the receiver lock; bind receipt consumption to the
exact live observer and reaped cycle; integrate whole-process ownership snapshots
and shutdown; and complete the requested signed/Fake causal matrix with bounded
teardown. The discarded draft did not establish those guarantees.

There are no final private APIs or state transitions delivered by this attempt.
No aggregate R04/R03a3, runtime startup, positive distributed idle, termination,
provider truth or HTTP correctness is claimed. Contracts and rubric are unchanged.

All three owned Cargo processes have exited. No test task or WorkerPool from the
passing temporary test remains; that test completed its existing teardown fixture.
Rust implementation and Cargo/build ownership are explicitly RELEASED.
