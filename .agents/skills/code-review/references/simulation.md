# Properties, models and simulation

Property tests, model checks and simulation workloads all draw their choices from one seeded
generator in `server/check` (the `nabedra-check` crate, a dev-only dependency). A failure is
therefore a seed that replays exactly and shrinks to a small case.

| Tool | Entry point | Check kind |
| --- | --- | --- |
| Property test | `nabedra_check::prop::check(name, f)`, with inputs from `nabedra_check::gen::Gen` | U |
| Configuration sampling | `nabedra_check::config::sample(gen, knobs)` | any |
| Explicit-state model | `nabedra_check::model::explore(model, max_states)` | M |
| History checker | `nabedra_check::checker` on a JSON-lines history from `nabedra_check::history` | H |
| Simulation workload | a cluster on one current-thread tokio runtime with paused time, driven by a nemesis | S |

## Run

Two environment variables control every run:

| Variable | Meaning | Default |
| --- | --- | --- |
| `NABEDRA_CASES` | Cases (or seeds) per property or workload | 256 |
| `NABEDRA_SEED` | First seed. When it is unset the crate chooses one, and a failure prints it | unset |

Run a registered check through its check file, so the run writes an evidence record:

```sh
python3 scripts/testing/run_check.py <check id>
```

To iterate on one test, run the check's command directly with a filter, for example
`NABEDRA_CASES=20 cargo test -j 2 --lib <module path> -- --test-threads=1`.

## Replay

On the first failure a property panics with the property name, the seed, the case number, the
shrunk tape length, the failure message and a replay line:

```text
replay: NABEDRA_SEED=<seed> NABEDRA_CASES=1
```

Prefix the same test command with that line. The run is deterministic, so it fails the same way.
A seed that does not reproduce on replay is a determinism bug in the test or the simulator. Record
it as a bug. Never rerun it to get a pass.

## Shrink

The generator records every choice on a tape. After a failure, `prop` shrinks the tape (shorter,
then smaller choices) for up to `max_shrink_steps` and reports the smallest failing case. Report
the shrunk case, not the first one. Keep the failure as a regression: a named test that replays the
seed with `Config { seed: Some(<seed>), cases: 1, ..Default::default() }` through `prop::check_with`.

## Determinism

A workload is deterministic only when every source of choice comes from the seed:

- no OS randomness: identifiers and random choices come from `nabedra_check::rng::SplitMix64` or
  the node's seeded generator;
- no wall clock: time is passed as an argument or read from the simulated clock;
- no real sleeps: wait on a notification, or advance paused time;
- ordered maps (`BTreeMap`) wherever iteration order reaches behavior;
- sampled configuration knobs are recorded in the evidence record.

T1 runs each workload twice per seed and compares the trace hashes. A mismatch fails the check.

## Local limits

Local simulation runs one thread per agent: `--test-threads=1`, and `-j 2` for the build. Locally
run at most the T0 budget, about 20 seeds of the touched workload, in under 60 s. Large seed counts
run on Actions through the `checks` dispatch input (see [evidence](evidence.md)).

## `tokio_unstable`

A simulation build sets `--cfg tokio_unstable` so that `Builder::rng_seed` makes `select!` choices
follow the seed. Only simulation builds set it. Product builds, Docker images and releases never
do, and the O02 and NS26 checks assert that no simulation feature or dev-only crate reaches a
product build. Changing `RUSTFLAGS` rebuilds the whole target directory, so run simulation builds
with this flag on Actions, not in a shared local target directory.

## What simulation evidence means

Simulation evidence is labelled as a fault model. It shows that the code survives the faults the
nemesis injects, under the simulator's model of the network, disk and clock. It never replaces a
real-environment check the register requires. A failing seed is a bug, never a flake. It goes on
the `known-open` list with an owner and an expiry, under the same governance as
`.github/quarantine.tsv`, and its cell stays red.
