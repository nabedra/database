# Properties, models and simulation

Property tests and simulation workloads use seeded choices. The dev-only
`nabedra-check` crate in `server/check` provides the property generator,
choice-tape shrinker and history checkers. The explicit-state model checker
enumerates actions and reports a counterexample trace.

| Tool | Entry point | Check kind |
| --- | --- | --- |
| Property test | `nabedra_check::prop::check(name, f)`, with inputs from `nabedra_check::gen::Gen` | U |
| Configuration sampling | `nabedra_check::config::sample(gen, knobs)` | any |
| Explicit-state model | `nabedra_check::model::explore(model, max_states)` | M |
| History checker | `nabedra_check::checker` on a JSON-lines history from `nabedra_check::history` | H |
| Simulation workload | a cluster on one current-thread tokio runtime with paused time, driven by a nemesis | S |

## Run

The property runner reads the following environment variables. Simulation
workloads use the same seed convention and document their own case budgets.

| Variable | Meaning | Default |
| --- | --- | --- |
| `NABEDRA_CASES` | Cases (or seeds) per property or workload | 256 |
| `NABEDRA_SEED` | First seed. When it is unset the crate chooses one, and a failure prints it | unset |

Set `CHECK_ID` to a registered check. The runner uses its check file and
writes an evidence record.

```sh
python3 scripts/testing/run_check.py "$CHECK_ID"
```

To iterate on one test, set `TEST_FILTER` to the test path and run the
check's command directly. For a library check, use
`NABEDRA_CASES=20 cargo test -j 2 --lib "$TEST_FILTER" -- --test-threads=1`
with the check's feature flags.

## Bring up a workload

When one reason makes most seeds red, a sweep says little about anything else, and neither does the next
one. Go in this order:

1. Run about 20 seeds without faults. Settle every reason that is red there first: it is a
   mistake in the harness, a rule that forbids a legal state, or a finding that needs no fault.
2. Add one fault profile at a time.
3. Rank red seeds by the reason they are red for alone, and diagnose the largest group first. A fix
   to a rule cites the text that makes the state legal and keeps its planted bug caught. Report
   seeds red only for open records apart from the rest. They stay red.
4. While fewer than half the seeds are clean, sweep 24 seeds per head. Run the full range on the
   head to merge, or after the largest group is settled.
5. Start the next workload when the largest group of this one has a cause.

## Replay

On the first failure a property panics with the property name, the seed, the case number, the
shrunk tape length, the failure message and a replay line:

```text
replay: NABEDRA_SEED=<seed> NABEDRA_CASES=1
```

Prefix the same test command with the two assignments after `replay:` and
keep the build, workload and other configuration the same. If the seed
fails to reproduce under those conditions, record a determinism bug in
the test or the simulator.

## Shrink

The generator records choices on a tape. After a failure, `prop` tries
shorter tapes and smaller choices for up to `max_shrink_steps`. Keep the
smallest case as a fixture, or write a named regression that replays the seed with
`Config { seed: Some(<seed>), cases: 1, ..Default::default() }` through
`prop::check_with`.

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
with this flag on Actions when the local target directory is shared.

## What simulation evidence means

Simulation evidence is labelled as a fault model. It shows that the code survives the faults the
nemesis injects, under the simulator's model of the network, disk and clock. It never replaces a
real-environment check the register requires. A failing seed is a bug, never a flake. It goes on
the `known-open` list with an owner and an expiry, under the same governance as
`.github/quarantine.tsv`, and its cell stays red.
