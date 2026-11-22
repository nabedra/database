# nabedra-check

Nabedra's in-tree test harness. It is a workspace member and a dev-dependency only, so no product
build links it. Its only dependencies are `serde` and `serde_json`.

Test it alone with `cargo test -p nabedra-check` from `server/`.

## Modules

| Module | Contents |
| --- | --- |
| `rng` | `SplitMix64`, the same stream as kv's test `Rng` |
| `gen` | `Gen`, a generator backed by a choice tape. Also re-exported as `nabedra_check::Gen`. |
| `prop` | `check` and `check_with`, the property runner, and `shrink`, the tape shrinker |
| `config` | `Knob` and `sample`, the configuration randomizer, plus `describe` for log lines |
| `evidence` | `Record`, one `nabedra.evidence/1` JSON record |

In edition 2024 `gen` is a reserved word. Import `nabedra_check::Gen` directly or use
`nabedra_check::generator::Gen`. The raw module path `nabedra_check::r#gen` works in every edition.

## Generators and the choice tape

Every draw records one `u64` choice. `Gen::from_seed(seed)` draws choices from `SplitMix64::new(seed)`.
`Gen::from_tape(tape)` replays them, and once the tape is exhausted every draw returns its minimal
value: 0, `false`, `lo` for `range_i64`, the first item for `pick`, and empty for `vec`, `bytes` and
`ascii`. `Gen::tape()` returns the choices consumed so far, and replaying it reproduces the draws.

Draws are laid out so that a smaller choice means a simpler value:

- `below(n)` stores the value itself. A replayed choice `>= n` is reduced modulo `n`.
- `chance(num, den)` is true when its choice is at least `den - num`, so it shrinks to false.
- `vec`, `bytes` and `ascii` draw the length first, then each element.
- `ascii` characters shrink toward `a`, then the other letters, digits, space and punctuation.

## Property runner

```rust
use nabedra_check::{Gen, prop};

#[test]
fn sorted_output_is_ordered() {
    prop::check("sort-ordered", |g: &mut Gen| {
        let mut v = g.vec(50, |g| g.range_i64(-100, 100));
        v.sort();
        if v.windows(2).all(|w| w[0] <= w[1]) { Ok(()) } else { Err(format!("{v:?}")) }
    });
}
```

`prop::Config::default()` reads `NABEDRA_CASES` (default 256) and `NABEDRA_SEED` (decimal or `0x`
hex; an unparsable value panics). Case 0 runs with the base seed itself, and case `i` with a seed
derived from it. Without `NABEDRA_SEED` the base seed is a fixed default mixed with the property
name, so runs are reproducible and each property gets its own stream.

A property fails when it returns `Err` or panics. Panics are caught with `catch_unwind`, and while
the property runs the panic hook records the text instead of printing it. The runner shrinks the
failing tape and panics with a report:

```text
property `seven-then-three` failed
  seed: 1234 (0x4d2)
  case: 17 of 1000
  shrunk tape length: 3 (from 25, 311 shrink runs): [2, 7, 3]
  failure: [7, 3]
  replay: NABEDRA_SEED=1234 NABEDRA_CASES=1
```

Running the test again with the replay line's environment reproduces the same failure as case 0.

## Shrinker

`prop::shrink(tape, message, max_steps, &f)` reruns the property on mutated tapes. It keeps a
mutation only if the property still fails and the consumed tape is simpler in shortlex order
(shorter, or equally long and lexicographically smaller), so it always terminates. The passes,
repeated until none makes progress or `max_steps` property runs are spent:

1. Delete chunks of 8, 4, 2 and 1 choices, ddmin style. A failed deletion is retried with the
   choice before the chunk decremented, which drops one `vec` element together with its length.
2. Per choice: replace it with 0; halve it while the property still fails; then decrement it, by
   binary search between the passing half and the failing value, then by one.
3. Swap adjacent choices that are out of order.
4. Move a choice's whole value into one of the next 8 choices, so a total spread across elements
   collects in one and the emptied ones can be deleted.

Simulation harnesses can call `prop::shrink` and `prop::run` directly with their own tapes.
`prop::check` and `check_with` retain the original case seed while shrinking.

## Configuration randomizer

```rust
use nabedra_check::config::{self, Knob};
const KNOBS: &[Knob] = &[
    Knob::Bool("direct_oltp"),
    Knob::Choice("eviction", &["lru", "lfu"]),
    Knob::IntRange("workers", 1, 8),
];
let mode = config::sample(g, KNOBS); // BTreeMap<String, serde_json::Value>
```

Sampling draws from the same `Gen`, so a configuration replays with its seed and shrinks to
`false`, the first choice and `lo`. Store the result in `Record::mode`.

## Evidence record fields

`evidence::Record` matches `docs/testing/evidence/schema.json` (`nabedra.evidence/1`). Every field is
required. An absent value for an `Option` field is represented by an explicit `null`. Unknown
fields fail to deserialize.
`Record::new(check, kind)` dates the record now, sets `run.request_id` to `local`, and starts with
`result` set to `"error"`, so a record whose runner never set a result cannot read as a pass.

`validate()` checks the schema and the matrix's assertion rules. A pass requires at least one
executed assertion and zero failures. A not-run record requires a reason. `write_to(dir)` validates
the record, enforces a 20 KiB (20,480 byte) cap, and writes
`<dir>/<yyyy>/<mm>/<request_id>--<check>.json`, using the year and month from `run.date`. Point `dir`
at `docs/testing/evidence/records`.

| Field | JSON type | Meaning |
| --- | --- | --- |
| `schema` | string | Always `"nabedra.evidence/1"` |
| `check` | string | Kebab-case check ID, at most 80 characters, equal to its `docs/testing/checks/<id>.toml` name |
| `kind` | string | One or more distinct letters from `M U S H F D R P K`, separated by single spaces, such as `"U S"` |
| `guarantees` | array of string | Guarantee IDs, such as `"KV-G02"` or `"OLAP-STR-G01"` |
| `scenarios` | array of string | Scenario IDs, such as `"O08"` |
| `sha` | string | 40-character lowercase commit SHA |
| `tree_clean` | boolean | Whether the working tree had no uncommitted changes |
| `build` | object | `profile` (string or null), `features` (array of string), `binary_sha256` (64 lowercase hex or null) |
| `mode` | object or null | Run mode and sampled knobs, string keys to scalar values, such as `{"direct_oltp": true}` |
| `run` | object | `url` (`https://` string or null), `attempt` (integer from 1, or null), `request_id` (string, `^[A-Za-z0-9][A-Za-z0-9._-]*$`), `runner` (string or null), `date` (RFC 3339 UTC) |
| `duration_s` | number | Wall-clock seconds, at least 0 |
| `result` | string | `"pass"`, `"fail"`, `"not-run"`, `"quarantined"` or `"error"` |
| `assertions` | object | `executed` (integer), `failed` (integer), `not_run` (at most 200 `{"id": string, "reason": string}`) |
| `seeds` | object or null | `range` (`[lo, hi]` inclusive, or null), `count` (integer), `failing` (at most 100 integers) |
| `sim_core_hours` | number or null | Simulation CPU core-hours spent |
| `virtual_hours` | number or null | Simulated time covered, in hours |
| `artifacts` | array of object | At most 50 `{"name": string, "sha256": 64 lowercase hex}` |

Seeds are full 64-bit unsigned integers. Readers must not round them through a float.

`fill_from_env()` uses `TESTED_SHA`, `git rev-parse HEAD`, then `GITHUB_SHA` for `sha`. It reads
run metadata from GitHub Actions, `tree_clean` from `git status`, and `request_id` from
`NABEDRA_REQUEST_ID`. Use `assertion(passed)` and `not_run(id, reason)` to account for assertions.
`conclude()` sets `result` to `fail`, `pass` or `not-run` from the counts.
