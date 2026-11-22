# Testing harness wave 1: common brief (read all of it)

Lead: the orchestrator thread (Claude Opus 5.5). Date 2026-11-22. Repository: zPy52/nabedra-database.
Base revision: `origin/main` at `bdf8d679` or later.

## Goal and deadline

We are implementing the approved testing and review harness plan:
`/Users/antoniopenapena/.claude/plans/your-only-job-is-harmonic-unicorn.md`. Revision 2 is on top and
revision 1 follows it. Read the sections your task names. The plan is approved: its open-question
recommendations are accepted, except the Cache to KV rename, which is NOT part of this wave.

Hard deadline: your PR is open, with its gate run started, by **19:30 Europe/Madrid** (check with
`TZ=Europe/Madrid date`). Everything is merged by 22:00. Favour a working, honest implementation
with the essential tests over breadth. If you must cut scope, cut it, and list the cut items in the PR
under "Not run" or "Deferred". Never fake a passing result.

Implementation first, essential tests second. Each deliverable needs at least one test that fails when
the deliverable is broken, such as a planted bug, a mutated fixture or a known-bad input. "Tests pass"
is not enough.

## Repository rules you must follow

- Read `AGENTS.md`, `.agents/skills/nabedra-change/SKILL.md`, `.agents/skills/file-pr/SKILL.md`,
  `.agents/skills/nabedra-builds/SKILL.md` and `.agents/skills/code-review/references/gh-actions.md`.
- Work in your own worktree. Your branch is `nabedra/<task-id-lowercase>`, for example
  `nabedra/th-check-core`.
- Minimise dependencies. Add no new third-party crate or npm or pip package unless your brief allows it.
  Python scripts use the stdlib only. `ring` stays the only crypto provider. Never add aws-lc or openssl.
- Docs style: plain prose, sentence-case headings, no em dashes, tables where useful. Text in skills and
  AGENTS.md is a generic protocol. Never name current tasks, agents or owners in it.
- Never commit secrets or `.env` contents. Create no AWS resources and no EC2 or EKS. S3 is allowed only
  through the test bucket and prefix rules in the plan. The bucket does not exist yet, so S3 checks
  must report `not-run: s3-unconfigured` when `NABEDRA_TEST_S3_BUCKET` is unset.

## This machine is weak: build rules

The machine has 4 cores, 16 GB of RAM and Docker capped at 8 GB, and about 19 agents share it.

- Server crate (`server/`, workspace members `.`, `kv` and soon `check`): always
  `export CARGO_TARGET_DIR=/Users/antoniopenapena/Documents/ResearchProjects/AI/nabedra-database/server/target`.
  Concurrent cargo runs wait on the lock. That is expected, so do not create a second target dir.
- Events crate (`server/events`, a separate workspace): always
  `export CARGO_TARGET_DIR=/Users/antoniopenapena/Documents/ResearchProjects/AI/nabedra-database/server/events/target`.
- `server/fuzz` (new, excluded workspace): its own target dir under the main checkout's `server/fuzz/target`.
- Locally, run only `cargo check` and `cargo test` filtered to your own modules, with `-j 2`. Never run the
  full OLTP library suite, acceptance or Docker image builds locally. Docker is allowed only for small
  single containers (Valkey, kind with 1 node), and you stop them when you are done.
- On macOS there is no `timeout` command. Use `perl -e 'alarm shift; exec @ARGV' <secs> <cmd>`.

## CI rules

- Nothing runs on push. Opening a PR to `main` starts the base gate, which is heavy, so open your PR
  once your work is ready. Avoid pushing to an open PR more than about twice.
- Dispatches follow gh-actions.md: give each a unique request ID, pin it to a pushed SHA, keep at most one
  in flight per branch, poll at most once a minute, and cancel nothing you did not start.
- A new generic dispatch input `checks` (comma-separated check IDs, see below) is being added by task
  TH-INFRA. Until it is on `main`, validate locally, and rely on the PR gate.
- Do not add workflow files unless you are TH-INFRA.

## Ownership (hot files, wave 1)

Only the owner edits a path. If you need a change in a path you do not own, do not edit it. Put the exact
change (a diff) in your final report under "Changes needed in paths I do not own", and carry on.

| Path | Owner |
| --- | --- |
| `server/check/Cargo.toml`, `server/check/src/lib.rs`, `server/check/src/{rng,gen,prop,config,evidence}.rs` | TH-CHECK-CORE |
| `server/check/src/model.rs`, `server/check/src/model/**`, `server/check/tests/model_*.rs`, `server/check/models/**` | TH-MODELS (adds one `pub mod model;` line to lib.rs) |
| `server/check/src/history/**`, `server/check/src/checker/**`, `server/check/tests/history_*.rs`, `server/check/tests/corpus/**` | TH-HISTORY (adds `pub mod history; pub mod checker;` lines to lib.rs) |
| `server/Cargo.toml`, `server/Cargo.lock` | TH-CHECK-CORE. Exception: TH-KV-CORE adds exactly one line, `test-hooks = ["cache"]`, under `[features]` |
| `server/src/cache/**` | TH-KV-CORE |
| `scripts/cache-acceptance/**` new files, `docs/testing/kv-conformance/**`, `server/tests/data/valkey-commands/**` | TH-KV-DIFF |
| `scripts/cache-acceptance/cluster.py` (existing) | TH-REALENV |
| `server/src/storage.rs`, `server/src/storage/**`, `scripts/local_store.py`, `scripts/test_local_store.py`, `scripts/testing/s3/**` | TH-OLAP-STORE |
| `server/src/engine/**` (the `force_full_scan` switch only), `server/src/scheduling*`, `server/src/request_queue*` (test modules), `server/tests/olap_oracles*.rs` | TH-OLAP-ORACLE |
| `server/tests/olap_logic*.rs`, `server/tests/logic/**`, `orm/python/tests/test_logic_replay.py`, `orm/typescript/test/logic-replay.test.*` | TH-OLAP-LOGIC |
| `server/events/src/**` except `src/http/**`, plus the `[dependencies]` and `[features]` of `server/events/Cargo.toml` | TH-SIM-EVENTS |
| `server/events/tests/**`, test modules inside `server/events/src/http/**`, and the `[dev-dependencies]` of `server/events/Cargo.toml` | TH-EVENTS |
| `server/events/Cargo.lock` | shared: whoever merges second regenerates it |
| `server/fuzz/**` | TH-FUZZ |
| `.github/workflows/**`, `.github/quarantine.tsv` | TH-INFRA |
| `docs/testing/modules.toml`, `docs/testing/evidence/schema.json`, `docs/testing/checks/README.md`, `scripts/testing/{matrix,run_check,evidence,catalog,release_gate}.py` and their tests, `docs/testing/coverage-matrix.*`, `docs/testing/catalog.json` | TH-MATRIX |
| `docs/testing/checks/<id>.toml` | the task that owns that check |
| `docs/testing/guarantees/**` | TH-REGISTERS |
| `docs/testing/bugs/**` | TH-BUGS |
| `scripts/testing/{licenses,pr_lint,repeat_gate,triage,ab}.py` and their tests, `docs/testing/perf/**` | TH-TOOLS |
| `scripts/testing/realenv/**`, `scripts/testing/kind/**` | TH-REALENV |
| `AGENTS.md`, `.agents/**`, `docs/testing/README.md` | TH-REVIEW |
| `orm/**` except TH-OLAP-LOGIC's two files, `orm/api-manifest.json`, `scripts/testing/api_manifest.py` | TH-CONFORMANCE |

Every Python test file lives next to its script as `test_<name>.py` and runs with
`python3 -m unittest`. Put shared `scripts/testing/__init__.py`-style helpers only in your own paths.

## Registering a check (every task that adds a runnable check)

Add one file per check, `docs/testing/checks/<id>.toml`. TH-MATRIX's `scripts/testing/run_check.py <id>`
runs it and writes an evidence record, and TH-INFRA's CI `checks` job runs it on Actions.

```toml
id = "kv-store-model"            # unique kebab-case, equal to the file name
module = "kv"                    # oltp | olap | olap-stream | kv | events | scheduler | orm-python | orm-typescript | lsm | check | testing
kinds = ["U"]                    # M U S H F D R P K (plan, revision 1, "check kinds")
guarantees = ["KV-G02", "KV-G03"]
scenarios = ["K01", "K02"]
tiers = ["T0", "T1"]             # T0 T1 T2 T3 weekly
command = ["cargo", "test", "--no-default-features", "--features", "cache", "--lib", "cache::store::model_tests"]
cwd = "server"                   # relative to the repository root
timeout_s = 900
needs = []                       # any of: docker, kind, s3, rust-nightly, node, python-orm, valkey
result_parser = "cargo-test"     # cargo-test | unittest | node-test | exit-code
env = { NABEDRA_CASES = "1000" } # optional
```

Guarantee IDs use the plan's registers: OLTP-G01 to G13, OLAP-G01 to G10, OLAP-STR-G01 to G06,
EVT-G01 to G14, KV-G01 to G12, ORM-G01 to G03 (API parity, identical results, versions in lockstep), and
SCHED-G01 to G04 (TH-REGISTERS defines them).

## The `nabedra-check` API (pinned: code against it now)

The crate is `server/check`, package `nabedra-check`, lib `nabedra_check`, edition 2024. Its only
dependencies are `serde` and `serde_json`, at the versions `server/Cargo.toml` already pins. It is a
workspace member and a `[dev-dependencies]` entry of the server crate (TH-CHECK-CORE adds both).
Events uses it through a path dev-dependency, `nabedra-check = { path = "../check" }`.

TH-CHECK-CORE pushes a compiling skeleton with these signatures to branch `nabedra/th-check-core`
within 30 minutes of starting. If you need the crate before it merges, run
`git fetch origin nabedra/th-check-core && git merge origin/nabedra/th-check-core`, and rebase on `main`
once it lands. If it is not on `origin` by 18:15, use a private 20-line SplitMix64 in your test module
and note it in the PR.

```rust
pub mod rng {      // deterministic, no OS randomness
    pub struct SplitMix64 { /* state */ }
    impl SplitMix64 { pub fn new(seed: u64) -> Self; pub fn next_u64(&mut self) -> u64; }
}
pub mod gen {      // generator backed by a choice tape (Hypothesis style), so failures shrink
    pub struct Gen { /* tape, rng, position */ }
    impl Gen {
        pub fn from_seed(seed: u64) -> Self;
        pub fn u64(&mut self) -> u64;
        pub fn below(&mut self, n: u64) -> u64;              // uniform in [0, n); n > 0
        pub fn range_i64(&mut self, lo: i64, hi: i64) -> i64; // inclusive
        pub fn bool(&mut self) -> bool;
        pub fn chance(&mut self, num: u32, den: u32) -> bool;
        pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T;
        pub fn bytes(&mut self, max_len: usize) -> Vec<u8>;
        pub fn ascii(&mut self, max_len: usize) -> String;   // printable ASCII
        pub fn vec<T>(&mut self, max_len: usize, f: impl FnMut(&mut Gen) -> T) -> Vec<T>;
        pub fn seed(&self) -> u64;
    }
}
pub mod prop {
    pub struct Config { pub cases: u64, pub seed: Option<u64>, pub max_shrink_steps: u64 }
    impl Default for Config {}  // cases from NABEDRA_CASES (default 256), seed from NABEDRA_SEED
    /// Runs `f` on `cases` generated inputs. On the first failure it shrinks the choice tape and
    /// panics with: property name, seed, case number, shrunk tape length, the failure message, and
    /// "replay: NABEDRA_SEED=<seed> NABEDRA_CASES=1".
    pub fn check(name: &str, f: impl Fn(&mut crate::gen::Gen) -> Result<(), String>);
    pub fn check_with(cfg: Config, name: &str, f: impl Fn(&mut crate::gen::Gen) -> Result<(), String>);
}
pub mod config {   // configuration randomizer (ClickHouse SettingsRandomizer style)
    pub enum Knob { Bool(&'static str), Choice(&'static str, &'static [&'static str]), IntRange(&'static str, i64, i64) }
    pub fn sample(g: &mut crate::gen::Gen, knobs: &[Knob]) -> std::collections::BTreeMap<String, serde_json::Value>;
}
pub mod evidence { // writes one "nabedra.evidence/1" JSON record (schema: docs/testing/evidence/schema.json)
    pub struct Record { /* fields from the plan, revision 1, "Evidence and tracking" */ }
    impl Record { pub fn new(check: &str, kind: &str) -> Self; pub fn write_to(&self, dir: &std::path::Path) -> std::io::Result<std::path::PathBuf>; }
}
pub mod model {    // explicit-state model checker (TH-MODELS)
    pub trait Model {
        type State: Clone + Eq + std::hash::Hash + std::fmt::Debug;
        type Action: Clone + std::fmt::Debug;
        fn init_states(&self) -> Vec<Self::State>;
        fn actions(&self, state: &Self::State, out: &mut Vec<Self::Action>);
        fn next_state(&self, state: &Self::State, action: &Self::Action) -> Option<Self::State>;
        /// Err(name and detail of the first violated invariant).
        fn check(&self, state: &Self::State) -> Result<(), String>;
    }
    pub struct Explored { pub states: usize, pub max_depth: usize }
    pub enum Outcome<M: Model> {
        Passed(Explored),
        Violated { invariant: String, trace: Vec<(Option<M::Action>, M::State)> }, // shortest (BFS)
        LimitReached(Explored),
    }
    pub fn explore<M: Model>(model: &M, max_states: usize) -> Outcome<M>;
}
pub mod history {  // JSON lines, one event per line (TH-HISTORY)
    // {"index":0,"type":"invoke","process":3,"f":"write","key":"k1","value":5,
    //  "request_id":"r-1","node":"n1","time_ns":123}
    // type: invoke | ok | fail | info   (info = unknown outcome, Jepsen's term)
    // f: read | write | cas | delete | txn | publish | poll | ack | incr | get | set ...
    // txn value: [["r","x",null],["w","y",3]] (Jepsen micro-ops)
    pub struct Event { /* serde fields as above, value: serde_json::Value */ }
    pub fn read_jsonl(r: impl std::io::BufRead) -> std::io::Result<Vec<Event>>;
    pub fn write_jsonl(w: impl std::io::Write, events: &[Event]) -> std::io::Result<()>;
}
pub mod checker {  // TH-HISTORY
    pub struct Verdict { pub valid: bool, pub anomalies: Vec<Anomaly>, pub checked_ops: usize }
    pub struct Anomaly { pub kind: String, pub detail: String, pub events: Vec<usize> }
    // linearizable::check_kv(&[Event]) (per-key registers, Wing-Gong with P-compositionality),
    // txn::check(&[Event]) (Elle-style: G0 G1a G1b G1c G-single G2), outcome::check (failed-write-observed),
    // queue::check (offsets, order, loss, redelivery), relaxed_kv::check (no invented values, no resurrection,
    // per-epoch order), edn::export (Jepsen EDN for Elle)
}
```

Real-environment Python code writes the same JSON-lines history format (TH-REALENV owns the Python writer).

## Finishing

1. Commit in logical steps. Messages look like `[testing] <what and why>`, ending with the trailer
   `Co-Authored-By: <your model name> <noreply@anthropic.com or noreply@openai.com>`.
2. Rebase on `origin/main` right before opening the PR.
3. Open a real PR (not a draft) to `main` following file-pr. The PR body must contain these four lines,
   which TH-TOOLS's PR linter will check:
   - `Guarantees: <IDs or none>`
   - `Evidence: <commands run and their raw result lines, CI links>`
   - `Not run: <every check you did not execute, and why>`
   - `Reviewers: pending cross-model review`

   End the body with `Created with <your model> in T3 Code.`
4. **Do not merge.** The lead runs a cross-model review and merges. Do not watch the PR.
5. Your final message is your report:
   - Status
   - Branch, PR URL, head SHA
   - What was implemented
   - What was cut
   - Commands run, with raw results
   - Planted bugs and whether they were caught
   - Changes needed in paths I do not own
   - Concerns

   The last line is exactly `PR <full URL>, head <SHA>, checks and reviews <state>.`

## Amendments (read these too)

- **Python.** Locally, `python3` is 3.6.8. Run every script and test with `python3.11` (at
  `/usr/local/opt/python@3.11/bin/python3.11`). New scripts need Python 3.11 or later, because they use
  `tomllib`, and start with `#!/usr/bin/env python3`. CI uses 3.12.
- **More ownership.**
  - TH-INFRA also owns `.agents/skills/code-review/references/gh-actions.md`, where it documents the
    new inputs and jobs, plus `scripts/check_engine_features.py` and `scripts/testing/ci/**`.
  - TH-BUGS owns `scripts/testing/bugs.py` and its test.
  - TH-FUZZ may make visibility-only one-line edits (`pub(crate)` to `#[doc(hidden)] pub`) on decoder
    functions in paths it does not own, and must list each one in its report.
- **Register format** (TH-REGISTERS writes it and TH-MATRIX parses it). Each
  `docs/testing/guarantees/<module>.md` has:
  - a line `Module: <module>`;
  - a line `Level: L1|L2|L3` (a module with mixed levels uses its highest and states the split in prose);
  - exactly one table whose header is `| ID | Guarantee | Failure model | Scenarios | Checks | n/a |`.

  In that table:
  - Checks is a space-separated list of kind letters, such as `M S H R`.
  - n/a holds `<kind>: <reason>` entries separated by `;`, or is empty.
  - A cell that is cloud-deferred reads `R: cloud-deferred (<what>)` in n/a, and keeps R in Checks.

**Never end your turn while waiting on a background command.** Nothing reliably wakes you. Run builds, tests and `gh run watch` in the foreground with a perl alarm timeout, then keep going until your final report.
