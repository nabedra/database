//! The seed sweep and the planted-bug search.
//!
//! A harness exposes one run as `Fn(seed, &mut Gen) -> SeedRun`. [`sweep`] runs a seed
//! range on `NABEDRA_SIM_JOBS` threads, one seed at a time per thread (the `World` is a
//! thread-local), and:
//!
//! - runs the first `NABEDRA_SIM_VERIFY_REPLAY` fraction of the range twice, and every
//!   failing seed twice. A different trace hash, event count or history is a determinism
//!   bug, reported apart from violations and never retried;
//! - writes failure artifacts for each failing or nondeterministic seed under
//!   `NABEDRA_SIM_ARTIFACTS` (default `<temp>/nabedra-sim`): `summary.json`, `history.jsonl`,
//!   `trace-tail.txt`, `trace.txt` with `NABEDRA_SIM_TRACE=full`, and with
//!   `NABEDRA_SIM_SHRINK=1` the shrunk `<seed>.tape`;
//! - prints a replay command for each failure, and returns a [`SweepReport`] whose result
//!   does not depend on the thread count.
//!
//! [`PlantedSearch`] runs seeds from a fixed start until a violation names one of the
//! expected invariants, and reports the lowest such seed, also independent of the thread
//! count.

use std::collections::BTreeMap;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use nabedra_check::Gen;
use nabedra_check::evidence::Seeds;
use serde_json::{Value, json};

use crate::rng::stream_seed;
use crate::shrink::{self, DEFAULT_SHRINK_BUDGET};

/// What one run of one seed produced.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SeedRun {
    pub seed: u64,
    pub trace_hash: u64,
    pub trace_events: u64,
    pub violations: Vec<String>,
    pub trace_tail: Vec<String>,
    /// The full trace, when the run kept it (`NABEDRA_SIM_TRACE=full`).
    pub full_trace: Option<Vec<String>>,
    /// The client history as JSON lines, compared byte for byte by the double run.
    pub history: Vec<u8>,
    /// The sampled configuration, written into the failure summary.
    pub config: Value,
    pub virtual_ms: u64,
    /// The choices the run drew from its generator.
    pub tape: Vec<u64>,
}

impl SeedRun {
    pub fn passed(&self) -> bool {
        self.violations.is_empty()
    }

    /// What two runs of one seed must agree on.
    fn agrees_with(&self, replay: &Self) -> bool {
        self.trace_hash == replay.trace_hash
            && self.trace_events == replay.trace_events
            && self.history == replay.history
            && self.violations == replay.violations
    }
}

/// The generator of a seed's workload and nemesis choices: its own stream of the seed.
pub fn workload_gen(seed: u64) -> Gen {
    Gen::from_seed(stream_seed(seed, "workload"))
}

fn env_var(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

/// An unsigned integer from the environment (decimal or `0x` hex), or `default`. Panics on
/// a value that does not parse, so a typo never runs other seeds.
pub fn env_u64(name: &str, default: u64) -> u64 {
    match env_var(name) {
        Some(value) => nabedra_check::prop::parse_u64(&value)
            .unwrap_or_else(|| panic!("{name} must be an unsigned integer, got {value:?}")),
        None => default,
    }
}

fn env_flag(name: &str) -> bool {
    env_var(name).is_some_and(|value| !matches!(value.trim(), "0" | "false" | "no"))
}

/// The fraction of seeds a sweep runs twice: `NABEDRA_SIM_VERIFY_REPLAY`, from 0 to 1.
fn env_fraction(name: &str) -> f64 {
    let Some(value) = env_var(name) else {
        return 0.0;
    };
    match value.trim().parse::<f64>() {
        Ok(fraction) if (0.0..=1.0).contains(&fraction) => fraction,
        _ => panic!("{name} must be a fraction from 0 to 1, got {value:?}"),
    }
}

/// How a sweep runs.
#[derive(Clone, Debug)]
pub struct SweepConfig {
    /// A short name for the harness or workload, used in artifact names.
    pub label: String,
    pub first: u64,
    pub count: u64,
    pub jobs: usize,
    /// The fraction of the range, from its start, that runs twice.
    pub verify_replay: f64,
    pub artifacts: PathBuf,
    /// The command a `NABEDRA_SEED=<s> NABEDRA_SEEDS=1` prefix makes replay one seed.
    pub replay: String,
    /// Refuse to run when tokio's rng is not seeded (no `--cfg tokio_unstable`).
    pub require_seeded: bool,
    /// Shrink each failing seed's tape.
    pub shrink: bool,
    pub shrink_budget: u64,
    /// Replay this tape for every seed instead of drawing a fresh one.
    pub tape: Option<Vec<u64>>,
}

impl SweepConfig {
    /// A sweep of seeds `first..first + count` on one thread, without replays, that runs
    /// seeded or not. For tests of the driver; harnesses use [`SweepConfig::from_env`].
    pub fn new(label: &str, first: u64, count: u64) -> Self {
        Self {
            label: label.to_owned(),
            first,
            count,
            jobs: 1,
            verify_replay: 0.0,
            artifacts: std::env::temp_dir().join("nabedra-sim"),
            replay: String::new(),
            require_seeded: false,
            shrink: false,
            shrink_budget: DEFAULT_SHRINK_BUDGET,
            tape: None,
        }
    }

    /// The sweep the environment asks for: `NABEDRA_SEED` (default 1), `NABEDRA_SEEDS`
    /// (default 20), `NABEDRA_SIM_JOBS` (default 1), `NABEDRA_SIM_VERIFY_REPLAY` (default
    /// 0), `NABEDRA_SIM_ARTIFACTS`, `NABEDRA_SIM_SHRINK` and `NABEDRA_SIM_TAPE`. It
    /// requires a seeded runtime.
    ///
    /// # Panics
    ///
    /// On a malformed variable, or a tape file that cannot be read.
    pub fn from_env(label: &str, replay: &str) -> Self {
        let mut config = Self::new(
            label,
            env_u64("NABEDRA_SEED", 1),
            env_u64("NABEDRA_SEEDS", 20),
        );
        config.jobs = env_u64("NABEDRA_SIM_JOBS", 1).max(1) as usize;
        config.verify_replay = env_fraction("NABEDRA_SIM_VERIFY_REPLAY");
        if let Some(dir) = env_var("NABEDRA_SIM_ARTIFACTS") {
            config.artifacts = PathBuf::from(dir);
        }
        config.replay = replay.to_owned();
        config.require_seeded = true;
        config.shrink = env_flag("NABEDRA_SIM_SHRINK");
        config.tape = env_var("NABEDRA_SIM_TAPE").map(|path| {
            shrink::read_tape(Path::new(&path))
                .unwrap_or_else(|error| panic!("read NABEDRA_SIM_TAPE {path}: {error}"))
        });
        config
    }

    /// Seeds, counted from the start of the range, that run twice.
    pub fn replayed(&self) -> u64 {
        (self.count as f64 * self.verify_replay).ceil() as u64
    }

    fn last(&self) -> u64 {
        assert!(self.count > 0, "a sweep needs at least one seed");
        self.first
            .checked_add(self.count - 1)
            .expect("seed range overflows u64")
    }

    fn generator(&self, seed: u64) -> Gen {
        match &self.tape {
            Some(tape) => Gen::from_tape(tape.clone()),
            None => workload_gen(seed),
        }
    }

    fn replay_line(&self, seed: u64) -> String {
        format!("NABEDRA_SEED={seed} NABEDRA_SEEDS=1 {}", self.replay)
            .trim_end()
            .to_owned()
    }
}

/// One seed's result in a sweep.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeedSummary {
    pub seed: u64,
    pub trace_hash: u64,
    pub trace_events: u64,
    pub virtual_ms: u64,
    pub replayed: bool,
    pub passed: bool,
    pub deterministic: bool,
}

/// The result of a sweep, ordered by seed.
#[derive(Clone, Debug)]
pub struct SweepReport {
    pub label: String,
    pub first: u64,
    pub count: u64,
    pub seeds: Vec<SeedSummary>,
    /// One report per failing or nondeterministic seed, in seed order.
    pub failures: Vec<String>,
    pub artifacts: Vec<PathBuf>,
}

impl SweepReport {
    /// Seeds with a violation.
    pub fn failing(&self) -> Vec<u64> {
        self.seeds
            .iter()
            .filter(|seed| !seed.passed)
            .map(|seed| seed.seed)
            .collect()
    }

    /// Seeds whose two runs differed.
    pub fn nondeterministic(&self) -> Vec<u64> {
        self.seeds
            .iter()
            .filter(|seed| !seed.deterministic)
            .map(|seed| seed.seed)
            .collect()
    }

    pub fn replayed(&self) -> u64 {
        self.seeds.iter().filter(|seed| seed.replayed).count() as u64
    }

    pub fn virtual_ms(&self) -> u64 {
        self.seeds.iter().map(|seed| seed.virtual_ms).sum()
    }

    pub fn passed(&self) -> bool {
        self.failing().is_empty() && self.nondeterministic().is_empty()
    }

    /// The `seeds` block of an evidence record.
    pub fn evidence_seeds(&self) -> Seeds {
        Seeds {
            range: Some([self.first, self.first + (self.count - 1)]),
            count: self.count,
            failing: self
                .seeds
                .iter()
                .filter(|seed| !seed.passed || !seed.deterministic)
                .map(|seed| seed.seed)
                .collect(),
        }
    }

    /// One line for the log.
    pub fn summary(&self) -> String {
        format!(
            "{} sim: {} seeds from {}: {} failed, {} nondeterministic, {} replayed, {} virtual s",
            self.label,
            self.count,
            self.first,
            self.failing().len(),
            self.nondeterministic().len(),
            self.replayed(),
            self.virtual_ms() / 1000,
        )
    }

    /// Panics with every failure report unless the sweep passed.
    pub fn assert_passed(&self) {
        assert!(
            self.passed(),
            "{}\nfailing seeds: {:?}\nnondeterministic seeds: {:?}\n\n{}",
            self.summary(),
            self.failing(),
            self.nondeterministic(),
            self.failures.join("\n\n")
        );
    }
}

/// Runs `run` once on `g`, turning a panic into a violation. The choices consumed stay
/// on `g`, which the shrinker reads back.
fn guarded(run: &(impl Fn(u64, &mut Gen) -> SeedRun + Sync), seed: u64, g: &mut Gen) -> SeedRun {
    let mut result =
        panic::catch_unwind(AssertUnwindSafe(|| run(seed, g))).unwrap_or_else(|payload| SeedRun {
            seed,
            violations: vec![format!("harness: panic: {}", panic_text(&*payload))],
            ..SeedRun::default()
        });
    result.seed = seed;
    if result.tape.is_empty() {
        result.tape = g.tape().to_vec();
    }
    result
}

fn panic_text(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|text| (*text).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".into())
}

/// Calls `f` for every item of `items` on up to `jobs` threads; results keep item order.
pub fn parallel_map<T: Sync, R: Send>(
    items: &[T],
    jobs: usize,
    f: impl Fn(&T) -> R + Sync,
) -> Vec<R> {
    let next = AtomicU64::new(0);
    let results: Mutex<BTreeMap<usize, R>> = Mutex::new(BTreeMap::new());
    let worker = || {
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed) as usize;
            let Some(item) = items.get(i) else {
                return;
            };
            let result = f(item);
            results
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(i, result);
        }
    };
    let jobs = jobs.clamp(1, items.len().max(1));
    if jobs == 1 {
        worker();
    } else {
        // Scoped worker threads that end with the sweep: no service thread is involved.
        std::thread::scope(|scope| {
            for _ in 0..jobs {
                scope.spawn(worker);
            }
        });
    }
    results
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .into_values()
        .collect()
}

struct Outcome {
    summary: SeedSummary,
    failure: Option<(String, Vec<PathBuf>)>,
}

/// Runs every seed of `config` and returns the report. Never retries a failure.
///
/// # Panics
///
/// When `config.require_seeded` is set and the build is not `--cfg tokio_unstable`.
pub fn sweep(config: &SweepConfig, run: impl Fn(u64, &mut Gen) -> SeedRun + Sync) -> SweepReport {
    assert!(
        !config.require_seeded || crate::runtime::SEEDED,
        "the {} sweep needs a seeded runtime: build with RUSTFLAGS=\"--cfg tokio_unstable\" and CARGO_TARGET_DIR=target/sim",
        config.label
    );
    let seeds: Vec<u64> = (config.first..=config.last()).collect();
    let replayed = config.replayed();
    let outcomes = parallel_map(&seeds, config.jobs, |&seed| {
        let first = guarded(&run, seed, &mut config.generator(seed));
        let twice = seed - config.first < replayed || !first.passed();
        let second = twice.then(|| guarded(&run, seed, &mut config.generator(seed)));
        let deterministic = second
            .as_ref()
            .is_none_or(|second| first.agrees_with(second));
        let summary = SeedSummary {
            seed,
            trace_hash: first.trace_hash,
            trace_events: first.trace_events,
            virtual_ms: first.virtual_ms,
            replayed: twice,
            passed: first.passed(),
            deterministic,
        };
        let failure = (!summary.passed || !deterministic)
            .then(|| report_failure(config, &run, &first, second.as_ref(), deterministic));
        Outcome { summary, failure }
    });
    let mut report = SweepReport {
        label: config.label.clone(),
        first: config.first,
        count: config.count,
        seeds: Vec::with_capacity(outcomes.len()),
        failures: Vec::new(),
        artifacts: Vec::new(),
    };
    for outcome in outcomes {
        report.seeds.push(outcome.summary);
        if let Some((text, paths)) = outcome.failure {
            report.failures.push(text);
            report.artifacts.extend(paths);
        }
    }
    eprintln!("{}", report.summary());
    report
}

/// Writes one seed's artifacts and returns its report and the files written.
fn report_failure(
    config: &SweepConfig,
    run: &(impl Fn(u64, &mut Gen) -> SeedRun + Sync),
    first: &SeedRun,
    second: Option<&SeedRun>,
    deterministic: bool,
) -> (String, Vec<PathBuf>) {
    let seed = first.seed;
    let dir = config
        .artifacts
        .join(format!("{}-seed-{seed}", config.label));
    let mut written = Vec::new();
    let mut notes = Vec::new();
    let shrunk = (config.shrink && !first.passed() && deterministic).then(|| {
        let kinds = shrink::kinds(&first.violations);
        shrink::shrink_tape(first.tape.clone(), &kinds, config.shrink_budget, |g| {
            guarded(run, seed, g).violations
        })
    });
    let mut write = |name: &str, bytes: &[u8]| {
        let path = dir.join(name);
        match std::fs::create_dir_all(&dir).and_then(|()| std::fs::write(&path, bytes)) {
            Ok(()) => written.push(path),
            Err(error) => notes.push(format!("could not write {}: {error}", path.display())),
        }
    };
    write("history.jsonl", &first.history);
    write("trace-tail.txt", first.trace_tail.join("\n").as_bytes());
    if let Some(full) = &first.full_trace {
        write("trace.txt", full.join("\n").as_bytes());
    }
    if let (Some(second), false) = (second, deterministic) {
        write(
            "trace-tail-replay.txt",
            second.trace_tail.join("\n").as_bytes(),
        );
        write("history-replay.jsonl", &second.history);
    }
    if let Some(shrunk) = &shrunk {
        write(
            &format!("{seed}.tape"),
            &serde_json::to_vec(&shrunk.tape).unwrap_or_default(),
        );
    }
    let summary = json!({
        "label": config.label,
        "seed": seed,
        "passed": first.passed(),
        "deterministic": deterministic,
        "violations": first.violations,
        "trace_hash": format!("{:016x}", first.trace_hash),
        "trace_events": first.trace_events,
        "replay_trace_hash": second.map(|second| format!("{:016x}", second.trace_hash)),
        "replay_trace_events": second.map(|second| second.trace_events),
        "replay_violations": second.map(|second| &second.violations),
        "virtual_ms": first.virtual_ms,
        "config": first.config,
        "tape_len": first.tape.len(),
        "shrunk_tape_len": shrunk.as_ref().map(|shrunk| shrunk.tape.len()),
        "replay": config.replay_line(seed),
    });
    write(
        "summary.json",
        &serde_json::to_vec_pretty(&summary).unwrap_or_default(),
    );
    let mut text = format!(
        "seed {seed} {} ({})\n  violations:\n    {}\n  config: {}\n  trace: {:016x} ({} events)\n  trace tail:\n    {}\n  artifacts: {}\n  replay: {}",
        if deterministic {
            "failed"
        } else {
            "is not deterministic"
        },
        config.label,
        first.violations.join("\n    "),
        first.config,
        first.trace_hash,
        first.trace_events,
        first.trace_tail.join("\n    "),
        dir.display(),
        config.replay_line(seed),
    );
    if let (Some(second), false) = (second, deterministic) {
        text.push_str(&format!(
            "\n  replay trace: {:016x} ({} events), history {} bytes against {}\n  replay violations:\n    {}\n  replay tail:\n    {}",
            second.trace_hash,
            second.trace_events,
            second.history.len(),
            first.history.len(),
            second.violations.join("\n    "),
            second.trace_tail.join("\n    "),
        ));
    }
    if let Some(shrunk) = &shrunk {
        text.push_str(&format!(
            "\n  shrunk tape: {} choices from {} ({} runs); replay it with NABEDRA_SIM_TAPE={}",
            shrunk.tape.len(),
            first.tape.len(),
            shrunk.steps,
            dir.join(format!("{seed}.tape")).display(),
        ));
    }
    for note in notes {
        text.push_str(&format!("\n  {note}"));
    }
    eprintln!("{text}");
    (text, written)
}

/// A search for a planted bug: seeds from `first` until a violation names one of `expect`.
#[derive(Clone, Debug)]
pub struct PlantedSearch {
    pub label: String,
    pub first: u64,
    pub budget: u64,
    pub jobs: usize,
    /// Invariant or anomaly names that count as catching the bug.
    pub expect: Vec<String>,
}

/// The planted bug was caught.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Found {
    pub seed: u64,
    /// Seeds from `first` up to and including `seed`.
    pub tried: u64,
    pub violations: Vec<String>,
}

impl PlantedSearch {
    pub fn new(label: &str, first: u64, budget: u64, expect: &[&str]) -> Self {
        Self {
            label: label.to_owned(),
            first,
            budget,
            jobs: 1,
            expect: expect.iter().map(|name| (*name).to_owned()).collect(),
        }
    }

    /// Like [`PlantedSearch::new`], with `NABEDRA_PLANTED_SEED`, `NABEDRA_PLANTED_SEEDS`
    /// and `NABEDRA_SIM_JOBS` overriding the start, the budget and the thread count.
    pub fn from_env(label: &str, first: u64, budget: u64, expect: &[&str]) -> Self {
        let mut search = Self::new(
            label,
            env_u64("NABEDRA_PLANTED_SEED", first),
            env_u64("NABEDRA_PLANTED_SEEDS", budget),
            expect,
        );
        search.jobs = env_u64("NABEDRA_SIM_JOBS", 1).max(1) as usize;
        search
    }

    fn catches(&self, violations: &[String]) -> bool {
        violations.iter().any(|violation| {
            let kind = shrink::violation_kind(violation);
            self.expect.iter().any(|name| kind == name)
        })
    }

    /// Runs seeds in batches of `jobs` and returns the lowest seed that caught the bug, or
    /// `Err` with the budget spent. `run` returns a seed's violations.
    pub fn run(&self, run: impl Fn(u64) -> Vec<String> + Sync) -> Result<Found, String> {
        let mut seeds = (0..self.budget).map_while(|offset| self.first.checked_add(offset));
        let mut tried = 0;
        loop {
            let batch: Vec<_> = seeds.by_ref().take(self.jobs.max(1)).collect();
            if batch.is_empty() {
                break;
            }
            let results = parallel_map(&batch, self.jobs, |&seed| {
                panic::catch_unwind(AssertUnwindSafe(|| run(seed))).unwrap_or_else(|payload| {
                    vec![format!("harness: panic: {}", panic_text(&*payload))]
                })
            });
            for (seed, violations) in batch.into_iter().zip(results) {
                tried += 1;
                if self.catches(&violations) {
                    eprintln!(
                        "planted bug {} caught at seed {seed} after {tried} seeds:\n  {}",
                        self.label,
                        violations.join("\n  ")
                    );
                    return Ok(Found {
                        seed,
                        tried,
                        violations,
                    });
                }
                if !violations.is_empty() {
                    eprintln!(
                        "planted bug {}: seed {seed} failed with other violations:\n  {}",
                        self.label,
                        violations.join("\n  ")
                    );
                }
            }
        }
        Err(format!(
            "planted bug {} was not caught in {tried} seeds from {}",
            self.label, self.first
        ))
    }
}
