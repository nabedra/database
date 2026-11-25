//! Seed selection, deterministic replay, and fault-model evidence for the OLTP harness.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use nabedra_check::Gen;
use nabedra_check::evidence::{Artifact, Record};
use nabedra_rt::time;
use nabedra_sim::shrink;
use nabedra_sim::sweep::{self, PlantedSearch, SeedRun, SweepConfig, SweepReport};
use serde_json::{Value, json};

use crate::{
    nemesis::Selection,
    panics::PANIC,
    workload::run_seed,
    workloads::{SCENARIOS, Scenarios, Workload, WorkloadChoice, clauses, topology},
};

const REPLAY: &str = "RUST_MIN_STACK=16777216 RUSTFLAGS=\"--cfg tokio_unstable\" CARGO_TARGET_DIR=target/sim \
    cargo test --locked --release -p nabedra-sim-oltp --lib sweep::tests::sweep -- \
    --exact --nocapture --test-threads=1";
const SMOKE: &str = "smoke";
const EXPECTED_DEFECT: &str = "non-linearizable";
/// The kind of a seed whose run panicked, and of one whose replay differed.
const HARNESS: &str = "harness";
const DETERMINISM: &str = "determinism";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum Defect {
    #[default]
    None,
    LostWrite,
    StaleRead,
}

impl Defect {
    fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::LostWrite => "lost-write",
            Self::StaleRead => "stale-read",
        }
    }

    fn from_env() -> Result<Self, String> {
        match env("NABEDRA_SIM_DEFECT").as_deref() {
            None | Some("none") => Ok(Self::None),
            Some("lost-write") => Ok(Self::LostWrite),
            Some("stale-read") => Ok(Self::StaleRead),
            Some(name) => Err(format!("unknown NABEDRA_SIM_DEFECT {name:?}")),
        }
    }
}

/// The invariants of design 3.7 that need the P10-PROBES event stream.
const PROBE_INVARIANTS: [&str; 10] = [
    "ElectionSafety",
    "MonotonicVote",
    "MonotonicCommit",
    "LogMatching",
    "AppliedPrefixesAgree",
    "MajorityReached",
    "DedupUnique over Applied",
    "NotAppliedNeverCommitted over Applied",
    "RangeClockRule1",
    "KeyspaceCoverage over RouteEpochPublished",
];

/// A completed run's named assertion verdicts. Omit families that did not execute.
pub(crate) struct RunResult {
    pub(crate) run: SeedRun,
    pub(crate) assertions: BTreeMap<&'static str, bool>,
    pub(crate) blocking_cpu: Option<Duration>,
    pub(crate) workload: Workload,
    /// Scenarios whose assertions executed in this seed.
    pub(crate) scenarios: Scenarios,
    pub(crate) summary: Value,
}

/// The workload and nemesis selection of a sweep (`NABEDRA_SIM_WORKLOAD`,
/// `NABEDRA_SIM_PROFILE`).
pub(crate) struct Setup {
    pub(crate) workloads: WorkloadChoice,
    pub(crate) selection: Selection,
    /// `NABEDRA_SIM_PROFILE=none`: run the workload without a nemesis.
    pub(crate) quiet: bool,
    /// A panic that a test plants in the run.
    #[cfg(test)]
    pub(crate) panic_at: Option<crate::panics::At>,
}

impl Setup {
    pub(crate) fn smoke() -> Self {
        Self {
            workloads: WorkloadChoice::parse(SMOKE).expect("smoke workload"),
            selection: Selection::parse(&[]).expect("default selection"),
            quiet: true,
            #[cfg(test)]
            panic_at: None,
        }
    }

    fn from_env() -> Result<Self, String> {
        let workloads = WorkloadChoice::parse(
            &env("NABEDRA_SIM_WORKLOAD").unwrap_or_else(|| SMOKE.to_owned()),
        )?;
        let profile = env_list("NABEDRA_SIM_PROFILE");
        if workloads.is_smoke() && !profile.is_empty() {
            return Err("the smoke workload does not accept nemesis profiles".into());
        }
        Ok(Self {
            workloads,
            quiet: Selection::quiet(&profile),
            selection: Selection::parse(&profile)?,
            #[cfg(test)]
            panic_at: None,
        })
    }

    /// The profile name of `seed` for reports.
    fn profile(&self, seed: u64) -> Option<&'static str> {
        (!self.workloads.is_smoke() && !self.quiet).then(|| self.selection.profile(seed).name)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Command {
    Run,
    Sweep,
    Shrink,
}

struct ObservedSeed {
    workload: Workload,
    scenarios: Scenarios,
    summary: Value,
    config: Value,
    config_hash: String,
    assertions: BTreeMap<&'static str, bool>,
    kinds: BTreeSet<String>,
    sample: Option<SeedRun>,
    measurements: Vec<Measurement>,
}

struct Measurement {
    wall_s: f64,
    cpu_s: Option<f64>,
    blocking_cpu_s: Option<f64>,
    process_peak_rss_kib: Option<u64>,
}

impl Measurement {
    fn json(&self) -> Value {
        json!({
            "wall_s": self.wall_s,
            "cpu_s": self.cpu_s,
            "blocking_cpu_s": self.blocking_cpu_s,
            "process_peak_rss_kib": self.process_peak_rss_kib,
        })
    }
}

#[derive(Clone, Copy)]
struct Usage {
    cpu_s: f64,
    process_peak_rss_kib: u64,
}

#[cfg(target_os = "linux")]
fn usage() -> Option<Usage> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the pointed-to struct on success. RUSAGE_THREAD
    // isolates CPU for the current simulation thread. Linux reports process-wide RSS.
    let usage = unsafe {
        if libc::getrusage(libc::RUSAGE_THREAD, usage.as_mut_ptr()) != 0 {
            return None;
        }
        usage.assume_init()
    };
    let seconds = |time: libc::timeval| time.tv_sec as f64 + time.tv_usec as f64 / 1_000_000.0;
    Some(Usage {
        cpu_s: seconds(usage.ru_utime) + seconds(usage.ru_stime),
        process_peak_rss_kib: usage.ru_maxrss as u64,
    })
}

#[cfg(not(target_os = "linux"))]
fn usage() -> Option<Usage> {
    None
}

fn measured(
    seed: u64,
    generator: &mut Gen,
    defect: Defect,
    setup: &Setup,
) -> (RunResult, Measurement) {
    let cpu_start = usage();
    // Both samples are outside the World installed by run_seed.
    let start = time::now_std();
    let result = run_seed(seed, generator, defect, setup);
    let elapsed = time::elapsed(start).as_secs_f64();
    let cpu_end = usage();
    let blocking_cpu_s = result.blocking_cpu.map(|duration| duration.as_secs_f64());
    let measurement = Measurement {
        wall_s: elapsed,
        cpu_s: cpu_start.zip(cpu_end).map(|(start, end)| {
            (end.cpu_s - start.cpu_s).max(0.0) + blocking_cpu_s.unwrap_or_default()
        }),
        blocking_cpu_s,
        process_peak_rss_kib: cpu_end.map(|end| end.process_peak_rss_kib),
    };
    (result, measurement)
}

fn env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn env_list(name: &str) -> Vec<String> {
    env(name).map_or_else(Vec::new, |value| {
        value
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(str::to_owned)
            .collect()
    })
}

/// The seed-search line `scripts/testing/sim_bugs.py` parses. `workload`, `profile` and
/// `scenarios` are informational additions to its contract.
fn seed_line(
    seed: u64,
    kinds: &BTreeSet<String>,
    workload: Workload,
    profile: Option<&str>,
    scenarios: &Scenarios,
) {
    println!(
        "nabedra-sim-seed: {}",
        json!({
            "seed": seed,
            "result": if kinds.is_empty() { "pass" } else { "violation" },
            "kinds": kinds,
            "workload": workload.name(),
            "profile": profile,
            "scenarios": scenarios,
        })
    );
}

fn run_search(
    config: &SweepConfig,
    defect: Defect,
    expect: Vec<String>,
    setup: &Setup,
) -> Result<bool, Box<dyn Error>> {
    if config.tape.is_some() || config.shrink {
        return Err("NABEDRA_SIM_EXPECT searches fresh seeds; use run or shrink for a tape".into());
    }
    // A panic is a broken run, not a detection.
    if let Some(kind) = expect
        .iter()
        .find(|kind| [HARNESS, DETERMINISM, PANIC].contains(&kind.as_str()))
    {
        return Err(format!("NABEDRA_SIM_EXPECT cannot search for {kind:?}").into());
    }
    let search = PlantedSearch {
        label: config.label.clone(),
        first: config.first,
        budget: config.count,
        jobs: config.jobs,
        expect,
    };
    let observed = Mutex::new(BTreeMap::new());
    let result = search.run(|seed| {
        let result = run_seed(seed, &mut sweep::workload_gen(seed), defect, setup);
        observed
            .lock()
            .expect("search observations poisoned")
            .insert(
                seed,
                (
                    shrink::kinds(&result.run.violations),
                    result.workload,
                    result.scenarios,
                ),
            );
        result.run.violations
    });
    // Core may finish other seeds in the same parallel batch. Report only the prefix
    // that the search considered and require every seed in it to have a result.
    let tried = result.as_ref().map_or(config.count, |found| found.tried);
    let observed = observed.into_inner().expect("search observations poisoned");
    for offset in 0..tried {
        let seed = config
            .first
            .checked_add(offset)
            .ok_or("seed range overflows u64")?;
        let (kinds, workload, scenarios) = searched(&observed, seed, setup);
        seed_line(seed, &kinds, workload, setup.profile(seed), &scenarios);
    }
    result.map(|_| true).map_err(Into::into)
}

type SearchRow = (BTreeSet<String>, Workload, Scenarios);

/// What the search reports for a seed of the prefix it considered. Every seed of the prefix
/// ran. One without an observation panicked in the harness, as in a sweep: it is reported as a
/// violation, and the seeds after it are still searched.
fn searched(observed: &BTreeMap<u64, SearchRow>, seed: u64, setup: &Setup) -> SearchRow {
    observed.get(&seed).cloned().unwrap_or_else(|| {
        (
            BTreeSet::from([HARNESS.to_owned()]),
            setup.workloads.of(seed),
            Scenarios::default(),
        )
    })
}

fn replay_command(defect: Defect) -> String {
    let mut prefix = String::new();
    for name in [
        "NABEDRA_SIM_WORKLOAD",
        "NABEDRA_SIM_PROFILE",
        "NABEDRA_SIM_BEYOND_TCP",
        topology::MODE_ENV,
        topology::DEFECT_ENV,
    ] {
        if let Some(value) = env(name) {
            prefix.push_str(&format!("{name}={value} "));
        }
    }
    if defect != Defect::None {
        prefix.push_str(&format!("NABEDRA_SIM_DEFECT={} ", defect.name()));
    }
    format!("{prefix}{REPLAY}")
}

/// Run the environment-selected seeds. `false` means an assertion failed.
pub fn run(command: Command) -> Result<bool, Box<dyn Error>> {
    let setup = Setup::from_env()?;
    let workload = setup.workloads.label();
    let defect = Defect::from_env()?;
    if defect != Defect::None && !setup.workloads.is_smoke() {
        return Err("seeded harness defects belong to the smoke workload".into());
    }
    let label = env("NABEDRA_SIM_CHECK_ID").unwrap_or_else(|| "oltp-sim".to_owned());
    if label.len() > 80
        || !label.split('-').all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        })
    {
        return Err(format!("invalid NABEDRA_SIM_CHECK_ID {label:?}").into());
    }
    let mut config = SweepConfig::from_env(&label, &replay_command(defect));
    if env("NABEDRA_SIM_ARTIFACTS").is_none() {
        config.artifacts = config
            .artifacts
            .join(format!("{label}-{}", std::process::id()));
    }
    if command != Command::Sweep {
        config.count = 1;
        config.jobs = 1;
        config.verify_replay = 1.0;
    }
    config.shrink |= command == Command::Shrink;
    if !nabedra_sim::runtime::SEEDED {
        return Err(
            "build with RUSTFLAGS=\"--cfg tokio_unstable\" and CARGO_TARGET_DIR=target/sim".into(),
        );
    }
    if config.tape.is_some() && config.count != 1 {
        return Err("set NABEDRA_SEEDS=1 when replaying NABEDRA_SIM_TAPE".into());
    }
    let expect = env_list("NABEDRA_SIM_EXPECT");
    if !expect.is_empty() {
        return run_search(&config, defect, expect, &setup);
    }

    let test_defects = env("NABEDRA_SIM_TEST_DEFECTS").as_deref() == Some("1");
    if test_defects
        && (defect != Defect::None || config.tape.is_some() || !setup.workloads.is_smoke())
    {
        return Err("seeded-defect checks require an unmodified smoke sweep".into());
    }
    let started = time::now_std();
    let observed = Mutex::new(BTreeMap::<u64, ObservedSeed>::new());
    let report = sweep::sweep(&config, |seed, generator| {
        let (result, measurement) = measured(seed, generator, defect, &setup);
        let mut observed = observed.lock().expect("sweep observations poisoned");
        let observation = observed.entry(seed).or_insert_with(|| {
            let config_hash = result.run.config["hash"].as_str().map_or_else(
                || {
                    let bytes =
                        serde_json::to_vec(&result.run.config).expect("sampled JSON configuration");
                    format!("{:016x}", nabedra_sim::fnv(&bytes))
                },
                str::to_owned,
            );
            ObservedSeed {
                workload: result.workload,
                scenarios: result.scenarios.clone(),
                summary: result.summary.clone(),
                config: result.run.config.clone(),
                config_hash,
                assertions: result.assertions,
                kinds: shrink::kinds(&result.run.violations),
                sample: (seed == config.first || result.run.full_trace.is_some())
                    .then(|| result.run.clone()),
                measurements: Vec::new(),
            }
        });
        if !observation.measurements.is_empty()
            && let Some(reference) = observation.sample.as_ref()
            && let Some(reference_trace) = reference.full_trace.as_ref()
            && let Some(current_trace) = result.run.full_trace.as_ref()
            && let Some((line, (before, after))) = reference_trace
                .iter()
                .zip(current_trace)
                .enumerate()
                .find(|(_, (before, after))| before != after)
        {
            eprintln!(
                "seed {seed} first differing trace line {}\n  first: {before}\n  replay: {after}",
                line + 1,
            );
        }
        observation.measurements.push(measurement);
        result.run
    });
    let observed = observed.into_inner().expect("sweep observations poisoned");
    let duration_s = time::elapsed(started).as_secs_f64();
    let mut assertions = BTreeMap::new();
    let mut config_hash = nabedra_sim::Fnv::default();
    let mut stats = Vec::with_capacity(report.seeds.len());
    let mut paths = report.artifacts.clone();
    let mut unjudged: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    // Scenario tag -> (seeds that executed it, seeds among them that failed).
    let mut scenario_seeds: BTreeMap<&'static str, (u64, u64)> = BTreeMap::new();
    std::fs::create_dir_all(&config.artifacts)?;
    for summary in &report.seeds {
        let seed = summary.seed;
        let observation = observed.get(&seed);
        let mut kinds = observation.map(|row| row.kinds.clone()).unwrap_or_default();
        if !summary.passed && observation.is_none() {
            kinds.insert(HARNESS.to_owned());
        }
        if !summary.deterministic {
            kinds.insert(DETERMINISM.to_owned());
        }
        let seed_workload = observation.map_or(setup.workloads.of(seed), |row| row.workload);
        let tags = observation
            .map(|row| row.scenarios.clone())
            .unwrap_or_default();
        if seed_workload == Workload::Topology {
            for rule in topology::not_judged(observation.map(|row| &row.summary)) {
                unjudged.entry(rule).or_default().push(seed);
            }
        }
        seed_line(seed, &kinds, seed_workload, setup.profile(seed), &tags);
        for tag in &tags {
            let entry = scenario_seeds.entry(tag).or_default();
            entry.0 += 1;
            entry.1 += u64::from(!kinds.is_empty());
        }

        if let Some(row) = observation {
            config_hash.write(row.config_hash.as_bytes());
            config_hash.write(b"\n");
            for (&name, &passed) in &row.assertions {
                assertions
                    .entry(name)
                    .and_modify(|all| *all &= passed)
                    .or_insert(passed);
            }
            if summary.passed
                && summary.deterministic
                && let Some(sample) = &row.sample
            {
                paths.extend(write_sample(&config, seed, sample)?);
            }
        }
        stats.push(json!({
            "seed": seed,
            "workload": seed_workload.name(),
            "profile": setup.profile(seed),
            "scenarios": tags,
            "summary": observation.map(|row| &row.summary),
            "config": observation.map(|row| &row.config),
            "config_hash": observation.map(|row| &row.config_hash),
            "trace_hash": format!("{:016x}", summary.trace_hash),
            "trace_events": summary.trace_events,
            "virtual_ms": summary.virtual_ms,
            "replayed": summary.replayed,
            "deterministic": summary.replayed.then_some(summary.deterministic),
            "violations": kinds,
            "measurements": observation.map(|row| row.measurements.iter().map(Measurement::json).collect::<Vec<_>>()),
            "assertions": observation.map(|row| &row.assertions),
        }));
    }
    assertions.insert("seed sweep", report.passed());
    let mut scenario_assertions = BTreeMap::new();
    for (tag, (seeds, failed)) in &scenario_seeds {
        scenario_assertions.insert(*tag, failed == &0);
        eprintln!(
            "Fault model clause {tag}: assertions executed in {seeds} seeds, {failed} failing"
        );
    }
    if report.replayed() > 0 {
        assertions.insert(
            "trace hash, event count and history byte replay",
            report.nondeterministic().is_empty(),
        );
    }

    let mut defect_reports = Vec::new();
    if test_defects && report.passed() {
        for defect in [Defect::LostWrite, Defect::StaleRead] {
            let tested = test_defect(&config, defect)?;
            assertions.insert(defect.name(), tested.detected);
            if let Some(reduced) = tested.reduced {
                assertions.insert("shrink and replay", reduced);
            }
            paths.extend(tested.paths);
            defect_reports.push(tested.report);
        }
    }
    let metrics = observed.values().flat_map(|row| &row.measurements);
    let total_cpu_s = metrics
        .clone()
        .map(|measurement| measurement.cpu_s)
        .try_fold(0.0, |total, cpu_s| cpu_s.map(|cpu_s| total + cpu_s));
    let peak_rss_kib = metrics
        .filter_map(|measurement| measurement.process_peak_rss_kib)
        .max();
    let config_hash = format!("{:016x}", config_hash.finish());
    let report_path = config.artifacts.join("sweep-report.json");
    std::fs::write(
        &report_path,
        serde_json::to_vec_pretty(&json!({
            "evidence": "fault-model",
            "workload": workload,
            "scenarios": scenario_seeds.iter().map(|(tag, (seeds, failed))| (*tag, json!({"seeds": seeds, "failing": failed}))).collect::<BTreeMap<_, _>>(),
            "direct_oltp": true,
            "defect": defect.name(),
            "jobs": config.jobs,
            "config_hash": config_hash,
            "duration_s": duration_s,
            "cpu_s": total_cpu_s,
            "process_peak_rss_kib": peak_rss_kib,
            "virtual_ms": report.virtual_ms(),
            "assertions": assertions,
            "defect_checks": defect_reports,
            "seeds": stats,
        }))?,
    )?;
    paths.insert(0, report_path);
    eprintln!(
        "Fault model: {} clean seeds, {} replayed, {:.3} s wall, {:?} simulation CPU s, \
         {:?} KiB process peak RSS, configuration {config_hash}",
        report.count - report.evidence_seeds().failing.len() as u64,
        report.replayed(),
        duration_s,
        total_cpu_s,
        peak_rss_kib,
    );

    write_record(
        &config,
        &report,
        RecordInput {
            workload,
            smoke: setup.workloads.is_smoke(),
            scenarios: scenario_assertions,
            assertions,
            unjudged,
            defects_skipped: test_defects && !report.passed(),
            duration_s,
            total_cpu_s,
            config_hash,
            defect,
            paths,
        },
    )
}

fn write_sample(config: &SweepConfig, seed: u64, run: &SeedRun) -> io::Result<Vec<PathBuf>> {
    let dir = config
        .artifacts
        .join(format!("{}-seed-{seed}", config.label));
    std::fs::create_dir_all(&dir)?;
    let history = dir.join("history.jsonl");
    std::fs::write(&history, &run.history)?;
    let tail = dir.join("trace-tail.txt");
    std::fs::write(&tail, run.trace_tail.join("\n"))?;
    let mut paths = vec![history, tail];
    if let Some(full) = &run.full_trace {
        let trace = dir.join("trace.txt");
        std::fs::write(&trace, full.join("\n"))?;
        paths.push(trace);
    }
    Ok(paths)
}

struct DefectTest {
    detected: bool,
    reduced: Option<bool>,
    paths: Vec<PathBuf>,
    report: Value,
}

fn test_defect(base: &SweepConfig, defect: Defect) -> Result<DefectTest, Box<dyn Error>> {
    let seed = base.first;
    let mut config = base.clone();
    config.label = format!("{}-{}", config.label, defect.name());
    config.count = 1;
    config.jobs = 1;
    config.verify_replay = 1.0;
    config.replay = replay_command(defect);
    config.shrink = defect == Defect::LostWrite;
    // The seeded control's deliberately removable tail needs only a few
    // candidates. Interactive shrinking retains the shared 200-run budget.
    config.shrink_budget = 4;

    let initial = Mutex::new(None::<(BTreeSet<String>, usize)>);
    let setup = Setup::smoke();
    let report = sweep::sweep(&config, |seed, generator| {
        let run = run_seed(seed, generator, defect, &setup).run;
        initial
            .lock()
            .expect("mutation observation poisoned")
            .get_or_insert_with(|| (shrink::kinds(&run.violations), generator.tape().len()));
        run
    });
    let (kinds, original_len) = initial
        .into_inner()
        .expect("mutation observation poisoned")
        .unwrap_or_default();
    let detected =
        kinds.len() == 1 && kinds.contains(EXPECTED_DEFECT) && report.nondeterministic().is_empty();
    let mut shrunk_len = None;
    let mut shrunk_replay_detected = None;
    let mut reduced = None;
    if config.shrink {
        let tape = config
            .artifacts
            .join(format!("{}-seed-{seed}/{seed}.tape", config.label));
        let shrunk = report
            .artifacts
            .contains(&tape)
            .then(|| shrink::read_tape(&tape))
            .transpose()?;
        reduced = Some(
            detected
                && shrunk.is_some_and(|tape| {
                    let len = tape.len();
                    shrunk_len = Some(len);
                    let replay = run_seed(seed, &mut Gen::from_tape(tape), defect, &setup).run;
                    let reproduced = shrink::kinds(&replay.violations).contains(EXPECTED_DEFECT);
                    shrunk_replay_detected = Some(reproduced);
                    len < original_len && reproduced
                }),
        );
    }
    let details = json!({
        "defect": defect.name(),
        "seed": seed,
        "detected": detected,
        "kinds": kinds,
        "tape_len": original_len,
        "shrunk_tape_len": shrunk_len,
        "shrunk_replay_detected": shrunk_replay_detected,
    });
    eprintln!("Fault model seeded defect: {details}");
    Ok(DefectTest {
        detected,
        reduced,
        paths: report.artifacts,
        report: details,
    })
}

struct RecordInput {
    workload: String,
    smoke: bool,
    /// Scenario tags that executed, and whether every seed that executed one passed.
    scenarios: BTreeMap<&'static str, bool>,
    assertions: BTreeMap<&'static str, bool>,
    /// Rules of the topology workload, and the seeds that did not judge them.
    unjudged: BTreeMap<String, Vec<u64>>,
    defects_skipped: bool,
    duration_s: f64,
    total_cpu_s: Option<f64>,
    config_hash: String,
    defect: Defect,
    paths: Vec<PathBuf>,
}

fn write_record(
    config: &SweepConfig,
    report: &SweepReport,
    input: RecordInput,
) -> Result<bool, Box<dyn Error>> {
    let mut record = Record::new(&config.label, "S H");
    record.fill_from_env();
    record.guarantees = env_list("NABEDRA_SIM_GUARANTEES");
    // Claim only scenarios whose every clause executed its assertions in this range.
    let complete: BTreeSet<&str> = SCENARIOS
        .iter()
        .filter(|(_, required)| {
            required
                .iter()
                .all(|clause| input.scenarios.contains_key(clause))
        })
        .map(|(scenario, _)| *scenario)
        .collect();
    record.scenarios = complete.iter().map(|tag| (*tag).to_owned()).collect();
    for listed in env_list("NABEDRA_SIM_SCENARIOS") {
        if !complete.contains(listed.as_str()) {
            eprintln!("Fault model scenario {listed}: listed by the check, no seed executed it");
        }
    }
    let binary = std::env::current_exe()?;
    record.build.profile = env("NABEDRA_SIM_BUILD_PROFILE").or_else(|| {
        binary
            .ancestors()
            .filter_map(Path::file_name)
            .filter_map(|name| name.to_str())
            .find(|name| *name == "release" || *name == "debug")
            .map(str::to_owned)
    });
    record.build.features = ["oltp", "sim"].map(str::to_owned).into();
    record.build.binary_sha256 = Some(sha256_file(&binary)?);
    record.mode = Some(BTreeMap::from([
        ("evidence".to_owned(), json!("fault-model")),
        ("workload".to_owned(), json!(input.workload)),
        ("direct_oltp".to_owned(), json!(true)),
        ("config_hash".to_owned(), json!(input.config_hash)),
        ("seeded_defect".to_owned(), json!(input.defect.name())),
        ("replayed".to_owned(), json!(report.replayed())),
    ]));
    for (key, name) in [
        ("topology_plan", topology::MODE_ENV),
        ("topology_defect", topology::DEFECT_ENV),
    ] {
        if let (Some(mode), Some(value)) = (record.mode.as_mut(), env(name)) {
            mode.insert(key.to_owned(), json!(value));
        }
    }
    record.duration_s = input.duration_s;
    let mut seeds = report.evidence_seeds();
    // The full list is always in the sweep report. The evidence schema caps this index.
    seeds.failing.truncate(100);
    record.seeds = Some(seeds);
    record.sim_core_hours = input.total_cpu_s.map(|seconds| seconds / 3600.0);
    record.virtual_hours = Some(report.virtual_ms() as f64 / 3_600_000.0);
    for passed in input.assertions.into_values() {
        record.assertion(passed);
    }
    for passed in input.scenarios.values() {
        record.assertion(*passed);
    }
    if input.defects_skipped {
        record.not_run("seeded defects", "the unmodified smoke sweep failed");
    }
    for invariant in PROBE_INVARIANTS {
        record.not_run(invariant, "P10-PROBES integration pending");
    }
    if input.workload == Workload::Topology.name() {
        for (rule, why) in crate::invariants::topology::NOT_RUN {
            record.not_run(rule, why);
        }
    }
    for (rule, seeds) in &input.unjudged {
        let why = format!(
            "not judged in {} of {} seeds: {seeds:?}",
            seeds.len(),
            report.count
        );
        record.not_run(rule, &why);
    }
    record.not_run(
        "ResourceBaseline ledger and RangeClock pending sets",
        "P10-PROBES integration pending",
    );
    record.not_run("ConsistentCommittedData", "P10-PROBES integration pending");
    if input.smoke {
        for (scenario, workload) in [("D04", "direct"), ("D05", "direct"), ("O15", "cold")] {
            record.not_run(scenario, &format!("runs in the {workload} workload"));
        }
    } else {
        for (scenario, required) in SCENARIOS {
            let missing: Vec<_> = required
                .iter()
                .filter(|clause| !input.scenarios.contains_key(*clause))
                .collect();
            for clause in &missing {
                let why = clauses::unreachable(clause)
                    .unwrap_or("no seed in this range executed its assertions");
                record.not_run(clause, why);
            }
            if !missing.is_empty() {
                record.not_run(scenario, &format!("clauses that did not run: {missing:?}"));
            }
        }
    }
    if report.replayed() == 0 {
        record.not_run(
            "replay sampling",
            "NABEDRA_SIM_VERIFY_REPLAY selected zero seeds",
        );
    }
    for path in input.paths.iter().take(50) {
        record.artifacts.push(Artifact {
            name: path.strip_prefix(&config.artifacts)?.display().to_string(),
            sha256: sha256_file(path)?,
        });
    }
    let outcome = record.conclude();
    let records = env("NABEDRA_SIM_RECORD_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| config.artifacts.join("records"));
    let path = record.write_to(&records)?;
    eprintln!("Fault-model evidence record {}", path.display());
    // The registered runner attests the SHA-256 of its command log. Embed the
    // richer native record in that log so the attestation also binds these fields.
    print!("{}", std::fs::read_to_string(path)?);
    Ok(outcome == nabedra_check::evidence::Outcome::Pass)
}

fn sha256_file(path: &Path) -> io::Result<String> {
    let mut context = ring::digest::Context::new(&ring::digest::SHA256);
    let mut file = std::fs::File::open(path)?;
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let bytes = file.read(&mut buffer)?;
        if bytes == 0 {
            break;
        }
        context.update(&buffer[..bytes]);
    }
    Ok(context
        .finish()
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    /// Seed 2 panics below the hit at seed 3. Its line is a violation of kind `harness`, the
    /// search goes on to the hit, and no panicked seed of the prefix is reported as a pass.
    #[test]
    fn a_panicking_seed_is_a_violation_and_the_search_goes_on() {
        let setup = Setup::smoke();
        for jobs in [1, 2] {
            let observed = Mutex::new(BTreeMap::<u64, SearchRow>::new());
            let mut search = PlantedSearch::new("panic", 1, 5, &["Expected"]);
            search.jobs = jobs;
            let found = search.run(|seed| {
                assert!(seed != 2, "planted panic");
                let violations = if seed == 3 {
                    vec!["t=1ms Expected: hit".to_owned()]
                } else {
                    Vec::new()
                };
                let row = (
                    shrink::kinds(&violations),
                    Workload::Smoke,
                    Scenarios::new(),
                );
                observed.lock().unwrap().insert(seed, row);
                violations
            });
            let found = found.expect("the hit above the panic");
            assert_eq!((found.seed, found.tried), (3, 3));
            let observed = observed.into_inner().unwrap();
            let kinds = |seed| searched(&observed, seed, &setup).0;
            assert!(kinds(1).is_empty());
            assert_eq!(kinds(2), BTreeSet::from([HARNESS.to_owned()]));
            assert!(kinds(3).contains("Expected"));
        }
    }

    /// The sweep itself: a body that panics in the first run, or only in the replay.
    #[test]
    fn a_panicking_seed_never_passes_a_sweep() {
        let dir = std::env::temp_dir().join(format!("p10-panic-{}", std::process::id()));
        for panicking in [0, 1] {
            let mut config = SweepConfig::new("panic", 1, 1);
            config.verify_replay = 1.0;
            config.artifacts = dir.clone();
            let runs = AtomicU64::new(0);
            let report = sweep::sweep(&config, |seed, _| {
                let run = runs.fetch_add(1, Ordering::SeqCst);
                assert!(run != panicking, "planted panic");
                SeedRun {
                    seed,
                    trace_hash: 7,
                    trace_events: 1,
                    ..SeedRun::default()
                }
            });
            assert_eq!(runs.load(Ordering::SeqCst), 2);
            assert!(!report.passed(), "panic in run {panicking}");
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A panic, or a replay that differed, cannot be what a search looks for.
    #[test]
    fn a_search_cannot_expect_a_panic() {
        let setup = Setup::smoke();
        let config = SweepConfig::new("panic", 1, 1);
        for kind in [HARNESS, DETERMINISM, PANIC] {
            let expect = vec!["Liveness".to_owned(), kind.to_owned()];
            assert!(run_search(&config, Defect::None, expect, &setup).is_err());
        }
    }

    #[test]
    fn sweep() {
        assert!(super::run(super::Command::Sweep).expect("OLTP smoke sweep report"));
    }
}
