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

use crate::workload::run_seed;

const REPLAY: &str = "RUST_MIN_STACK=16777216 RUSTFLAGS=\"--cfg tokio_unstable\" CARGO_TARGET_DIR=target/sim \
    cargo test --locked --release -p nabedra-sim-oltp --lib sweep::tests::sweep -- \
    --exact --nocapture --test-threads=1";
const SMOKE: &str = "smoke";
const EXPECTED_DEFECT: &str = "non-linearizable";

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

/// A completed run's named assertion verdicts. Omit families that did not execute.
pub(crate) struct RunResult {
    pub(crate) run: SeedRun,
    pub(crate) assertions: BTreeMap<&'static str, bool>,
    pub(crate) blocking_cpu: Option<Duration>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Command {
    Run,
    Sweep,
    Shrink,
}

struct ObservedSeed {
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

fn measured(seed: u64, generator: &mut Gen, defect: Defect) -> (RunResult, Measurement) {
    let cpu_start = usage();
    // Both samples are outside the World installed by run_seed.
    let start = time::now_std();
    let result = run_seed(seed, generator, defect);
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

fn seed_line(seed: u64, kinds: &BTreeSet<String>) {
    println!(
        "nabedra-sim-seed: {}",
        json!({
            "seed": seed,
            "result": if kinds.is_empty() { "pass" } else { "violation" },
            "kinds": kinds,
        })
    );
}

fn run_search(
    config: &SweepConfig,
    defect: Defect,
    expect: Vec<String>,
) -> Result<bool, Box<dyn Error>> {
    if config.tape.is_some() || config.shrink {
        return Err("NABEDRA_SIM_EXPECT searches fresh seeds; use run or shrink for a tape".into());
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
        let run = run_seed(seed, &mut sweep::workload_gen(seed), defect).run;
        observed
            .lock()
            .expect("search observations poisoned")
            .insert(seed, shrink::kinds(&run.violations));
        run.violations
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
        let kinds = observed
            .get(&seed)
            .ok_or_else(|| format!("seed {seed} did not finish"))?;
        seed_line(seed, kinds);
    }
    result.map(|_| true).map_err(Into::into)
}

fn replay_command(defect: Defect) -> String {
    if defect == Defect::None {
        REPLAY.to_owned()
    } else {
        format!("NABEDRA_SIM_DEFECT={} {REPLAY}", defect.name())
    }
}

/// Run the environment-selected seeds. `false` means an assertion failed.
pub fn run(command: Command) -> Result<bool, Box<dyn Error>> {
    let workload = env("NABEDRA_SIM_WORKLOAD").unwrap_or_else(|| SMOKE.to_owned());
    if workload != SMOKE {
        return Err(format!("OLTP workload {workload:?} is unavailable; use {SMOKE}").into());
    }
    if env("NABEDRA_SIM_PROFILE").is_some() {
        return Err("the smoke workload does not accept nemesis profiles".into());
    }
    let defect = Defect::from_env()?;
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
        return run_search(&config, defect, expect);
    }

    let test_defects = env("NABEDRA_SIM_TEST_DEFECTS").as_deref() == Some("1");
    if test_defects && (defect != Defect::None || config.tape.is_some()) {
        return Err("seeded-defect checks require an unmodified smoke sweep".into());
    }
    let started = time::now_std();
    let observed = Mutex::new(BTreeMap::<u64, ObservedSeed>::new());
    let report = sweep::sweep(&config, |seed, generator| {
        let (result, measurement) = measured(seed, generator, defect);
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
    std::fs::create_dir_all(&config.artifacts)?;
    for summary in &report.seeds {
        let seed = summary.seed;
        let observation = observed.get(&seed);
        let mut kinds = observation.map(|row| row.kinds.clone()).unwrap_or_default();
        if !summary.passed && observation.is_none() {
            kinds.insert("harness".to_owned());
        }
        if !summary.deterministic {
            kinds.insert("determinism".to_owned());
        }
        seed_line(seed, &kinds);

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
            assertions,
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
    let report = sweep::sweep(&config, |seed, generator| {
        let run = run_seed(seed, generator, defect).run;
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
                    let replay = run_seed(seed, &mut Gen::from_tape(tape), defect).run;
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
    assertions: BTreeMap<&'static str, bool>,
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
    record.scenarios = env_list("NABEDRA_SIM_SCENARIOS");
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
        ("workload".to_owned(), json!(SMOKE)),
        ("direct_oltp".to_owned(), json!(true)),
        ("config_hash".to_owned(), json!(input.config_hash)),
        ("seeded_defect".to_owned(), json!(input.defect.name())),
        ("replayed".to_owned(), json!(report.replayed())),
    ]));
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
    if input.defects_skipped {
        record.not_run("seeded defects", "the unmodified smoke sweep failed");
    }
    record.not_run("probe-driven invariants", "P10-PROBES integration pending");
    for (scenario, workload) in [("D04", "direct"), ("D05", "direct"), ("O15", "cold")] {
        record.not_run(
            scenario,
            &format!("requires the {workload} workload from P10-WORKLOADS"),
        );
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
    #[test]
    fn sweep() {
        assert!(super::run(super::Command::Sweep).expect("OLTP smoke sweep report"));
    }
}
