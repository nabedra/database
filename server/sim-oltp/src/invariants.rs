//! End-to-end checks over client history, mounted disks and public observations.
//! The probe stream ([`crate::probes`]) adds the protocol invariants in [`protocol`],
//! checked synchronously as each probe arrives.

pub mod protocol;

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use nabedra_check::{checker::Verdict, history::Event};
use nabedra_rt::{
    fs::FaultFs,
    sim::{VolumeInfo, World},
};
use nabedra_sim::raft::MAX_VIOLATIONS;
use serde_json::Value;

use crate::probes::{ProbeEvent, Probes, describe};
use protocol::Ledgers;

pub const STEP: Duration = Duration::from_millis(10);
pub const RECOVERY_LIMIT: Duration = Duration::from_secs(30);

/// How many observations in a row, each at least [`STEP`] after the one before, must find a
/// worker's resources at their baseline before it counts as settled.
///
/// The harness's network delays a message by 1 ms. On a slower one, message traffic alone keeps
/// the ledgers off their baseline: with one-way delays of 1, 2 and 3 ms 20 of 20 smoke seeds
/// settled, at 5 ms 5 of 20 were red on ResourceBaseline, and at 10 ms all 19 that were judged.
pub const SETTLED_PASSES: u32 = 3;

/// Prefix of the trace line that counts how often each probe rule ran. A rule it does not
/// name did not run, and is not a pass.
pub const RULES_RAN: &str = "probe rules ran";

pub type TraceObserver = Arc<dyn Fn(u64, String) + Send + Sync>;

pub(crate) fn elapsed_ms(world: &World) -> u64 {
    u64::try_from(world.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[derive(Clone, Default)]
pub struct Invariants {
    checks: Arc<Mutex<Checks>>,
}

#[derive(Default)]
pub(crate) struct Checks {
    volumes: BTreeMap<u64, VolumeInfo>,
    deadlines: BTreeMap<String, Duration>,
    resources: BTreeMap<u64, ResourceUsage>,
    /// Per worker: every ledger at the resource baseline, from probes.
    ledger_baseline: BTreeMap<u64, Ledgers>,
    /// Per worker: what [`Invariants::resources_at_baseline`] last observed.
    settling: BTreeMap<u64, Settling>,
    /// Per worker: what [`Invariants::resources_steady`] last observed.
    steady: BTreeMap<u64, Steady>,
    reported: BTreeSet<(String, String)>,
    violations: Vec<String>,
    trace: Option<TraceObserver>,
    probes: Option<Probes>,
}

/// A worker's observations after the workload, up to the last one.
struct Settling {
    /// When the last counted observation was made.
    at: Duration,
    /// Observations in a row at the baseline, up to and including the last one. A step that
    /// finds a ledger off its baseline ends the row.
    passes: u32,
    /// The last observation was the [`SETTLED_PASSES`]th in a row, or a later one.
    settled: bool,
    stats: Value,
    /// Ledgers compared at the last observation, and what was off the last time any was.
    ledgers: u64,
    off: Vec<String>,
}

/// A worker's observations before the workload, up to the last one.
struct Steady {
    at: Duration,
    /// Equal observations in a row, up to and including the last one.
    passes: u32,
    stats: Value,
    ledgers: Option<Ledgers>,
}

/// The pass that an observation at `now` is: the next one, or the same one within a step.
fn next_pass(now: Duration, at: Duration, passes: u32) -> (Duration, u32) {
    if passes > 0 && now < at + STEP {
        return (at, passes);
    }
    (now, passes + 1)
}

/// A half-open key range. An unbounded end is represented by `None`.
pub struct KeyRange<K> {
    pub start: Option<K>,
    pub end: Option<K>,
}

impl Invariants {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_trace(&self, trace: TraceObserver) {
        self.checks.lock().unwrap().trace = Some(trace);
    }

    /// Installs the run's probe sink, so the protocol invariants run and probes enter the
    /// run trace. Call it before any node starts.
    pub fn install_probes(&self, world: &Arc<World>, workload: &str) -> Probes {
        Probes::install(world, self, workload)
    }

    /// The probe stream, once installed. A nemesis subscribes to it for triggers.
    pub fn probes(&self) -> Option<Probes> {
        self.checks.lock().unwrap().probes.clone()
    }

    pub(crate) fn set_probes(&self, probes: Probes) {
        self.checks.lock().unwrap().probes = Some(probes);
    }

    pub(crate) fn downgrade(&self) -> Weak<Mutex<Checks>> {
        Arc::downgrade(&self.checks)
    }

    pub(crate) fn from_checks(checks: Arc<Mutex<Checks>>) -> Self {
        Self { checks }
    }

    /// Adds a probe to the hashed run trace.
    pub(crate) fn record_probe(&self, world: &World, event: &ProbeEvent) {
        self.trace(world, describe(event));
    }

    /// NotAppliedNeverCommitted over the finished client history. Call it last: it also
    /// traces how often each rule ran.
    pub fn check_outcomes(&self, world: &World, events: &[Event]) {
        if let Some(probes) = self.probes() {
            probes.check_outcomes(world, events);
            self.trace(world, format!("{RULES_RAN} {:?}", probes.exercised()));
        }
    }

    /// True when no probe-driven invariant reported a violation, in a run that stayed within
    /// the fault model.
    pub fn probe_invariants_hold(&self) -> bool {
        self.checks.lock().unwrap().violations.iter().all(|line| {
            let kind = nabedra_sim::shrink::violation_kind(line);
            !protocol::PROBE_INVARIANTS.contains(&kind) && kind != protocol::BEYOND_FAULT_MODEL
        })
    }

    /// Records the expected identity from the mount operation. A workload can
    /// register a new identity after an intentional wipe or device replacement.
    pub fn track_volume(&self, node: u64, info: VolumeInfo) {
        self.checks.lock().unwrap().volumes.insert(node, info);
    }

    /// Recheck the mounted image and public convergence deadlines at each step.
    pub fn step(&self, world: &World) {
        let settling: Vec<u64> = {
            let mut checks = self.checks.lock().unwrap();
            checks.step(world);
            let settling = checks.settling.iter();
            let clean = settling.filter(|(_, worker)| worker.passes > 0);
            clean.map(|(worker, _)| *worker).collect()
        };
        for worker in settling {
            let Some(ledgers) = self.ledgers(worker) else {
                continue;
            };
            let mut checks = self.checks.lock().unwrap();
            let off = checks.ledgers_moved(worker, &ledgers);
            if let Some(settling) = checks.settling.get_mut(&worker)
                && !off.is_empty()
            {
                settling.passes = 0;
                settling.off = off;
            }
        }
    }

    /// Check the cut before reopening or replacing the volume. The old image
    /// stays halted even after another incarnation starts on the same device.
    pub fn check_crash(&self, world: &World, node: u64, old_image: &FaultFs) {
        if world.is_up(node) || !old_image.is_crashed() {
            self.violate(
                world,
                "CrashOrder",
                format!("n{node} or its old image is still active"),
            );
        }
        let services = world.services(node);
        if !services.is_empty() {
            self.violate(
                world,
                "CrashOrder",
                format!("n{node} still has services {services:?}"),
            );
        }
    }

    /// Arm a public liveness expectation, such as all workers reporting ready
    /// after a heal. Use a distinct label for each overlapping expectation.
    pub fn expect_recovery(&self, world: &World, label: &str, bound: Duration) {
        let deadline = world.elapsed().saturating_add(bound);
        let old = self
            .checks
            .lock()
            .unwrap()
            .deadlines
            .insert(label.to_owned(), deadline);
        assert!(
            old.is_none(),
            "liveness expectation {label:?} already armed"
        );
    }

    /// Discharge the expectation only after the workload observes success
    /// through the relevant public endpoint.
    pub fn recovered(&self, world: &World, label: &str) {
        let mut checks = self.checks.lock().unwrap();
        let deadline = checks
            .deadlines
            .remove(label)
            .expect("known liveness expectation");
        if world.elapsed() > deadline {
            checks.violate(world, "Liveness", deadline_message(label, deadline));
        }
    }

    /// Before [`Self::capture_resources`]: true once the last [`SETTLED_PASSES`] observations
    /// of `worker` were equal, so that its baseline holds no charge of a message in flight.
    pub fn resources_steady(&self, world: &World, worker: u64, stats: &Value) -> bool {
        let ledgers = self.ledgers(worker);
        let mut checks = self.checks.lock().unwrap();
        let now = world.elapsed();
        let usage = ResourceUsage::from_stats(stats);
        let (at, passes) = match checks.steady.get(&worker) {
            Some(last)
                if usage.is_some()
                    && last.ledgers == ledgers
                    && ResourceUsage::from_stats(&last.stats) == usage =>
            {
                next_pass(now, last.at, last.passes)
            }
            _ => (now, 1),
        };
        let observed = Steady {
            at,
            passes,
            stats: stats.clone(),
            ledgers,
        };
        checks.steady.insert(worker, observed);
        passes >= SETTLED_PASSES
    }

    /// At quiescence, capture the transient gauges that worker stats expose, and every ledger
    /// the probes report: those [`Self::resources_steady`] observed with the same `stats`, or
    /// the current ones.
    /// Retained queue responses and caches can legitimately grow after work.
    pub fn capture_resources(&self, world: &World, worker: u64, stats: &Value) {
        let steady = self.checks.lock().unwrap().steady.remove(&worker);
        let ledgers = match steady {
            Some(steady) if steady.stats == *stats => steady.ledgers,
            _ => self.ledgers(worker),
        };
        let mut checks = self.checks.lock().unwrap();
        if let Some(ledgers) = ledgers {
            checks.ledger_baseline.insert(worker, ledgers);
        }
        match ResourceUsage::from_stats(stats) {
            Some(usage) => {
                checks.check_storage_devices(world, worker, &usage);
                checks.resources.insert(worker, usage);
            }
            None => checks.violate(
                world,
                "ResourceBaseline",
                format!("n{worker} stats missing resource gauges"),
            ),
        }
    }

    /// ResourceBaseline: "at quiescence, every ledger is back at baseline". `stats` is the
    /// observation that [`Self::resources_at_baseline`] made last. It must be the last of
    /// [`SETTLED_PASSES`] observations in a row that found every ledger the probes report
    /// equal to its baseline, and the public gauges at or below theirs. Design 3.7 checks
    /// after every 10 ms step, so every step between those observations must find the ledgers
    /// there too.
    ///
    /// One clean observation is not enough: a charge held 50 of every 51 ms is clean at some
    /// instant. The observations and the steps have different periods, so a charge in phase
    /// with one of them is seldom in phase with the other. No message class is exempt. A Raft
    /// message in flight ends the row, and a later one is complete, because the message ends
    /// and the next one is a heartbeat away.
    ///
    /// What it cannot see:
    ///
    /// - a charge that comes and goes, if the samples fall where it is gone. Only these pairs
    ///   were measured (held/gone in ms), and no general bound follows from them. In the
    ///   product on smoke seeds 1, 2 and 18, red every time: 2/1, 5/5, 8/2, 9/1, 10/10, 12/4,
    ///   14/2, 15/1, 19/1, 18/2, 28/2, 45/5, 40/10, 49/1, 80/20, 90/10, 99/1, 150/10, 180/20,
    ///   190/10, 285/15, 270/30, 475/25, 490/10, 980/20 and 999/1. Missed: 1/9, 160/40,
    ///   400/100, 450/50, 800/200, 900/100 and 950/50, and 1/1 in 4 of 9 runs. In the settling
    ///   loop alone, red in this many phases: 1/4 in 1 of 5, 2/3 in 2 of 5, 1/12 in 0 of 13,
    ///   1/15 in 1 of 16, 2/18 in 4 of 20, 2/23 in 0 of 25 and 3/27 in 9 of 30. With requests
    ///   of 5 or 10 ms there, 4/1 was missed in 2 of 50 cases, 9/1 in 1 and 8/2 in 4 of 100;
    /// - a charge first taken after the last observation: one taken 40 ms or more after
    ///   settling starts was missed. The workload stops observing once
    ///   every worker has settled at the same pass, or at its limit: if one worker never
    ///   settles, another is judged by what the limit caught, a message in flight included;
    /// - a leak by exactly what the baseline held, when the baseline was not steady
    ///   ([`Self::resources_steady`]) and held it. A baseline that caught a message in
    ///   flight is a false red instead when nothing leaks;
    /// - the compute waiters queue of `nabedra-resources`, owners that died with their charge,
    ///   and gauges of the public stats below their baseline;
    /// - without probes, every ledger but the public gauges, and the steps.
    pub fn check_resources(&self, world: &World, worker: u64, stats: &Value) {
        let mut checks = self.checks.lock().unwrap();
        let settled = checks.settling.remove(&worker);
        if let (Some(probes), Some(settled)) = (&checks.probes, &settled) {
            probes.ran(protocol::RESOURCE_BASELINE, settled.ledgers);
        }
        let problem = match settled {
            Some(settled) if settled.stats == *stats => {
                (!settled.settled).then(|| {
                    format!(
                        "n{worker} did not stay at its baseline after quiescence: clean in its last {} of {SETTLED_PASSES} observations, last off by {}",
                        settled.passes,
                        settled.off.join(", ")
                    )
                })
            }
            _ => Some(format!(
                "n{worker} was judged without the observation that settled it"
            )),
        };
        if let Some(problem) = problem {
            checks.violate(world, "ResourceBaseline", problem);
        }
        match ResourceUsage::from_stats(stats) {
            Some(now) => checks.check_storage_devices(world, worker, &now),
            None => checks.violate(
                world,
                "ResourceBaseline",
                format!("n{worker} stats missing final resource gauges"),
            ),
        }
    }

    /// Observes `worker` for [`Self::check_resources`], and says whether it has settled: the
    /// last [`SETTLED_PASSES`] observations found its resources at their baseline. Missing
    /// observations leave the expectation pending.
    pub fn resources_at_baseline(&self, worker: u64, stats: &Value) -> bool {
        let ledgers = self.ledgers(worker);
        let mut checks = self.checks.lock().unwrap();
        let now = nabedra_rt::sim::world().map_or(Duration::ZERO, |world| world.elapsed());
        let mut off = match &ledgers {
            Some(now) => checks.ledgers_moved(worker, now),
            None => Vec::new(),
        };
        match (
            checks.resources.get(&worker),
            ResourceUsage::from_stats(stats),
        ) {
            (Some(baseline), Some(now)) if now.within(baseline) => {}
            (Some(baseline), Some(now)) => off.push(format!("{now:?} (baseline {baseline:?})")),
            (None, _) => off.push("no baseline".into()),
            (_, None) => off.push("no resource gauges in the stats".into()),
        }
        let last = checks.settling.remove(&worker);
        let (at, passes) = match &last {
            _ if !off.is_empty() => (now, 0),
            Some(last) => next_pass(now, last.at, last.passes),
            None => (now, 1),
        };
        if off.is_empty() {
            off = last.map_or_else(Vec::new, |last| last.off);
        }
        let observed = Settling {
            at,
            passes,
            settled: passes >= SETTLED_PASSES,
            stats: stats.clone(),
            ledgers: ledgers.map_or(0, |ledgers| ledgers.len() as u64),
            off,
        };
        checks.settling.insert(worker, observed);
        passes >= SETTLED_PASSES
    }

    /// Verify the workload reached the direct path using public worker stats.
    pub fn check_direct_path(&self, world: &World, worker_stats: &[(u64, Value)]) {
        let active = worker_stats.iter().any(|(_, stats)| {
            stats
                .pointer("/direct_capability/active")
                .and_then(Value::as_bool)
                == Some(true)
                && stats
                    .pointer("/direct_capability/direct_oltp")
                    .and_then(Value::as_bool)
                    == Some(true)
        });
        let observed = |counter| {
            worker_stats.iter().any(|(_, stats)| {
                stats
                    .pointer(counter)
                    .and_then(Value::as_u64)
                    .is_some_and(|count| count > 0)
            })
        };
        if !(active && observed("/direct/entries_applied") && observed("/direct/gets_done")) {
            self.violate(world, "DirectPath", "worker stats did not observe an active direct path, an applied mutation and a completed get");
        }
    }

    /// Check a complete observed public route map for a table and epoch. The
    /// probe stream is needed to observe every publication, including epochs
    /// that no client saw.
    pub fn check_keyspace<K: Ord + Debug>(
        &self,
        world: &World,
        table: &str,
        epoch: u64,
        ranges: &[KeyRange<K>],
    ) {
        let mut ordered: Vec<_> = ranges.iter().collect();
        ordered.sort_by(|a, b| a.start.cmp(&b.start));

        let full = ordered.first().is_some_and(|range| range.start.is_none())
            && ordered.last().is_some_and(|range| range.end.is_none());
        let nonempty = ordered
            .iter()
            .all(|range| match (&range.start, &range.end) {
                (Some(start), Some(end)) => start < end,
                _ => true,
            });
        let adjacent = ordered
            .windows(2)
            .all(|pair| pair[0].end.is_some() && pair[0].end == pair[1].start);
        if !(full && nonempty && adjacent) {
            self.violate(
                world,
                "KeyspaceCoverage",
                format!("table {table} epoch {epoch} does not cover its key space"),
            );
        }
    }

    pub fn check_history(&self, world: &World, verdict: &Verdict) {
        for anomaly in &verdict.anomalies {
            self.violate(
                world,
                &anomaly.kind,
                format!("{} (events {:?})", anomaly.detail, anomaly.events),
            );
        }
    }

    pub fn finish(&self, world: &World) {
        let probes = self.probes();
        let mut checks = self.checks.lock().unwrap();
        checks.step(world);
        for (label, deadline) in std::mem::take(&mut checks.deadlines) {
            checks.violate(world, "Liveness", deadline_message(&label, deadline));
        }
        drop(checks);
        if let Some(probes) = &probes {
            probes.check_routes(world);
        }
        if let Some(Err(error)) = probes.map(|probes| probes.finish()) {
            self.violate(
                world,
                "TraceSink",
                format!("cannot write the nabedra.trace/1 file: {error}"),
            );
        }
    }

    /// A probe snapshot of `worker`'s ledgers, without holding the checks lock.
    fn ledgers(&self, worker: u64) -> Option<Ledgers> {
        self.probes().map(|probes| probes.snapshot(worker))
    }

    pub fn violate(&self, world: &World, invariant: &str, detail: impl Into<String>) {
        self.checks
            .lock()
            .unwrap()
            .violate(world, invariant, detail);
    }

    /// The trace observer adds evidence for each failure to the time-stamped
    /// run trace. The sweep preserves that trace tail beside these violations.
    pub fn violations(&self) -> Vec<String> {
        self.checks.lock().unwrap().violations.clone()
    }

    fn trace(&self, world: &World, line: String) {
        let observer = self.checks.lock().unwrap().trace.clone();
        if let Some(observer) = observer {
            observer(elapsed_ms(world), line);
        }
    }
}

fn deadline_message(label: &str, deadline: Duration) -> String {
    format!(
        "{label} did not converge by virtual {}ms",
        deadline.as_millis()
    )
}

impl Checks {
    /// ResourceBaseline from probes: the ledgers of `worker` that differ from its baseline.
    /// A ledger its baseline did not have starts at zero.
    fn ledgers_moved(&self, worker: u64, now: &Ledgers) -> Vec<String> {
        let Some(baseline) = self.ledger_baseline.get(&worker) else {
            return vec!["no baseline snapshot".into()];
        };
        if now.is_empty() {
            return vec!["no ledger reported".into()];
        }
        let keys: BTreeSet<_> = baseline.keys().chain(now.keys()).collect();
        keys.into_iter()
            .filter_map(|key| {
                let at = |ledgers: &Ledgers| ledgers.get(key).copied().unwrap_or(0);
                (at(baseline) != at(now)).then(|| {
                    let group = key
                        .1
                        .map_or_else(String::new, |group| format!(" of group {group}"));
                    format!("{:?}{group} {} (baseline {})", key.0, at(now), at(baseline))
                })
            })
            .collect()
    }

    fn check_storage_devices(&mut self, world: &World, node: u64, usage: &ResourceUsage) {
        if let Some(info) = self.volumes.get(&node)
            && !usage.disk.keys().copied().eq([info.dev])
        {
            let reported: Vec<_> = usage.disk.keys().collect();
            self.violate(
                world,
                "MountedDeviceIdentity",
                format!(
                    "n{node} reports pool devices {reported:?}, mounted device {}",
                    info.dev
                ),
            );
        }
    }

    fn step(&mut self, world: &World) {
        let mut failed = Vec::new();
        let mut devices = BTreeSet::new();
        for (&node, info) in &self.volumes {
            if !devices.insert(info.dev) {
                failed.push(format!(
                    "n{node} shares device {} with another node",
                    info.dev
                ));
            }
            if world.volume_info(node, &info.mount_path).as_ref() != Some(info)
                || world
                    .volume(node, &info.mount_path)
                    .is_none_or(|image| image.device() != info.dev)
            {
                failed.push(format!(
                    "n{node} mount {} no longer identifies device {}",
                    info.mount_path.display(),
                    info.dev
                ));
            }
        }
        if world.unmounted_path_errors() > 0 {
            failed.push(format!(
                "{} accesses to unmounted paths",
                world.unmounted_path_errors()
            ));
        }
        for detail in failed {
            self.violate(world, "MountedDeviceIdentity", detail);
        }
        let now = world.elapsed();
        let overdue: Vec<_> = self
            .deadlines
            .iter()
            .filter(|(_, deadline)| now > **deadline)
            .map(|(label, &deadline)| deadline_message(label, deadline))
            .collect();
        for detail in overdue {
            self.violate(world, "Liveness", detail);
        }
    }

    fn violate(&mut self, world: &World, name: &str, detail: impl Into<String>) {
        let detail = detail.into();
        // Cap each kind on its own, so a frequent one never hides another.
        let reported = self
            .violations
            .iter()
            .filter(|line| nabedra_sim::shrink::violation_kind(line) == name)
            .count();
        if reported == MAX_VIOLATIONS || !self.reported.insert((name.to_owned(), detail.clone())) {
            return;
        }
        let now = elapsed_ms(world);
        if let Some(trace) = &self.trace {
            trace(now, format!("violation {name}: {detail}"));
        }
        self.violations.push(format!("t={now}ms {name}: {detail}"));
    }
}

#[derive(Debug, PartialEq)]
struct ResourceUsage {
    working_decode_bytes: u64,
    native_threads: u64,
    direct_in_flight: u64,
    disk: BTreeMap<u64, DiskUsage>,
}

#[derive(Debug, PartialEq)]
struct DiskUsage {
    reserved_bytes: u64,
    reservations: u64,
}

impl ResourceUsage {
    fn from_stats(stats: &Value) -> Option<Self> {
        let mut usage = Self {
            working_decode_bytes: stats.get("working_decode_bytes")?.as_u64()?,
            native_threads: 0,
            direct_in_flight: match stats.get("direct")? {
                Value::Null => 0,
                direct => direct.get("in_flight")?.as_u64()?,
            },
            disk: BTreeMap::new(),
        };
        for entry in stats.get("scheduler")?.as_array()? {
            usage.native_threads = usage.native_threads.checked_add(entry.get(1)?.as_u64()?)?;
        }
        for entry in stats.get("storage_reservations")?.as_array()? {
            let device = entry.get("device")?.as_u64()?;
            let disk = DiskUsage {
                reserved_bytes: entry.get("reserved_bytes")?.as_u64()?,
                reservations: entry.get("reservations")?.as_u64()?,
            };
            if usage.disk.insert(device, disk).is_some() {
                return None;
            }
        }
        Some(usage)
    }

    fn within(&self, baseline: &Self) -> bool {
        self.working_decode_bytes <= baseline.working_decode_bytes
            && self.native_threads <= baseline.native_threads
            && self.direct_in_flight <= baseline.direct_in_flight
            && self.disk.keys().eq(baseline.disk.keys())
            && self.disk.iter().all(|(device, now)| {
                let baseline = &baseline.disk[device];
                now.reserved_bytes <= baseline.reserved_bytes
                    && now.reservations <= baseline.reservations
            })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

    use nabedra_oltp_types::sim_probe::{Ledger, Probe, emit, track_ledgers};
    use nabedra_rt::{
        sim::{self, NodeClock, in_node},
        time,
    };
    use nabedra_sim::shrink;
    use serde_json::json;

    use super::*;

    const BASE: u64 = 1_000;
    const CHARGE: u64 = 564;

    fn stats() -> Value {
        json!({"working_decode_bytes": 0, "direct": null, "scheduler": [], "storage_reservations": []})
    }

    /// Captures the baseline of a worker that holds `BASE`, then settles it and judges it
    /// as the workloads do, while `held(ms)` says whether it also holds a charge `ms` after
    /// the capture.
    fn judge(held: impl Fn(u64) -> bool + Send + Sync + 'static) -> Vec<String> {
        sim::run(1, |world| async move {
            world.add_node(1, NodeClock::default());
            let checks = Invariants::new();
            checks.install_probes(&world, "unit");
            let bytes = Arc::new(AtomicU64::new(BASE));
            in_node(1, async {
                track_ledgers(&bytes, |bytes: &AtomicU64| {
                    emit(|| Probe::LedgerSnapshot {
                        ledger: Ledger::ChargedBytes("progress"),
                        group: None,
                        outstanding: bytes.load(SeqCst),
                    });
                });
            })
            .await;
            checks.capture_resources(&world, 1, &stats());
            let start = world.elapsed();
            let charge = async {
                loop {
                    let ms = (world.elapsed() - start).as_millis() as u64;
                    bytes.store(BASE + if held(ms) { CHARGE } else { 0 }, SeqCst);
                    time::sleep(Duration::from_millis(1)).await;
                }
            };
            let settle = async {
                let deadline = start + Duration::from_secs(2);
                loop {
                    // A stats request takes a moment.
                    time::sleep(Duration::from_millis(3)).await;
                    let stats = stats();
                    if checks.resources_at_baseline(1, &stats) || world.elapsed() >= deadline {
                        checks.check_resources(&world, 1, &stats);
                        return;
                    }
                    time::sleep(STEP).await;
                }
            };
            let steps = async {
                loop {
                    time::sleep(STEP).await;
                    checks.step(&world);
                }
            };
            tokio::select! {
                biased;
                () = charge => unreachable!("the charge never ends"),
                () = steps => unreachable!("the steps never end"),
                () = settle => {}
            }
            checks.violations()
        })
    }

    #[test]
    fn a_recurring_charge_is_not_back_at_baseline() {
        // B2 of the review of #116. Held `on` of every `on + off` ms.
        for (on, off) in [(9, 1), (50, 1), (200, 1), (5, 5), (50, 10), (10, 3), (7, 6)] {
            // In every phase, so also in phase with the observations.
            for phase in 0..on + off {
                let found = judge(move |ms| (ms + phase) % (on + off) < on);
                let kinds = shrink::kinds(&found);
                assert!(
                    kinds.contains(protocol::RESOURCE_BASELINE),
                    "{on} of every {} ms, phase {phase}: {found:?}",
                    on + off
                );
            }
        }
        let held = judge(|_| true);
        assert!(shrink::kinds(&held).contains(protocol::RESOURCE_BASELINE));
    }

    #[test]
    fn a_worker_is_judged_by_the_observation_that_settled_it() {
        let judged = |settle: bool, other: bool| {
            sim::run(1, |world| async move {
                world.add_node(1, NodeClock::default());
                let checks = Invariants::new();
                checks.capture_resources(&world, 1, &stats());
                // Observations at one instant are one observation.
                for _ in 0..=SETTLED_PASSES {
                    assert!(!settle || !checks.resources_at_baseline(1, &stats()));
                }
                while settle && !checks.resources_at_baseline(1, &stats()) {
                    time::sleep(STEP).await;
                }
                assert!(!settle || world.elapsed() >= 2 * STEP);
                let mut now = stats();
                if other {
                    now["working_decode_bytes"] = json!(0.5);
                }
                checks.check_resources(&world, 1, &now);
                checks.violations()
            })
        };
        assert!(judged(true, false).is_empty());
        assert_eq!(shrink::kinds(&judged(false, false)).len(), 1);
        assert_eq!(shrink::kinds(&judged(true, true)).len(), 1);
    }

    #[test]
    fn a_steady_baseline_holds_no_message_in_flight() {
        let steady = |held: fn(u64) -> bool| {
            sim::run(1, move |world| async move {
                world.add_node(1, NodeClock::default());
                let checks = Invariants::new();
                let observe = || {
                    let mut now = stats();
                    let ms = world.elapsed().as_millis() as u64;
                    now["working_decode_bytes"] = json!(if held(ms) { CHARGE } else { 0 });
                    now
                };
                let mut now = observe();
                while !checks.resources_steady(&world, 1, &now) {
                    time::sleep(STEP).await;
                    now = observe();
                }
                checks.capture_resources(&world, 1, &now);
                assert_eq!(now["working_decode_bytes"], 0);
                world.elapsed().as_millis()
            })
        };
        assert_eq!(steady(|_| false), 20);
        // A message in the first observation, and one in the second.
        assert_eq!(steady(|ms| ms < 5), 30);
        assert_eq!(steady(|ms| (10..12).contains(&ms)), 40);
    }

    #[test]
    fn work_that_ends_and_short_messages_are_back_at_baseline() {
        assert!(judge(|_| false).is_empty());
        // Work that ends within the settling limit.
        assert!(judge(|ms| ms < 700).is_empty());
        // A heartbeat every 50 ms that holds its frame for 2 ms.
        for phase in 0..50 {
            let found = judge(move |ms| (ms + phase) % 50 < 2);
            assert!(found.is_empty(), "phase {phase}: {found:?}");
        }
    }
}
