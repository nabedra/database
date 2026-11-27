//! End-to-end checks over client history, mounted disks and public observations.
//! The probe stream ([`crate::probes`]) adds the protocol invariants in [`protocol`],
//! checked synchronously as each probe arrives.

pub mod protocol;
pub mod resources;

pub(crate) mod membership;
#[cfg(test)]
mod membership_tests;

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

pub(crate) mod queued;
pub(crate) mod topology;

use nabedra_check::{checker::Verdict, history::Event};
use nabedra_rt::{
    fs::FaultFs,
    sim::{VolumeInfo, World},
};
use nabedra_sim::raft::MAX_VIOLATIONS;
use serde_json::Value;

use nabedra_oltp_types::sim_probe::Ledger;

use crate::{
    history::Bodies,
    probes::{ProbeEvent, Probes, describe},
};
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
    /// Per worker: its process's incarnation when the baseline was taken.
    baseline_incarnation: BTreeMap<u64, u64>,
    /// Per worker: pools that a replica move mounted after the baseline.
    added_pools: BTreeMap<u64, u64>,
    /// Workers that led a range whose replica a move removed.
    removed_learner_leaders: BTreeSet<u64>,
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
    pub fn check_outcomes(&self, world: &World, events: &[Event], bodies: &Bodies) {
        if let Some(probes) = self.probes() {
            probes.check_outcomes(world, events, bodies);
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
        let incarnation = checks.probes.as_ref().map(|probes| probes.incarnation(worker));
        if let Some(incarnation) = incarnation {
            checks.baseline_incarnation.insert(worker, incarnation);
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
        // Lead decision 20: a process that restarted after the baseline has no judged
        // baseline. It stays red, and its line says so, so that it is never mistaken for a
        // leak of the process that took the baseline.
        let restarted = match (&checks.probes, checks.baseline_incarnation.get(&worker)) {
            (Some(probes), Some(&before)) => {
                let now = probes.incarnation(worker);
                (now != before).then(|| format!("{before} to {now}"))
            }
            _ => None,
        };
        let problem = match (problem, restarted) {
            (Some(problem), Some(restarts)) => Some(format!(
                "{problem} (restarted after its baseline, incarnation {restarts}: no judged baseline, lead decision 20)"
            )),
            (problem, _) => problem,
        };
        let problem = problem.map(|problem| {
            if checks.removed_learner_leaders.contains(&worker) && problem.contains("ScratchBytes")
            {
                format!(
                    "{problem} (the signature of open record {REMOVED_LEARNER_CHARGE}: n{worker} led a range whose replica a move removed)"
                )
            } else {
                problem
            }
        });
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
        // The public decode gauge counts cached routes too: compare it without them.
        let routes = (
            ledgers.as_ref().map_or(0, route_cache_bytes),
            checks.ledger_baseline.get(&worker).map_or(0, route_cache_bytes),
        );
        match (
            checks.resources.get(&worker),
            ResourceUsage::from_stats(stats),
        ) {
            (Some(baseline), Some(now)) if now.within(baseline, routes) => {}
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

    /// Declares that a replica move mounted `pools` pools on `worker` after its baseline. Each
    /// is charged [`POOL_OVERHEAD_BYTES`] to the scheduler, which ResourceBaseline then
    /// expects on top of the baseline, exactly.
    pub(crate) fn pools_added(&self, worker: u64, pools: u64) {
        self.checks.lock().unwrap().added_pools.insert(worker, pools);
    }

    /// Declares that `worker` led a range whose replica a move removed, so that a scratch
    /// charge left on it names the open record of that signature.
    pub(crate) fn led_removed_learner(&self, worker: u64) {
        self.checks
            .lock()
            .unwrap()
            .removed_learner_leaders
            .insert(worker);
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
    pub(crate) fn ledgers(&self, worker: u64) -> Option<Ledgers> {
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

/// The Control replica route cache's bounds, as `ROUTE_CACHE_ENTRIES` and
/// `ROUTE_CACHE_ENTRY_BYTES` in oltp-replication's `authority/control/endpoint.rs` state
/// them. Restated, not imported, so that a product change to them shows here.
pub(crate) const ROUTE_CACHE_ENTRIES: u64 = 64;
pub(crate) const ROUTE_CACHE_ENTRY_BYTES: u64 = 4 << 10;

/// What a mounted pool charges the scheduler, as `POOL_OVERHEAD_BYTES` in
/// `nabedra-resources`' `memory/worker.rs` states it. Restated, not imported.
pub(crate) const POOL_OVERHEAD_BYTES: u64 = 64 << 20;

/// The open record whose signature is a scratch charge left on the leader of a range whose
/// replica a move removed.
const REMOVED_LEARNER_CHARGE: &str = "OLTP-sim-removed-learner-charge";

/// The ledgers that count a cached route's working-memory charge with every other one.
const HOLD_ROUTES: [Ledger; 2] = [Ledger::ScratchBytes, Ledger::SchedulerBytes];

/// The route cache is a bounded cache, not transient work: how full it is depends on which
/// groups a process resolved since it started, so a restart or a move changes it. It is
/// judged against its own bounds ([`route_cache_over`]), and everything else stays judged
/// strictly: this returns `ledgers` with the cache's bytes taken out of [`HOLD_ROUTES`], by
/// what the cache itself reports, and its own ledgers gone. Nothing is taken out of a
/// worker whose cache reported nothing, and no fixed allowance ever. Err when the cache
/// reports more bytes than a ledger that includes them holds.
pub(crate) fn without_route_cache(ledgers: &Ledgers) -> Result<Ledgers, String> {
    let routes = route_cache_bytes(ledgers);
    let mut rest = ledgers.clone();
    for ledger in [
        Ledger::RouteCaches,
        Ledger::RouteCacheEntries,
        Ledger::RouteCacheBytes,
    ] {
        rest.remove(&(ledger, None));
    }
    if routes == 0 {
        return Ok(rest);
    }
    for ledger in HOLD_ROUTES {
        let Some(held) = rest.get_mut(&(ledger, None)) else {
            return Err(format!("the route cache holds {routes} B and no {ledger:?} is reported"));
        };
        *held = held.checked_sub(routes).ok_or_else(|| {
            format!("the route cache holds {routes} B, more than all of {ledger:?} {held}")
        })?;
    }
    Ok(rest)
}

/// What the route cache reports holding.
pub(crate) fn route_cache_bytes(ledgers: &Ledgers) -> u64 {
    ledgers
        .get(&(Ledger::RouteCacheBytes, None))
        .copied()
        .unwrap_or(0)
}

/// The route cache's bounds that `ledgers` break, independent of any baseline: at most
/// [`ROUTE_CACHE_ENTRIES`] routes per cache, each at most [`ROUTE_CACHE_ENTRY_BYTES`]. A
/// worker may run more than one Control client, so the bound is per cache that reported.
pub(crate) fn route_cache_over(ledgers: &Ledgers) -> Option<String> {
    let at = |ledger| ledgers.get(&(ledger, None)).copied().unwrap_or(0);
    let (caches, entries) = (at(Ledger::RouteCaches), at(Ledger::RouteCacheEntries));
    let bytes = route_cache_bytes(ledgers);
    let cap = caches * ROUTE_CACHE_ENTRIES;
    (entries > cap || bytes > entries * ROUTE_CACHE_ENTRY_BYTES).then(|| {
        format!(
            "the route cache holds {entries} routes in {bytes} B, over its bound of {cap} routes of at most {ROUTE_CACHE_ENTRY_BYTES} B"
        )
    })
}

/// `before` and `now` without the route cache, to compare strictly, or why the cache is off
/// by itself: over its bounds now, or reporting more than the ledgers that include it.
pub(crate) fn split_route_cache(
    before: &Ledgers,
    now: &Ledgers,
) -> Result<(Ledgers, Ledgers), String> {
    let before = without_route_cache(before).map_err(|error| format!("before, {error}"))?;
    let rest = without_route_cache(now)?;
    match route_cache_over(now) {
        Some(over) => Err(over),
        None => Ok((before, rest)),
    }
}

impl Checks {
    /// ResourceBaseline from probes: the ledgers of `worker` that differ from its baseline.
    /// A ledger its baseline did not have starts at zero. The route cache is split off on
    /// both sides ([`without_route_cache`]) and judged by its bounds alone.
    fn ledgers_moved(&self, worker: u64, now: &Ledgers) -> Vec<String> {
        let Some(baseline) = self.ledger_baseline.get(&worker) else {
            return vec!["no baseline snapshot".into()];
        };
        if now.is_empty() {
            return vec!["no ledger reported".into()];
        }
        let (baseline, mut now) = match split_route_cache(baseline, now) {
            Ok(split) => split,
            Err(off) => return vec![off],
        };
        // A pool a move mounted is charged to the scheduler: expect exactly that on top of the
        // baseline, from the number of pools, never an allowance off the total.
        let pools = self.added_pools.get(&worker).copied().unwrap_or(0);
        if pools > 0 {
            let charge = pools * POOL_OVERHEAD_BYTES;
            let Some(held) = now.get_mut(&(Ledger::SchedulerBytes, None)) else {
                return vec![format!("{pools} added pools and no SchedulerBytes reported")];
            };
            let Some(rest) = held.checked_sub(charge) else {
                return vec![format!(
                    "{pools} added pools charge {charge} B, more than all of SchedulerBytes {held}"
                )];
            };
            *held = rest;
        }
        let (baseline, now) = (&baseline, &now);
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

    /// At or below `baseline`, where `routes` are the bytes the route cache held now and at
    /// the baseline, which the decode gauge includes ([`without_route_cache`]).
    fn within(&self, baseline: &Self, (routes, baseline_routes): (u64, u64)) -> bool {
        self.working_decode_bytes.saturating_add(baseline_routes)
            <= baseline.working_decode_bytes.saturating_add(routes)
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

    /// What a worker holds `ms` after its baseline: routes in its cache, the bytes of each,
    /// and other working memory beyond the baseline's.
    #[derive(Clone, Copy)]
    struct Held {
        routes: u64,
        route_bytes: u64,
        other: u64,
    }

    /// The bytes of one cached route of a group with three replicas, as `sim_triage.py`
    /// measured them.
    const ROUTE: u64 = 692;

    /// Like [`judge`], for a worker with a Control route cache: four routes at the baseline,
    /// then what `held(ms)` says. A cached route is charged to the scratch and scheduler
    /// ledgers and to the public decode gauge, as `reserve_scratch(ByteClass::Data, ..)` does.
    fn judge_routes(held: impl Fn(u64) -> Held + Send + Sync + 'static) -> Vec<String> {
        #[derive(Default)]
        struct Worker {
            routes: AtomicU64,
            route_bytes: AtomicU64,
            other: AtomicU64,
        }
        impl Worker {
            fn scratch(&self) -> u64 {
                BASE + self.routes.load(SeqCst) * self.route_bytes.load(SeqCst)
                    + self.other.load(SeqCst)
            }
            fn set(&self, held: Held) {
                self.routes.store(held.routes, SeqCst);
                self.route_bytes.store(held.route_bytes, SeqCst);
                self.other.store(held.other, SeqCst);
            }
        }
        sim::run(1, |world| async move {
            world.add_node(1, NodeClock::default());
            let checks = Invariants::new();
            checks.install_probes(&world, "unit");
            let worker = Arc::new(Worker::default());
            worker.set(Held {
                routes: 4,
                route_bytes: ROUTE,
                other: 0,
            });
            fn ledger(ledger: Ledger, outstanding: u64) {
                emit(|| Probe::LedgerSnapshot {
                    ledger,
                    group: None,
                    outstanding,
                });
            }
            // The cache reports itself as the product's does: a ledger owner of its own.
            struct Cache(Arc<Worker>);
            let cache = Arc::new(Cache(worker.clone()));
            in_node(1, async {
                track_ledgers(&worker, |worker: &Worker| {
                    ledger(Ledger::ScratchBytes, worker.scratch());
                    ledger(Ledger::SchedulerBytes, worker.scratch());
                });
                track_ledgers(&cache, |Cache(worker): &Cache| {
                    let routes = worker.routes.load(SeqCst);
                    ledger(Ledger::RouteCaches, 1);
                    ledger(Ledger::RouteCacheEntries, routes);
                    ledger(
                        Ledger::RouteCacheBytes,
                        routes * worker.route_bytes.load(SeqCst),
                    );
                });
            })
            .await;
            let stats = || {
                let mut stats = stats();
                stats["working_decode_bytes"] = json!(worker.scratch());
                stats
            };
            checks.capture_resources(&world, 1, &stats());
            let start = world.elapsed();
            let charge = async {
                loop {
                    let ms = (world.elapsed() - start).as_millis() as u64;
                    worker.set(held(ms));
                    time::sleep(Duration::from_millis(1)).await;
                }
            };
            let settle = async {
                let deadline = start + Duration::from_secs(2);
                loop {
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
    fn a_warm_route_cache_is_judged_by_its_bounds() {
        let routes = |routes, route_bytes, other| {
            move |_| Held {
                routes,
                route_bytes,
                other,
            }
        };
        // The clean twins: the same cache, one that a restart or a move filled further or
        // emptied, and one at its cap of 64 routes of at most 4 KiB.
        for clean in [
            routes(4, ROUTE, 0),
            routes(5, ROUTE, 0),
            routes(1, ROUTE, 0),
            routes(64, ROUTE_CACHE_ENTRY_BYTES, 0),
        ] {
            let found = judge_routes(clean);
            assert!(found.is_empty(), "{found:?}");
        }
        // A route filled while the baseline settles, then work that ends.
        let found = judge_routes(|ms| Held {
            routes: 5,
            route_bytes: ROUTE,
            other: if ms < 700 { CHARGE } else { 0 },
        });
        assert!(found.is_empty(), "{found:?}");
    }

    #[test]
    fn a_route_cache_does_not_excuse_a_held_lease() {
        let routes = |routes, route_bytes, other| {
            move |_| Held {
                routes,
                route_bytes,
                other,
            }
        };
        // The failing twins: a warm cache beside a transient lease that is never released,
        // one the size of a route, and one that comes back every 51 ms; a cache over its
        // count and one whose routes are over their size.
        for (leak, says) in [
            (judge_routes(routes(5, ROUTE, ROUTE)), "ScratchBytes"),
            (judge_routes(routes(5, ROUTE, CHARGE)), "SchedulerBytes"),
            (judge_routes(routes(1, ROUTE, 3 * ROUTE)), "ScratchBytes"),
            (judge_routes(routes(65, ROUTE, 0)), "over its bound"),
            (judge_routes(routes(5, ROUTE_CACHE_ENTRY_BYTES + 1, 0)), "over its bound"),
            (
                judge_routes(|ms| Held {
                    routes: 5,
                    route_bytes: ROUTE,
                    other: if ms % 51 < 50 { CHARGE } else { 0 },
                }),
                "ScratchBytes",
            ),
        ] {
            assert!(
                shrink::kinds(&leak).contains(protocol::RESOURCE_BASELINE),
                "{leak:?}"
            );
            assert!(leak.iter().any(|line| line.contains(says)), "{says}: {leak:?}");
        }
    }

    /// A pool that a move mounted is expected on top of the baseline, exactly: one pool more
    /// or less, or a lease beside it, is red.
    #[test]
    fn added_pools_are_expected_exactly() {
        let sched = |bytes| Ledgers::from([((Ledger::SchedulerBytes, None), bytes)]);
        let moved = |pools, now| {
            let mut checks = Checks::default();
            checks.ledger_baseline.insert(1, sched(1_000));
            checks.added_pools.insert(1, pools);
            checks.ledgers_moved(1, &sched(now))
        };
        assert!(moved(2, 1_000 + 2 * POOL_OVERHEAD_BYTES).is_empty());
        assert!(moved(0, 1_000).is_empty());
        for (pools, now) in [
            (1, 1_000 + 2 * POOL_OVERHEAD_BYTES),
            (2, 1_000 + POOL_OVERHEAD_BYTES),
            (2, 1_692 + 2 * POOL_OVERHEAD_BYTES),
            (0, 1_000 + POOL_OVERHEAD_BYTES),
        ] {
            assert_eq!(moved(pools, now).len(), 1, "{pools} pools, {now} B");
        }
    }

    #[test]
    fn a_route_cache_split_is_exact() {
        let ledgers = |scratch, scheduler, routes: Option<(u64, u64)>| {
            let mut ledgers = Ledgers::from([
                ((Ledger::ScratchBytes, None), scratch),
                ((Ledger::SchedulerBytes, None), scheduler),
            ]);
            if let Some((entries, bytes)) = routes {
                ledgers.insert((Ledger::RouteCaches, None), 1);
                ledgers.insert((Ledger::RouteCacheEntries, None), entries);
                ledgers.insert((Ledger::RouteCacheBytes, None), bytes);
            }
            ledgers
        };
        let rest = without_route_cache(&ledgers(5_000, 6_000, Some((2, 1_384)))).unwrap();
        assert_eq!(rest, ledgers(3_616, 4_616, None));
        // Nothing is taken out without a report, and a report larger than its ledgers is
        // an error, not zero.
        assert_eq!(
            without_route_cache(&ledgers(5_000, 6_000, None)).unwrap(),
            ledgers(5_000, 6_000, None)
        );
        assert!(without_route_cache(&ledgers(1_000, 6_000, Some((2, 1_384)))).is_err());
        let mut alone = ledgers(0, 0, Some((2, 1_384)));
        alone.remove(&(Ledger::ScratchBytes, None));
        assert!(without_route_cache(&alone).is_err());
        // Routes without a cache that reported itself are over any bound.
        let mut stray = ledgers(5_000, 6_000, Some((2, 1_384)));
        stray.remove(&(Ledger::RouteCaches, None));
        assert!(route_cache_over(&stray).is_some());
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
