//! End-to-end checks over client history, mounted disks and public observations.
//! The optional probe stream lets workloads check protocol events synchronously.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    sync::{Arc, Mutex},
    time::Duration,
};

pub(crate) mod queued;

use nabedra_check::checker::Verdict;
use nabedra_rt::{
    fs::FaultFs,
    sim::{VolumeInfo, World},
};
use nabedra_sim::raft::MAX_VIOLATIONS;
use serde_json::Value;

pub const STEP: Duration = Duration::from_millis(10);
pub const RECOVERY_LIMIT: Duration = Duration::from_secs(30);

pub type TraceObserver = Arc<dyn Fn(u64, String) + Send + Sync>;

pub(crate) fn elapsed_ms(world: &World) -> u64 {
    u64::try_from(world.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[derive(Clone, Default)]
pub struct Invariants {
    checks: Arc<Mutex<Checks>>,
}

#[derive(Default)]
struct Checks {
    volumes: BTreeMap<u64, VolumeInfo>,
    deadlines: BTreeMap<String, Duration>,
    resources: BTreeMap<u64, ResourceUsage>,
    reported: BTreeSet<(String, String)>,
    violations: Vec<String>,
    trace: Option<TraceObserver>,
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

    /// Records the expected identity from the mount operation. A workload can
    /// register a new identity after an intentional wipe or device replacement.
    pub fn track_volume(&self, node: u64, info: VolumeInfo) {
        self.checks.lock().unwrap().volumes.insert(node, info);
    }

    /// Recheck the mounted image and public convergence deadlines at each step.
    pub fn step(&self, world: &World) {
        let mut checks = self.checks.lock().unwrap();
        checks.step(world);
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

    /// At quiescence, capture the transient gauges that worker stats expose.
    /// Retained queue responses and caches can legitimately grow after work.
    pub fn capture_resources(&self, world: &World, worker: u64, stats: &Value) {
        let mut checks = self.checks.lock().unwrap();
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

    /// Checks the public subset of ResourceBaseline. The full resource ledger
    /// and RangeClock pending sets require their probe observations.
    pub fn check_resources(&self, world: &World, worker: u64, stats: &Value) {
        let mut checks = self.checks.lock().unwrap();
        let Some(now) = ResourceUsage::from_stats(stats) else {
            checks.violate(
                world,
                "ResourceBaseline",
                format!("n{worker} stats missing final resource gauges"),
            );
            return;
        };
        checks.check_storage_devices(world, worker, &now);
        let Some(baseline) = checks.resources.get(&worker) else {
            checks.violate(
                world,
                "ResourceBaseline",
                format!("n{worker} has no baseline"),
            );
            return;
        };
        if !now.within(baseline) {
            let detail = format!("n{worker} has {now:?} after quiescence, baseline {baseline:?}");
            checks.violate(world, "ResourceBaseline", detail);
        }
    }

    /// Allows a workload to wait for transient work to settle before asserting
    /// ResourceBaseline. Missing observations leave the expectation pending.
    pub fn resources_at_baseline(&self, worker: u64, stats: &Value) -> bool {
        let checks = self.checks.lock().unwrap();
        let (Some(baseline), Some(now)) = (
            checks.resources.get(&worker),
            ResourceUsage::from_stats(stats),
        ) else {
            return false;
        };
        now.within(baseline)
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
        let mut checks = self.checks.lock().unwrap();
        checks.step(world);
        for (label, deadline) in std::mem::take(&mut checks.deadlines) {
            checks.violate(world, "Liveness", deadline_message(&label, deadline));
        }
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
        if self.violations.len() == MAX_VIOLATIONS
            || !self.reported.insert((name.to_owned(), detail.clone()))
        {
            return;
        }
        let now = elapsed_ms(world);
        if let Some(trace) = &self.trace {
            trace(now, format!("violation {name}: {detail}"));
        }
        self.violations.push(format!("t={now}ms {name}: {detail}"));
    }
}

#[derive(Debug)]
struct ResourceUsage {
    working_decode_bytes: u64,
    native_threads: u64,
    direct_in_flight: u64,
    disk: BTreeMap<u64, DiskUsage>,
}

#[derive(Debug)]
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

type Handler<P> = Box<dyn FnMut(&World, &P, &Invariants) + Send>;
type Describe<P> = Box<dyn Fn(&P) -> String + Send>;

/// Typed adapter for the OLTP probe producer. The product sink can forward its
/// `Probe` to `emit`, while each workload can register checks and fault triggers.
/// Choose a trace descriptor that excludes payload bytes and cryptographic nonces.
pub struct ProbeSink<P> {
    invariants: Invariants,
    describe: Describe<P>,
    handlers: Vec<Handler<P>>,
}

impl<P: 'static> ProbeSink<P> {
    pub fn new(invariants: Invariants, describe: impl Fn(&P) -> String + Send + 'static) -> Self {
        Self {
            invariants,
            describe: Box::new(describe),
            handlers: Vec::new(),
        }
    }

    pub fn subscribe(&mut self, handler: impl FnMut(&World, &P, &Invariants) + Send + 'static) {
        self.handlers.push(Box::new(handler));
    }

    pub fn install(self, world: &World) {
        world.insert_ext(self);
    }

    pub fn emit(world: &World, probe: &P) {
        world.with_ext::<Self, _>(|sink| {
            sink.invariants
                .trace(world, format!("probe {}", (sink.describe)(probe)));
            for handler in &mut sink.handlers {
                handler(world, probe, &sink.invariants);
            }
        });
    }
}
