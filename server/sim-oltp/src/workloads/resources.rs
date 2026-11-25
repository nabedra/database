//! The `resources` overlay of design 3.6: "It cancels client futures at a random poll, drains
//! workers during work, and runs with arenas and lanes at their limits".
//!
//! It runs on top of `direct`, `cold` or `move` (`NABEDRA_SIM_WORKLOAD=resources`, or
//! `direct,resources`), and changes nothing when it is off: it draws from its own stream, never
//! from the workload tape, and the client code it hooks asks for a cut only if it is installed.
//!
//! During the chaos window it
//!
//! - cuts a share of the client requests: after a drawn poll, and a drawn time after it, the
//!   request's future is dropped. A client sees a lost reply and retries the identity;
//! - fills the admission limits from one or both gateways, in windows. Requests are shed while a
//!   window is open, and the lanes drain when it ends, by themselves or all at once;
//! - drains one worker while such a window is open, and stalls a process in it: the process
//!   stops right after a drawn clock read, in the middle of a job or a poll, so that deadlines
//!   pass with a request half done. In half of the seeds every process stops now and then for a
//!   few seconds;
//! - suspends syncs for a few milliseconds, in half of the seeds.
//!
//! The process limits it lowers are the ones the harness samples anyway. After the heal it runs
//! one quiet stage: the same load with cuts, between two resource points that must be equal
//! ([`crate::invariants::resources`]).

use std::{
    collections::{BTreeMap, BTreeSet},
    future::{Future, poll_fn},
    pin::pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, Ordering},
    },
    task::Poll,
    time::Duration,
};

use nabedra_check::Gen;
use nabedra_protocol::protocol::ErrorCode;
use nabedra_rt::{
    ids::uuid_v4,
    sim::{ClockRead, World, in_node},
    time,
};
use nabedra_sim::rng::stream_seed;
use serde_json::{Value, json};
use tokio::{sync::watch, task::JoinHandle};

use super::{
    Scenarios,
    clients::{self, Ctx, Request},
};
use crate::{
    bootstrap::{CLIENTS, GATEWAYS, WORKERS},
    config::{Config, MIN_APPLY_REPLY_BYTES},
    disk::DiskCut,
    history::Completion,
    invariants::{Invariants, STEP, resources::Stage},
    nemesis::apply::Chaos,
    node::Cluster,
};

/// What a client whose request was cut sees.
pub(crate) const CANCELLED: &str = "the client cancelled the request";
/// The keys that every read pass covers. Every other key is written once.
const READ_KEYS: i64 = 8;
/// How long a stage may wait for the request deadline of its cut requests and the settling.
const STAGE_SETTLE: Duration = Duration::from_secs(2);
/// A request deadline that a stall can outlast.
const SHORT: Duration = Duration::from_millis(500);
/// How much longer than a deadline a stall may last.
const PAST: Duration = Duration::from_millis(150);
/// The share, per mille, of polls and jobs that a time with many stalls stops.
const FREEZES: u32 = 4;
/// Bound of the point before the stage, and of the convergence after it.
const STAGE_POINT: Duration = Duration::from_secs(10);

/// The cuts of one run, in the World while the overlay is on.
struct Cuts {
    g: Gen,
    per_mille: u64,
    storm: watch::Sender<u64>,
    /// Cut requests, by the poll after which they were cut. Zero is a storm.
    cut: BTreeMap<u32, u64>,
    sent: u64,
}

/// Where one request is cut.
pub(crate) struct Cut {
    polls: Option<u32>,
    delay: Duration,
    storm: watch::Receiver<u64>,
}

/// Where the next request of `client` is cut, if the overlay is on and cuts it.
pub(crate) fn cut(client: u64) -> Option<Cut> {
    if !CLIENTS.contains(&client) {
        return None;
    }
    nabedra_rt::sim::world()?.with_ext::<Cuts, _>(Cuts::draw)?
}

impl Cuts {
    fn draw(&mut self) -> Option<Cut> {
        if self.per_mille == 0 {
            return None;
        }
        self.sent += 1;
        let polls = 1 + self.g.below(LONGEST_CALL) as u32;
        let longest = *self.g.pick(&[0, 2, 20, 300]);
        let delay = Duration::from_millis(self.g.below(longest + 1));
        Some(Cut {
            polls: (self.g.below(1_000) < self.per_mille).then_some(polls),
            delay,
            storm: self.storm.subscribe(),
        })
    }
}

impl Cut {
    /// Runs `call` up to its cut. `None` if it was cut.
    pub(crate) async fn run<F: Future>(mut self, call: F) -> Option<F::Output> {
        let mut call = pin!(call);
        let mut polls = 0;
        let cut_after = self.polls;
        let first = poll_fn(|cx| {
            polls += 1;
            match call.as_mut().poll(cx) {
                Poll::Ready(output) => Poll::Ready(Some(output)),
                Poll::Pending if Some(polls) == cut_after => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        });
        tokio::select! {
            biased;
            output = first => if output.is_some() {
                return output;
            },
            _ = self.storm.changed() => {
                count(0);
                return None;
            }
        }
        tokio::select! {
            biased;
            output = &mut call => return Some(output),
            () = time::sleep(self.delay) => count(cut_after.unwrap_or(0)),
            _ = self.storm.changed() => count(0),
        }
        None
    }
}

fn count(polls: u32) {
    if let Some(world) = nabedra_rt::sim::world() {
        world.with_ext::<Cuts, _>(|cuts| *cuts.cut.entry(polls).or_default() += 1);
    }
}

/// A request is polled this often at most: to send, for the head and for the body.
const LONGEST_CALL: u64 = 3;
/// The share of the client requests that are cut while the chaos window is open.
const CHAOS_CUTS: u64 = 150;
/// The same in the quiet stage.
const STAGE_CUTS: u64 = 500;

/// The limits this overlay sets, before any process starts.
pub(crate) fn limits(seed: u64, config: &mut Config, short_deadline: bool) {
    let mut g = Gen::from_seed(stream_seed(seed, "oltp-resources-limits"));
    if g.bool() {
        config.force("executor.max_in_flight", json!(4 + g.below(5)));
    }
    if g.bool() {
        config.force("data.apply_reply_bytes", json!(MIN_APPLY_REPLY_BYTES));
    }
    if short_deadline && g.below(4) > 0 {
        // Short enough to pass while a worker stalls with the request half done.
        config.force("request_deadline_ms", json!(250 + g.below(200)));
    }
    if g.bool() {
        // One slot for each of the five processes.
        config.force("transport.session_slots", json!(5));
    }
}

/// A window of load past the limits.
#[derive(Debug)]
struct Window {
    at: Duration,
    gateways: Vec<usize>,
    lanes: u64,
    each: u64,
    storm_after: Option<Duration>,
    stall: Option<Stall>,
}

/// Where native work is reserved and dispatched. Most other clock reads are in pings, ticks and
/// timers, so a stall at any read seldom stops a request there.
const RESERVATION: &str = "oltp/pool/resources.rs";

/// A process stops after its `nth` clock read from now, in any file or in [`RESERVATION`], for
/// `hold`.
#[derive(Clone, Copy, Debug)]
struct Stall {
    node: u64,
    reservation: bool,
    nth: u64,
    hold: Duration,
}

impl Stall {
    fn draw(g: &mut Gen, deadline: Duration) -> Self {
        // Past a short deadline. A long one cannot pass without losing more than a request.
        let short = deadline <= SHORT;
        let longest = if short {
            (deadline + PAST).as_millis() as u64
        } else {
            150
        };
        // A gateway reserves no native work.
        let reservation = g.below(3) > 0;
        let nodes: Vec<u64> = if reservation {
            WORKERS.into()
        } else {
            WORKERS.into_iter().chain(GATEWAYS).collect()
        };
        Self {
            node: *g.pick(&nodes),
            reservation,
            nth: 1 + g.below(if reservation { 60 } else { 400 }),
            hold: Duration::from_millis(if short {
                longest / 2 + g.below(longest / 2)
            } else {
                20 + g.below(longest - 19)
            }),
        }
    }

    /// `stalled` names the processes with a stall, so each has one at a time.
    async fn run(self, world: Arc<World>, stalled: Arc<Mutex<BTreeSet<u64>>>) {
        let mut read = ClockRead::nth(self.nth);
        if self.reservation {
            read = read.in_file(RESERVATION);
        }
        world.freeze_after_clock_read(self.node, read);
        world.frozen_at_read(self.node).await;
        time::sleep(self.hold).await;
        world.thaw(self.node);
        stalled.lock().unwrap().remove(&self.node);
    }
}

/// How requests of the load ended.
#[derive(Default)]
struct Load {
    ok: u64,
    shed: u64,
    refused: u64,
    unknown: u64,
}

pub(crate) struct Overlay {
    seed: u64,
    pauses: bool,
    windows: Vec<Window>,
    /// When a worker drains, which one, and for how long it stays stopped.
    drain: Option<(Duration, u64, Duration)>,
    restart: Option<(u64, Duration)>,
    drained: Option<u64>,
    /// When every process stops now and then, from and to, and for how long at most.
    freezes: Option<(Duration, Duration, Duration)>,
    table: Option<Ctx>,
    next_key: Arc<AtomicI64>,
    load: Arc<Mutex<Load>>,
    froze: bool,
    tasks: Vec<JoinHandle<()>>,
    stalled: Arc<Mutex<BTreeSet<u64>>>,
    lanes: u64,
    stage: Value,
}

impl Overlay {
    pub(crate) fn draw(seed: u64, chaos: Duration, deadline: Duration) -> Self {
        let mut g = Gen::from_seed(stream_seed(seed, "oltp-resources"));
        let span = chaos.as_millis() as u64;
        let mut windows: Vec<Window> = (0..2 + g.below(2))
            .map(|_| Window {
                at: Duration::from_millis(500 + g.below(span - 2_000)),
                gateways: match g.below(3) {
                    0 => vec![0],
                    1 => vec![1],
                    _ => vec![0, 1],
                },
                lanes: 16 + g.below(49),
                each: 1 + g.below(3),
                storm_after: g.bool().then(|| Duration::from_millis(g.below(50))),
                stall: g.bool().then(|| Stall::draw(&mut g, deadline)),
            })
            .collect();
        windows.sort_by_key(|window| window.at);
        let drain = g.bool().then(|| {
            let at = g.pick(&windows).at + Duration::from_millis(g.below(50));
            let stopped = Duration::from_millis(300 + g.below(2_700));
            (at, *g.pick(&WORKERS), stopped)
        });
        let freezes = g.bool().then(|| {
            let from = Duration::from_millis(1_000 + g.below(span / 2));
            let hold = (deadline + PAST).min(Duration::from_millis(600));
            (from, from + Duration::from_millis(2_000 + g.below(4_000)), hold)
        });
        Self {
            seed,
            freezes,
            pauses: g.bool(),
            windows,
            drain,
            restart: None,
            drained: None,
            table: None,
            next_key: Arc::new(AtomicI64::new(READ_KEYS)),
            load: Arc::default(),
            froze: freezes.is_some(),
            tasks: Vec::new(),
            stalled: Arc::default(),
            lanes: 0,
            stage: Value::Null,
        }
    }

    /// Uses `table`, which must exist before the resource baseline, and starts cutting.
    /// The times of the windows run from now.
    pub(crate) fn arm(&mut self, cluster: &Cluster, ctx: &Ctx, table: String) {
        let own = Ctx::new(
            ctx.world.clone(),
            ctx.checks.clone(),
            ctx.history.clone(),
            table,
            ctx.token.clone(),
            ctx.timeout.saturating_sub(clients::REPLY_GRACE),
            READ_KEYS,
        );
        self.table = Some(own);
        let now = cluster.world.elapsed();
        for window in &mut self.windows {
            window.at += now;
        }
        if let Some((at, ..)) = &mut self.drain {
            *at += now;
        }
        if let Some((from, to, _)) = &mut self.freezes {
            *from += now;
            *to += now;
        }
        cluster.world.insert_ext(Cuts {
            g: Gen::from_seed(stream_seed(self.seed, "oltp-resources-cuts")),
            per_mille: CHAOS_CUTS,
            storm: watch::Sender::new(0),
            cut: BTreeMap::new(),
            sent: 0,
        });
        if self.pauses {
            cluster
                .world
                .set_storage_pauses(50, Duration::from_millis(3));
        }
        cluster.record(format!(
            "resources overlay windows {:?} drain {:?} freezes {:?} pauses {}",
            self.windows, self.drain, self.freezes, self.pauses
        ));
    }

    /// Starts what is due. `chaos` restarts the workers that the overlay drains, if it runs.
    pub(crate) async fn tick(
        &mut self,
        cluster: &mut Cluster,
        ctx: &Ctx,
        mut chaos: Option<&mut Chaos>,
    ) {
        let now = cluster.world.elapsed();
        while self.windows.first().is_some_and(|window| window.at <= now) {
            let window = self.windows.remove(0);
            cluster.record(format!("resources overlay load {window:?}"));
            for gateway in &window.gateways {
                self.press(*gateway, window.lanes, window.each);
            }
            // The nemesis thaws what it froze itself.
            if let Some(stall) = window.stall
                && cluster.frozen().is_empty()
                && cluster.is_running(stall.node)
            {
                self.stall(cluster, stall);
            }
            if let Some(after) = window.storm_after {
                let world = cluster.world.clone();
                self.tasks.push(nabedra_rt::task::spawn(async move {
                    time::sleep(after).await;
                    storm(&world);
                }));
            }
        }
        if let Some((from, to, hold)) = self.freezes
            && from <= now
        {
            let ended = to <= now;
            let per_mille = if ended { 0 } else { FREEZES };
            cluster.world.set_clock_freezes(per_mille, hold);
            if ended {
                self.freezes = None;
            }
        }
        if let Some((at, node, stopped)) = self.drain
            && at <= now
        {
            self.drain = None;
            if cluster.healthy() && cluster.is_running(node) {
                cluster.record(format!("resources overlay drains n{node} during work"));
                self.drained = Some(node);
                match chaos.as_mut() {
                    Some(chaos) => chaos.drain_worker(cluster, ctx, node, stopped).await,
                    None => {
                        if let Err(error) = cluster.drain(node).await {
                            drain_failed(cluster, node, &error);
                        }
                        cluster.crash(node, DiskCut::Kill).await;
                        self.restart = Some((node, now + stopped));
                    }
                }
            } else {
                cluster.record(format!("resources overlay drain of n{node} skipped"));
            }
        }
        if let Some((node, at)) = self.restart
            && at <= now
        {
            self.restart_drained(cluster, node).await;
        }
    }

    async fn restart_drained(&mut self, cluster: &mut Cluster, node: u64) {
        self.restart = None;
        if let Err(error) = cluster.restart_worker(node, DiskCut::Kill).await {
            cluster.checks.violate(
                &cluster.world,
                "Liveness",
                format!("n{node} did not restart after the overlay drained it: {error}"),
            );
        }
    }

    /// Ends the chaos window: no more cuts, load or pauses, and every worker runs.
    pub(crate) async fn stop(&mut self, cluster: &mut Cluster) {
        set_cuts(&cluster.world, 0);
        cluster.world.set_storage_pauses(0, Duration::ZERO);
        cluster.world.set_clock_freezes(0, Duration::ZERO);
        self.freezes = None;
        self.windows.clear();
        self.drain = None;
        if let Some((node, _)) = self.restart {
            self.restart_drained(cluster, node).await;
        }
    }

    /// Waits for the load of the chaos window. Every request has its own deadline.
    async fn join(&mut self) {
        for task in std::mem::take(&mut self.tasks) {
            let _ = task.await;
        }
    }

    fn stall(&mut self, cluster: &Cluster, stall: Stall) {
        // A second one would end at the first one's thaw, and its own freeze would last.
        if !self.stalled.lock().unwrap().insert(stall.node) {
            return;
        }
        let world = cluster.world.clone();
        let stalled = self.stalled.clone();
        self.tasks
            .push(nabedra_rt::task::spawn(stall.run(world, stalled)));
    }

    /// Starts `lanes` concurrent clients of one gateway, each sending `each` requests.
    fn press(&mut self, gateway: usize, lanes: u64, each: u64) {
        let Some(ctx) = &self.table else { return };
        for _ in 0..lanes {
            self.lanes += 1;
            let stream = stream_seed(self.seed, "oltp-resources-lane").wrapping_add(self.lanes);
            self.tasks.push(nabedra_rt::task::spawn(lane(
                ctx.clone(),
                self.next_key.clone(),
                self.load.clone(),
                gateway,
                each,
                Gen::from_seed(stream),
            )));
        }
    }

    /// The quiet stage, on a converged cluster without other load. Returns the clauses whose
    /// assertions ran.
    pub(crate) async fn stage(&mut self, cluster: &Cluster, deadline: Duration) -> Scenarios {
        let mut clauses = Scenarios::new();
        let Some(probes) = cluster.checks.probes() else {
            cluster.record("resources overlay stage not run: no probes".into());
            return clauses;
        };
        self.join().await;
        let mut g = Gen::from_seed(stream_seed(self.seed, "oltp-resources-stage"));
        // One uncut round first, so what a first request keeps (routes, sessions) is in the
        // point before the stage.
        let rounds = 2 + g.below(2);
        let lanes = 24 + g.below(41);
        self.press(0, lanes, 1);
        self.press(1, lanes, 1);
        self.join().await;
        self.read_pass().await;

        let nodes: Vec<u64> = WORKERS.into_iter().chain(GATEWAYS).collect();
        let mut stage = Stage::default();
        let limit = cluster.world.elapsed() + STAGE_POINT;
        let steady = loop {
            let steady = sample(cluster, &nodes, |node, at, ledgers| {
                stage.before(node, at, ledgers)
            });
            if steady || cluster.world.elapsed() >= limit {
                break steady;
            }
            time::sleep(STEP).await;
        };
        cluster.record(format!("resources overlay point before the stage, steady {steady}"));

        let before = probes.guard_counts();
        let (cut_before, _) = cuts(&cluster.world);
        let (ok_before, shed_before) = self.answered();
        set_cuts(&cluster.world, STAGE_CUTS);
        for _ in 0..rounds {
            let stalls = [(); 2].map(|()| Stall::draw(&mut g, deadline));
            cluster.record(format!("resources overlay stage round, {stalls:?}"));
            let many = deadline <= SHORT && g.bool();
            if many {
                // Every process stops now and then, so some request is caught half done.
                cluster.world.set_clock_freezes(FREEZES, deadline + PAST);
            } else {
                for stall in stalls {
                    self.stall(cluster, stall);
                }
            }
            self.press(0, lanes, 2);
            self.press(1, lanes, 2);
            if many {
                time::sleep(Duration::from_millis(200 + g.below(800))).await;
                cluster.world.set_clock_freezes(0, Duration::ZERO);
            }
            if g.bool() {
                time::sleep(Duration::from_millis(g.below(20))).await;
                storm(&cluster.world);
            }
            self.join().await;
        }
        set_cuts(&cluster.world, 0);
        // Leadership may have moved, and that takes messages.
        let limit = cluster.world.elapsed() + STAGE_POINT;
        while !cluster.converged().await {
            if cluster.world.elapsed() >= limit {
                cluster.checks.violate(
                    &cluster.world,
                    "Liveness",
                    format!("no convergence {STAGE_POINT:?} after the overlay's stage"),
                );
                return clauses;
            }
            time::sleep(STEP).await;
        }
        self.read_pass().await;

        let since = cluster.world.elapsed();
        // A cut request may run on the servers up to its own deadline.
        let limit = since + deadline + STAGE_SETTLE;
        loop {
            let settled = sample(cluster, &nodes, |node, at, ledgers| {
                stage.after(node, at, ledgers)
            });
            if settled || cluster.world.elapsed() >= limit {
                break;
            }
            time::sleep(STEP).await;
        }
        let mut back = true;
        for &node in &nodes {
            probes.ran(crate::invariants::resources::OVERLAY_BASELINE, 1);
            if let Some((kind, detail)) = stage.judge(node) {
                back = false;
                cluster.checks.violate(&cluster.world, kind, detail);
            }
            probes.check_lost_guards(&cluster.world, node, since);
        }
        let after = probes.guard_counts();
        let (cut, _) = cuts(&cluster.world);
        let (ok, shed) = self.answered();
        let (unstarted, abandoned) = (
            after.unstarted - before.unstarted,
            after.abandoned - before.abandoned,
        );
        if unstarted > 0 {
            clauses.insert("F03:reservation");
        }
        if abandoned > 0 {
            clauses.insert("F03:dispatch");
        }
        // A stage that cut nothing, or where nothing succeeded, judged nothing.
        if cut == cut_before || ok == ok_before {
            cluster.checks.violate(
                &cluster.world,
                "OverlayProgress",
                format!(
                    "the overlay's stage cut {} requests and {} succeeded",
                    cut - cut_before,
                    ok - ok_before
                ),
            );
        }
        self.stage = json!({
            "steady": steady,
            "back": back,
            "waited_ms": (cluster.world.elapsed() - since).as_millis() as u64,
            "cut": cut - cut_before,
            "ok": ok - ok_before,
            "shed": shed - shed_before,
            "leases_unstarted": unstarted,
            "work_abandoned": abandoned,
        });
        clauses
    }

    fn answered(&self) -> (u64, u64) {
        let load = self.load.lock().unwrap();
        (load.ok, load.shed)
    }

    /// One read of each of the same keys through each gateway.
    async fn read_pass(&self) {
        let Some(ctx) = &self.table else { return };
        let mut client = ctx.new_client();
        for (index, gateway) in GATEWAYS.into_iter().enumerate() {
            for key in 1..=READ_KEYS {
                let node = CLIENTS[index % CLIENTS.len()];
                let id = in_node(node, async { uuid_v4() }).await;
                let read = Request::get(&ctx.table, key, id);
                clients::attempt(ctx, &mut client, node, gateway, &read, true).await;
            }
        }
    }

    /// The run's report: what the overlay did.
    pub(crate) fn summary(&self, checks: &Invariants, world: &World) -> Value {
        let load = self.load.lock().unwrap();
        let guards = checks.probes().map(|probes| probes.guard_counts());
        let polls = world
            .with_ext::<Cuts, _>(|cuts| cuts.cut.clone())
            .unwrap_or_default();
        let (cut, sent) = cuts(world);
        json!({
            "pauses": self.pauses,
            "froze": self.froze,
            "drained": self.drained,
            "requests": sent,
            "cut": cut,
            "cut_by_poll": polls,
            "load": {"ok": load.ok, "shed": load.shed, "refused": load.refused, "unknown": load.unknown},
            "guards": guards.map(|guards| json!({
                "acquired": guards.acquired,
                "released": guards.released,
                "unstarted": guards.unstarted,
                "abandoned": guards.abandoned,
            })),
            "stage": self.stage,
        })
    }
}

/// One client of the load. Its keys are its own, apart from those that every read pass covers.
async fn lane(
    ctx: Ctx,
    next_key: Arc<AtomicI64>,
    load: Arc<Mutex<Load>>,
    gateway: usize,
    each: u64,
    mut g: Gen,
) {
    let mut client = ctx.new_client();
    let node = *g.pick(&CLIENTS);
    for _ in 0..each {
        let id = in_node(node, async { uuid_v4() }).await;
        let request = match g.below(10) {
            0..4 => Request::get(&ctx.table, 1 + g.below(READ_KEYS as u64) as i64, id),
            4..8 => {
                let key = next_key.fetch_add(1, Ordering::Relaxed) + 1;
                Request::row("table.upsert", &ctx.table, key, key, id)
            }
            _ => {
                let rows = 2 + g.below(7) as i64;
                let first = next_key.fetch_add(rows, Ordering::Relaxed) + 1;
                Request::rows(&ctx.table, first, rows, id)
            }
        };
        let answer = clients::attempt(&ctx, &mut client, node, GATEWAYS[gateway], &request, true).await;
        {
            let mut load = load.lock().unwrap();
            *match &answer {
                Completion::Ok(_) => &mut load.ok,
                Completion::Refused(error) if error.code == ErrorCode::Overloaded => &mut load.shed,
                answer if clients::unknown(answer) => &mut load.unknown,
                _ => &mut load.refused,
            } += 1;
        }
        time::sleep(Duration::from_millis(1 + g.below(10))).await;
    }
}

/// Samples every node once. True if `settled` says so for each.
fn sample(
    cluster: &Cluster,
    nodes: &[u64],
    mut settled: impl FnMut(u64, Duration, &crate::invariants::protocol::Ledgers) -> bool,
) -> bool {
    let now = cluster.world.elapsed();
    let mut all = true;
    for &node in nodes {
        let ledgers = cluster.checks.ledgers(node).unwrap_or_default();
        all &= !ledgers.is_empty() && settled(node, now, &ledgers);
    }
    all
}

fn set_cuts(world: &World, per_mille: u64) {
    world.with_ext::<Cuts, _>(|cuts| cuts.per_mille = per_mille);
}

/// Cuts every request in flight.
fn storm(world: &World) {
    world.with_ext::<Cuts, _>(|cuts| cuts.storm.send_modify(|storms| *storms += 1));
}

/// Requests cut so far, and requests sent while cuts were on.
fn cuts(world: &World) -> (u64, u64) {
    world
        .with_ext::<Cuts, _>(|cuts| (cuts.cut.values().sum(), cuts.sent))
        .unwrap_or_default()
}

pub(crate) fn drain_failed(cluster: &Cluster, node: u64, error: &str) {
    cluster.checks.violate(
        &cluster.world,
        "ProcessExit",
        format!("drain of n{node}: {error}"),
    );
}

#[cfg(test)]
mod tests {
    use nabedra_rt::sim;

    use super::*;

    fn installed(world: &World, per_mille: u64) {
        world.insert_ext(Cuts {
            g: Gen::from_seed(1),
            per_mille,
            storm: watch::Sender::new(0),
            cut: BTreeMap::new(),
            sent: 0,
        });
    }

    #[test]
    fn a_request_is_cut_only_where_the_overlay_says() {
        sim::run(1, |world| async move {
            // Off, and for a process that is no client.
            assert!(cut(CLIENTS[0]).is_none());
            installed(&world, 1_000);
            assert!(cut(WORKERS[0]).is_none());
            assert!(cut(CLIENTS[0]).is_some_and(|cut| cut.polls.is_some()));
            set_cuts(&world, 0);
            assert!(cut(CLIENTS[0]).is_none());

            set_cuts(&world, 1_000);
            let at = |polls, delay| Cut {
                polls,
                delay: Duration::from_millis(delay),
                storm: cut(CLIENTS[0]).unwrap().storm,
            };
            let call = |ms| time::sleep(Duration::from_millis(ms));
            // Done before its cut, cut after its first poll, and cut later.
            assert!(at(None, 0).run(call(5)).await.is_some());
            assert!(at(Some(1), 9).run(call(5)).await.is_some());
            assert!(at(Some(1), 0).run(call(5)).await.is_none());
            let start = world.elapsed();
            assert!(at(Some(1), 3).run(call(50)).await.is_none());
            assert_eq!((world.elapsed() - start).as_millis(), 3);
            // A request that ends before that poll is not cut.
            assert!(at(Some(9), 0).run(call(5)).await.is_some());
            assert_eq!(cuts(&world).0, 2);
        });
    }

    #[test]
    fn a_storm_cuts_every_request_in_flight() {
        sim::run(1, |world| async move {
            installed(&world, 1);
            let calls: Vec<_> = (0..3)
                .map(|_| {
                    let cut = Cut {
                        polls: None,
                        delay: Duration::ZERO,
                        storm: cut(CLIENTS[0]).unwrap().storm,
                    };
                    nabedra_rt::task::spawn(cut.run(time::sleep(Duration::from_secs(9))))
                })
                .collect();
            time::sleep(Duration::from_millis(5)).await;
            storm(&world);
            for call in calls {
                assert!(call.await.unwrap().is_none());
            }
            assert_eq!(world.elapsed().as_millis(), 5);
            assert_eq!(cuts(&world).0, 3);
        });
    }

    #[test]
    fn a_seed_draws_the_same_overlay() {
        for seed in 1..50 {
            let chaos = Duration::from_secs(20);
            let [first, second] = [(); 2].map(|()| {
                let overlay = Overlay::draw(seed, chaos, Duration::from_millis(300));
                format!("{:?} {:?} {}", overlay.windows, overlay.drain, overlay.pauses)
            });
            assert_eq!(first, second);
            let [first, second] = [(); 2].map(|()| {
                let mut config = Config::sample(seed);
                limits(seed, &mut config, true);
                config.report()
            });
            assert_eq!(first, second);
        }
    }
}
