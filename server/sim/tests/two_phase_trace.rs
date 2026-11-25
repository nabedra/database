//! The trace writer end to end: a simulated two-phase commit whose traces TLC checks against
//! `specs/selftest/TwoPhaseTrace.tla` (the `sim-two-phase-trace` check).
//!
//! One transaction manager with a decision log on a simulated volume and three resource
//! managers run on the real World, over a [`NetPlan`] that drops, delays and duplicates
//! messages, with crashes of the manager and one-way partitions. The same test binary writes
//! traces that TLC must accept, and traces it must refuse:
//!
//! - hand-edited ones: a step dropped, a fault dropped, two steps swapped, a variable and a
//!   message corrupted. Each names the line TLC must stop at (`.at-<n>.ndjson`);
//! - traces of two planted defects of the workload itself, which the writer accepts because
//!   they are well formed and only the spec can refuse;
//! - traces that carry less than their actions declare, under `incomplete/`. The writer
//!   refuses to write them. Whether a hand-edited file is refused is a question about the
//!   trace specs and the lint, so a check of its own (`sim-two-phase-trace-incomplete`) asks
//!   it. Each name says who must refuse it: TLC at a line, or the lint (`.at-lint.ndjson`).
//!   The unedited traces they were made from are in `clean/` and must be accepted there.
//!
//! With `NABEDRA_TRACE_DIR` set the accepted traces go there and the others to directories
//! beside it, so no trace directory holds another; otherwise everything goes to a temporary
//! directory. Outside the check only what Rust can see is checked: that the runs do what they
//! should, that the bytes replay, and that every refused trace differs from an accepted one
//! as intended.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use nabedra_check::Gen;
use nabedra_rt::fs::OpenMode;
use nabedra_rt::sim::{Cut, NodeClock, World};
use nabedra_sim::net::{Filters, NetPlan, Plan};
use nabedra_sim::rng::stream;
use nabedra_sim::runtime::{self, virtual_ms};
use nabedra_sim::sweep::workload_gen;
use nabedra_sim::tla_trace::{Action, Builder, FaultLine, NodeRef, set};
use nabedra_sim::{SplitMix64, TlaSteps, TlaTrace, Trace};
use serde_json::{Value, json};
use tokio::sync::mpsc;

const TM: u64 = 1;
const RMS: [u64; 3] = [2, 3, 4];
/// The manager's decisions are one message to a multicast point, as in the spec, where one
/// record in the channel serves every resource manager.
const MULTICAST: u64 = 9;
const LOG_DIR: &str = "/tm";
const DECISION_TIMER: Duration = Duration::from_millis(400);
const RESEND_TIMER: Duration = Duration::from_millis(150);
const CHAOS_MS: u64 = 1_200;
/// Past 2^31 ns, so every trace has times TLC's integers cannot hold.
const RUN_MS: u64 = 2_500;
const MAX_CRASHES: u64 = 2;
const MAX_DROPS: u64 = 64;

/// A seeded defect of the workload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Defect {
    None,
    /// Commits once any resource manager is prepared (the spec's mutant of that name).
    CommitOnePrepared,
    /// Announces a decision without syncing it, so a crash forgets it.
    UnsyncedDecision,
    /// The manager's task panics right after it announces a decision.
    ManagerPanics,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Message {
    Prepared(u64),
    Commit,
    Abort,
}

impl Message {
    fn record(self) -> Value {
        match self {
            Self::Prepared(rm) => json!({"type": "Prepared", "rm": name(rm)}),
            Self::Commit => json!({"type": "Commit"}),
            Self::Abort => json!({"type": "Abort"}),
        }
    }
}

fn name(node: u64) -> String {
    match node {
        TM => "tm".into(),
        rm => format!("r{}", rm - 1),
    }
}

/// The trace of one run: what each TwoPhase action must carry, and how the simulated faults
/// become the spec's.
fn trace_builder() -> Builder {
    const TM_VARS: [&str; 3] = ["tmDecision", "tmPrepared", "tmUp"];
    let mut builder = TlaTrace::builder("TwoPhase", "two-phase")
        .params(json!({
            "RM": set(RMS.map(name)),
            "MaxCrashes": MAX_CRASHES,
            "MaxDrops": MAX_DROPS,
        }))
        .node(TM, "tm")
        .action(
            Action::new("RMPrepare", &["rmState"])
                .msg(&["rm"])
                .msg_value("type", "Prepared"),
        )
        .action(Action::new("RMChooseToAbort", &["rmState"]))
        .action(Action::new("RMRcvCommit", &["rmState"]).msg_value("type", "Commit"))
        .action(Action::new("RMRcvAbort", &["rmState"]).msg_value("type", "Abort"))
        .action(
            Action::new("TMRcvPrepared", &TM_VARS)
                .msg(&["rm"])
                .msg_value("type", "Prepared"),
        )
        .action(Action::new("TMCommit", &TM_VARS).msg_value("type", "Commit"))
        .action(Action::new("TMTimeout", &TM_VARS).msg_value("type", "Abort"))
        .action(Action::new("TMResend", &TM_VARS).msg(&["type"]))
        // The World crashes and restarts the process, so what the process knows is not
        // there to log. The volatile state is, by definition of a crash.
        .action(Action::new("TMCrash", &["tmPrepared", "tmUp"]))
        .action(Action::new("TMRestart", &["tmPrepared", "tmUp"]))
        .action(
            Action::unconstrained(
                "DropMessage",
                "it changes only msgs and the drop counter; the message is in msg",
            )
            .msg(&["type"])
            .msg_optional("rm"),
        )
        .faults(fault_line);
    for rm in RMS {
        builder = builder.node(rm, &name(rm));
    }
    builder
}

fn fault_line(mut line: FaultLine) -> Option<FaultLine> {
    let up = match line.action.as_str() {
        "Crash" => false,
        "Restart" => true,
        // The network loses a copy. The spec loses the record, so the line stands only for
        // the last copy ([`Cluster::send`] says which). Its trace spec wants every drop at
        // the manager, whoever sent the message.
        "DropMessage" => {
            line.node = NodeRef::World(TM);
            line.msg = Some(line.about.take()?);
            return Some(line);
        }
        // Copies and blocked links show up as the drops they cause.
        _ => return None,
    };
    if line.node != NodeRef::World(TM) {
        return Some(line);
    }
    line.action = format!("TM{}", line.action);
    line.vars = json!({"tmPrepared": [], "tmUp": up});
    line.msg = None;
    Some(line)
}

struct Shared {
    world: Arc<World>,
    net: NetPlan,
    rng: SplitMix64,
    trace: Trace,
    steps: Option<TlaSteps>,
    rms: BTreeMap<u64, &'static str>,
    /// Copies of each message between its send and its delivery.
    copies: BTreeMap<Message, u64>,
    inbox: Option<mpsc::UnboundedSender<u64>>,
    defect: Defect,
}

#[derive(Clone)]
struct Cluster(Arc<Mutex<Shared>>);

impl Cluster {
    fn lock(&self) -> MutexGuard<'_, Shared> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn step(&self, node: u64, action: &str, vars: Value, message: Option<Message>) {
        let mut shared = self.lock();
        let now = virtual_ms(&shared.world);
        let line = format!("{action} {} {vars} {message:?}", name(node));
        shared.trace.record(now, format_args!("{line}"));
        if let Some(steps) = &shared.steps {
            steps.step(node, action, vars, message.map(Message::record));
        }
    }

    /// Sends `message`, and delivers the copies the network lets through.
    fn send(&self, from: u64, to: u64, message: Message) {
        let (delays, rms) = {
            let mut shared = self.lock();
            // With another copy on its way, losing this one loses nothing the spec knows.
            let last = shared
                .copies
                .get(&message)
                .is_none_or(|copies| *copies == 0);
            let about = || if last { message.record() } else { Value::Null };
            let plan = shared.net.plan_about(from, to, about);
            let now = virtual_ms(&shared.world);
            shared
                .trace
                .record(now, format_args!("send {message:?} {plan:?}"));
            let Plan::Deliver {
                delay_ms,
                duplicate,
            } = plan
            else {
                return;
            };
            let copies = 1 + u64::from(duplicate);
            *shared.copies.entry(message).or_default() += copies;
            let delays: Vec<u64> = (0..copies).map(|copy| delay_ms + copy * 120).collect();
            // Each resource manager sees a decision a little later than the last one did.
            let rms: Vec<(u64, u64)> = RMS
                .map(|rm| (rm, shared.rng.below(40)))
                .into_iter()
                .collect();
            (delays, rms)
        };
        for delay in delays {
            let (cluster, rms) = (self.clone(), rms.clone());
            nabedra_rt::task::spawn(async move {
                nabedra_rt::time::sleep(Duration::from_millis(delay)).await;
                cluster.deliver(message, &rms).await;
                *cluster.lock().copies.entry(message).or_default() -= 1;
            });
        }
    }

    async fn deliver(&self, message: Message, rms: &[(u64, u64)]) {
        if let Message::Prepared(rm) = message {
            let shared = self.lock();
            if let Some(inbox) = &shared.inbox {
                // The manager may be down; then the message is lost with its process.
                let _ = inbox.send(rm);
            }
            return;
        }
        for (rm, wait) in rms {
            nabedra_rt::time::sleep(Duration::from_millis(*wait)).await;
            self.decide(*rm, message);
        }
    }

    fn rm_state(&self, rm: u64) -> &'static str {
        self.lock().rms[&rm]
    }

    /// Sets a resource manager's durable state, logs the step, then sends its message.
    fn rm_step(&self, rm: u64, action: &str, to: &'static str, message: Option<Message>) {
        self.lock().rms.insert(rm, to);
        self.step(rm, action, json!({"rmState": to}), message);
    }

    fn decide(&self, rm: u64, decision: Message) {
        match (decision, self.rm_state(rm)) {
            (Message::Commit, "prepared") => {
                self.rm_step(rm, "RMRcvCommit", "committed", Some(decision));
            }
            (Message::Abort, "working" | "prepared") => {
                self.rm_step(rm, "RMRcvAbort", "aborted", Some(decision));
            }
            _ => {}
        }
    }

    async fn resource_manager(self, rm: u64, wait_ms: u64, gives_up: bool) {
        nabedra_rt::time::sleep(Duration::from_millis(wait_ms)).await;
        if self.rm_state(rm) != "working" {
            return;
        }
        if gives_up {
            self.rm_step(rm, "RMChooseToAbort", "aborted", None);
            return;
        }
        let message = Message::Prepared(rm);
        self.rm_step(rm, "RMPrepare", "prepared", Some(message));
        self.send(rm, TM, message);
    }

    fn start_manager(&self) {
        let (inbox, messages) = mpsc::unbounded_channel();
        let world = {
            let mut shared = self.lock();
            shared.inbox = Some(inbox);
            shared.world.clone()
        };
        world.spawn_node(TM, Manager::run(self.clone(), messages));
    }
}

/// The transaction manager's process: its volatile state, over the decision log.
struct Manager {
    cluster: Cluster,
    decision: &'static str,
    prepared: BTreeSet<u64>,
}

impl Manager {
    async fn run(cluster: Cluster, mut messages: mpsc::UnboundedReceiver<u64>) {
        let mut manager = Self {
            decision: read_decision(),
            prepared: BTreeSet::new(),
            cluster,
        };
        let deadline = nabedra_rt::time::sleep(DECISION_TIMER);
        tokio::pin!(deadline);
        loop {
            let undecided = manager.decision == "init";
            tokio::select! {
                biased;
                Some(rm) = messages.recv(), if undecided => manager.prepared(rm),
                () = &mut deadline, if undecided => manager.decide("TMTimeout", "aborted"),
                () = nabedra_rt::time::sleep(RESEND_TIMER), if !undecided => {
                    manager.log("TMResend", Some(manager.announcement()));
                    manager.announce();
                }
            }
        }
    }

    fn log(&self, action: &str, message: Option<Message>) {
        let vars = json!({
            "tmDecision": self.decision,
            "tmPrepared": set(self.prepared.iter().copied().map(name)),
            "tmUp": true,
        });
        self.cluster.step(TM, action, vars, message);
    }

    fn prepared(&mut self, rm: u64) {
        if !self.prepared.insert(rm) {
            return;
        }
        self.log("TMRcvPrepared", Some(Message::Prepared(rm)));
        let enough = match self.cluster.lock().defect {
            Defect::CommitOnePrepared => 1,
            _ => RMS.len(),
        };
        if self.prepared.len() >= enough {
            self.decide("TMCommit", "committed");
        }
    }

    /// Makes a decision durable, logs it, then announces it.
    fn decide(&mut self, action: &str, decision: &'static str) {
        let sync = self.cluster.lock().defect != Defect::UnsyncedDecision;
        write_decision(decision, sync);
        self.decision = decision;
        self.log(action, Some(self.announcement()));
        self.announce();
        let defect = self.cluster.lock().defect;
        assert_ne!(defect, Defect::ManagerPanics, "planted manager panic");
    }

    fn announcement(&self) -> Message {
        if self.decision == "committed" {
            Message::Commit
        } else {
            Message::Abort
        }
    }

    fn announce(&self) {
        self.cluster.send(TM, MULTICAST, self.announcement());
    }
}

fn log_path() -> PathBuf {
    Path::new(LOG_DIR).join("decision")
}

fn write_decision(decision: &str, sync: bool) {
    let disk = nabedra_rt::fs::real();
    let file = disk.open(&log_path(), OpenMode::CreateNew).unwrap();
    file.write_all_at(&decision.as_bytes()[..6], 0).unwrap();
    if sync {
        file.sync_all().unwrap();
        disk.sync_dir(Path::new(LOG_DIR)).unwrap();
    }
}

fn read_decision() -> &'static str {
    let disk = nabedra_rt::fs::real();
    if !disk.exists(&log_path()).unwrap() {
        return "init";
    }
    let mut bytes = [0; 6];
    let file = disk.open(&log_path(), OpenMode::Read).unwrap();
    match file.read_exact_at(&mut bytes, 0) {
        Ok(()) if &bytes == b"commit" => "committed",
        Ok(()) if &bytes == b"aborte" => "aborted",
        read => panic!("the decision log holds {bytes:?} ({read:?})"),
    }
}

/// A chaos step and the virtual millisecond it happens at.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Chaos {
    Crash,
    Restart(u8),
    Block(u64),
    Heal,
}

fn chaos(g: &mut Gen, defect: Defect) -> Vec<(u64, Chaos)> {
    let mut plan = Vec::new();
    let mut at = 0;
    for _ in 0..g.below(MAX_CRASHES + 1) {
        at += 50 + g.below(350);
        plan.push((at, Chaos::Crash));
        at += 20 + g.below(130);
        // The planted defect is only visible when the unsynced decision is really gone.
        let cut = if defect == Defect::UnsyncedDecision {
            1
        } else {
            g.below(4) as u8
        };
        plan.push((at, Chaos::Restart(cut)));
    }
    if g.bool() {
        let from = g.below(400);
        plan.push((from, Chaos::Block(*g.pick(&RMS))));
        plan.push((from + 50 + g.below(300), Chaos::Heal));
    }
    plan.sort_unstable();
    plan
}

struct Run {
    hash: u64,
    rms: BTreeMap<u64, &'static str>,
    trace: Option<PathBuf>,
    /// Why the trace was not published.
    refused: Option<String>,
}

/// Where a run's trace goes.
enum Out<'a> {
    Off,
    Env,
    Dir(&'a Path),
}

/// Runs one seed. Everything random comes from it.
fn run(seed: u64, defect: Defect, out: Out<'_>) -> Run {
    runtime::run(seed, |world| async move {
        let mut g = workload_gen(seed);
        for node in [TM].into_iter().chain(RMS) {
            world.add_node(node, NodeClock::default());
        }
        world.mount(TM, LOG_DIR);
        let trace = match out {
            Out::Off => None,
            Out::Env => trace_builder().install(&world).unwrap(),
            Out::Dir(dir) => Some(trace_builder().install_in(&world, dir).unwrap()),
        };
        let mut net = NetPlan::new(seed);
        net.set_filters(Filters {
            drop_per_mille: *g.pick(&[0, 100, 250]),
            dup_per_mille: 200,
            delay_max_ms: 60,
        });
        if let Some(trace) = &trace {
            trace.tap_net(&mut net);
        }
        let cluster = Cluster(Arc::new(Mutex::new(Shared {
            world: world.clone(),
            net,
            rng: stream(seed, "multicast"),
            trace: Trace::with_tail(0),
            steps: trace.as_ref().map(TlaTrace::steps),
            rms: RMS.map(|rm| (rm, "working")).into(),
            copies: BTreeMap::new(),
            inbox: None,
            defect,
        })));
        cluster.start_manager();
        for rm in RMS {
            let (wait, gives_up) = (g.below(250), g.below(12) == 0);
            world.spawn_node(rm, cluster.clone().resource_manager(rm, wait, gives_up));
        }
        for (at, action) in chaos(&mut g, defect) {
            let now = virtual_ms(&world);
            assert!(at >= now && at < CHAOS_MS, "chaos at {at} ms, now {now} ms");
            nabedra_rt::time::sleep(Duration::from_millis(at - now)).await;
            match action {
                Chaos::Crash => {
                    cluster.lock().inbox = None;
                    world.crash(TM);
                }
                Chaos::Restart(cut) => {
                    world.restart_with(
                        TM,
                        match cut {
                            0 => Cut::Kill,
                            1 => Cut::Crash,
                            2 => Cut::PowerLoss { seed },
                            _ => Cut::PowerLoss4k { seed },
                        },
                    );
                    cluster.start_manager();
                }
                Chaos::Block(rm) => cluster.lock().net.block(rm, TM),
                Chaos::Heal => cluster.lock().net.heal_links(),
            }
        }
        nabedra_rt::time::sleep(Duration::from_millis(CHAOS_MS - virtual_ms(&world))).await;
        cluster.lock().net.heal_all();
        nabedra_rt::time::sleep(Duration::from_millis(RUN_MS - CHAOS_MS)).await;
        let finished = trace.map(TlaTrace::finish);
        let (trace, refused) = match finished {
            None => (None, None),
            Some(Ok(path)) => (Some(path), None),
            Some(Err(error)) => (None, Some(error.to_string())),
        };
        world.crash(TM);
        let shared = cluster.lock();
        Run {
            hash: shared.trace.hash(),
            rms: shared.rms.clone(),
            trace,
            refused,
        }
    })
}

/// A parsed trace file that can be edited by hand.
#[derive(Clone)]
struct Doc {
    header: String,
    steps: Vec<Value>,
}

impl Doc {
    fn read(path: &Path) -> Self {
        let text = fs::read_to_string(path).unwrap();
        let mut lines = text.lines();
        Self {
            header: lines.next().unwrap().to_owned(),
            steps: lines
                .map(|line| serde_json::from_str(line).unwrap())
                .collect(),
        }
    }

    fn action(&self, at: usize) -> &str {
        self.steps[at]["action"].as_str().unwrap()
    }

    fn find(&self, from: usize, matches: impl Fn(&Value) -> bool) -> Option<usize> {
        (from..self.steps.len()).find(|at| matches(&self.steps[*at]))
    }

    fn named(&self, from: usize, action: &str) -> Option<usize> {
        self.find(from, |step| step["action"] == action)
    }

    /// Writes the edited trace with the line numbers a writer would have given it, so
    /// nothing but the edit itself is wrong with it.
    fn write(&self, path: &Path) {
        let mut text = self.header.clone() + "\n";
        let mut last = 0;
        for (seq, step) in self.steps.iter().enumerate() {
            let mut step = step.clone();
            step["seq"] = seq.into();
            last = last.max(step["t"].as_u64().unwrap());
            step["t"] = last.into();
            text += &step.to_string();
            text.push('\n');
        }
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, text).unwrap();
    }
}

/// One hand edit: what it does to a trace, and the line at which no behavior of the spec
/// explains the result. `None` when the trace has no place for the edit.
type Edit = fn(&mut Doc) -> Option<usize>;

/// Drops a resource manager's prepare; nobody sent the message the manager then reads.
fn drop_step(doc: &mut Doc) -> Option<usize> {
    let received = doc.named(0, "TMRcvPrepared")?;
    let rm = doc.steps[received]["msg"]["rm"].clone();
    let prepare = doc.find(0, |step| {
        step["action"] == "RMPrepare" && step["node"] == rm
    })?;
    doc.steps.remove(prepare);
    doc.named(0, "TMRcvPrepared")
}

/// Drops a fault line, so the manager restarts without having crashed.
fn drop_fault(doc: &mut Doc) -> Option<usize> {
    let crash = doc.named(0, "TMCrash")?;
    doc.steps.remove(crash);
    let restart = doc.named(0, "TMRestart")?;
    // Nothing but the restart may contradict a manager that never went down.
    (crash..restart)
        .all(|at| !doc.action(at).starts_with("TM"))
        .then_some(restart)
}

/// Swaps the last prepare the manager reads with its commit, so it commits too early. The
/// variables are what the manager would log in that order, so only the order is wrong.
fn reorder_steps(doc: &mut Doc) -> Option<usize> {
    let commit = doc.named(0, "TMCommit")?;
    let received = commit.checked_sub(1)?;
    if doc.action(received) != "TMRcvPrepared" {
        return None;
    }
    let rm = doc.steps[received]["msg"]["rm"].clone();
    let mut fewer = doc.steps[received]["vars"]["tmPrepared"].clone();
    fewer
        .as_array_mut()
        .unwrap()
        .retain(|prepared| *prepared != rm);
    doc.steps.swap(received, commit);
    doc.steps[received]["vars"]["tmPrepared"] = fewer;
    doc.steps[commit]["vars"]["tmDecision"] = "committed".into();
    Some(received)
}

/// Logs a wrong variable: the manager has read one prepare fewer than it says.
fn corrupt_variable(doc: &mut Doc) -> Option<usize> {
    let at = doc.named(0, "TMRcvPrepared")?;
    doc.steps[at]["vars"]["tmPrepared"] = json!([]);
    Some(at)
}

/// Logs a wrong state of a resource manager.
fn corrupt_state(doc: &mut Doc) -> Option<usize> {
    let at = doc.named(0, "RMRcvCommit")?;
    doc.steps[at]["vars"]["rmState"] = "aborted".into();
    Some(at)
}

/// Logs the wrong message: one no resource manager sent yet.
fn corrupt_message(doc: &mut Doc) -> Option<usize> {
    let at = doc.named(0, "TMRcvPrepared")?;
    let unsent = RMS.map(name).into_iter().find(|rm| {
        doc.find(0, |step| {
            step["action"] == "RMPrepare" && step["node"] == rm.as_str()
        })
        .is_none_or(|sent| sent > at)
    })?;
    doc.steps[at]["msg"]["rm"] = unsent.as_str().into();
    Some(at)
}

const EDITS: [(&str, Edit); 6] = [
    ("drop-step", drop_step),
    ("drop-fault", drop_fault),
    ("reorder-steps", reorder_steps),
    ("corrupt-variable", corrupt_variable),
    ("corrupt-state", corrupt_state),
    ("corrupt-message", corrupt_message),
];

/// Traces that carry less than their actions declare, or something TLC misreads.
/// With each the line TLC must refuse, or `None` when the lint must refuse the file.
fn incomplete(doc: &Doc) -> Vec<(&'static str, Option<usize>, Doc)> {
    let first = doc.named(0, "TMRcvPrepared").unwrap();
    let mut edits = Vec::new();
    let mut edit = |label, change: &dyn Fn(&mut Doc) -> Option<usize>| {
        let mut doc = doc.clone();
        let at = change(&mut doc);
        edits.push((label, at, doc));
    };
    // One required variable is missing from one line.
    edit("omitted-variable", &|doc| {
        let vars = doc.steps[first]["vars"].as_object_mut().unwrap();
        vars.remove("tmPrepared").unwrap();
        Some(first)
    });
    // The variable is there, wrong, under a misspelt name.
    edit("misspelt-variable", &|doc| {
        let vars = doc.steps[first]["vars"].as_object_mut().unwrap();
        vars.remove("tmPrepared").unwrap();
        vars.insert("tmPrepard".into(), json!(["r1", "r2", "r3"]));
        Some(first)
    });
    // Every variable is there and right, beside one the spec does not have.
    edit("unknown-variable", &|doc| {
        let vars = doc.steps[first]["vars"].as_object_mut().unwrap();
        vars.insert("tmPrepard".into(), json!(["r1", "r2", "r3"]));
        Some(first)
    });
    // Nothing but action names, nodes and messages.
    edit("bare-actions", &|doc| {
        let first = doc.find(0, |step| step["vars"] != json!({}));
        for step in &mut doc.steps {
            step["vars"] = json!({});
        }
        first
    });
    // The commit announces nothing, or an abort.
    let commit = doc.named(0, "TMCommit").unwrap();
    edit("missing-message", &|doc| {
        doc.steps[commit]
            .as_object_mut()
            .unwrap()
            .remove("msg")
            .unwrap();
        Some(commit)
    });
    edit("wrong-message", &|doc| {
        doc.steps[commit]["msg"]["type"] = "Abort".into();
        Some(commit)
    });
    // A wrong value that TLC reads as the right one: 2^32 + n wraps to n.
    edit("oversized-integer", &|doc| {
        doc.header = doc
            .header
            .replace("\"MaxCrashes\":2", "\"MaxCrashes\":4294967298");
        assert!(doc.header.contains("4294967298"));
        None
    });
    edits
}

/// A directory for a test of its own, removed at the end.
struct Scratch(PathBuf);

impl Scratch {
    fn new(label: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("nabedra-2pc-{label}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// 24 small seeds, and two that TLC's 32-bit integers cannot hold.
fn seeds() -> Vec<u64> {
    (1..=24).chain([1 << 32, u64::MAX]).collect()
}

#[test]
fn traces_for_tlc() {
    let scratch;
    let (root, out): (PathBuf, fn(&Path) -> Out<'_>) = match std::env::var_os("NABEDRA_TRACE_DIR") {
        Some(dir) => {
            let root = PathBuf::from(dir);
            let stale = fs::read_dir(&root)
                .map(|entries| entries.count())
                .unwrap_or(0);
            assert_eq!(stale, 0, "{} is not empty", root.display());
            (root, |_| Out::Env)
        }
        None => {
            scratch = Scratch::new("traces");
            (scratch.0.join("accepted"), |dir| Out::Dir(dir))
        }
    };

    let mut seen: BTreeMap<String, u64> = BTreeMap::new();
    let mut docs = Vec::new();
    for seed in seeds() {
        let run = run(seed, Defect::None, out(&root));
        let states: BTreeSet<&str> = run.rms.values().copied().collect();
        assert!(
            states.iter().eq(["aborted"].iter()) || states.iter().eq(["committed"].iter()),
            "seed {seed}: the resource managers ended {:?}",
            run.rms
        );
        assert_eq!(run.refused, None);
        let path = run.trace.expect("the run was traced");
        assert_eq!(
            path.file_name().unwrap().to_str().unwrap(),
            format!("TwoPhase-{seed}.ndjson")
        );
        let doc = Doc::read(&path);
        assert!(doc.steps.last().unwrap()["t"].as_u64().unwrap() > 1 << 31);
        for at in 0..doc.steps.len() {
            *seen.entry(doc.action(at).to_owned()).or_default() += 1;
        }
        docs.push((seed, doc));
    }
    // The accepted traces reach every action of the spec, so none is accepted for free.
    let actions = [
        "DropMessage",
        "RMChooseToAbort",
        "RMPrepare",
        "RMRcvAbort",
        "RMRcvCommit",
        "TMCommit",
        "TMCrash",
        "TMRcvPrepared",
        "TMResend",
        "TMRestart",
        "TMTimeout",
    ];
    assert!(seen.keys().eq(actions.iter()), "{seen:?}");

    let rejected = root.with_file_name("rejected");
    for (label, edit) in EDITS {
        // Three seeds each, so no rejection rests on one schedule.
        let edited: Vec<(u64, usize, Doc)> = docs
            .iter()
            .filter_map(|(seed, doc)| {
                let mut edited = doc.clone();
                let at = edit(&mut edited)?;
                Some((*seed, at, edited))
            })
            .take(3)
            .collect();
        assert_eq!(edited.len(), 3, "{label} fits too few traces");
        for (seed, at, doc) in edited {
            doc.write(&rejected.join(format!("TwoPhase-{seed}-{label}.at-{at}.ndjson")));
        }
    }

    planted(&root, &rejected);

    let (seed, doc) = docs
        .iter()
        .find(|(_, doc)| doc.named(0, "TMCommit").is_some())
        .expect("a run commits");
    let dir = root.with_file_name("incomplete");
    for (label, at, doc) in incomplete(doc) {
        let at = at.map_or_else(|| "lint".into(), |at| at.to_string());
        doc.write(&dir.join(format!("TwoPhase-{label}.at-{at}.ndjson")));
    }
    // A wrong value under a key the line also has right. No `Value` can hold it, so the
    // writer cannot write it; the file is edited as text.
    let recorded = root.join(format!("TwoPhase-{seed}.ndjson"));
    let text = fs::read_to_string(&recorded).unwrap();
    let key = "\"vars\":{\"rmState\":";
    assert!(text.contains(key));
    let twice = text.replacen(key, "\"vars\":{\"rmState\":\"aborted\",\"rmState\":", 1);
    fs::write(dir.join("TwoPhase-duplicate-key.at-lint.ndjson"), twice).unwrap();

    // What the incomplete traces were made from: the edits are all that is wrong with them.
    let clean = root.with_file_name("clean");
    doc.write(&clean.join(format!("TwoPhase-{seed}-rewritten.ndjson")));
    fs::copy(&recorded, clean.join(format!("TwoPhase-{seed}.ndjson"))).unwrap();

    let count = |dir: &Path| {
        fs::read_dir(dir)
            .unwrap()
            .filter(|entry| {
                let name = entry.as_ref().unwrap().file_name().into_string().unwrap();
                name.ends_with(".ndjson")
            })
            .count()
    };
    assert_eq!(count(&root), seeds().len());
    assert_eq!(count(&rejected), EDITS.len() * 3 + 2);
    assert_eq!(count(&dir), 8);
    assert_eq!(count(&clean), 2);
}

/// Traces of the two defects. Their first impossible step is computed from the defect, and
/// the same seeds without it are among the accepted traces.
fn planted(root: &Path, rejected: &Path) {
    let scratch = Scratch::new("planted");
    // Commits with a prepare missing: the commit itself is impossible.
    let early = |doc: &Doc| {
        doc.named(0, "TMCommit").filter(|at| {
            doc.steps[*at]["vars"]["tmPrepared"]
                .as_array()
                .unwrap()
                .len()
                < RMS.len()
        })
    };
    // Decides, crashes and forgets: the first thing the new process does is impossible.
    let forgot = |doc: &Doc| {
        let decided = doc.find(0, |step| {
            step["action"] == "TMCommit" || step["action"] == "TMTimeout"
        })?;
        let restart = doc.named(decided, "TMRestart")?;
        let next = doc.find(restart + 1, |step| step["node"] == "tm")?;
        (doc.action(next) != "TMResend").then_some(next)
    };
    type FirstBad<'a> = &'a dyn Fn(&Doc) -> Option<usize>;
    for (label, defect, first_bad) in [
        (
            "commit-one-prepared",
            Defect::CommitOnePrepared,
            &early as FirstBad<'_>,
        ),
        ("unsynced-decision", Defect::UnsyncedDecision, &forgot),
    ] {
        let found = seeds().into_iter().find_map(|seed| {
            let path = run(seed, defect, Out::Dir(&scratch.0)).trace.unwrap();
            let at = first_bad(&Doc::read(&path))?;
            Some((seed, at, path))
        });
        let (seed, at, path) = found.unwrap_or_else(|| panic!("no seed shows {label}"));
        assert!(root.join(format!("TwoPhase-{seed}.ndjson")).exists());
        // Unedited: the writer had no reason to refuse it.
        fs::copy(
            &path,
            rejected.join(format!("TwoPhase-{seed}-{label}.at-{at}.ndjson")),
        )
        .unwrap();
        fs::remove_dir_all(&scratch.0).unwrap();
        fs::create_dir_all(&scratch.0).unwrap();
    }
}

#[test]
fn tracing_changes_no_run_and_replays_byte_for_byte() {
    let scratch = Scratch::new("replay");
    let mut hashes = BTreeSet::new();
    for seed in [1, 2, 3, 9] {
        let off = run(seed, Defect::None, Out::Off);
        let on = run(seed, Defect::None, Out::Dir(&scratch.0));
        assert_eq!((off.hash, &off.rms), (on.hash, &on.rms));
        assert!(off.trace.is_none());
        // The writer itself compares the replay with the first trace.
        let again = run(seed, Defect::None, Out::Dir(&scratch.0));
        assert_eq!(again.trace, on.trace);
        hashes.insert(off.hash);
    }
    assert_eq!(hashes.len(), 4, "the seeds ran the same schedule");
}

/// The run goes on without its manager, and what it logged is a valid prefix, which TLC
/// accepts. Only the writer can tell.
#[test]
fn a_run_whose_manager_panicked_publishes_nothing() {
    let scratch = Scratch::new("manager-panics");
    for seed in seeds() {
        let run = run(seed, Defect::ManagerPanics, Out::Dir(&scratch.0));
        let refused = run
            .refused
            .unwrap_or_else(|| panic!("seed {seed} was published"));
        assert!(refused.contains("panicked at"), "{refused}");
    }
    let published = fs::read_dir(&scratch.0)
        .unwrap()
        .filter(|entry| entry.as_ref().unwrap().path().extension() == Some("ndjson".as_ref()))
        .count();
    assert_eq!(published, 0);
}

/// `tla_trace.py` does not require the postcondition that makes a rejection a rejection.
#[test]
fn the_trace_cfg_rejects_unexplained_lines() {
    let cfg = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../specs/selftest/TwoPhaseTrace.cfg");
    let cfg = fs::read_to_string(cfg).unwrap();
    let lists = |keyword: &str, name: &str| {
        cfg.lines().any(|line| {
            let mut words = line.split_whitespace();
            words.next() == Some(keyword) && words.any(|word| word == name)
        })
    };
    assert!(lists("POSTCONDITION", "TraceAccepted"), "{cfg}");
    assert!(lists("INVARIANTS", "TraceIndexIsLevel"), "{cfg}");
    assert!(lists("CHECK_DEADLOCK", "FALSE"), "{cfg}");
}

/// What the writer does with the lines of the `incomplete` files.
#[test]
fn the_writer_refuses_what_the_incomplete_files_carry() {
    let cases: [(&str, &str, Value); 4] = [
        (
            "omitted-variable",
            "required variables are missing: [\"tmPrepared\"]",
            json!({"tmDecision": "init", "tmUp": true}),
        ),
        (
            "misspelt-variable",
            "required variables are missing: [\"tmPrepared\"]",
            json!({"tmDecision": "init", "tmPrepard": ["r1"], "tmUp": true}),
        ),
        ("bare-actions", "required variables are missing", json!({})),
        (
            "oversized-integer",
            "32 bits",
            json!({"tmDecision": 4_294_967_298_i64, "tmPrepared": ["r1"], "tmUp": true}),
        ),
    ];
    for (label, why, vars) in cases {
        let scratch = Scratch::new(label);
        let error = runtime::run(1, |world| {
            world.add_node(TM, NodeClock::default());
            let trace = trace_builder().install_in(&world, &scratch.0).unwrap();
            let message = Some(Message::Prepared(2).record());
            trace.steps().step(TM, "TMRcvPrepared", vars, message);
            std::future::ready(trace.finish().unwrap_err().to_string())
        });
        assert!(error.contains(why), "{label}: {error}");
        assert!(!scratch.0.join("TwoPhase-1.ndjson").exists(), "{label}");
    }
    for (label, why, message) in [
        ("missing-message", "the required message is missing", None),
        (
            "wrong-message",
            "message type is \"Abort\", not \"Commit\"",
            Some(Message::Abort.record()),
        ),
    ] {
        let scratch = Scratch::new(label);
        let error = runtime::run(1, |world| {
            world.add_node(TM, NodeClock::default());
            let trace = trace_builder().install_in(&world, &scratch.0).unwrap();
            let vars = json!({"tmDecision": "committed", "tmPrepared": [], "tmUp": true});
            trace.steps().step(TM, "TMCommit", vars, message);
            std::future::ready(trace.finish().unwrap_err().to_string())
        });
        assert!(error.contains(why), "{label}: {error}");
    }
    // A run without steps is not a trace.
    let scratch = Scratch::new("empty");
    let error = runtime::run(1, |world| {
        let trace = trace_builder().install_in(&world, &scratch.0).unwrap();
        std::future::ready(trace.finish().unwrap_err().to_string())
    });
    assert!(error.contains("none of its 0 steps"), "{error}");
    assert!(!scratch.0.join("TwoPhase-1.ndjson").exists());
    let scratch = Scratch::new("params");
    let error = runtime::run(1, |world| {
        let builder = trace_builder().params(json!({"MaxCrashes": 4_294_967_298_i64}));
        std::future::ready(
            builder
                .install_in(&world, &scratch.0)
                .unwrap_err()
                .to_string(),
        )
    });
    assert!(error.contains("params: MaxCrashes"), "{error}");
}
