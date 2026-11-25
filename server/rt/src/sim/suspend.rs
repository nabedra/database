//! Suspending code in the middle: storage jobs at a filesystem call, and a whole node
//! between a clock read and the code that uses it.
//!
//! # Storage pauses
//!
//! Blocking work runs on a helper thread while the simulation thread waits, so it is
//! atomic: no crash lands inside it and no other task runs. A *pause* stops the helper
//! thread at a filesystem call and gives control back to the simulation thread. The rest
//! of the cluster, the rest of the node included, runs on in virtual time, as it does
//! while a real thread waits for an fsync. The job then resumes, or its node crashes.
//!
//! - [`World::pause_storage`] stops a chosen call: before it, between a write and its
//!   fsync, or after it, between the fsync and whatever the job acknowledges with it.
//! - [`World::set_storage_pauses`] stops seeded ones.
//!
//! A sync that stops before it runs covers what other jobs write meanwhile, as a thread
//! preempted just before the call. The middle of a sync has no point of its own: a crash
//! before it, with a power-loss cut, leaves each unsynced sector new, old or torn.
//!
//! Both are off by default, and a run that uses neither takes the same steps as before.
//! They apply to [`crate::task::spawn_blocking`] jobs, to one-shots, and to stepped
//! services that their driver steps or that a blocking job drives inline. A service
//! stepped inline on the simulation thread itself stays atomic.
//!
//! A crash of the node unwinds its suspended jobs at once, after the disk cut and before
//! any task is aborted: destructors run, on halted volumes, and no later line of the job
//! does. Code that waits for a service inline ([`crate::task::drive_until`]) and finds
//! nothing else to do resumes its node's suspended jobs, as it would have waited for them.
//!
//! # Clock freezes
//!
//! [`World::freeze`] stops a node's tasks at their next poll, so never between a clock
//! read and the code that follows it without an `.await`. A clock freeze does: right after
//! a read of the node's clock through [`crate::time`] (or [`clock_read`], for clocks this
//! crate does not own) the reading code is suspended and the node frozen, as by a SIGSTOP
//! that lands there. When the node thaws, the code goes on with the value it read.
//!
//! - [`World::freeze_after_clock_read`] picks one read; the harness thaws the node.
//! - [`World::set_clock_freezes`] picks seeded reads and thaws after a seeded time.
//!
//! Both are off by default. While a rule is set for a node, its tasks are polled on a
//! helper thread, as blocking work is, so that a poll can stop in the middle. They are
//! scheduled the same way, but a task they wake joins the runtime's injection queue, so a
//! run with a rule set is another schedule than one without. Polls keep tokio's seeded
//! random choices (`sim::runtime_seeded`). A read inside [`World::with_ext`] never stops.
//! Code outside a node's tasks (`sim::in_node`) and services stepped inline on the
//! simulation thread are not stopped either.
//!
//! # Locks
//!
//! Suspended code keeps the locks it holds. A real thread would too, but there another
//! thread that wants one waits, and here it stops the whole simulation: the run then
//! fails after [`HANG`] of real time. Choose calls that the product makes outside its
//! locks, and look at a frozen node only through means that take none of its locks.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::panic::Location;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::time::Instant;

use super::services::Bell;
use super::{NodeContext, SimEvent, SplitMix64, World, lock, world};
use crate::fs::FsOp;

/// Real time without progress after which a suspended job counts as holding a lock that
/// the rest of the simulation needs.
const HANG: Duration = Duration::from_secs(if cfg!(test) { 10 } else { 60 });

/// Where a pause stops a filesystem call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    /// Before the call: nothing of it has happened.
    Before,
    /// After the call succeeded, before its caller sees the result.
    After,
}

/// A one-shot rule for [`World::pause_storage`].
#[derive(Clone, Debug)]
pub struct StoragePause {
    op: FsOp,
    side: Side,
    nth: u64,
    path_prefix: Option<PathBuf>,
    hold: Option<Duration>,
}

impl StoragePause {
    fn new(op: FsOp, side: Side) -> StoragePause {
        assert!(
            matches!(
                op,
                FsOp::Write | FsOp::SyncData | FsOp::SyncAll | FsOp::SyncDir | FsOp::Rename
            ),
            "{op:?} calls have no pause point"
        );
        StoragePause {
            op,
            side,
            nth: 1,
            path_prefix: None,
            hold: None,
        }
    }

    /// Stops the next `op`, a write, a sync or a rename, before it runs.
    pub fn before(op: FsOp) -> StoragePause {
        StoragePause::new(op, Side::Before)
    }

    /// Stops the next `op` that succeeds, before its caller sees the result.
    pub fn after(op: FsOp) -> StoragePause {
        StoragePause::new(op, Side::After)
    }

    /// Stops the n-th matching call instead (1 is the next one).
    pub fn nth(mut self, n: u64) -> StoragePause {
        assert!(n >= 1, "calls are counted from 1");
        self.nth = n;
        self
    }

    /// Only calls on paths under `prefix` match (for file calls, the path it was opened as).
    pub fn path_prefix(mut self, prefix: impl Into<PathBuf>) -> StoragePause {
        self.path_prefix = Some(prefix.into());
        self
    }

    /// Resumes the job after `hold` of virtual time, unless [`World::resume_storage`] does
    /// it earlier. Without it the job stays suspended until then.
    pub fn hold(mut self, hold: Duration) -> StoragePause {
        self.hold = Some(hold);
        self
    }
}

/// A suspended call, as [`World::paused_storage`] reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PausedCall {
    pub op: FsOp,
    pub side: Side,
    pub path: PathBuf,
}

/// A one-shot rule for [`World::freeze_after_clock_read`].
#[derive(Clone, Debug)]
pub struct ClockRead {
    nth: u64,
    file: Option<String>,
}

impl ClockRead {
    /// The n-th matching read (1 is the next one).
    pub fn nth(n: u64) -> ClockRead {
        assert!(n >= 1, "reads are counted from 1");
        ClockRead { nth: n, file: None }
    }

    /// Only reads in source files whose path ends with `suffix` match. A read through a
    /// helper function counts as the helper's, unless it is `#[track_caller]`.
    pub fn in_file(mut self, suffix: impl Into<String>) -> ClockRead {
        self.file = Some(suffix.into());
        self
    }
}

/// Where a helper thread stopped.
#[derive(Clone, Debug)]
enum At {
    Call(PausedCall),
    Clock,
}

/// Where a helper thread stopped, and for how long at most.
#[derive(Clone, Debug)]
struct Stop {
    at: At,
    hold: Option<Duration>,
}

/// What runs on a helper thread.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    /// Blocking work. It stops at filesystem calls and clock reads.
    Job,
    /// One poll of a task. It stops at clock reads.
    Poll,
}

/// The seeded freeze of one job or poll: after which of its reads, and for how long.
type Freeze = Option<(u64, Duration)>;

/// How a suspended helper thread goes on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Resume {
    Run,
    /// Run without stopping again: another thread waits for the end.
    RunToEnd,
    /// Unwind: the node crashed, or nothing owns the job any more.
    Cancel,
}

/// What a helper thread tells the thread that waits for it.
enum Event {
    Done,
    Suspended(Stop),
}

/// The panic payload that unwinds a cancelled job.
struct Cancelled;

/// The world's pause rules and its suspended jobs.
#[derive(Debug)]
pub(super) struct Faults {
    rules: Mutex<Rules>,
    /// Whether any rule was ever set, so an unarmed run takes no lock.
    armed: AtomicBool,
    suspended: Mutex<Vec<Arc<Suspended>>>,
    /// Rung when a job suspends.
    bell: Bell,
    /// Polls and jobs so far, which tell a suspended thread that the simulation moves.
    progress: AtomicU64,
}

#[derive(Debug)]
struct Rules {
    rng: SplitMix64,
    storage: Vec<(u64, StoragePause)>,
    storage_per_mille: u64,
    storage_max: Duration,
    clock: Vec<(u64, ClockRead)>,
    clock_per_mille: u64,
    clock_max: Duration,
}

/// A seeded freeze lands on one of the first few clock reads of a job or poll.
const READS: u64 = 4;

impl Rules {
    /// A seeded time in `(0, max]`.
    fn draw(&mut self, max: Duration) -> Duration {
        let max = u64::try_from(max.as_nanos()).unwrap_or(u64::MAX);
        Duration::from_nanos(1 + self.rng.below(max))
    }

    fn storage(&mut self, node: u64, op: FsOp, side: Side, path: &Path) -> Option<Stop> {
        let mut hold = None;
        let mut fired = None;
        for (index, (owner, rule)) in self.storage.iter_mut().enumerate() {
            let matches = *owner == node
                && (rule.op, rule.side) == (op, side)
                && rule
                    .path_prefix
                    .as_ref()
                    .is_none_or(|p| path.starts_with(p));
            if matches {
                rule.nth -= 1;
                if rule.nth == 0 {
                    fired = Some(index);
                    break;
                }
            }
        }
        if let Some(index) = fired {
            hold = self.storage.remove(index).1.hold;
        } else {
            let sync = matches!(op, FsOp::SyncData | FsOp::SyncAll | FsOp::SyncDir);
            if self.storage_per_mille == 0
                || !sync
                || self.rng.below(1_000) >= self.storage_per_mille
            {
                return None;
            }
            hold.replace(self.draw(self.storage_max));
        }
        Some(Stop {
            at: At::Call(PausedCall {
                op,
                side,
                path: path.to_path_buf(),
            }),
            hold,
        })
    }

    /// Whether a rule stops this read, which uses the rule up.
    fn clock(&mut self, node: u64, at: &Location<'_>) -> bool {
        let matches = |(owner, read): &&mut (u64, ClockRead)| {
            *owner == node && read.file.as_ref().is_none_or(|f| at.file().ends_with(f))
        };
        let Some((_, read)) = self.clock.iter_mut().find(matches) else {
            return false;
        };
        read.nth -= 1;
        let fired = read.nth == 0;
        self.clock.retain(|(_, read)| read.nth > 0);
        fired
    }

    fn freeze(&mut self) -> Freeze {
        if self.clock_per_mille == 0 || self.rng.below(1_000) >= self.clock_per_mille {
            return None;
        }
        let read = 1 + self.rng.below(READS);
        Some((read, self.draw(self.clock_max)))
    }
}

impl Faults {
    pub(super) fn new(seed: u64) -> Faults {
        Faults {
            rules: Mutex::new(Rules {
                // A stream of its own, so these draws never shift the nodes' id streams.
                rng: SplitMix64::new(seed ^ 0xFA17_5EED_0000_0A0B),
                storage: Vec::new(),
                storage_per_mille: 0,
                storage_max: Duration::ZERO,
                clock: Vec::new(),
                clock_per_mille: 0,
                clock_max: Duration::ZERO,
            }),
            armed: AtomicBool::new(false),
            suspended: Mutex::new(Vec::new()),
            bell: Bell::default(),
            progress: AtomicU64::new(0),
        }
    }

    pub(super) fn moved(&self) {
        self.progress.fetch_add(1, Ordering::Relaxed);
    }

    fn armed(&self) -> bool {
        self.armed.load(Ordering::Relaxed)
    }

    /// The seeded freeze of a job about to start.
    fn job(&self) -> Freeze {
        self.armed().then(|| lock(&self.rules).freeze()).flatten()
    }

    /// How a poll of `node` about to start must run: on a helper thread if it is `Some`,
    /// with that freeze and that seed for tokio's random choices.
    pub(super) fn poll(&self, node: u64) -> Option<(Freeze, u64)> {
        if !self.armed() {
            return None;
        }
        let mut rules = lock(&self.rules);
        let freeze = rules.freeze();
        if freeze.is_none() && !rules.clock.iter().any(|(owner, _)| *owner == node) {
            return None;
        }
        Some((freeze, rules.rng.next_u64()))
    }

    fn jobs(&self, context: NodeContext) -> Vec<Arc<Suspended>> {
        lock(&self.suspended)
            .iter()
            .filter(|job| job.context == context)
            .cloned()
            .collect()
    }

    /// Unwinds every suspended job of the crashed `context` and waits for each.
    pub(super) fn cancel(&self, world: &World, context: NodeContext) {
        for job in self.jobs(context) {
            job.advance(world, Resume::Cancel);
        }
    }

    /// Runs the context's first suspended job to its end, for code that waits inline.
    pub(super) fn resume_one(&self, world: &World, context: Option<NodeContext>) -> bool {
        let job = context.and_then(|context| {
            self.jobs(context)
                .into_iter()
                .find(|job| job.kind == Kind::Job && job.stopped())
        });
        let Some(job) = job else { return false };
        job.advance(world, Resume::RunToEnd);
        true
    }
}

/// A job or poll whose helper thread stopped at least once.
struct Suspended {
    context: NodeContext,
    kind: Kind,
    ends: Mutex<Ends>,
    /// Locked while a thread waits for the job.
    events: Mutex<Receiver<Event>>,
    /// Rung when the job may go on or went on.
    bell: Bell,
}

impl std::fmt::Debug for Suspended {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Suspended")
            .field("node", &self.context.id)
            .field("at", &lock(&self.ends).stop.as_ref().map(|s| &s.at))
            .finish_non_exhaustive()
    }
}

struct Ends {
    resume: Sender<Resume>,
    /// Where the job stands; `None` while it runs and once it has ended.
    stop: Option<Stop>,
    /// When it goes on by itself.
    until: Option<Instant>,
    released: bool,
}

impl Suspended {
    fn stopped(&self) -> bool {
        lock(&self.ends).stop.is_some()
    }

    /// Lets the job run until it stops again or ends, and waits for that. Nothing happens
    /// if it is not stopped.
    fn advance(&self, world: &World, how: Resume) {
        {
            let mut ends = lock(&self.ends);
            if ends.stop.take().is_none() {
                return;
            }
            // A helper thread that gave up waiting has left its `Done`.
            let _ = ends.resume.send(how);
        }
        let event = wait(world, &lock(&self.events));
        match event {
            Event::Suspended(stop) => {
                world.thaw_after(self.context.id, &stop);
                lock(&self.ends).set(stop);
                world.faults.bell.ring();
            }
            Event::Done => lock(&world.faults.suspended).retain(|job| !std::ptr::eq(&**job, self)),
        }
        self.bell.ring();
    }

    /// Returns once the job may go on, or went on.
    async fn due(&self) {
        loop {
            let seen = self.bell.generation();
            let until = {
                let ends = lock(&self.ends);
                let over = ends.until.is_some_and(|until| until <= Instant::now());
                if ends.stop.is_none() || ends.released || over {
                    return;
                }
                ends.until
            };
            let changed = self.bell.changed(seen);
            match until {
                Some(until) => drop(tokio::time::timeout_at(until, changed).await),
                None => changed.await,
            }
        }
    }
}

impl Ends {
    fn set(&mut self, stop: Stop) {
        // Code stopped at a clock read has no time of its own: it goes on at the thaw.
        let hold = match stop.at {
            At::Call(_) => stop.hold,
            At::Clock => Some(Duration::ZERO),
        };
        self.until = hold.map(|hold| Instant::now() + hold);
        self.released = false;
        self.stop = Some(stop);
    }
}

/// Waits for a helper thread. While other jobs are suspended, one of them may hold a lock
/// this one needs, which nothing would release: that fails instead of hanging.
fn wait(world: &World, events: &Receiver<Event>) -> Event {
    match events.recv_timeout(HANG) {
        Ok(event) => return event,
        Err(RecvTimeoutError::Disconnected) => panic!("the simulation blocking helper replies"),
        Err(RecvTimeoutError::Timeout) => {}
    }
    let suspended = lock(&world.faults.suspended).len();
    assert!(
        suspended == 0,
        "a blocking job made no progress in {HANG:?} while {suspended} others are suspended: \
         one of them probably holds a lock it needs"
    );
    events
        .recv()
        .expect("the simulation blocking helper replies")
}

/// Where a job leaves its outcome.
type Slot<R> = Arc<Mutex<Option<std::thread::Result<R>>>>;

fn taken<R>(slot: &Slot<R>) -> Option<std::thread::Result<R>> {
    lock(slot).take()
}

/// The first stop or the end of a started job.
pub(super) enum Run<R> {
    Done(std::thread::Result<R>),
    Suspended(Parked<R>),
}

impl<R> Run<R> {
    /// The job's outcome, once its pauses are over.
    pub(super) async fn outcome(self) -> std::thread::Result<R> {
        match self {
            Run::Done(outcome) => outcome,
            Run::Suspended(parked) => parked.finish().await,
        }
    }
}

/// The owner of a suspended job. Dropping it finishes the job, as an aborted join handle
/// does not stop blocking work, and cancels it after the run.
pub(super) struct Parked<R> {
    job: Arc<Suspended>,
    slot: Slot<R>,
    world: Weak<World>,
}

impl<R> Parked<R> {
    /// Lets the code run until it stops again or ends.
    pub(super) fn resume(self, world: &World) -> Run<R> {
        self.job.advance(world, Resume::Run);
        match taken(&self.slot) {
            Some(outcome) => Run::Done(outcome),
            None => Run::Suspended(self),
        }
    }

    async fn finish(self) -> std::thread::Result<R> {
        loop {
            self.job.due().await;
            self.thawed().await;
            if let Some(outcome) = taken(&self.slot) {
                return outcome;
            }
            let Some(world) = self.world.upgrade() else {
                return Err(Box::new(Cancelled));
            };
            self.job.advance(&world, Resume::Run);
        }
    }

    /// Returns once a job that stands at a clock read may go on, which is when its node
    /// thaws. A task of the node can be in the middle of the poll that started or resumed
    /// the job, so the task's own gate does not see to it.
    async fn thawed(&self) {
        std::future::poll_fn(|cx| {
            let at_read = {
                let ends = lock(&self.job.ends);
                matches!(ends.stop.as_ref().map(|stop| &stop.at), Some(At::Clock))
            };
            match self.world.upgrade() {
                Some(world) if at_read && world.park_if_frozen(self.job.context.id, cx.waker()) => {
                    Poll::Pending
                }
                _ => Poll::Ready(()),
            }
        })
        .await;
    }
}

impl<R> Drop for Parked<R> {
    fn drop(&mut self) {
        let Some(owner) = self.world.upgrade() else {
            return;
        };
        // Once the run is over, its world is not installed here. A task that goes away
        // takes its poll with it.
        let running = self.job.kind == Kind::Job
            && world().is_some_and(|world| Arc::ptr_eq(&world, &owner))
            && owner.context_active(Some(self.job.context));
        self.job.advance(
            &owner,
            if running {
                Resume::RunToEnd
            } else {
                Resume::Cancel
            },
        );
    }
}

/// A job's two ends: the helper thread takes the link, the waiting thread the rest.
pub(super) struct Job<R> {
    context: Option<NodeContext>,
    kind: Kind,
    resume: Sender<Resume>,
    events: Receiver<Event>,
    slot: Slot<R>,
}

/// The helper thread's end of a job.
pub(super) struct Link<R> {
    context: Option<NodeContext>,
    kind: Kind,
    freeze: Freeze,
    events: Sender<Event>,
    resume: Receiver<Resume>,
    slot: Slot<R>,
}

impl<R> Job<R> {
    /// Blocking work of `context`.
    pub(super) fn blocking(world: &World, context: Option<NodeContext>) -> (Job<R>, Link<R>) {
        Job::new(Kind::Job, context, world.faults.job())
    }

    /// A poll of a task of `context`, with the freeze [`Faults::poll`] drew for it.
    pub(super) fn poll(context: Option<NodeContext>, freeze: Freeze) -> (Job<R>, Link<R>) {
        Job::new(Kind::Poll, context, freeze)
    }

    fn new(kind: Kind, context: Option<NodeContext>, freeze: Freeze) -> (Job<R>, Link<R>) {
        let (events_in, events) = std::sync::mpsc::channel();
        let (resume, resume_out) = std::sync::mpsc::channel();
        let slot = Slot::default();
        let link = Link {
            context,
            kind,
            freeze,
            events: events_in,
            resume: resume_out,
            slot: slot.clone(),
        };
        let job = Job {
            context,
            kind,
            resume,
            events,
            slot,
        };
        (job, link)
    }

    /// Waits until the job, sent to a helper thread, ends or stops for the first time.
    pub(super) fn started(self, world: &Arc<World>) -> Run<R> {
        let stop = match wait(world, &self.events) {
            Event::Done => {
                return Run::Done(taken(&self.slot).expect("a finished job left its outcome"));
            }
            Event::Suspended(stop) => stop,
        };
        let context = self.context.expect("only a node's code stops");
        world.thaw_after(context.id, &stop);
        let mut ends = Ends {
            resume: self.resume,
            stop: None,
            until: None,
            released: false,
        };
        ends.set(stop);
        let job = Arc::new(Suspended {
            context,
            kind: self.kind,
            ends: Mutex::new(ends),
            events: Mutex::new(self.events),
            bell: Bell::default(),
        });
        lock(&world.faults.suspended).push(job.clone());
        world.faults.bell.ring();
        Run::Suspended(Parked {
            job,
            slot: self.slot,
            world: Arc::downgrade(world),
        })
    }
}

/// A job that has ended and has not been reported.
pub(super) struct Finished<R> {
    outcome: std::thread::Result<R>,
    events: Sender<Event>,
    slot: Slot<R>,
}

impl<R> Finished<R> {
    /// Wakes the waiting thread.
    pub(super) fn report(self) {
        *lock(&self.slot) = Some(self.outcome);
        let _ = self.events.send(Event::Done);
    }
}

impl<R> Link<R> {
    /// Runs the job on this helper thread.
    pub(super) fn run(self, world: &Arc<World>, work: impl FnOnce() -> R) -> Finished<R> {
        let stoppable = self.context.map(|context| {
            Rc::new(Stoppable {
                context,
                kind: self.kind,
                freeze: Cell::new(self.freeze),
                world: Arc::downgrade(world),
                events: self.events.clone(),
                resume: self.resume,
                steps_below: Cell::new(world.services.mid_step()),
                to_end: Cell::new(false),
                cancelled: Cell::new(false),
            })
        });
        let outer = STOPPABLE.replace(stoppable);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(work));
        STOPPABLE.set(outer);
        Finished {
            outcome,
            events: self.events,
            slot: self.slot,
        }
    }
}

thread_local! {
    /// The job this helper thread runs, if it belongs to a node.
    static STOPPABLE: RefCell<Option<Rc<Stoppable>>> = const { RefCell::new(None) };
}

/// The running job, as its pause points see it.
struct Stoppable {
    context: NodeContext,
    kind: Kind,
    freeze: Cell<Freeze>,
    world: Weak<World>,
    events: Sender<Event>,
    resume: Receiver<Resume>,
    /// Services that were mid-step below this job.
    steps_below: Cell<usize>,
    to_end: Cell<bool>,
    cancelled: Cell<bool>,
}

impl Stoppable {
    /// The code this thread runs, if it may stop at a point.
    fn here() -> Option<(Rc<Stoppable>, Arc<World>)> {
        let job = STOPPABLE.try_with(|job| job.borrow().clone()).ok()??;
        // Destructors run through, and so does code that something waits for.
        // An extension slot is empty while its code runs.
        if std::thread::panicking() || job.to_end.get() || super::in_ext() {
            return None;
        }
        if job.cancelled.get() {
            // The code caught the unwinding.
            std::panic::resume_unwind(Box::new(Cancelled));
        }
        let world = job.world.upgrade()?;
        (world.faults.armed() && world.context_active(Some(job.context))).then_some((job, world))
    }

    /// Gives control back to the waiting thread until it resumes this one.
    fn suspend(&self, world: &World, stop: Stop) {
        // The services this code is stepping are mid-step, but not on the stack of whoever
        // runs meanwhile.
        let steps = world.services.take_steps(self.steps_below.get());
        let node = self.context.id;
        match &stop.at {
            At::Call(call) => world.emit(&SimEvent::StoragePaused { node, call }),
            At::Clock => {
                world.freeze(node);
                world.emit(&SimEvent::ClockFrozen { node });
            }
        }
        let paused = matches!(stop.at, At::Call(_));
        let _ = self.events.send(Event::Suspended(stop));
        let how = self.resumed(world);
        self.steps_below.set(world.services.mid_step());
        world.services.put_steps(steps);
        if how == Resume::Cancel {
            self.cancelled.set(true);
            std::panic::resume_unwind(Box::new(Cancelled));
        }
        self.to_end.set(how == Resume::RunToEnd);
        if paused {
            world.emit(&SimEvent::StorageResumed { node });
        }
    }

    fn resumed(&self, world: &World) -> Resume {
        let mut progress = world.faults.progress.load(Ordering::Relaxed);
        loop {
            match self.resume.recv_timeout(HANG * 5) {
                Ok(how) => return how,
                Err(RecvTimeoutError::Disconnected) => return Resume::Cancel,
                Err(RecvTimeoutError::Timeout) => {}
            }
            let now = world.faults.progress.load(Ordering::Relaxed);
            assert!(
                now != progress,
                "the simulation stood still for {:?} while this job was suspended: the job \
                 probably holds a lock the simulation thread needs",
                HANG * 5
            );
            progress = now;
        }
    }
}

/// A pause point of the filesystem seam. `FaultFs` calls it outside its own lock.
pub(crate) fn storage_point(op: FsOp, side: Side, path: &Path) {
    let Some((job, world)) = Stoppable::here() else {
        return;
    };
    if job.kind == Kind::Job {
        let stop = lock(&world.faults.rules).storage(job.context.id, op, side, path);
        if let Some(stop) = stop {
            job.suspend(&world, stop);
        }
    }
}

/// A freeze point of the clock seam, after a read.
pub(crate) fn clock_point(at: &Location<'_>) {
    let Some((job, world)) = Stoppable::here() else {
        return;
    };
    let mut hold = None;
    let mut seeded = false;
    if let Some((reads, time)) = job.freeze.get() {
        seeded = reads == 1;
        hold = seeded.then_some(time);
        job.freeze.set((!seeded).then_some((reads - 1, time)));
    }
    if seeded || lock(&world.faults.rules).clock(job.context.id, at) {
        let at = At::Clock;
        job.suspend(&world, Stop { at, hold });
    }
}

/// Reports a read of the current node's clock that did not go through [`crate::time`], to
/// the clock freezes. Call it once the value is read, outside [`World::with_ext`].
#[track_caller]
pub fn clock_read() {
    clock_point(Location::caller());
}

/// A task's future and the outcome of one poll of it.
pub(super) type Polled<F> = (
    Pin<Box<F>>,
    std::thread::Result<Poll<<F as Future>::Output>>,
);

/// Polls `future` on a helper thread. A throwaway runtime seeds tokio's random choices on
/// this thread and gives the poll a task's cooperative budget; the simulation's own
/// runtime is the current one, for timers and spawned tasks.
pub(super) fn poll_once<F: Future>(
    seed: u64,
    simulation: &Handle,
    mut future: Pin<Box<F>>,
    waker: &Waker,
) -> Polled<F> {
    let runtime = super::runtime_seeded(seed).expect("build a poll's runtime");
    let outcome = runtime.block_on(async {
        let outcome = {
            let _entered = simulation.enter();
            let poll = || future.as_mut().poll(&mut Context::from_waker(waker));
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(poll))
        };
        // Wakers the poll deferred (`tokio::task::yield_now`) belong to this thread's
        // runtime, which wakes them when its own task yields.
        tokio::task::yield_now().await;
        outcome
    });
    (future, outcome)
}

/// Whether a poll ended because its node crashed.
pub(super) fn was_cancelled(panic: &(dyn std::any::Any + Send)) -> bool {
    panic.is::<Cancelled>()
}

impl World {
    /// Suspends the job of `node` that makes the filesystem call `pause` describes, once.
    /// Rules of one node are matched in the order they were set.
    pub fn pause_storage(&self, node: u64, pause: StoragePause) {
        lock(&self.faults.rules).storage.push((node, pause));
        self.faults.armed.store(true, Ordering::Relaxed);
    }

    /// Suspends, with probability `per_mille / 1000`, each sync a node's job makes, before
    /// it and again after it, for a seeded virtual time in `(0, max]`. Off by default.
    pub fn set_storage_pauses(&self, per_mille: u32, max: Duration) {
        let mut rules = lock(&self.faults.rules);
        rules.storage_per_mille = if max.is_zero() {
            0
        } else {
            u64::from(per_mille.min(1_000))
        };
        rules.storage_max = max;
        self.faults.armed.store(true, Ordering::Relaxed);
    }

    /// The calls `node`'s jobs are suspended at, in the order they stopped.
    pub fn paused_storage(&self, node: u64) -> Vec<PausedCall> {
        lock(&self.faults.suspended)
            .iter()
            .filter(|job| job.context.id == node)
            .filter_map(|job| match &lock(&job.ends).stop.as_ref()?.at {
                At::Call(call) => Some(call.clone()),
                At::Clock => None,
            })
            .collect()
    }

    /// Returns once a job of `node` is suspended, at once if one is.
    pub async fn storage_paused(&self, node: u64) {
        loop {
            let seen = self.faults.bell.generation();
            if !self.paused_storage(node).is_empty() {
                return;
            }
            self.faults.bell.changed(seen).await;
        }
    }

    /// Lets `node`'s suspended jobs go on, each from its own task.
    pub fn resume_storage(&self, node: u64) {
        for job in lock(&self.faults.suspended).iter() {
            if job.context.id == node {
                lock(&job.ends).released = true;
                job.bell.ring();
            }
        }
    }

    /// Freezes `node` right after the clock read `read` describes, once, with the reading
    /// code suspended there, until [`World::thaw`].
    pub fn freeze_after_clock_read(&self, node: u64, read: ClockRead) {
        lock(&self.faults.rules).clock.push((node, read));
        self.faults.armed.store(true, Ordering::Relaxed);
    }

    /// Chooses, with probability `per_mille / 1000`, polls and jobs of nodes, and freezes
    /// the node right after one of their first clock reads, for a seeded virtual time in
    /// `(0, max]`. Off by default.
    pub fn set_clock_freezes(&self, per_mille: u32, max: Duration) {
        let mut rules = lock(&self.faults.rules);
        rules.clock_per_mille = if max.is_zero() {
            0
        } else {
            u64::from(per_mille.min(1_000))
        };
        rules.clock_max = max;
        self.faults.armed.store(true, Ordering::Relaxed);
    }

    /// Whether `node` is frozen right after a clock read.
    pub fn is_frozen_at_read(&self, node: u64) -> bool {
        let at_read = |job: &Arc<Suspended>| {
            let ends = lock(&job.ends);
            matches!(ends.stop.as_ref().map(|stop| &stop.at), Some(At::Clock))
        };
        self.is_frozen(node)
            && lock(&self.faults.suspended)
                .iter()
                .any(|job| job.context.id == node && at_read(job))
    }

    /// Returns once `node` is frozen right after a clock read, at once if it is.
    pub async fn frozen_at_read(&self, node: u64) {
        loop {
            let seen = self.faults.bell.generation();
            if self.is_frozen_at_read(node) {
                return;
            }
            self.faults.bell.changed(seen).await;
        }
    }

    /// Thaws `node`, which a seeded clock freeze stopped, after its time, unless the node
    /// was frozen anew meanwhile.
    fn thaw_after(&self, node: u64, stop: &Stop) {
        let (At::Clock, Some(hold)) = (&stop.at, stop.hold) else {
            return;
        };
        let (world, freeze) = (self.me.clone(), self.freezes(node));
        Handle::current().spawn(async move {
            tokio::time::sleep(hold).await;
            if let Some(world) = world.upgrade()
                && world.freezes(node) == freeze
            {
                world.thaw(node);
            }
        });
    }
}

#[cfg(test)]
mod tests;
