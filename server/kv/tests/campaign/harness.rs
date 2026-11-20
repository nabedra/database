//! The campaign executor: runs a step list on a `FaultFs` machine, cuts it (crash at the
//! n-th mutating operation, process kill, power loss), recovers, checks the prefix rule
//! and continues on the recovered store, generation after generation.
//!
//! Every failure is returned as a message naming the step, so the shrinker can replay
//! sub-lists without panicking.

use std::any::Any;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Waker};
use std::time::{Duration, Instant};

use nabedra_kv::fs::{Fault, FaultError, FaultFs, Fs, FsOp};
use nabedra_kv::{Background, Durability, Error, MemtableBudget, Opened, Options, Snapshot, Store};

use crate::common::{ModelBatch, Op, Rng, group_prefix, small_options};
use crate::oracle::{self, History, Model, ReadFailure};
use crate::sleepy::{Sleepy, SleepyFs};
use crate::workload::{CutKind, FaultSpec, Mix, Step};

pub const DIR: &str = "/db/kv";

pub fn dir() -> PathBuf {
    PathBuf::from(DIR)
}

/// Engine sizes, small enough that segments roll and retire, memtables flush, levels
/// compact and journal GC relocates within a few hundred steps.
#[derive(Clone, Copy, Debug)]
pub struct Profile {
    pub segment: u64,
    pub group: usize,
    pub max_journal: u64,
    pub memtable: usize,
    pub max_replay: u64,
}

pub fn profile(i: u64) -> Profile {
    match i % 3 {
        0 => Profile {
            segment: 12 << 10,
            group: 4 << 10,
            max_journal: 6 * (12 << 10),
            memtable: 24 << 10,
            max_replay: 48 << 10,
        },
        1 => Profile {
            segment: 32 << 10,
            group: 16 << 10,
            max_journal: 6 * (32 << 10),
            memtable: 16 << 10,
            max_replay: 32 << 10,
        },
        _ => Profile {
            segment: 64 << 10,
            group: 64 << 10,
            max_journal: 1 << 20,
            memtable: 48 << 10,
            max_replay: 96 << 10,
        },
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub seed: u64,
    pub mix: Mix,
    pub profile: Profile,
    /// NS17: percent of background filesystem calls that sleep, if any.
    pub sleepy: Option<u64>,
    /// Bit-flip trials on the final clean image.
    pub damage_trials: usize,
}

impl Config {
    pub fn new(seed: u64, mix: Mix) -> Config {
        Config {
            seed,
            mix,
            profile: profile(seed),
            sleepy: None,
            damage_trials: 0,
        }
    }
}

/// What a run did. Summed over runs for the report.
#[derive(Clone, Debug, Default)]
pub struct Report {
    pub runs: u64,
    pub steps: u64,
    pub writes: u64,
    pub cancelled: u64,
    pub no_space_writes: u64,
    pub syncs: u64,
    pub reads: u64,
    pub flushes: u64,
    pub compactions: u64,
    pub gcs: u64,
    pub releases: u64,
    pub reopens: u64,
    pub faults: u64,
    pub cuts: u64,
    pub crash_cuts: u64,
    pub power_loss_cuts: u64,
    pub kill_cuts: u64,
    pub fault_cuts: u64,
    /// Submitted batches that a recovery dropped (never acknowledged durable).
    pub unsynced_dropped: u64,
    pub mutating_ops: u64,
    pub guards_checked: u64,
    pub naps: u64,
    /// A crash right after a recovery reopened with more tail truncations than the
    /// recovery itself (leftover garbage in a recycled segment, see `recover`).
    pub repeat_tails: u64,
    pub damage: crate::damage::DamageCounts,
}

impl Report {
    pub fn add(&mut self, o: &Report) {
        self.runs += o.runs;
        self.steps += o.steps;
        self.writes += o.writes;
        self.cancelled += o.cancelled;
        self.no_space_writes += o.no_space_writes;
        self.syncs += o.syncs;
        self.reads += o.reads;
        self.flushes += o.flushes;
        self.compactions += o.compactions;
        self.gcs += o.gcs;
        self.releases += o.releases;
        self.reopens += o.reopens;
        self.faults += o.faults;
        self.cuts += o.cuts;
        self.crash_cuts += o.crash_cuts;
        self.power_loss_cuts += o.power_loss_cuts;
        self.kill_cuts += o.kill_cuts;
        self.fault_cuts += o.fault_cuts;
        self.unsynced_dropped += o.unsynced_dropped;
        self.mutating_ops += o.mutating_ops;
        self.guards_checked += o.guards_checked;
        self.naps += o.naps;
        self.repeat_tails += o.repeat_tails;
        self.damage.add(&o.damage);
    }
}

/// Progress for the watchdog: a probable deadlock aborts with the last step's context.
pub struct Watchdog {
    last: AtomicU64,
    context: Mutex<String>,
    start: Instant,
}

impl Watchdog {
    pub fn start(limit: Duration) -> Arc<Watchdog> {
        let w = Arc::new(Watchdog {
            last: AtomicU64::new(0),
            context: Mutex::new(String::new()),
            start: Instant::now(),
        });
        let weak = Arc::downgrade(&w);
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(Duration::from_millis(200));
                let Some(w) = weak.upgrade() else { return };
                let now = w.start.elapsed().as_millis() as u64;
                if now.saturating_sub(w.last.load(Ordering::Acquire)) > limit.as_millis() as u64 {
                    eprintln!(
                        "NS10 watchdog: no progress for {limit:?} (probable deadlock) at {}",
                        w.context.lock().unwrap_or_else(|p| p.into_inner())
                    );
                    std::process::abort();
                }
            }
        });
        w
    }

    pub fn beat(&self, context: impl FnOnce() -> String) {
        self.last
            .store(self.start.elapsed().as_millis() as u64, Ordering::Release);
        if let Ok(mut c) = self.context.try_lock() {
            *c = context();
        }
    }
}

#[derive(Default)]
struct Guards {
    created: AtomicU64,
    dropped: AtomicU64,
    checked: AtomicU64,
    bad: Mutex<Option<String>>,
}

/// NS17: an `on_done` payload. The journal thread must drop it, and only once the write is
/// visible. A latched store (a cut or a fault) skips the checks.
struct OrderGuard {
    store: Arc<Store>,
    seq: Arc<OnceLock<u64>>,
    guards: Arc<Guards>,
}

impl Drop for OrderGuard {
    fn drop(&mut self) {
        if let Some(&seq) = self.seq.get()
            && !self.store.requires_recovery()
        {
            let thread = std::thread::current().name().map(str::to_owned);
            let visible = self.store.visible_seq();
            if thread.as_deref() != Some("kv-journal") || visible < seq {
                let mut bad = self.guards.bad.lock().unwrap_or_else(|p| p.into_inner());
                bad.get_or_insert(format!(
                    "order guard of seq {seq} dropped on {thread:?} with visible_seq {visible}"
                ));
            }
            self.guards.checked.fetch_add(1, Ordering::Relaxed);
        }
        self.guards.dropped.fetch_add(1, Ordering::Release);
    }
}

enum Flow {
    Continue,
    Cut(CutKind, &'static str),
    Fail(String),
}

pub struct Exec<'a> {
    cfg: &'a Config,
    watchdog: Option<&'a Watchdog>,
    bg: Arc<Background>,
    budget: Arc<MemtableBudget>,
    pub fs: FaultFs,
    sleepy: Option<Arc<Sleepy>>,
    store: Option<Arc<Store>>,
    pub hist: History,
    pub model: Model,
    held: Vec<(Snapshot, Model)>,
    armed: Option<CutKind>,
    fault: Option<(FaultSpec, u64)>,
    /// A fault was injected since the last recovery: errors are expected from here on.
    faulted: bool,
    /// A cancelled commit no waited commit has covered yet.
    unresolved: bool,
    guards: Arc<Guards>,
    pub report: Report,
    barriers: u64,
    step: usize,
    /// Needles of acknowledged values, for the damage trials: (value prefix, batch scope).
    pub needles: Vec<(Vec<u8>, Vec<u8>)>,
}

fn fault_rules(spec: FaultSpec) -> Vec<Fault> {
    let runs = dir().join("runs");
    let no_space_runs = || {
        Fault::on(FsOp::Write)
            .path_prefix(dir().join("runs"))
            .error(FaultError::Enospc)
            .repeat()
    };
    match spec {
        FaultSpec::EioWrite(n) => vec![Fault::on(FsOp::Write).nth(n)],
        FaultSpec::EioSyncData(n) => vec![Fault::on(FsOp::SyncData).nth(n)],
        FaultSpec::EioSyncAll(n) => vec![Fault::on(FsOp::SyncAll).nth(n)],
        FaultSpec::EioSyncDir(n) => vec![Fault::on(FsOp::SyncDir).nth(n)],
        FaultSpec::EioRename(n) => vec![Fault::on(FsOp::Rename).nth(n)],
        FaultSpec::EioReadRun(n) => vec![Fault::on(FsOp::Read).path_prefix(runs).nth(n)],
        FaultSpec::NoSpaceRuns => vec![no_space_runs()],
        FaultSpec::NoSpacePreallocate => vec![
            Fault::on(FsOp::Preallocate)
                .error(FaultError::Enospc)
                .repeat(),
            no_space_runs(),
        ],
        FaultSpec::NoSpaceAll => vec![Fault::on(FsOp::Write).error(FaultError::Enospc).repeat()],
    }
}

pub fn release_batch(group: u64) -> ModelBatch {
    let prefix = group_prefix(group);
    let mut end = prefix.clone();
    *end.last_mut().unwrap() += 1;
    ModelBatch {
        scope: prefix.clone(),
        ops: vec![Op::DeleteRange(prefix, end)],
    }
}

impl<'a> Exec<'a> {
    pub fn new(cfg: &'a Config, watchdog: Option<&'a Watchdog>) -> Exec<'a> {
        let fs = FaultFs::new();
        fs.create_dir_all_durable(Path::new("/db"));
        Exec {
            cfg,
            watchdog,
            bg: Background::new(2).unwrap(),
            budget: Arc::new(MemtableBudget::new(8 << 20)),
            fs,
            sleepy: None,
            store: None,
            hist: History::default(),
            model: Model::new(),
            held: Vec::new(),
            armed: None,
            fault: None,
            faulted: false,
            unresolved: false,
            guards: Arc::default(),
            report: Report {
                runs: 1,
                ..Report::default()
            },
            barriers: 0,
            step: 0,
            needles: Vec::new(),
        }
    }

    pub fn options(&mut self, fs: &FaultFs) -> Options {
        let handle: Arc<dyn Fs> = match self.cfg.sleepy {
            Some(percent) => {
                let s = Sleepy::new(fs.clone(), self.cfg.seed ^ self.report.cuts, percent);
                if let Some(old) = self.sleepy.replace(s.clone()) {
                    self.report.naps += old.naps.load(Ordering::Relaxed);
                }
                Arc::new(SleepyFs(s))
            }
            None => Arc::new(fs.clone()),
        };
        options_for(self.cfg, handle, self.bg.clone(), self.budget.clone())
    }

    fn open(&mut self, fs: &FaultFs) -> nabedra_kv::Result<Opened> {
        let o = self.options(fs);
        Store::open(&dir(), o)
    }

    fn store(&self) -> &Arc<Store> {
        self.store.as_ref().expect("store is open")
    }

    fn ctx(&self) -> String {
        format!("seed {} step {}", self.cfg.seed, self.step)
    }

    /// Runs `steps`, cutting and recovering as they say, then closes cleanly and checks
    /// the final state after a reopen. Returns the closed machine's filesystem.
    pub fn run(&mut self, steps: &[Step]) -> Result<FaultFs, String> {
        self.start()?;
        self.steps(steps)?;
        self.finish()
    }

    /// Creates the store.
    pub fn start(&mut self) -> Result<(), String> {
        let fs = self.fs.clone();
        let opened = self
            .open(&fs)
            .map_err(|e| format!("seed {}: create failed: {e}", self.cfg.seed))?;
        self.store = Some(Arc::new(opened.store));
        Ok(())
    }

    /// Runs `steps` after the ones already run (step numbers continue).
    pub fn steps(&mut self, steps: &[Step]) -> Result<(), String> {
        for step in steps {
            self.step += 1;
            if let Some(w) = self.watchdog {
                let i = self.step;
                w.beat(|| format!("seed {} step {i}: {step:?}", self.cfg.seed));
            }
            self.report.steps += 1;
            match self.exec(step) {
                Flow::Continue => {}
                Flow::Cut(kind, why) => self.cut(kind, why)?,
                Flow::Fail(msg) => return Err(format!("{}: {msg}", self.ctx())),
            }
            if let Some((spec, left)) = self.fault {
                if left <= 1 {
                    self.fs.clear_faults();
                    self.fault = None;
                } else {
                    self.fault = Some((spec, left - 1));
                }
            }
        }
        Ok(())
    }

    fn exec(&mut self, step: &Step) -> Flow {
        match step {
            Step::Write {
                batch,
                sync,
                cancel,
            } => self.write(batch, *sync, *cancel),
            Step::Sync => {
                self.report.syncs += 1;
                let r = self.store().sync().and_then(|c| c.wait());
                match r {
                    Ok(_) => {
                        self.unresolved = false;
                        self.hist.durable = self.hist.batches.len();
                        Flow::Continue
                    }
                    Err(Error::NoSpace) if !self.store().requires_recovery() => Flow::Continue,
                    Err(e) => self.on_error(e, "sync"),
                }
            }
            Step::TakeSnapshot => {
                if let Some(f) = self.resolve() {
                    return f;
                }
                if self.held.len() < 4 {
                    let snap = self.store().snapshot();
                    self.held.push((snap, self.model.clone()));
                }
                Flow::Continue
            }
            Step::DropSnapshot(r) => {
                if !self.held.is_empty() {
                    let i = (*r % self.held.len() as u64) as usize;
                    self.held.remove(i);
                }
                Flow::Continue
            }
            Step::Gets(seed) | Step::Walk(seed) => {
                if let Some(f) = self.resolve() {
                    return f;
                }
                self.report.reads += 1;
                let walk = matches!(step, Step::Walk(_));
                let mut rng = Rng(*seed);
                let mix = self.cfg.mix;
                let snap = self.store().snapshot();
                let check = |rng: &mut Rng, snap: &Snapshot, m: &Model| {
                    if walk {
                        oracle::check_walk(rng, &mix, snap, m)
                    } else {
                        oracle::check_gets(rng, &mix, snap, m)
                    }
                };
                let mut r = check(&mut rng, &snap, &self.model);
                if r.is_ok() && !self.held.is_empty() {
                    let i = rng.below(self.held.len() as u64) as usize;
                    let (s, m) = &self.held[i];
                    r = check(&mut rng, s, m).map_err(|f| match f {
                        ReadFailure::Mismatch(m) => {
                            ReadFailure::Mismatch(format!("held snapshot {}: {m}", s.seq()))
                        }
                        e => e,
                    });
                }
                match r {
                    Ok(()) => Flow::Continue,
                    Err(ReadFailure::Mismatch(m)) => Flow::Fail(format!(
                        "{} mismatch: {m}",
                        if walk { "walk" } else { "get" }
                    )),
                    Err(ReadFailure::Error(e)) => self.read_error(e),
                }
            }
            Step::Flush => {
                self.report.flushes += 1;
                let r = self.store().flush();
                self.maintenance(r, "flush")
            }
            Step::Compact => {
                self.report.compactions += 1;
                let r = self.store().compact();
                self.maintenance(r, "compact")
            }
            Step::Gc => {
                self.report.gcs += 1;
                let r = self.store().gc_journal().map(|_| ());
                self.maintenance(r, "gc_journal")
            }
            Step::Release(g) => {
                if let Some(f) = self.resolve() {
                    return f;
                }
                self.report.releases += 1;
                let b = release_batch(*g % self.cfg.mix.groups);
                let r = self.store().release_scope(&b.scope);
                self.hist.batches.push(b.clone());
                b.apply(&mut self.model);
                match r {
                    Ok(_) => {
                        self.hist.durable = self.hist.batches.len();
                        Flow::Continue
                    }
                    // The delete may be durable already: stop here and recover.
                    Err(Error::NoSpace) if self.faulted => Flow::Cut(CutKind::Crash, "release"),
                    Err(e) => self.on_error(e, "release_scope"),
                }
            }
            Step::Reopen => self.reopen(),
            Step::Fault { spec, window } => {
                if let Some(f) = self.resolve() {
                    return f;
                }
                if self.fault.is_none() {
                    self.report.faults += 1;
                    for rule in fault_rules(*spec) {
                        self.fs.inject(rule);
                    }
                    self.fault = Some((*spec, *window));
                    self.faulted = true;
                }
                Flow::Continue
            }
            Step::Cut(kind) => Flow::Cut(*kind, "cut step"),
            Step::ArmCrash { after, kind } => {
                if self.armed.is_none() {
                    self.fs.crash_after(self.fs.mutating_ops() + after);
                    self.armed = Some(*kind);
                }
                Flow::Continue
            }
        }
    }

    fn write(&mut self, batch: &ModelBatch, sync: bool, cancel: bool) -> Flow {
        self.report.writes += 1;
        let cancel = cancel && self.fault.is_none();
        let seq_cell = Arc::new(OnceLock::new());
        let payload: Box<dyn Any + Send> = if self.fault.is_none() {
            self.guards.created.fetch_add(1, Ordering::Relaxed);
            Box::new(OrderGuard {
                store: self.store().clone(),
                seq: seq_cell.clone(),
                guards: self.guards.clone(),
            })
        } else {
            Box::new(())
        };
        let d = if sync {
            Durability::Sync
        } else {
            Durability::NoSync
        };
        let commit = match self.store().submit(batch.to_batch(), d, payload) {
            Ok(c) => c,
            Err(e) => {
                // Never queued: it cannot be applied.
                return self.on_error(e, "submit");
            }
        };
        let _ = seq_cell.set(commit.seq());
        if cancel {
            self.report.cancelled += 1;
            self.hist.batches.push(batch.clone());
            batch.apply(&mut self.model);
            self.unresolved = true;
            let mut fut = Box::pin(commit.done());
            let mut cx = Context::from_waker(Waker::noop());
            let _ = Pin::as_mut(&mut fut).poll(&mut cx);
            return Flow::Continue;
        }
        let r = commit.wait();
        match r {
            Err(Error::NoSpace) if !self.store().requires_recovery() => {
                self.report.no_space_writes += 1;
                return Flow::Continue;
            }
            _ => {}
        }
        self.hist.batches.push(batch.clone());
        batch.apply(&mut self.model);
        match r {
            Ok(_) => {
                self.unresolved = false;
                if sync {
                    self.hist.durable = self.hist.batches.len();
                }
                self.remember_needles(batch);
                Flow::Continue
            }
            Err(e) => self.on_error(e, "commit"),
        }
    }

    fn remember_needles(&mut self, batch: &ModelBatch) {
        for op in &batch.ops {
            if let Op::Put(_, v) | Op::PutJournal(_, v) = op
                && let Some(end) = v.iter().position(|&b| b == b'>')
            {
                if self.needles.len() >= 256 {
                    self.needles.remove(0);
                }
                self.needles.push((v[..=end].to_vec(), batch.scope.clone()));
            }
        }
    }

    /// Makes every cancelled commit visible with a waited write of a marker key.
    fn resolve(&mut self) -> Option<Flow> {
        if !self.unresolved {
            return None;
        }
        self.barriers += 1;
        let b = ModelBatch {
            scope: Vec::new(),
            ops: vec![Op::Put(
                b"s/barrier".to_vec(),
                self.barriers.to_le_bytes().to_vec(),
            )],
        };
        match self.write(&b, false, false) {
            Flow::Continue => None,
            other => Some(other),
        }
    }

    fn maintenance(&mut self, r: nabedra_kv::Result<()>, what: &'static str) -> Flow {
        match r {
            Ok(()) => Flow::Continue,
            Err(Error::NoSpace) if self.faulted => Flow::Continue,
            Err(e) => self.on_error(e, what),
        }
    }

    fn read_error(&mut self, e: Error) -> Flow {
        if self.faulted && matches!(e, Error::Io(_)) && !self.fs.is_crashed() {
            return Flow::Continue;
        }
        self.on_error(e, "read")
    }

    fn on_error(&mut self, e: Error, what: &'static str) -> Flow {
        if self.fs.is_crashed()
            && let Some(kind) = self.armed
        {
            return Flow::Cut(kind, "armed crash");
        }
        if self.faulted {
            self.report.fault_cuts += 1;
            let kind = if self.step.is_multiple_of(2) {
                CutKind::Crash
            } else {
                CutKind::PowerLoss(self.cfg.seed ^ self.step as u64)
            };
            return Flow::Cut(kind, "fault");
        }
        Flow::Fail(format!("unexpected {what} error: {e} ({e:?})"))
    }

    /// Clean close, then reopen on the same machine.
    fn reopen(&mut self) -> Flow {
        self.report.reopens += 1;
        for (snap, m) in &self.held {
            match oracle::dump_both(snap) {
                Ok(d) if &d == m => {}
                Ok(d) => {
                    return Flow::Fail(format!(
                        "held snapshot before reopen: {}",
                        oracle::first_difference(&d, m)
                    ));
                }
                Err(e) => return Flow::Fail(format!("held snapshot before reopen: {e}")),
            }
        }
        self.held.clear();
        let store = match self.take_store() {
            Ok(s) => s,
            Err(msg) => return Flow::Fail(msg),
        };
        if let Err(e) = store.close() {
            return self.on_error(e, "close");
        }
        if let Err(msg) = self.check_budget("close") {
            return Flow::Fail(msg);
        }
        // Clean close made everything durable.
        self.hist = History::new(self.model.clone());
        self.unresolved = false;
        let fs = self.fs.clone();
        match self.open(&fs) {
            Ok(opened) => {
                if !opened.quarantined.is_empty() {
                    return Flow::Fail(format!("reopen quarantined {:?}", opened.quarantined));
                }
                self.store = Some(Arc::new(opened.store));
                match oracle::dump_both(&self.store().snapshot()) {
                    Ok(d) if d == self.model => Flow::Continue,
                    Ok(d) => Flow::Fail(format!(
                        "after clean reopen: {}",
                        oracle::first_difference(&d, &self.model)
                    )),
                    Err(e) => Flow::Fail(format!("after clean reopen: {e}")),
                }
            }
            Err(e) => {
                if self.fs.is_crashed()
                    && let Some(kind) = self.armed
                {
                    return Flow::Cut(kind, "armed crash during reopen");
                }
                Flow::Fail(format!("clean reopen failed: {e}"))
            }
        }
    }

    /// Waits for every order guard to drop, then hands out the only store handle.
    fn take_store(&mut self) -> Result<Store, String> {
        let deadline = Instant::now() + Duration::from_secs(60);
        while self.guards.dropped.load(Ordering::Acquire)
            < self.guards.created.load(Ordering::Acquire)
        {
            if Instant::now() > deadline {
                return Err(format!(
                    "{}: {} of {} order guards never dropped",
                    self.ctx(),
                    self.guards.created.load(Ordering::Acquire)
                        - self.guards.dropped.load(Ordering::Acquire),
                    self.guards.created.load(Ordering::Acquire)
                ));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        if let Some(bad) = self
            .guards
            .bad
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            return Err(format!("{}: NS17: {bad}", self.ctx()));
        }
        self.report.guards_checked += self.guards.checked.swap(0, Ordering::Relaxed);
        let arc = self.store.take().expect("store is open");
        Arc::try_unwrap(arc).map_err(|_| format!("{}: store still shared", self.ctx()))
    }

    fn check_budget(&self, when: &str) -> Result<(), String> {
        let used = self.budget.used();
        if used != 0 {
            return Err(format!(
                "{}: NS17: memtable budget still charged {used} B after {when}",
                self.ctx()
            ));
        }
        Ok(())
    }

    /// Takes the image `kind` describes, stops the old machine, recovers and continues.
    fn cut(&mut self, kind: CutKind, why: &'static str) -> Result<(), String> {
        self.report.cuts += 1;
        match kind {
            CutKind::Crash => self.report.crash_cuts += 1,
            CutKind::Kill => self.report.kill_cuts += 1,
            _ => self.report.power_loss_cuts += 1,
        }
        let image = match kind.power_loss_mode() {
            Some((seed, mode)) => self.fs.power_loss_with(seed, mode),
            None if kind == CutKind::Kill => self.fs.kill(),
            None => self.fs.crash(),
        };
        self.fs.halt();
        self.report.mutating_ops += self.fs.mutating_ops();
        self.held.clear();
        if self.store.is_some() {
            let store = self.take_store()?;
            drop(store);
        }
        self.check_budget("a cut")?;
        let ctx = format!("{} cut {kind:?} ({why})", self.ctx());
        self.recover(image, &ctx)
    }

    /// Opens `image`, checks the prefix rule, crashes right after recovery, reopens and
    /// checks again, and continues on the second store.
    fn recover(&mut self, image: FaultFs, ctx: &str) -> Result<(), String> {
        let opened = self
            .open(&image)
            .map_err(|e| format!("{ctx}: reopen failed: {e} ({e:?})"))?;
        if !opened.quarantined.is_empty() {
            return Err(format!("{ctx}: quarantined {:?}", opened.quarantined));
        }
        let state =
            oracle::dump_both(&opened.store.snapshot()).map_err(|e| format!("{ctx}: {e}"))?;
        let (p, m) = self
            .hist
            .find_prefix(&state)
            .map_err(|e| format!("{ctx}: {e}"))?;
        self.report.unsynced_dropped += (self.hist.batches.len() - p) as u64;
        let mix = self.cfg.mix;
        let mut rng = Rng(self.cfg.seed ^ (self.step as u64) << 20);
        for _ in 0..2 {
            let snap = opened.store.snapshot();
            let r = oracle::check_walk(&mut rng, &mix, &snap, &m)
                .and_then(|()| oracle::check_gets(&mut rng, &mix, &snap, &m));
            match r {
                Ok(()) => {}
                Err(ReadFailure::Mismatch(msg)) => {
                    return Err(format!("{ctx}: after recovery: {msg}"));
                }
                Err(ReadFailure::Error(e)) => return Err(format!("{ctx}: after recovery: {e}")),
            }
        }
        let first_report = opened.recovery.clone();
        // A crash right after the first recovery.
        image.halt();
        drop(opened);
        self.check_budget("the first recovery")?;
        self.report.mutating_ops += image.mutating_ops();
        let image2 = image.crash();
        let opened2 = self
            .open(&image2)
            .map_err(|e| format!("{ctx}: second reopen failed: {e} ({e:?})"))?;
        if !opened2.quarantined.is_empty() {
            return Err(format!(
                "{ctx}: second reopen quarantined {:?}",
                opened2.quarantined
            ));
        }
        // The journal fences or re-epochs a torn end instead of zeroing it, and activates
        // recycled segments without zeroing their record area. Garbage a power loss left
        // there (destroyed 4 KiB units) reads as a torn tail until it is overwritten, so
        // a crash right after recovery can report a tail again. Only the state must match.
        if opened2.recovery.tail_truncations > first_report.tail_truncations {
            self.report.repeat_tails += 1;
            if std::env::var("NS10_DEBUG").is_ok() {
                eprintln!(
                    "{ctx}: repeat tail {:?} after {first_report:?}",
                    opened2.recovery
                );
                for (name, fs) in [("first", &image), ("second", &image2)] {
                    for c in fs.calls() {
                        if c.path.starts_with("/db/kv/journal")
                            && matches!(c.op, FsOp::Write | FsOp::Rename | FsOp::Remove)
                        {
                            eprintln!(
                                "  {name} open: {:?} {} {:?} @{}+{}",
                                c.op,
                                c.path.display(),
                                c.target,
                                c.offset,
                                c.len
                            );
                        }
                    }
                }
            }
        }
        let state2 = oracle::dump_both(&opened2.store.snapshot())
            .map_err(|e| format!("{ctx}: second reopen: {e}"))?;
        if state2 != m {
            return Err(format!(
                "{ctx}: second reopen differs from the first: {}",
                oracle::first_difference(&state2, &m)
            ));
        }
        self.fs = image2;
        self.store = Some(Arc::new(opened2.store));
        self.guards = Arc::default();
        self.hist = History::new(m.clone());
        self.model = m;
        self.armed = None;
        self.fault = None;
        self.faulted = false;
        self.unresolved = false;
        Ok(())
    }

    /// Disarms, closes cleanly, reopens and checks the exact final state. Returns the
    /// filesystem of the closed store.
    pub fn finish(&mut self) -> Result<FaultFs, String> {
        if self.armed.is_some() {
            if self.fs.is_crashed() {
                let kind = self.armed.unwrap();
                self.cut(kind, "armed crash at the end")?;
            } else {
                self.fs.crash_after(u64::MAX);
                self.armed = None;
            }
        }
        if self.fault.take().is_some() {
            self.fs.clear_faults();
        }
        match self.resolve() {
            None | Some(Flow::Continue) => {}
            Some(Flow::Cut(kind, why)) => self.cut(kind, why)?,
            Some(Flow::Fail(msg)) => return Err(format!("{}: {msg}", self.ctx())),
        }
        if self.faulted && self.store().requires_recovery() {
            self.cut(CutKind::Crash, "latched at the end")?;
        }
        self.held.clear();
        let store = self.take_store()?;
        if let Err(e) = store.close() {
            if self.faulted {
                // A background fault latched during close: recover from disk.
                self.store = None;
                let image = self.fs.crash();
                self.fs.halt();
                self.recover(image, &format!("{} close after fault", self.ctx()))?;
                return self.finish();
            }
            return Err(format!("{}: final close failed: {e}", self.ctx()));
        }
        self.check_budget("the final close")?;
        let fs = self.fs.clone();
        let opened = self
            .open(&fs)
            .map_err(|e| format!("{}: final reopen failed: {e}", self.ctx()))?;
        if !opened.quarantined.is_empty() {
            return Err(format!(
                "{}: final reopen quarantined {:?}",
                self.ctx(),
                opened.quarantined
            ));
        }
        let state = oracle::dump_both(&opened.store.snapshot())?;
        if state != self.model {
            return Err(format!(
                "{}: final state after a clean close: {}",
                self.ctx(),
                oracle::first_difference(&state, &self.model)
            ));
        }
        opened
            .store
            .close()
            .map_err(|e| format!("{}: close after the final check: {e}", self.ctx()))?;
        self.check_budget("the last close")?;
        self.report.mutating_ops += self.fs.mutating_ops();
        if let Some(s) = self.sleepy.take() {
            self.report.naps += s.naps.load(Ordering::Relaxed);
        }
        Ok(self.fs.clone())
    }
}

pub fn options_for(
    cfg: &Config,
    fs: Arc<dyn Fs>,
    bg: Arc<Background>,
    budget: Arc<MemtableBudget>,
) -> Options {
    let p = cfg.profile;
    let mut o = small_options(fs, bg);
    o.memtables = budget;
    o.journal.segment_bytes = p.segment;
    o.journal.max_group_bytes = p.group;
    o.max_journal_bytes = p.max_journal;
    o.memtable_bytes = p.memtable;
    o.max_replay_bytes = p.max_replay;
    o
}

/// One run of `steps` under `cfg`, with the damage trials on its final image.
pub fn run_steps(
    cfg: &Config,
    steps: &[Step],
    watchdog: Option<&Watchdog>,
) -> Result<Report, String> {
    let mut exec = Exec::new(cfg, watchdog);
    let fs = exec.run(steps)?;
    let mut report = std::mem::take(&mut exec.report);
    if cfg.damage_trials > 0 {
        let counts = crate::damage::trials(cfg, &fs, &exec.model, &exec.needles)?;
        report.damage.add(&counts);
    }
    Ok(report)
}
