//! `nabedra.trace/1` traces for TLA+ trace validation: one writer, shared by every simulator.
//!
//! A workload of a specced module installs a [`TlaTrace`] for one spec. It writes one JSON
//! line per protocol step, at the point where the step takes effect, through [`TlaSteps`],
//! and the fault steps come from the simulation itself ([`faults`]). The file is
//! `$NABEDRA_TRACE_DIR/<Spec>-<seed>.ndjson`. With the variable unset nothing is installed:
//! no file, no observer. `scripts/testing/tla_trace.py` lints the file and checks it with
//! TLC against `<Spec>Trace.tla`.
//!
//! # What the writer refuses
//!
//! A trace speaks only for what its lines carry, and [`faults`] lists the faults that no
//! line carries today. What it does carry must be complete, so every problem below makes
//! [`TlaTrace::finish`] fail, and no `.ndjson` file is left:
//!
//! - an action the workload did not declare, or one whose variables or message fields are
//!   not the declared ones. The writer cannot see the trace spec, so the declarations and
//!   the spec's checks have to be kept equal by hand. An action that carries no variable
//!   must say why ([`Action::unconstrained`]);
//! - empty `params`, no action with variables, or a run whose lines carry none;
//! - a value TLC would not read as written: null, a float, or an integer outside signed 32
//!   bits, which TLC's JSON reader wraps without an error. A field that can outgrow them is
//!   declared [`Builder::wide`] and is a decimal string on every line. `t` and `seed` are
//!   exempt because the format requires 64 bits there, so a trace spec must never use them;
//! - a step of a node that is down, under its World id or its spec name, or a step outside
//!   the simulation's runtime (there is no virtual time there), or time that goes backward.
//!   A step that a task of an earlier incarnation logs after the node is up again is not
//!   found: the World does not say which incarnation a task is of;
//! - a panic anywhere in the run, caught or not ([`runs`], which names two kinds that are
//!   not seen);
//! - any write error;
//! - no commit SHA from git, or sources that are not that commit's ([`commit`], which says
//!   what it does not find).
//!
//! # A trace cut short never looks complete
//!
//! Any prefix of a valid trace is itself a valid trace, so TLC cannot see that one was cut.
//! Lines go to a `.partial` file that only this run writes, and only `finish` gives the file
//! its name, after a flush and an fsync. A run that panics, aborts, fails or forgets to
//! finish leaves no `.ndjson`, and `tla_trace.py` reads only those. Dropping an unfinished
//! trace panics.
//!
//! A published trace loses its name again (`.withdrawn`) when the directory cannot be
//! synced, a step follows `finish`, or the run panics while its World lives. A panic after
//! that, such as in a check of the run's result, leaves the trace, which is complete.
//!
//! A run that starts over a trace of its seed moves that aside first (`.previous`), and its
//! bytes must come out equal. Of two processes that write one seed into one directory at the
//! same time, the second to finish must have the same bytes, or it fails and the first one's
//! file stays. A process that starts after the other has finished moves its file aside, and
//! if it then fails, neither file has the name.
//!
//! # Determinism
//!
//! The writer only observes. It draws nothing, never sleeps, and reads only
//! [`World::elapsed`], so tracing does not change a run, and a seed always gives the same
//! bytes. Object keys are written in sorted order whatever features `serde_json` has.

pub mod commit;
pub mod faults;
pub mod runs;

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use nabedra_rt::sim::World;
use serde_json::{Map, Value};

use commit::Commit;
pub use commit::{SHA_VAR, UNCLEAN_VAR};
pub use faults::{Fault, FaultLine};
pub use runs::attributed;

/// The format name in every header.
pub const FORMAT: &str = "nabedra.trace/1";

/// Where traces go. Unset or empty means no traces.
pub const DIR_VAR: &str = "NABEDRA_TRACE_DIR";

type FaultMap = Box<dyn Fn(FaultLine) -> Option<FaultLine> + Send>;

/// A node of the trace: a World node, or a name for something that is not one, such as a
/// client or a transport that takes steps of its own.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeRef {
    World(u64),
    Named(String),
}

impl From<u64> for NodeRef {
    fn from(node: u64) -> Self {
        Self::World(node)
    }
}

impl From<&str> for NodeRef {
    fn from(name: &str) -> Self {
        Self::Named(name.to_owned())
    }
}

/// One spec action a workload logs, and what every one of its lines must carry.
#[derive(Clone, Debug)]
pub struct Action {
    name: String,
    vars: BTreeSet<String>,
    /// The fields of its message, with the only value each may have, if it is fixed.
    msg: Option<BTreeMap<String, Option<Value>>>,
    /// Message fields that only some lines carry.
    optional: BTreeSet<String>,
    unconstrained: Option<String>,
}

impl Action {
    /// An action whose lines carry exactly `vars`, under their spec names.
    pub fn new(name: &str, vars: &[&str]) -> Self {
        Self {
            name: name.to_owned(),
            vars: vars.iter().map(|var| (*var).to_owned()).collect(),
            msg: None,
            optional: BTreeSet::new(),
            unconstrained: None,
        }
    }

    /// An action whose lines carry no variable, with the reason. The trace spec then learns
    /// nothing from such a line but its name, its node and its message.
    pub fn unconstrained(name: &str, why: &str) -> Self {
        Self {
            unconstrained: Some(why.to_owned()),
            ..Self::new(name, &[])
        }
    }

    /// Every line also carries a message with exactly `fields`, besides those of
    /// [`Action::msg_value`] and [`Action::msg_optional`]. Without any of them an action's
    /// lines carry no message.
    #[must_use]
    pub fn msg(mut self, fields: &[&str]) -> Self {
        let declared = self.msg.get_or_insert_default();
        declared.extend(fields.iter().map(|field| ((*field).to_owned(), None)));
        self
    }

    /// A message field that has the same value on every line, such as the message type the
    /// action sends or receives. A trace spec need not look at it, so the writer does.
    #[must_use]
    pub fn msg_value(mut self, field: &str, value: impl Into<Value>) -> Self {
        let declared = self.msg.get_or_insert_default();
        declared.insert(field.to_owned(), Some(value.into()));
        self
    }

    /// A message field that only some lines carry.
    #[must_use]
    pub fn msg_optional(mut self, field: &str) -> Self {
        self.optional.insert(field.to_owned());
        self
    }

    fn check(&self, vars: &Value, msg: Option<&Value>) -> Result<(), String> {
        let name = &self.name;
        let Value::Object(vars) = vars else {
            return Err(format!("{name}: vars is not an object"));
        };
        let missing = absent(self.vars.iter(), vars);
        if !missing.is_empty() {
            return Err(format!(
                "{name}: required variables are missing: {missing:?}"
            ));
        }
        let extra: Vec<&String> = vars
            .keys()
            .filter(|var| !self.vars.contains(*var))
            .collect();
        if !extra.is_empty() {
            return Err(format!("{name}: undeclared variables: {extra:?}"));
        }
        match (&self.msg, msg) {
            (None, None) => Ok(()),
            (None, Some(_)) => Err(format!("{name}: carries an undeclared message")),
            (Some(_), None) => Err(format!("{name}: the required message is missing")),
            (Some(fields), Some(Value::Object(msg))) => {
                let missing = absent(fields.keys(), msg);
                if !missing.is_empty() {
                    return Err(format!("{name}: message fields are missing: {missing:?}"));
                }
                for (field, value) in msg {
                    match fields.get(field) {
                        Some(Some(only)) if only != value => {
                            return Err(format!("{name}: message {field} is {value}, not {only}"));
                        }
                        Some(_) => {}
                        None if self.optional.contains(field) => {}
                        None => return Err(format!("{name}: undeclared message field {field:?}")),
                    }
                }
                Ok(())
            }
            (Some(_), Some(_)) => Err(format!("{name}: msg is not an object")),
        }
    }
}

fn absent<'a>(
    required: impl Iterator<Item = &'a String>,
    given: &Map<String, Value>,
) -> Vec<&'a String> {
    required.filter(|key| !given.contains_key(*key)).collect()
}

/// A spec set: a sorted JSON array without repeats.
pub fn set(items: impl IntoIterator<Item = impl Into<Value>>) -> Value {
    let mut items: Vec<Value> = items.into_iter().map(Into::into).collect();
    items.sort_by_cached_key(|item| (item.as_i64(), item.to_string()));
    items.dedup();
    Value::Array(items)
}

/// A trace to install. Build it with the spec's name, then declare everything.
pub struct Builder {
    spec: String,
    workload: String,
    params: Value,
    nodes: BTreeMap<u64, String>,
    actions: Vec<Action>,
    wide: BTreeSet<String>,
    faults: FaultMap,
    delays: bool,
}

impl Builder {
    /// The constants the trace spec reads, under their spec names.
    #[must_use]
    pub fn params(mut self, params: Value) -> Self {
        self.params = params;
        self
    }

    /// The spec's name for a World node. A node without one is its decimal id.
    #[must_use]
    pub fn node(mut self, node: u64, name: &str) -> Self {
        self.nodes.insert(node, name.to_owned());
        self
    }

    #[must_use]
    pub fn action(mut self, action: Action) -> Self {
        self.actions.push(action);
        self
    }

    /// A field of `vars`, `msg` or `params`, at any depth, whose integers can outgrow 32 bits
    /// (an id, a hash, a timestamp). Its integers, and those inside it, are written as
    /// decimal strings on every line, so its type never depends on a value. An integer that
    /// outgrows 32 bits anywhere else is an error.
    #[must_use]
    pub fn wide(mut self, field: &str) -> Self {
        self.wide.insert(field.to_owned());
        self
    }

    /// Maps each vocabulary fault to the spec's own action. The function may rename the
    /// line, fill its variables and message, or return `None` for a fault the spec does
    /// not model. A line it changes must be a declared action and meet its declaration.
    /// Without this, faults keep their vocabulary lines. The function runs under the
    /// writer's lock, so it must not use the trace or wait.
    #[must_use]
    pub fn faults(mut self, map: impl Fn(FaultLine) -> Option<FaultLine> + Send + 'static) -> Self {
        self.faults = Box::new(map);
        self
    }

    /// Also logs a `DelayMessage` line for every delayed message (off by default).
    #[must_use]
    pub fn delays(mut self, delays: bool) -> Self {
        self.delays = delays;
        self
    }

    /// Opens the trace of this run under `NABEDRA_TRACE_DIR` and subscribes it to `world`.
    /// `Ok(None)` when the variable is unset or empty, or while a sweep shrinks a failing
    /// tape ([`suppressed`]). Install it before the run's first fault or step.
    ///
    /// Fails when the sources are not those of the commit the trace would name, unless
    /// `NABEDRA_TRACE_UNCLEAN=mark`.
    pub fn install(self, world: &Arc<World>) -> io::Result<Option<TlaTrace>> {
        match trace_dir(std::env::var_os(DIR_VAR)) {
            Some(dir) if !SUPPRESSED.get() => {
                let unclean = std::env::var(UNCLEAN_VAR).ok();
                let commit = commit::current()
                    .and_then(|commit| commit.strict(unclean.as_deref()))
                    .map_err(invalid)?;
                self.subscribe(world, &dir, &commit).map(Some)
            }
            _ => Ok(None),
        }
    }

    /// [`Builder::install`] into `dir`, whatever the environment says. A trace of unclean
    /// sources says so in its workload name.
    pub fn install_in(self, world: &Arc<World>, dir: &Path) -> io::Result<TlaTrace> {
        let commit = commit::current().map_err(invalid)?;
        self.subscribe(world, dir, &commit)
    }

    fn subscribe(self, world: &Arc<World>, dir: &Path, commit: &Commit) -> io::Result<TlaTrace> {
        let trace = TlaTrace::open(self, world, dir, commit)?;
        let observed = trace.steps();
        world.observe(Arc::new(move |event| observed.observe(event)));
        Ok(trace)
    }

    fn validate(&self) -> Result<(), String> {
        if !identifier(&self.spec) {
            return Err(format!("{:?} is not a spec name", self.spec));
        }
        if self.workload.trim().is_empty() {
            return Err("the workload has no name".into());
        }
        if self.params.as_object().is_none_or(Map::is_empty) {
            return Err("params must name the spec's constants".into());
        }
        let mut names = BTreeSet::new();
        for action in &self.actions {
            let name = &action.name;
            if !identifier(name) {
                return Err(format!("{name:?} is not an action name"));
            }
            if !names.insert(name) {
                return Err(format!("{name} is declared twice"));
            }
            match &action.unconstrained {
                None if action.vars.is_empty() => {
                    return Err(format!(
                        "{name} declares no variables; use Action::unconstrained with a reason"
                    ));
                }
                Some(why) if why.trim().is_empty() => {
                    return Err(format!("{name} gives no reason for carrying no variables"));
                }
                _ => {}
            }
        }
        let mut names = BTreeSet::from([faults::WORLD.to_owned()]);
        for (id, name) in &self.nodes {
            let other = name.parse::<u64>().is_ok_and(|named| named != *id);
            if name.trim().is_empty() || other || !names.insert(name.clone()) {
                return Err(format!(
                    "node {id} cannot be named {name:?}: blank, or the name of something else"
                ));
            }
        }
        if self.actions.iter().all(|action| action.vars.is_empty()) {
            return Err("no declared action carries a variable, so a trace shows nothing".into());
        }
        Ok(())
    }

    fn header(&self, seed: u64, commit: &Commit) -> Result<String, String> {
        let mut header = format!("{{\"trace\":\"{FORMAT}\",\"spec\":");
        let plain = BTreeSet::new();
        canonical(&Value::from(self.spec.as_str()), &plain, false, &mut header)?;
        header.push_str(&format!(",\"seed\":{seed},\"sha\":\"{}\"", commit.sha));
        header.push_str(",\"workload\":");
        canonical(
            &Value::from(commit.workload(&self.workload)),
            &plain,
            false,
            &mut header,
        )?;
        header.push_str(",\"params\":");
        canonical(&self.params, &self.wide, false, &mut header)
            .map_err(|error| format!("params: {error}"))?;
        header.push_str("}\n");
        Ok(header)
    }
}

/// A TLA+ identifier, as `tla_trace.py lint` accepts spec and action names.
fn identifier(name: &str) -> bool {
    !name.starts_with(|first: char| first.is_ascii_digit())
        && !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

/// The open trace of one run. It must be [finished](TlaTrace::finish).
#[must_use = "a trace that is not finished is never published"]
#[derive(Debug)]
pub struct TlaTrace {
    steps: TlaSteps,
}

/// Writes steps of an open trace. Clone it into every part of a harness that logs steps.
#[derive(Clone)]
pub struct TlaSteps {
    inner: Arc<Mutex<Inner>>,
}

impl std::fmt::Debug for TlaSteps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlaSteps").finish_non_exhaustive()
    }
}

enum Stage {
    Open {
        out: BufWriter<Sink>,
        partial: PathBuf,
    },
    Published,
    Abandoned,
}

struct Inner {
    run: u64,
    world: Weak<World>,
    path: PathBuf,
    /// An earlier trace of the same seed, moved aside while this run is open.
    previous: Option<PathBuf>,
    stage: Stage,
    seq: u64,
    last_t: u64,
    /// Lines that carry a variable.
    constrained: u64,
    error: Option<String>,
    nodes: BTreeMap<u64, String>,
    actions: BTreeMap<String, Action>,
    wide: BTreeSet<String>,
    faults: FaultMap,
    delays: bool,
    tracker: faults::Tracker,
}

/// The file under the buffer, with the write errors the tests ask for.
struct Sink(File);

impl Sink {
    fn create(path: &Path) -> io::Result<Self> {
        #[cfg(test)]
        if tests::fails(tests::Fail::FullDisk) {
            return Ok(Self(fs::OpenOptions::new().write(true).open("/dev/full")?));
        }
        // New, so no other run writes into it.
        File::create_new(path).map(Self)
    }

    fn sync_dir(dir: &File) -> io::Result<()> {
        #[cfg(test)]
        if tests::fails(tests::Fail::SyncDir) {
            return Err(io::Error::other("injected directory sync error"));
        }
        #[cfg(test)]
        if tests::fails(tests::Fail::PanicWhileNaming) {
            let _ = std::panic::catch_unwind(|| panic!("planted"));
        }
        dir.sync_all()
    }
}

impl io::Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        #[cfg(test)]
        if tests::fails(tests::Fail::OneWrite) {
            return Err(io::Error::other("injected write error"));
        }
        self.0.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

thread_local! {
    static SUPPRESSED: Cell<bool> = const { Cell::new(false) };
}

/// Runs `f` with [`Builder::install`] off on this thread. A sweep shrinks with it, since a
/// candidate tape is a different run of the same seed and would take its file.
pub fn suppressed<T>(f: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            SUPPRESSED.set(self.0);
        }
    }
    let _restore = Restore(SUPPRESSED.replace(true));
    f()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn invalid(error: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, format!("tla trace: {error}"))
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    name.into()
}

impl TlaTrace {
    /// Starts describing a trace of `spec`, recorded by `workload`.
    pub fn builder(spec: &str, workload: &str) -> Builder {
        Builder {
            spec: spec.to_owned(),
            workload: workload.to_owned(),
            params: Value::Null,
            nodes: BTreeMap::new(),
            actions: Vec::new(),
            wide: BTreeSet::new(),
            faults: Box::new(Some),
            delays: false,
        }
    }

    fn open(builder: Builder, world: &Arc<World>, dir: &Path, commit: &Commit) -> io::Result<Self> {
        builder.validate().map_err(invalid)?;
        let seed = world.seed();
        let header = builder.header(seed, commit).map_err(invalid)?;
        fs::create_dir_all(dir)?;
        let path = dir
            .canonicalize()?
            .join(format!("{}-{seed}.ndjson", builder.spec));
        let run = runs::open(world, &path).map_err(invalid)?;
        // Owns the registered run from here on.
        let trace = Self {
            steps: TlaSteps {
                inner: Arc::new(Mutex::new(Inner {
                    run,
                    world: Arc::downgrade(world),
                    path: path.clone(),
                    previous: None,
                    stage: Stage::Abandoned,
                    seq: 0,
                    last_t: 0,
                    constrained: 0,
                    error: None,
                    nodes: builder.nodes,
                    actions: builder
                        .actions
                        .into_iter()
                        .map(|action| (action.name.clone(), action))
                        .collect(),
                    wide: builder.wide,
                    faults: builder.faults,
                    delays: builder.delays,
                    tracker: faults::Tracker::default(),
                })),
            },
        };
        // Another process may trace the same seed into the same directory.
        static OPENED: AtomicU64 = AtomicU64::new(0);
        let opened = OPENED.fetch_add(1, Ordering::Relaxed);
        let own = with_suffix(&path, &format!(".{}-{opened}", std::process::id()));
        // From here a trace of an earlier run of this seed no longer looks like this run's.
        if path.exists() {
            let previous = with_suffix(&own, ".previous");
            fs::rename(&path, &previous)?;
            lock(&trace.steps.inner).previous = Some(previous);
        }
        let partial = with_suffix(&own, ".partial");
        let mut out = BufWriter::new(Sink::create(&partial)?);
        out.write_all(header.as_bytes())?;
        lock(&trace.steps.inner).stage = Stage::Open { out, partial };
        Ok(trace)
    }

    /// A handle for the parts of the harness that log steps.
    pub fn steps(&self) -> TlaSteps {
        self.steps.clone()
    }

    /// Logs what `net` does to messages and links. Only for routers that act on each plan
    /// as it is drawn; a stream transport that turns a planned drop into something else
    /// must log its own fates.
    pub fn tap_net(&self, net: &mut crate::net::NetPlan) {
        let steps = self.steps();
        net.set_tap(Some(Arc::new(move |event| steps.observe_net(event))));
    }

    /// Publishes the trace and returns its path. Call it when the workload's last step is
    /// logged; faults after it, such as the harness tearing the cluster down, are not part
    /// of the trace. Every problem the run had is an error here, and then no `.ndjson` file
    /// exists.
    pub fn finish(self) -> io::Result<PathBuf> {
        // Dropping `self` forgets a run that published nothing.
        lock(&self.steps.inner).publish()
    }
}

impl Drop for TlaTrace {
    fn drop(&mut self) {
        let (run, path, unfinished) = {
            let mut inner = lock(&self.steps.inner);
            let unfinished = matches!(inner.stage, Stage::Open { .. });
            if unfinished {
                // Flushes what there is into the partial file.
                inner.stage = Stage::Abandoned;
            }
            (inner.run, inner.path.clone(), unfinished)
        };
        runs::release(run);
        assert!(
            !unfinished || std::thread::panicking(),
            "tla trace {} was dropped without finish()",
            path.display()
        );
    }
}

impl TlaSteps {
    /// Logs one protocol step, at the point where it takes effect. `action` is a declared
    /// spec action, `vars` exactly the variables it declared, with the post-state of `node`'s
    /// own entries, and `msg` the spec's record of the message sent or received.
    ///
    /// A step the writer refuses makes [`TlaTrace::finish`] fail. A client's or a transport's
    /// own step (a deadline that passes, a reply that is thrown away) is logged by whoever
    /// decides it, under its own [`NodeRef::Named`].
    ///
    /// # Panics
    ///
    /// After `finish`: the published trace misses a step, so it loses its name first.
    pub fn step(&self, node: impl Into<NodeRef>, action: &str, vars: Value, msg: Option<Value>) {
        let mut inner = lock(&self.inner);
        if matches!(inner.stage, Stage::Published) {
            let path = inner.path.clone();
            inner.stage = Stage::Abandoned;
            drop(inner);
            runs::withdraw(&path);
            panic!(
                "tla trace {}: step {action} came after finish(); the trace is withdrawn",
                path.display()
            );
        }
        let node = inner.resolve(node.into());
        if let NodeRef::World(id) = node
            && !inner.world.upgrade().is_some_and(|world| world.is_up(id))
        {
            return inner.fail(&format!("{action}: node {id} is not up"));
        }
        let checked = match inner.actions.get(action) {
            Some(declared) => declared.check(&vars, msg.as_ref()),
            None => Err(format!("action {action} is not declared")),
        };
        match checked {
            Ok(()) => inner.write(&node, action, &vars, msg.as_ref()),
            Err(error) => inner.fail(&error),
        }
    }
}

impl Inner {
    /// Records the first problem. The run goes on, since a panic here could be swallowed
    /// by a task; `finish` reports it.
    fn fail(&mut self, error: &str) {
        if matches!(self.stage, Stage::Open { .. }) && self.error.is_none() {
            eprintln!("tla trace {}: {error}", self.path.display());
            self.error = Some(format!("seq {}: {error}", self.seq));
        }
    }

    /// A spec name or decimal id of a World node stands for the node itself.
    fn resolve(&self, node: NodeRef) -> NodeRef {
        let NodeRef::Named(name) = &node else {
            return node;
        };
        let named = self.nodes.iter().find(|(_, known)| *known == name);
        let id = named.map(|(id, _)| *id).or_else(|| name.parse().ok());
        id.map_or(node, NodeRef::World)
    }

    fn node_name(&self, node: &NodeRef) -> String {
        match node {
            NodeRef::World(id) => self
                .nodes
                .get(id)
                .cloned()
                .unwrap_or_else(|| id.to_string()),
            NodeRef::Named(name) => name.clone(),
        }
    }

    fn now(&self) -> Result<u64, String> {
        let world = self
            .world
            .upgrade()
            .ok_or("the simulated world has ended")?;
        // Outside a runtime tokio's clock is the real one.
        if tokio::runtime::Handle::try_current().is_err() {
            return Err("there is no virtual time outside the simulation's runtime".into());
        }
        let t = u64::try_from(world.elapsed().as_nanos())
            .map_err(|_| "virtual time is beyond 64 bits of nanoseconds")?;
        if t < self.last_t {
            return Err(format!("t {t} is before the last step's {}", self.last_t));
        }
        Ok(t)
    }

    /// The time and text of the next line.
    fn line(
        &self,
        node: &NodeRef,
        action: &str,
        vars: &Value,
        msg: Option<&Value>,
    ) -> Result<(u64, String), String> {
        let node = self.node_name(node);
        if node.trim().is_empty() {
            return Err("the node has a blank name".into());
        }
        let t = self.now()?;
        let mut line = format!("{{\"seq\":{},\"t\":{t},\"node\":", self.seq);
        canonical(&Value::from(node), &self.wide, false, &mut line)?;
        line.push_str(&format!(",\"action\":\"{action}\",\"vars\":"));
        canonical(vars, &self.wide, false, &mut line)?;
        if let Some(msg) = msg {
            line.push_str(",\"msg\":");
            canonical(msg, &self.wide, false, &mut line)?;
        }
        line.push_str("}\n");
        Ok((t, line))
    }

    /// Writes one line that already met its declaration.
    fn write(&mut self, node: &NodeRef, action: &str, vars: &Value, msg: Option<&Value>) {
        if self.error.is_some() || !matches!(self.stage, Stage::Open { .. }) {
            return;
        }
        let (t, line) = match self.line(node, action, vars, msg) {
            Ok(line) => line,
            Err(error) => return self.fail(&format!("{action}: {error}")),
        };
        let Stage::Open { out, .. } = &mut self.stage else {
            return;
        };
        if let Err(error) = out.write_all(line.as_bytes()) {
            return self.fail(&format!("{action}: write failed: {error}"));
        }
        self.seq += 1;
        self.last_t = t;
        self.constrained += u64::from(vars.as_object().is_some_and(|vars| !vars.is_empty()));
    }

    /// Ends the run. Only a run without any problem leaves [`Stage::Published`].
    fn publish(&mut self) -> io::Result<PathBuf> {
        let stage = std::mem::replace(&mut self.stage, Stage::Abandoned);
        let refused = |why: String| invalid(format!("{}: {why}", self.path.display()));
        let Stage::Open { mut out, partial } = stage else {
            return Err(refused("it is not open".into()));
        };
        if let Some(error) = &self.error {
            return Err(refused(error.clone()));
        }
        if std::thread::panicking() {
            return Err(refused("the run is panicking".into()));
        }
        if !runs::watching() {
            return Err(refused(
                "the panic hook was replaced, so nobody knows whether the run panicked".into(),
            ));
        }
        if let Some(panic) = runs::panicked(self.run) {
            return Err(refused(panic));
        }
        if self.constrained == 0 {
            return Err(refused(format!(
                "none of its {} steps carries a variable, so it shows nothing",
                self.seq
            )));
        }
        out.flush()?;
        out.get_ref().0.sync_all()?;
        drop(out);
        if let Some(previous) = &self.previous
            && fs::read(&partial)? != fs::read(previous)?
        {
            return Err(refused(format!(
                "it differs from the earlier trace of this seed, kept as {}: the run is not \
                 deterministic, or the directory holds another build's or workload's traces",
                previous.display()
            )));
        }
        // Opened before the file has its name, so a directory that cannot be synced leaves none.
        let dir = match self.path.parent() {
            Some(dir) => File::open(dir)?,
            None => return Err(refused("it has no directory".into())),
        };
        // A link, because a rename would replace a file that another process just published.
        match fs::hard_link(&partial, &self.path) {
            Ok(()) => {
                let named =
                    Sink::sync_dir(&dir).and_then(|()| runs::published(self.run).map_err(&refused));
                if let Err(error) = named {
                    runs::withdraw(&self.path);
                    return Err(error);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                if fs::read(&partial)? != fs::read(&self.path)? {
                    return Err(refused(
                        "another process published a different trace of this seed".into(),
                    ));
                }
                runs::published(self.run).map_err(&refused)?;
            }
            Err(error) => return Err(error),
        }
        self.stage = Stage::Published;
        // Leftovers that no checker reads.
        let _ = fs::remove_file(&partial);
        if let Some(previous) = &self.previous {
            let _ = fs::remove_file(previous);
        }
        Ok(self.path.clone())
    }
}

/// Appends `value` in one fixed form: sorted keys, no spaces, only what TLC reads as written.
/// Integers inside a field named in `wide` become decimal strings.
fn canonical(
    value: &Value,
    wide: &BTreeSet<String>,
    in_wide: bool,
    out: &mut String,
) -> Result<(), String> {
    match value {
        Value::Null => return Err("null has no TLA+ value".into()),
        Value::Bool(value) => out.push_str(if *value { "true" } else { "false" }),
        Value::Number(number) if in_wide => {
            if !number.is_i64() && !number.is_u64() {
                return Err(format!("{number} is not an integer"));
            }
            out.push_str(&format!("\"{number}\""));
        }
        Value::Number(number) => match number.as_i64().and_then(|n| i32::try_from(n).ok()) {
            Some(number) => out.push_str(&number.to_string()),
            None => {
                return Err(format!(
                    "{number} is not an integer TLC reads (32 bits); declare its field wide"
                ));
            }
        },
        Value::String(_) => out.push_str(&value.to_string()),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                canonical(item, wide, in_wide, out)?;
            }
            out.push(']');
        }
        Value::Object(fields) => {
            let sorted: BTreeMap<&String, &Value> = fields.iter().collect();
            out.push('{');
            for (i, (key, item)) in sorted.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::from(key.as_str()).to_string());
                out.push(':');
                canonical(item, wide, in_wide || wide.contains(key), out)
                    .map_err(|error| format!("{key}: {error}"))?;
            }
            out.push('}');
        }
    }
    Ok(())
}

fn trace_dir(value: Option<OsString>) -> Option<PathBuf> {
    value.filter(|dir| !dir.is_empty()).map(PathBuf::from)
}

#[cfg(test)]
mod tests;
