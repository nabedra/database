//! The runs this process traces, and which of them panicked.
//!
//! A panic in a simulated task does not stop a run: tokio keeps it in a `JoinHandle` that
//! nobody may ask. The lines up to it are a valid prefix, which TLC accepts, so only the
//! writer can tell. A process-wide panic hook, in front of the one it finds, marks:
//!
//! - every trace of the World installed on the panicking thread: the simulation thread, its
//!   blocking helpers and its service threads;
//! - with no World there, every open trace, because any of them may have started that
//!   thread. A thread that says its panics are its own ([`attributed`], as the sweeps do)
//!   marks only the traces it opened.
//!
//! A marked trace is not published, and one already published loses its name. A panic that
//! the code caught counts too. `resume_unwind` alone runs no hook; it passes on a panic
//! that did, or the cancellation of a suspended job, which is not one.
//!
//! Code that replaces the hook would blind this, so `open` and `finish` test it with a panic
//! of their own. `finish` refuses when that goes unseen, and an `open` that has to install
//! the hook again marks the runs that were open without it. A hook that other code replaces
//! and puts back in between goes unnoticed.

use std::cell::Cell;
use std::fs::{self, File};
use std::panic::{self, PanicHookInfo};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread::{self, ThreadId};

use nabedra_rt::sim::World;

use super::{lock, with_suffix};

struct Run {
    id: u64,
    world: Weak<World>,
    thread: ThreadId,
    path: PathBuf,
    published: bool,
    panic: Option<String>,
}

struct Runs {
    next: u64,
    list: Vec<Run>,
}

static RUNS: Mutex<Runs> = Mutex::new(Runs {
    next: 0,
    list: Vec::new(),
});

thread_local! {
    static ATTRIBUTED: Cell<bool> = const { Cell::new(false) };
    static SEEN: Cell<bool> = const { Cell::new(false) };
}

/// The payload of the hook's own test.
struct Probe;

/// Runs `f`, whose panics on this thread outside a World belong to no trace that another
/// thread opened.
pub fn attributed<T>(f: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            ATTRIBUTED.set(self.0);
        }
    }
    let _restore = Restore(ATTRIBUTED.replace(true));
    f()
}

/// Held while the hook is tested or installed, since the process has none for a moment
/// between taking the old one and setting the new one.
static HOOK: Mutex<()> = Mutex::new(());

/// Whether a panic on this thread reaches the hook.
pub(super) fn watching() -> bool {
    let _hook = lock(&HOOK);
    probe()
}

fn probe() -> bool {
    SEEN.set(false);
    let _ = panic::catch_unwind(|| panic::panic_any(Probe));
    SEEN.get()
}

fn watch() -> Result<(), String> {
    // Without it the first test would print a panic message.
    static INSTALLED: AtomicBool = AtomicBool::new(false);
    let _hook = lock(&HOOK);
    if INSTALLED.swap(true, Ordering::Relaxed) && probe() {
        return Ok(());
    }
    let before = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        if !note(info) {
            before(info);
        }
    }));
    for run in &mut lock(&RUNS).list {
        if !run.published {
            run.panic
                .get_or_insert("the panic hook was replaced during the run".into());
        }
    }
    if probe() {
        Ok(())
    } else {
        Err("the panic hook cannot be installed".into())
    }
}

/// Registers a run that writes `path`.
pub(super) fn open(world: &Arc<World>, path: &Path) -> Result<u64, String> {
    watch()?;
    let mut runs = lock(&RUNS);
    runs.list
        .retain(|run| !run.published || run.world.strong_count() > 0);
    if runs
        .list
        .iter()
        .any(|run| !run.published && run.path == path)
    {
        return Err(format!(
            "{} is already being written in this process",
            path.display()
        ));
    }
    let id = runs.next;
    runs.next += 1;
    runs.list.push(Run {
        id,
        world: Arc::downgrade(world),
        thread: thread::current().id(),
        path: path.to_owned(),
        published: false,
        panic: None,
    });
    Ok(id)
}

/// Forgets a run that published nothing.
pub(super) fn release(id: u64) {
    lock(&RUNS).list.retain(|run| run.published || run.id != id);
}

/// The panic a run had.
pub(super) fn panicked(id: u64) -> Option<String> {
    let runs = lock(&RUNS);
    let run = runs.list.iter().find(|run| run.id == id)?;
    run.panic.clone()
}

/// Marks a run published, unless it panicked meanwhile. From here a panic withdraws its file.
pub(super) fn published(id: u64) -> Result<(), String> {
    let mut runs = lock(&RUNS);
    match runs.list.iter_mut().find(|run| run.id == id) {
        Some(Run {
            panic: Some(panic), ..
        }) => Err(panic.clone()),
        Some(run) => {
            run.published = true;
            Ok(())
        }
        None => Err("the run is not registered".into()),
    }
}

/// Takes the published name from a trace that turned out not to be complete.
pub(super) fn withdraw(path: &Path) {
    let aside = with_suffix(path, ".withdrawn");
    if fs::rename(path, aside).is_err() && fs::remove_file(path).is_err() && path.exists() {
        // The name cannot be taken away, so the file must not read as a trace.
        let _ = File::create(path);
    }
}

/// Marks the runs this panic belongs to. True for the hook's own test.
fn note(info: &PanicHookInfo<'_>) -> bool {
    if info.payload().is::<Probe>() {
        SEEN.set(true);
        return true;
    }
    let thread = thread::current().id();
    let world = nabedra_rt::sim::world();
    let alone = ATTRIBUTED.get();
    let place = info.location().map_or_else(
        || "an unknown place".to_owned(),
        |at| format!("{}:{}", at.file(), at.line()),
    );
    let mut lost = Vec::new();
    {
        let mut runs = lock(&RUNS);
        let last = runs
            .list
            .iter()
            .rfind(|run| run.thread == thread)
            .map(|run| run.id);
        for run in &mut runs.list {
            let own = match &world {
                Some(world) => Weak::as_ptr(&run.world) == Arc::as_ptr(world),
                None if run.published => Some(run.id) == last && run.world.strong_count() > 0,
                None => run.thread == thread || !alone,
            };
            if own && run.panic.is_none() {
                run.panic = Some(format!("the run panicked at {place}"));
                if run.published {
                    lost.push(run.path.clone());
                }
            }
        }
    }
    for path in lost {
        eprintln!(
            "tla trace {}: withdrawn, the run panicked at {place}",
            path.display()
        );
        withdraw(&path);
    }
    false
}
