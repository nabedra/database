//! Panics of a run. A panic in the run's own body unwinds to the sweep, which reports
//! `harness`. One in a spawned task, in a product task or in a blocking job ends only that
//! task, and the runtime drops it. The hook here keeps every panic of a world, so a seed in
//! which anything panicked is never clean.

use std::{
    collections::BTreeMap,
    panic::{self, PanicHookInfo},
    sync::{Arc, Mutex, MutexGuard, Once, PoisonError},
};

use nabedra_rt::sim::{self, World};

/// The violation kind of a panic outside the run's own body.
pub(crate) const PANIC: &str = "Panic";

/// Per watched world, by address: its panics.
static LOG: Mutex<BTreeMap<usize, Vec<String>>> = Mutex::new(BTreeMap::new());

fn log() -> MutexGuard<'static, BTreeMap<usize, Vec<String>>> {
    LOG.lock().unwrap_or_else(PoisonError::into_inner)
}

fn install() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let previous = panic::take_hook();
        panic::set_hook(Box::new(move |info| {
            record(info);
            previous(info);
        }));
    });
}

fn record(info: &PanicHookInfo<'_>) {
    let payload = info.payload();
    let text = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
        .replace('\n', " | ");
    let at = info
        .location()
        .map_or_else(String::new, |at| format!(" at {}:{}", at.file(), at.line()));
    let panicked = format!("{text}{at}");
    let world = sim::world().map(|world| Arc::as_ptr(&world) as usize);
    let mut log = log();
    match world {
        Some(world) => {
            if let Some(panics) = log.get_mut(&world) {
                panics.push(panicked);
            }
        }
        // Outside every world nothing says whose panic it is, so no running seed is clean.
        None => log
            .values_mut()
            .for_each(|panics| panics.push(format!("{panicked} (outside every world)"))),
    }
}

/// Keeps the panics of one world from its creation to [`Watch::finish`].
pub(crate) struct Watch(usize);

impl Watch {
    pub(crate) fn new(world: &Arc<World>) -> Self {
        install();
        let key = Arc::as_ptr(world) as usize;
        log().insert(key, Vec::new());
        Self(key)
    }

    /// Every panic since the watch began, in order.
    pub(crate) fn finish(self) -> Vec<String> {
        log().remove(&self.0).unwrap_or_default()
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        log().remove(&self.0);
    }
}

/// Where a test makes a run panic.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum At {
    /// The run's own body.
    Body,
    /// A client task, under load.
    Client,
    /// The task that runs every step check.
    Ticker,
    /// A task that nothing joins.
    Task,
    /// A task of a worker's process.
    Node,
    /// A blocking job whose handle is dropped.
    Blocking,
}

#[cfg(test)]
impl At {
    pub(crate) const ALL: [At; 6] = [
        At::Body,
        At::Client,
        At::Ticker,
        At::Task,
        At::Node,
        At::Blocking,
    ];
}

/// A planted panic: where, and whether it fired.
#[cfg(test)]
pub(crate) struct Planted {
    pub(crate) at: At,
    pub(crate) fired: bool,
}

/// Panics once if the run has a panic planted here.
#[cfg(test)]
pub(crate) fn plant(world: &World, at: At) {
    let fire = world.with_ext::<Planted, _>(|planted| {
        let fire = planted.at == at && !planted.fired;
        planted.fired |= fire;
        fire
    });
    if fire == Some(true) {
        panic!("planted panic in {at:?}");
    }
}

#[cfg(test)]
mod tests {
    use std::panic::AssertUnwindSafe;

    use nabedra_sim::{shrink, sweep::workload_gen};

    use super::*;
    use crate::{
        nemesis::{SWEPT, Selection},
        sweep::{Defect, Setup},
        workload::run_seed,
        workloads::WorkloadChoice,
    };

    /// The kinds a sweep reports for `seed` with a panic planted at `at`.
    fn kinds(workload: &str, profile: &str, at: Option<At>, seed: u64) -> Vec<String> {
        let list: Vec<String> = if workload == "smoke" {
            Vec::new()
        } else {
            vec![profile.to_owned()]
        };
        let setup = Setup {
            workloads: WorkloadChoice::parse(workload).unwrap(),
            quiet: workload == "smoke" || Selection::quiet(&list),
            selection: Selection::parse(&list).unwrap(),
            panic_at: at,
        };
        let run = panic::catch_unwind(AssertUnwindSafe(|| {
            run_seed(seed, &mut workload_gen(seed), Defect::None, &setup)
        }));
        match run {
            Ok(result) => shrink::kinds(&result.run.violations).into_iter().collect(),
            // What the sweep and the search report for an unwound run.
            Err(_) => vec!["harness".into()],
        }
    }

    fn check(workload: &str, profile: &str, at: At, seed: u64) {
        let got = kinds(workload, profile, Some(at), seed);
        let want = if at == At::Body { "harness" } else { PANIC };
        assert!(
            got.iter().any(|kind| kind == want),
            "{workload} {profile} seed {seed}: a panic in {at:?} reported {got:?}"
        );
        println!("planted panic {workload} {profile} {at:?} seed {seed}: {got:?}");
    }

    /// Every swept profile, and none, with a panic in two places that rotate.
    #[test]
    fn a_panic_is_never_a_clean_seed() {
        for (index, profile) in SWEPT.into_iter().chain(["none"]).enumerate() {
            check("direct", profile, At::ALL[index % At::ALL.len()], 9);
            check("direct", profile, At::ALL[(index + 3) % At::ALL.len()], 9);
        }
        for at in [At::Body, At::Ticker, At::Task] {
            check("smoke", "none", at, 1);
        }
    }

    /// Every workload, profile and place. Run it by name.
    #[test]
    #[ignore = "the full matrix, about 100 runs"]
    fn no_panic_is_a_clean_seed_anywhere() {
        let mut profiles = SWEPT.to_vec();
        profiles.push("none");
        if std::env::var("NABEDRA_SIM_BEYOND_TCP").as_deref() == Ok("1") {
            profiles.push("beyond-tcp");
        }
        for workload in ["direct", "cold", "move"] {
            for profile in &profiles {
                for at in At::ALL {
                    check(workload, profile, at, 9);
                }
            }
        }
        for at in [At::Body, At::Ticker, At::Task, At::Node, At::Blocking] {
            check("smoke", "none", at, 1);
        }
    }

    /// Without a planted panic nothing is recorded, so the hook adds no red seed by itself.
    #[test]
    fn a_run_without_a_panic_reports_none() {
        let got = kinds("smoke", "none", None, 1);
        assert!(got.iter().all(|kind| kind != PANIC && kind != "harness"));
    }
}
