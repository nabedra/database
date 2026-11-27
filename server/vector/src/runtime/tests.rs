//! Priority classes, admission before work, the pinned shared-nothing cores and budgets.
//! Scenarios of `docs/testing/scheduler-contract.md`: Q02 (explicit overload before acceptance),
//! U03 (seeded event sequences, conservation), F02 (a panicking task is contained), F03 (queued
//! work settles once at shutdown), M01/M03 (budgets checked before growth, an impossible task
//! fails explicitly).

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use nabedra_check::prop::check;
use nabedra_rt::sync::Condvar;

use super::*;
use crate::types::Error;

fn limits(tasks: usize, cost: u64) -> Limits {
    Limits {
        tasks: [tasks; 5],
        cost: [cost; 5],
    }
}

// ----- The scheduler.

#[test]
fn vs17_apply_gets_its_fifth_under_contention() {
    let mut s = Scheduler::new(limits(10_000, u64::MAX / 8));
    for i in 0..5000u32 {
        s.submit(Class::Apply, 1, i).unwrap();
        s.submit(Class::Interactive, 1, i).unwrap();
    }
    let ran: Vec<Class> = (0..1000).map(|_| s.pop().unwrap().0).collect();
    let apply = ran.iter().filter(|c| **c == Class::Apply).count();
    assert!((199..=201).contains(&apply), "apply ran {apply} of 1000");
    // Over any window of 50, apply had about its fifth: the share is kept as it goes.
    for w in ran.windows(50) {
        let a = w.iter().filter(|c| **c == Class::Apply).count();
        assert!((9..=11).contains(&a), "{a}");
    }
}

#[test]
fn vs17_unequal_costs_share_by_work_not_by_count() {
    let mut s = Scheduler::new(limits(10_000, u64::MAX / 8));
    for i in 0..2000u32 {
        s.submit(Class::Apply, 1, i).unwrap();
        s.submit(Class::Batch, 10, i).unwrap();
    }
    let mut work = [0u64; 2];
    for _ in 0..1000 {
        match s.pop().unwrap().0 {
            Class::Apply => work[0] += 1,
            _ => work[1] += 10,
        }
    }
    let share = work[0] as f64 / (work[0] + work[1]) as f64;
    assert!((0.19..=0.21).contains(&share), "{share}");
}

#[test]
fn vs17_classes_run_in_priority_order_and_nothing_idles() {
    let mut s = Scheduler::new(Limits::default());
    for c in [Class::Exact, Class::Batch, Class::Maintenance, Class::Interactive] {
        s.submit(c, 1, c).unwrap();
    }
    let order: Vec<Class> = std::iter::from_fn(|| s.pop().map(|t| t.0)).collect();
    assert_eq!(
        order,
        [Class::Interactive, Class::Maintenance, Class::Batch, Class::Exact]
    );
    // Apply alone takes the whole core.
    for i in 0..10 {
        s.submit(Class::Apply, 5, Class::Apply).unwrap();
        let _ = i;
    }
    assert_eq!(std::iter::from_fn(|| s.pop()).count(), 10);
}

#[test]
fn vs17_overload_is_refused_before_work_starts() {
    let mut s = Scheduler::new(Limits {
        tasks: [2, 3, 3, 3, 3],
        cost: [100, 50, 50, 50, 50],
    });
    for i in 0..3 {
        s.submit(Class::Interactive, 10, i).unwrap();
    }
    assert_eq!(s.submit(Class::Interactive, 1, 9), Err(Error::Overloaded));
    // Queued cost is a limit of its own, and a task larger than its class ever holds fails.
    assert_eq!(s.submit(Class::Batch, 51, 9), Err(Error::Overloaded));
    s.submit(Class::Batch, 40, 1).unwrap();
    assert_eq!(s.submit(Class::Batch, 11, 2), Err(Error::Overloaded));
    // A full apply queue pauses the feed: Busy, never a refused write.
    s.submit(Class::Apply, 1, 1).unwrap();
    s.submit(Class::Apply, 1, 2).unwrap();
    assert_eq!(s.submit(Class::Apply, 1, 3), Err(Error::Busy));
    // Refused tasks never run; room comes back as tasks leave.
    let ran: Vec<_> = std::iter::from_fn(|| s.pop()).collect();
    assert_eq!(ran.len(), 6);
    assert!(!ran.contains(&(Class::Interactive, 9)));
    s.submit(Class::Interactive, 10, 4).unwrap();
}

/// Seeded sequences of submissions and dispatches: every accepted task comes out exactly once,
/// nothing refused comes out, and queued cost always matches what is queued (U03).
#[test]
fn vs17_every_accepted_task_runs_once() {
    check("vs17_every_accepted_task_runs_once", |g| {
        let lim = Limits {
            tasks: [g.below(6) as usize + 1; 5],
            cost: [g.below(60) + 1; 5],
        };
        let mut s = Scheduler::new(lim);
        let (mut accepted, mut ran) = (BTreeSet::new(), BTreeSet::new());
        let mut queued = [0u64; 5];
        let mut costs = std::collections::BTreeMap::new();
        for id in 0..g.below(200) {
            if g.chance(2, 5) {
                if let Some((c, t)) = s.pop() {
                    if !ran.insert(t) || !accepted.contains(&t) {
                        return Err(format!("{t} ran twice or was never accepted"));
                    }
                    queued[c as usize] -= costs[&t];
                }
            } else {
                let c = *g.pick(&Class::ALL);
                let cost = g.below(30);
                if s.submit(c, cost, id).is_ok() {
                    accepted.insert(id);
                    costs.insert(id, cost);
                    queued[c as usize] += cost;
                }
            }
            if s.queued() != queued {
                return Err(format!("queued {:?} against {queued:?}", s.queued()));
            }
        }
        while let Some((_, t)) = s.pop() {
            if !ran.insert(t) {
                return Err(format!("{t} ran twice"));
            }
        }
        if ran != accepted {
            return Err("an accepted task never ran".into());
        }
        Ok(())
    });
}

// ----- The cores.

/// A gate the test opens.
#[derive(Default)]
struct Gate {
    open: Mutex<bool>,
    cv: Condvar,
}

impl Gate {
    fn wait(&self) {
        let g = self.open.lock().unwrap();
        drop(self.cv.wait_while(g, |o| !*o).unwrap());
    }
    fn open(&self) {
        *self.open.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.cv.notify_all();
    }
}

/// Opens a gate when the test ends, also by a failed assertion, so the runtime's threads can be
/// joined instead of hanging.
struct Opens(Arc<Gate>);

impl Drop for Opens {
    fn drop(&mut self) {
        self.0.open();
    }
}

#[test]
fn vs17_each_core_is_pinned_and_owns_its_state() {
    let n = 4;
    // The state is not Send: it can only live on the thread that made it.
    let cores = Cores::start(n, Limits::default(), true, |core| {
        (std::rc::Rc::new(core), Vec::<usize>::new())
    })
    .unwrap();
    let allowed = nabedra_rt::task::allowed_cpus().unwrap();
    if allowed.len() >= n {
        let pinned: BTreeSet<_> = cores.cpus().iter().map(|c| c.unwrap()).collect();
        assert_eq!(pinned.len(), n, "{:?}", cores.cpus());
    }
    for core in 0..n {
        let (me, cpu) = cores.call(core, Class::Interactive, 1, |s, ctx| (*s.0, ctx.cpu())).unwrap();
        assert_eq!(me, core);
        assert_eq!(cpu, cores.cpus()[core]);
    }
    // A token goes round the ring by messages; each core records it in its own state.
    let done = Arc::new(Gate::default());
    let d = done.clone();
    fn hop(s: &mut (std::rc::Rc<usize>, Vec<usize>), ctx: &Ctx<(std::rc::Rc<usize>, Vec<usize>)>, left: usize, done: Arc<Gate>) {
        s.1.push(left);
        if left == 0 {
            done.open();
            return;
        }
        let next = (ctx.core() + 1) % ctx.cores();
        ctx.send(next, Class::Interactive, 1, move |s, ctx| hop(s, ctx, left - 1, done))
            .unwrap();
    }
    cores.submit(0, Class::Interactive, 1, move |s, ctx| hop(s, ctx, 11, d)).unwrap();
    done.wait();
    for core in 0..n {
        let seen = cores.call(core, Class::Interactive, 1, |s, _| s.1.clone()).unwrap();
        let want: Vec<usize> = (0..12).rev().filter(|l| (11 - l) % n == core).collect();
        assert_eq!(seen, want, "core {core}");
    }
    cores.shutdown();
}

#[test]
fn vs17_a_busy_core_refuses_more_before_starting_it() {
    let cores = Cores::start(2, limits(3, 1000), false, |_| 0u64).unwrap();
    let gate = Arc::new(Gate::default());
    let started = Arc::new(Gate::default());
    let ran = Arc::new(AtomicU64::new(0));
    let _opens = Opens(gate.clone());
    let (g, st) = (gate.clone(), started.clone());
    cores.submit(0, Class::Interactive, 1, move |_, _| {
        st.open();
        g.wait();
    })
    .unwrap();
    started.wait();
    for _ in 0..3 {
        let r = ran.clone();
        cores.submit(0, Class::Interactive, 1, move |_, _| {
            r.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    }
    let r = ran.clone();
    let refused = cores.submit(0, Class::Interactive, 1, move |_, _| {
        r.fetch_add(100, Ordering::SeqCst);
    });
    assert_eq!(refused.err(), Some(Error::Overloaded));
    // The other core is not affected.
    assert_eq!(cores.call(1, Class::Interactive, 1, |s, _| *s + 1).unwrap(), 1);
    gate.open();
    // Another class: the interactive queue may still be full while the three run.
    assert_eq!(cores.call(0, Class::Batch, 1, |_, _| ()).ok(), Some(()));
    assert_eq!(ran.load(Ordering::SeqCst), 3, "the refused task ran");
}

#[test]
fn vs17_a_panicking_task_is_contained() {
    let cores = Cores::start(1, Limits::default(), false, |_| 7u32).unwrap();
    let r = cores.call(0, Class::Batch, 1, |_, _| -> u32 { panic!("boom") });
    assert!(matches!(r, Err(Error::Internal(_))));
    // Unwinding drops the answer before the core increments its counter.
    // The next call runs after the core has finished processing that panic.
    assert_eq!(cores.call(0, Class::Interactive, 1, |s, _| *s).unwrap(), 7);
    assert_eq!(cores.panics(), 1);
}

#[test]
fn vs17_shutdown_settles_queued_callers() {
    let cores = Arc::new(Cores::start(1, Limits::default(), false, |_| ()).unwrap());
    let gate = Arc::new(Gate::default());
    let started = Arc::new(Gate::default());
    let _opens = Opens(gate.clone());
    let (g, st) = (gate.clone(), started.clone());
    cores.submit(0, Class::Interactive, 1, move |_, _| {
        st.open();
        g.wait();
    })
    .unwrap();
    started.wait();
    let c = cores.clone();
    let waiter = nabedra_rt::task::spawn_service("waiter", move || c.call(0, Class::Batch, 1, |_, _| 5)).unwrap();
    while cores.queued(0) == 0 {
        std::thread::yield_now();
    }
    // Stopping drops the queued call, which answers with an error, and refuses new work.
    cores.stop();
    assert!(matches!(waiter.join().unwrap(), Err(Error::Internal(_))));
    assert_eq!(cores.submit(0, Class::Apply, 1, |_, _| ()).err(), Some(Error::Overloaded));
    gate.open();
    let Ok(cores) = Arc::try_unwrap(cores) else {
        panic!("the waiter still holds the runtime");
    };
    cores.shutdown();
}

// ----- Budgets.

#[test]
fn vs17_a_meter_refuses_before_the_limit_and_stays_refused() {
    let mut m = Meter::new(Budget {
        cpu: 10,
        io: 100,
        memory: 50,
    });
    m.cpu(10).unwrap();
    assert_eq!(m.cpu(1), Err(Error::OverBudget("cpu")));
    // Once over, nothing more is charged.
    assert_eq!(m.io(1), Err(Error::OverBudget("cpu")));
    let mut m = Meter::new(Budget { memory: 50, ..Budget::UNLIMITED });
    m.grow(40).unwrap();
    m.shrink(30);
    m.grow(40).unwrap();
    assert_eq!(m.used().peak, 50);
    assert_eq!(m.grow(1), Err(Error::OverBudget("memory")));
    assert_eq!(m.used().memory, 50, "the refused growth is not charged");
    assert_eq!(Meter::new(Budget { io: 3, ..Budget::UNLIMITED }).io(u64::MAX), Err(Error::OverBudget("io")));
}

// ----- Review rv-dense (2026-11-27).

/// Limits are public and a task declares its own cost, so a class may accept costs near
/// `u64::MAX`. `pop` must still not overflow while it sums the work served: it runs outside the
/// core's `catch_unwind`, so a panic there kills the core's thread.
#[test]
#[ignore = "VEC-BUG-scheduler-served-sum-overflow"]
fn vs50_a_pop_after_costs_near_the_limit_does_not_overflow() {
    let mut s = Scheduler::new(limits(16, u64::MAX));
    s.submit(Class::Apply, u64::MAX, 0u8).unwrap();
    assert!(s.pop().is_some());
    s.submit(Class::Interactive, u64::MAX, 1u8).unwrap();
    // served is now [u64::MAX / 2, u64::MAX, 0, 0, 0]: their sum overflows.
    assert!(s.pop().is_some());
}

/// The same through the cores: after such a pop the core's thread is gone, and a later call
/// waits for ever instead of answering.
#[test]
#[ignore = "VEC-BUG-scheduler-served-sum-overflow"]
fn vs51_a_core_answers_after_a_task_of_huge_declared_cost() {
    let cores = Arc::new(Cores::start(1, limits(16, u64::MAX), false, |_| 3u32).unwrap());
    assert_eq!(cores.call(0, Class::Apply, u64::MAX, |s, _| *s).unwrap(), 3);
    let _ = cores.call(0, Class::Interactive, u64::MAX, |s, _| *s);
    let (tell, told) = std::sync::mpsc::channel();
    let c = cores.clone();
    nabedra_rt::task::spawn_service("vs51-caller", move || {
        let _ = tell.send(c.call(0, Class::Interactive, 1, |s, _| *s));
    })
    .unwrap();
    let answer = told.recv_timeout(std::time::Duration::from_secs(5));
    assert!(answer.is_ok(), "no answer within 5 s: the core's thread died in pop");
}
