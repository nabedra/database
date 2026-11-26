//! Adversarial tests of the per-core runtime and the query budgets (task N): the apply share,
//! priority order, admission, shutdown with messages in flight, pinning on a subset of CPUs,
//! shared-nothing state, budgets that end a query without a partial result, and scans that read
//! alike from RAM and from NVMe. Written by someone who did not write the code under test.

mod common;

use std::cell::Cell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;
use std::time::Duration;

use common::*;
use nabedra_check::Gen;
use nabedra_check::prop::{Config as Runs, check_with};
use nabedra_rt::task;
use nabedra_vector::index::{ExactCodec, Index, Query, ReadAt, RecallEstimate};
use nabedra_vector::runtime::{APPLY_SHARE, Budget, Class, Cores, Limits, Memory, Meter, Scheduler};
use nabedra_vector::store::{Backend, NvmeVectors, RamVectors, Vectors};
use nabedra_vector::types::{Error, Op};

const OTHERS: [Class; 4] = [Class::Interactive, Class::Maintenance, Class::Batch, Class::Exact];

fn big() -> Limits {
    Limits { tasks: [usize::MAX; 5], cost: [u64::MAX / 8; 5] }
}

fn pick_class(g: &mut Gen) -> Class {
    *g.pick(&Class::ALL)
}

// ===== The scheduler.

/// While others have work, apply gets a fifth of the served cost: not less (no starvation), and
/// not more than a task over (no stealing), whatever the costs and whichever classes saturate.
#[test]
fn nr01_apply_keeps_its_fifth_under_saturation_from_any_class() {
    let runs = Runs { cases: Runs::default().cases / 2 + 1, ..Runs::default() };
    check_with(runs, "nr01", |g| {
        let mut busy: Vec<Class> = OTHERS.iter().copied().filter(|_| g.bool()).collect();
        if busy.is_empty() {
            busy.push(*g.pick(&OTHERS));
        }
        let (apply_max, other_max) = (g.below(60) + 1, g.below(60) + 1);
        let mut s: Scheduler<(u64, u64)> = Scheduler::new(big());
        let mut next = 0u64;
        for _ in 0..3000 {
            let mut add = |s: &mut Scheduler<(u64, u64)>, c: Class, max: u64, g: &mut Gen| -> Result<(), String> {
                let cost = g.below(max + 1);
                next += 1;
                s.submit(c, cost, (next, cost)).map_err(|e| e.to_string())
            };
            add(&mut s, Class::Apply, apply_max, g)?;
            for c in &busy {
                add(&mut s, *c, other_max, g)?;
            }
        }
        let (mut apply, mut total) = (0u64, 0u64);
        let max = apply_max.max(other_max).max(1);
        for i in 0..2500 {
            let (class, (_, cost)) = s.pop().ok_or("nothing to pop")?;
            let units = cost.max(1);
            total += units;
            if class == Class::Apply {
                apply += units;
            }
            // Every queue is still full: apply may not lag or lead by more than one task.
            if apply * 100 + max * 100 < total * APPLY_SHARE {
                return Err(format!("pop {i}: apply served {apply} of {total}, below its share"));
            }
            if apply * 100 > total * APPLY_SHARE + max * 100 {
                return Err(format!("pop {i}: apply served {apply} of {total}, above its share, with {busy:?} waiting"));
            }
        }
        Ok(())
    });
}

/// Pops follow the rules, whatever is submitted and popped: FIFO inside a class, strict priority
/// among the others, apply out of turn only when it is owed its share, nothing idles, and every
/// task that was accepted is popped once.
#[test]
fn nr02_pops_follow_priority_order_fifo_and_the_share() {
    check_with(Runs::default(), "nr02", |g| {
        let mut s: Scheduler<(Class, u64)> = Scheduler::new(big());
        let order = [Class::Interactive, Class::Maintenance, Class::Batch, Class::Exact];
        let mut queued: BTreeMap<Class, std::collections::VecDeque<u64>> = BTreeMap::new();
        let mut served = [0u64; 5];
        let mut next = 0u64;
        let mut popped = 0usize;
        for step in 0..g.below(400) + 1 {
            if g.below(5) < 3 {
                let c = pick_class(g);
                let cost = g.below(40);
                next += 1;
                s.submit(c, cost, (c, next)).map_err(|e| e.to_string())?;
                // The model remembers the cost with the id.
                queued.entry(c).or_default().push_back(next * 1000 + cost);
            } else {
                let had = queued.values().any(|q| !q.is_empty());
                let before: u64 = served.iter().sum();
                let apply_owed = served[0] * 100 < before * APPLY_SHARE;
                let Some((c, (cc, id))) = s.pop() else {
                    if had {
                        return Err(format!("step {step}: nothing popped, with work queued"));
                    }
                    continue;
                };
                popped += 1;
                if c != cc {
                    return Err("the class of a task changed".into());
                }
                let front = queued.get_mut(&c).and_then(|q| q.pop_front()).ok_or("popped from an empty class")?;
                if front / 1000 != id {
                    return Err(format!("step {step}: {c:?} is not first in, first out"));
                }
                let apply_waits = queued.get(&Class::Apply).is_some_and(|q| !q.is_empty());
                if c != Class::Apply {
                    if let Some(better) = order.iter().take_while(|o| **o != c).find(|o| queued.get(o).is_some_and(|q| !q.is_empty())) {
                        return Err(format!("step {step}: {c:?} went before {better:?}"));
                    }
                    if apply_waits && apply_owed {
                        return Err(format!("step {step}: apply was owed its share and waited"));
                    }
                } else if !apply_owed && order.iter().any(|o| queued.get(o).is_some_and(|q| !q.is_empty())) {
                    return Err(format!("step {step}: apply went out of turn"));
                }
                served[c as usize] += (front % 1000).max(1);
            }
        }
        let left: usize = queued.values().map(|q| q.len()).sum();
        if s.len() != left || popped + left != next as usize {
            return Err("a task was lost or doubled".into());
        }
        Ok(())
    });
}

/// A task is refused before it starts when its class holds its limit of tasks or of cost: `Busy`
/// for apply and `Overloaded` for the rest, nothing queued, nothing counted, and no other class
/// notices.
#[test]
fn nr03_admission_refuses_exactly_at_the_limits_and_changes_nothing() {
    check_with(Runs::default(), "nr03", |g| {
        let limits = Limits {
            tasks: std::array::from_fn(|_| g.below(6) as usize),
            cost: std::array::from_fn(|_| *g.pick(&[0u64, 5, 20, 100, u64::MAX / 8])),
        };
        let mut s: Scheduler<u32> = Scheduler::new(limits);
        let mut tasks = [0usize; 5];
        let mut cost = [0u64; 5];
        for i in 0..g.below(120) + 1 {
            if g.below(4) == 0 {
                if let Some((c, _)) = s.pop() {
                    // The queue gave one back: find the class by the totals.
                    let _ = c;
                    tasks[c as usize] -= 1;
                    // Costs are recomputed from the scheduler below.
                    cost = s.queued();
                }
                continue;
            }
            let c = pick_class(g);
            let add = *g.pick(&[0u64, 1, 3, 10, 50, 1 << 40, u64::MAX]);
            let i_ = c as usize;
            let fits = tasks[i_] < limits.tasks[i_] && cost[i_].checked_add(add).is_some_and(|t| t <= limits.cost[i_]);
            let (len, queued) = (s.len(), s.queued());
            match (fits, s.submit(c, add, i as u32)) {
                (true, Ok(())) => {
                    tasks[i_] += 1;
                    cost[i_] += add;
                }
                (false, Err(e)) => {
                    let want = if c == Class::Apply { Error::Busy } else { Error::Overloaded };
                    if e != want {
                        return Err(format!("{c:?} refused with {e}"));
                    }
                    if s.len() != len || s.queued() != queued {
                        return Err("a refused task changed the queue".into());
                    }
                }
                (fits, r) => return Err(format!("{c:?} cost {add}: fits {fits}, got {r:?}; tasks {tasks:?} cost {cost:?}")),
            }
            if s.queued() != cost {
                return Err(format!("queued cost {:?} against {cost:?}", s.queued()));
            }
        }
        Ok(())
    });
}

/// A flood of hostile submissions leaves the queues bounded and the scheduler sane.
#[test]
fn nr04_a_flood_is_bounded_by_the_limits() {
    let limits = Limits::default();
    let mut s: Scheduler<Box<[u8; 64]>> = Scheduler::new(limits);
    let mut g = Gen::from_seed(5);
    let mut refused = 0usize;
    for _ in 0..300_000 {
        let c = pick_class(&mut g);
        let cost = *g.pick(&[0u64, 1, u64::MAX, u64::MAX / 8, u64::MAX / 8 + 1, 1 << 50]);
        if s.submit(c, cost, Box::new([0; 64])).is_err() {
            refused += 1;
        }
        if g.below(7) == 0 {
            let _ = s.pop();
        }
        assert!(s.len() <= limits.tasks.iter().sum::<usize>());
    }
    assert!(refused > 1000);
    while s.pop().is_some() {}
    assert!(s.is_empty());
    assert_eq!(s.queued(), [0; 5]);
    // A pop after a long run of huge costs does not overflow.
    let mut s: Scheduler<u8> = Scheduler::new(big());
    for _ in 0..50 {
        for c in Class::ALL {
            s.submit(c, u64::MAX / 8, 0).unwrap();
        }
        while s.pop().is_some() {}
    }
}

// ===== The cores.

struct Gate {
    open: Mutex<bool>,
    cv: nabedra_rt::sync::Condvar,
}

impl Gate {
    fn new() -> Arc<Gate> {
        Arc::new(Gate { open: Mutex::new(false), cv: nabedra_rt::sync::Condvar::new() })
    }

    fn wait(&self) {
        let g = lock(&self.open);
        let _g = self.cv.wait_while(g, |o| !*o).unwrap_or_else(std::sync::PoisonError::into_inner);
    }

    fn open(&self) {
        *lock(&self.open) = true;
        self.cv.notify_all();
    }
}

/// The core is parked in a job until the gate opens.
fn park(cores: &Cores<usize>, core: usize, gate: &Arc<Gate>) {
    let started = Arc::new(AtomicBool::new(false));
    let (g, s) = (gate.clone(), started.clone());
    cores
        .submit(core, Class::Apply, 0, move |_, _| {
            s.store(true, Ordering::SeqCst);
            g.wait();
        })
        .unwrap();
    while !started.load(Ordering::SeqCst) {
        std::thread::yield_now();
    }
}

/// Overload is refused before any work: the job never runs and never touches the state, a full
/// apply queue says `Busy`, a full query queue says `Overloaded`, and a full class does not
/// block the others.
#[test]
fn nr05_a_refused_job_never_runs_and_a_full_apply_queue_says_busy() {
    let limits = Limits { tasks: [3, 2, 2, 2, 2], cost: [u64::MAX / 8; 5] };
    let cores: Arc<Cores<usize>> = Arc::new(Cores::start(2, limits, false, |_| 0).unwrap());
    let gate = Gate::new();
    park(&cores, 0, &gate);
    let ran = Arc::new(AtomicUsize::new(0));
    let refusal = |c: Class| if c == Class::Apply { Error::Busy } else { Error::Overloaded };
    let job = |ran: &Arc<AtomicUsize>| {
        let ran = ran.clone();
        move |s: &mut usize, _: &nabedra_vector::runtime::Ctx<usize>| {
            *s += 1;
            ran.fetch_add(1, Ordering::SeqCst);
        }
    };
    for c in Class::ALL {
        let cap = limits.tasks[c as usize];
        for _ in 0..cap {
            cores.submit(0, c, 1, job(&ran)).unwrap();
        }
        for _ in 0..3 {
            assert_eq!(cores.submit(0, c, 1, job(&ran)), Err(refusal(c)), "{c:?}");
        }
        // `call` refuses the same way, without waiting.
        let r = within(5, "a refused call", {
            let (ran, cores) = (ran.clone(), cores.clone());
            move || {
                cores.call(0, c, 1, move |_, _| {
                    ran.fetch_add(1000, Ordering::SeqCst);
                })
            }
        });
        assert_eq!(r, Err(refusal(c)), "{c:?}");
    }
    // The other core is not affected.
    assert_eq!(cores.call(1, Class::Apply, 1, |s, _| *s + 7).unwrap(), 7);
    assert_eq!(cores.queued(0), 3 + 2 + 2 + 2 + 2);
    gate.open();
    // Drain: a call of the lowest class on core 0 comes after the rest of its class only.
    while cores.queued(0) > 0 {
        std::thread::yield_now();
    }
    let n = cores.call(0, Class::Apply, 1, |s, _| *s).unwrap();
    assert_eq!(n, 11, "only accepted jobs ran");
    assert_eq!(ran.load(Ordering::SeqCst), 11, "a refused job ran");
}

/// With every class queued behind a parked core, the order of execution is the scheduler's:
/// strict priority among the four classes, with apply taking its fifth.
#[test]
fn nr06_cores_run_queued_work_in_priority_order_with_the_apply_share() {
    let cores: Cores<usize> = Cores::start(1, big(), false, |_| 0).unwrap();
    let gate = Gate::new();
    park(&cores, 0, &gate);
    let log = Arc::new(Mutex::new(Vec::new()));
    for round in 0..30 {
        for c in [Class::Exact, Class::Batch, Class::Maintenance, Class::Interactive, Class::Apply] {
            let log = log.clone();
            cores.submit(0, c, 1, move |_, _| lock(&log).push((c, round))).unwrap();
        }
    }
    gate.open();
    let log = within(10, "the queued jobs", {
        let log = log.clone();
        move || {
            while lock(&log).len() < 150 {
                std::thread::yield_now();
            }
            lock(&log).clone()
        }
    });
    assert_eq!(log.len(), 150);
    let first = &log[..100];
    let apply = first.iter().filter(|(c, _)| *c == Class::Apply).count();
    assert!((18..=22).contains(&apply), "apply ran {apply} of the first 100");
    let mut last = [0usize; 5];
    for (c, round) in first {
        // FIFO inside the class.
        assert!(*round >= last[*c as usize]);
        last[*c as usize] = *round;
    }
    // Strict priority among the others: no maintenance before interactive is done, and so on.
    let pos = |c: Class, n: usize| log.iter().enumerate().filter(|(_, (x, _))| *x == c).nth(n).map(|(i, _)| i);
    for w in [(Class::Interactive, Class::Maintenance), (Class::Maintenance, Class::Batch), (Class::Batch, Class::Exact)] {
        assert!(pos(w.0, 29) < pos(w.1, 0), "{w:?}");
    }
}

/// Cores share nothing: the state is made on the thread of its core (it is not even `Send`), runs
/// jobs only there, and a message to another core runs there with that core's state.
#[test]
fn nr07_state_stays_on_its_core_and_messages_run_on_the_receiver() {
    thread_local! { static ME: Cell<usize> = const { Cell::new(usize::MAX) }; }
    let made: Arc<Mutex<BTreeMap<usize, ThreadId>>> = Arc::default();
    let m2 = made.clone();
    type State = (Rc<Cell<u64>>, ThreadId, usize);
    let cores: Cores<State> = Cores::start(4, big(), false, move |c| {
        let id = std::thread::current().id();
        lock(&m2).insert(c, id);
        ME.with(|m| m.set(c));
        (Rc::new(Cell::new(0)), id, c)
    })
    .unwrap();
    // `start` returns once every thread is up; a state is made right after, and a call waits for it.
    for c in 0..4 {
        cores.call(c, Class::Apply, 0, |_, _| ()).unwrap();
    }
    let ids: Vec<ThreadId> = lock(&made).values().copied().collect();
    assert_eq!(ids.len(), 4);
    assert_eq!(ids.iter().collect::<std::collections::HashSet<_>>().len(), 4, "two cores share a thread");
    let bad = Arc::new(AtomicU64::new(0));
    for round in 0..200u64 {
        for from in 0..4usize {
            let to = (from + 1 + round as usize % 3) % 4;
            let bad = bad.clone();
            cores
                .call(from, Class::Interactive, 1, move |s, ctx| {
                    if std::thread::current().id() != s.1 || ME.with(Cell::get) != s.2 || ctx.core() != s.2 {
                        bad.fetch_add(1, Ordering::SeqCst);
                    }
                    s.0.set(s.0.get() + 1);
                    let bad2 = bad.clone();
                    ctx.send(to, Class::Interactive, 1, move |s2, ctx2| {
                        if std::thread::current().id() != s2.1 || ctx2.core() != to || s2.2 != to {
                            bad2.fetch_add(1, Ordering::SeqCst);
                        }
                        s2.0.set(s2.0.get() + 1000);
                    })
                    .unwrap();
                })
                .unwrap();
        }
    }
    // Every core's counter is its own: the sum of what it was sent and what it did.
    let mut total = 0;
    for c in 0..4 {
        total += cores.call(c, Class::Exact, 1, |s, _| s.0.get()).unwrap();
    }
    assert_eq!(bad.load(Ordering::SeqCst), 0, "a job ran on the wrong thread or with the wrong state");
    assert_eq!(total, 800 * 1001);
}

struct Token(Arc<AtomicU64>);

impl Drop for Token {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Shutdown while jobs and the messages they send are in flight: every job and message is dropped
/// exactly once, run or not, and nothing runs after `shutdown` returned.
#[test]
fn nr08_stopping_with_messages_in_flight_drops_every_job_once_and_runs_none_after() {
    fn spread(token: Token, ran: Arc<AtomicU64>, made: Arc<AtomicU64>, left: u32) -> impl FnOnce(&mut u64, &nabedra_vector::runtime::Ctx<u64>) + Send {
        move |s, ctx| {
            *s += 1;
            ran.fetch_add(1, Ordering::SeqCst);
            if left > 0 {
                for to in 0..ctx.cores() {
                    made.fetch_add(1, Ordering::SeqCst);
                    let t = Token(token.0.clone());
                    let _ = ctx.send(to, Class::Batch, 1, spread(t, ran.clone(), made.clone(), left - 1));
                }
            }
            drop(token);
        }
    }
    for iter in 0..100u64 {
        let (ran, made, dropped) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
        let cores: Cores<u64> = Cores::start(3, big(), false, |_| 0).unwrap();
        for i in 0..200u64 {
            made.fetch_add(1, Ordering::SeqCst);
            let job = spread(Token(dropped.clone()), ran.clone(), made.clone(), 4);
            let _ = cores.submit((i % 3) as usize, Class::ALL[(i % 5) as usize], 1, job);
        }
        std::thread::sleep(Duration::from_micros(100 + (iter * 53) % 2000));
        within(10, "shutdown", move || cores.shutdown());
        let ran_after = ran.load(Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(2));
        assert_eq!(ran.load(Ordering::SeqCst), ran_after, "a job ran after shutdown");
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            made.load(Ordering::SeqCst),
            "iteration {iter}: a job or a message was dropped twice or never"
        );
    }
}

/// A caller that races `stop` does not wait for ever: its job is either run or dropped, and the
/// call answers either way.
#[test]
#[ignore = "open bug VECTOR-cores-stop-strands-a-racing-job: a job queued as stop drains is never run nor dropped and its caller waits for ever"]
fn nr09_a_call_racing_stop_always_gets_an_answer() {
    for iter in 0..400u64 {
        let cores: Arc<Cores<u64>> = Arc::new(Cores::start(1, big(), false, |_| 0).unwrap());
        let hs: Vec<_> = (0..3)
            .map(|_| {
                let cores = cores.clone();
                std::thread::spawn(move || {
                    for _ in 0..50_000 {
                        if cores.call(0, Class::Interactive, 1, |s, _| {
                            *s += 1;
                        })
                        .is_err()
                        {
                            return;
                        }
                    }
                })
            })
            .collect();
        std::thread::sleep(Duration::from_micros(50 + iter % 300));
        cores.stop();
        within(10, &format!("iteration {iter}: a call that raced stop never answered"), move || {
            hs.into_iter().for_each(|h| h.join().unwrap());
        });
    }
}

/// A queued job is dropped with the cores, once, and its caller learns.
#[test]
fn nr10_dropping_the_cores_drops_queued_jobs_once_and_settles_their_callers() {
    let dropped = Arc::new(AtomicU64::new(0));
    let cores: Arc<Cores<usize>> = Arc::new(Cores::start(1, big(), false, |_| 0).unwrap());
    let gate = Gate::new();
    park(&cores, 0, &gate);
    let answers: Vec<_> = (0..20)
        .map(|_| {
            let (cores, token) = (cores.clone(), Token(dropped.clone()));
            std::thread::spawn(move || cores.call(0, Class::Batch, 1, move |_, _| drop(token)))
        })
        .collect();
    while cores.queued(0) < 20 {
        std::thread::yield_now();
    }
    cores.stop();
    for a in answers {
        let r = within(10, "a queued caller", move || a.join().unwrap());
        assert!(r.is_err(), "a job that was dropped answered Ok");
    }
    assert_eq!(dropped.load(Ordering::SeqCst), 20, "queued jobs are dropped once");
    gate.open();
    let cores = Arc::try_unwrap(cores).ok().unwrap();
    within(10, "shutdown", move || cores.shutdown());
    assert_eq!(dropped.load(Ordering::SeqCst), 20);
}

/// A core whose state cannot be made must not leave its callers waiting for ever.
#[test]
#[ignore = "open bug VECTOR-cores-dead-core-hangs-callers: a core whose make panicked queues work that nothing runs"]
fn nr11_a_core_that_failed_to_start_does_not_hang_its_callers() {
    let cores: Arc<Cores<usize>> = Arc::new(
        Cores::start(2, Limits::default(), false, |c| {
            if c == 1 {
                // A state that cannot be made, such as a file that will not open.
                std::panic::panic_any("core 1 cannot open its file");
            }
            0
        })
        .unwrap(),
    );
    assert_eq!(cores.call(0, Class::Interactive, 1, |s, _| *s + 1).unwrap(), 1);
    let c2 = cores.clone();
    let r = within(5, "a call to a core that never started", move || c2.call(1, Class::Interactive, 1, |s, _| *s));
    assert!(r.is_err(), "{r:?}");
}

/// A job that panics is contained and counted, its caller learns, and the core serves on.
#[test]
fn nr12_a_panicking_job_is_contained_and_counted() {
    let cores: Cores<usize> = Cores::start(2, Limits::default(), false, |_| 0).unwrap();
    for i in 0..20 {
        let r = cores.call(i % 2, Class::Interactive, 1, |_, _| -> u8 { std::panic::panic_any("boom") });
        assert!(r.is_err());
    }
    // A core counts a panic after it told the caller, and runs its next job after that.
    assert_eq!(cores.call(0, Class::Interactive, 1, |s, _| *s).unwrap(), 0);
    assert_eq!(cores.call(1, Class::Interactive, 1, |s, _| *s).unwrap(), 0);
    assert_eq!(cores.panics(), 20);
    assert_eq!(cores.submit(2, Class::Apply, 1, |_, _| ()), Err(Error::Internal("no such core")));
    assert_eq!(cores.submit(usize::MAX, Class::Apply, 1, |_, _| ()), Err(Error::Internal("no such core")));
    assert!(Cores::<usize>::start(0, Limits::default(), false, |_| 0).is_err());
}

// ===== Pinning.

fn cpus_in_job(cores: &Cores<usize>, c: usize) -> (Option<usize>, Vec<usize>) {
    cores.call(c, Class::Interactive, 1, |_, ctx| (ctx.cpu(), task::allowed_cpus().unwrap())).unwrap()
}

/// A process confined to one CPU pins every core to it, and one confined elsewhere never pins
/// to a CPU outside its set.
#[test]
fn nr13_cores_pin_inside_the_allowed_set() {
    let all = task::allowed_cpus().unwrap();
    assert!(!all.is_empty());
    let one = *all.last().unwrap();
    // This test's thread is its own: confine it, and the cores it starts inherit the set.
    task::pin_current_thread(one).unwrap();
    assert_eq!(task::allowed_cpus().unwrap(), vec![one]);
    let cores: Cores<usize> = Cores::start(3, Limits::default(), true, |_| 0).unwrap();
    assert_eq!(cores.cpus(), &[Some(one), Some(one), Some(one)]);
    for c in 0..3 {
        assert_eq!(cpus_in_job(&cores, c), (Some(one), vec![one]));
    }
    let unpinned: Cores<usize> = Cores::start(2, Limits::default(), false, |_| 0).unwrap();
    assert_eq!(unpinned.cpus(), &[None, None]);
    for c in 0..2 {
        assert_eq!(cpus_in_job(&unpinned, c), (None, vec![one]), "an unpinned core keeps the inherited set");
    }
}

/// The child half of `nr14`: runs only under `taskset`.
#[test]
fn nr14_child_pins_on_a_subset_of_two_cpus() {
    if std::env::var_os("NR14_CHILD").is_none() {
        return;
    }
    let allowed = task::allowed_cpus().unwrap();
    assert_eq!(allowed.len(), 2, "the child was not confined to two CPUs: {allowed:?}");
    let (a, b) = (allowed[0], allowed[1]);
    let cores: Cores<usize> = Cores::start(5, Limits::default(), true, |_| 0).unwrap();
    assert_eq!(cores.cpus(), &[Some(a), Some(b), Some(a), Some(b), Some(a)], "cores cycle over the subset");
    for c in 0..5 {
        let want = cores.cpus()[c].unwrap();
        assert_eq!(cpus_in_job(&cores, c), (Some(want), vec![want]), "core {c} is not on its CPU");
    }
    let one: Cores<usize> = Cores::start(1, Limits::default(), true, |_| 0).unwrap();
    assert_eq!(one.cpus(), &[Some(a)]);
    let free: Cores<usize> = Cores::start(3, Limits::default(), false, |_| 0).unwrap();
    assert_eq!(free.cpus(), &[None, None, None]);
    for c in 0..3 {
        assert_eq!(cpus_in_job(&free, c).1, vec![a, b]);
    }
}

#[test]
fn nr14_pinning_follows_a_subset_of_cpus() {
    if std::env::var_os("NR14_CHILD").is_some() {
        return;
    }
    let all = task::allowed_cpus().unwrap();
    if all.len() < 4 {
        panic!("this test needs four CPUs to pick two of");
    }
    let pair = format!("{},{}", all[all.len() - 3], all[all.len() - 1]);
    let exe = std::env::current_exe().unwrap();
    let out = std::process::Command::new("taskset")
        .args(["-c", &pair])
        .arg(exe)
        .args(["--exact", "nr14_child_pins_on_a_subset_of_two_cpus", "--test-threads=1", "--nocapture"])
        .env("NR14_CHILD", "1")
        .output()
        .expect("taskset is needed for this test");
    let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    assert!(out.status.success(), "the child failed under taskset -c {pair}:\n{text}");
    assert!(text.contains("1 passed"), "the child test did not run:\n{text}");
}

// ===== Budgets.

/// A meter charges before the work, never past its limit, latches the first resource that ran
/// out for every later charge, and does not forget on `shrink`; the same charges always give the
/// same answers.
#[test]
fn nr15_a_meter_never_exceeds_its_budget_and_stays_refused() {
    check_with(Runs::default(), "nr15", |g| {
        let lim = |g: &mut Gen| *g.pick(&[0u64, 1, 10, 100, 1000, u64::MAX]);
        let budget = Budget { cpu: lim(g), io: lim(g), memory: lim(g) };
        let steps: Vec<(u8, u64)> = (0..g.below(60) + 1)
            .map(|_| (g.below(4) as u8, *g.pick(&[0u64, 1, 2, 7, 50, 999, u64::MAX / 2, u64::MAX])))
            .collect();
        let run = || {
            let mut m = Meter::new(budget);
            let mut out = Vec::new();
            for (what, n) in &steps {
                let r = match what {
                    0 => m.cpu(*n),
                    1 => m.io(*n),
                    2 => m.grow(*n),
                    _ => {
                        m.shrink(*n);
                        Ok(())
                    }
                };
                out.push((r.clone(), m.used(), m.over()));
            }
            out
        };
        let (a, b) = (run(), run());
        if a != b {
            return Err("two runs of the same charges differ".into());
        }
        let mut latched: Option<&'static str> = None;
        let (mut prev_cpu, mut prev_io) = (0, 0);
        for ((what, n), (r, used, over)) in steps.iter().zip(&a) {
            if used.cpu > budget.cpu || used.io > budget.io || used.peak > budget.memory {
                return Err(format!("{used:?} passed {budget:?}"));
            }
            if used.cpu < prev_cpu || used.io < prev_io {
                return Err("a counter went back".into());
            }
            (prev_cpu, prev_io) = (used.cpu, used.io);
            match (latched, r) {
                (Some(l), Err(Error::OverBudget(w))) if *w == l && *over == Some(l) => {}
                (Some(_), Ok(())) if *what == 3 => {}
                (Some(l), other) => return Err(format!("after {l} ran out: {what} {n}: {other:?}")),
                (None, Err(Error::OverBudget(w))) => {
                    let want = ["cpu", "io", "memory"][*what as usize];
                    if *w != want || *over != Some(want) {
                        return Err(format!("{w} ran out on a charge of {want}"));
                    }
                    latched = Some(*w);
                }
                (None, Err(e)) => return Err(format!("{e}")),
                (None, Ok(())) => {
                    if over.is_some() {
                        return Err("over without an error".into());
                    }
                }
            }
        }
        Ok(())
    });
}

/// The code memory is exact under contention: with N threads asking for extensions at once, the
/// grants are exactly what fits, and nothing is granted twice.
#[test]
fn nr16_code_memory_grants_exactly_what_fits_under_contention() {
    for round in 0..50u64 {
        let bytes = 7 + round % 5;
        let limit = 1000 + round * 13;
        let codes = round * 11;
        let mem = Arc::new(Memory::new(limit));
        mem.charge_codes(codes);
        let granted = Arc::new(AtomicU64::new(0));
        let hs: Vec<_> = (0..8)
            .map(|_| {
                let (mem, granted) = (mem.clone(), granted.clone());
                std::thread::spawn(move || {
                    for _ in 0..500 {
                        if mem.try_extension(bytes) {
                            granted.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                })
            })
            .collect();
        hs.into_iter().for_each(|h| h.join().unwrap());
        let n = granted.load(Ordering::SeqCst);
        assert_eq!(n, ((limit - codes) / bytes).min(4000), "round {round}");
        assert_eq!(mem.used(), (codes, n * bytes));
        for _ in 0..n {
            mem.release_extension(bytes);
        }
        assert_eq!(mem.used(), (codes, 0));
        // Hostile sizes.
        assert!(!mem.try_extension(u64::MAX));
        mem.charge_codes(u64::MAX);
        assert!(!mem.try_extension(1));
        mem.release_codes(u64::MAX);
    }
}

// ===== Scans under budgets and from RAM or NVMe.

const DIM: usize = 6;

struct World {
    ix: Index<ExactCodec>,
    _files: Option<Files>,
}

fn world(kind: &str, dim: usize, rows: &[(u64, Option<Vec<f32>>)]) -> World {
    let (vectors, f): (Option<Arc<dyn Vectors>>, Option<Files>) = match kind {
        "rows" => (None, None),
        "ram" => (Some(Arc::new(RamVectors::new())), None),
        other => {
            let b = if other == "uring" { Backend::Uring } else { Backend::Pread };
            let f = files("world", b, 3);
            let c = Counting::new(Arc::new(NvmeVectors::on(f.disks.clone(), dim).unwrap()));
            (Some(c as Arc<dyn Vectors>), Some(f))
        }
    };
    let mut ix = serving(ix_config(dim, vectors));
    feed(&mut ix, rows, 0).unwrap();
    World { ix, _files: f }
}

fn rows_of(g: &mut Gen, dim: usize, n: u64) -> Vec<(u64, Option<Vec<f32>>)> {
    (0..n).map(|k| (k, (!g.chance(1, 8)).then(|| grid(g, dim)))).collect()
}

fn query(g: &mut Gen, dim: usize) -> Query {
    let op = *g.pick(&[Op::Cosine, Op::Dot, Op::L2]);
    let mut vec = grid(g, dim);
    if op == Op::Cosine && vec.iter().all(|x| *x == 0.0) {
        vec[0] = 1.0;
    }
    Query { op, vec, limit: g.below(15) as usize + 1, filter: None, exact: g.bool(), recall: 0.9 }
}

fn kinds() -> Vec<&'static str> {
    let mut k = vec!["rows", "ram", "pread"];
    if backends().contains(&Backend::Uring) {
        k.push("uring");
    }
    k
}

type Spent = (Result<Vec<nabedra_vector::index::Hit>, Error>, Option<nabedra_vector::runtime::Usage>, usize);

/// Drives a scan by hand and returns the result, what it spent, and the units it visited.
fn run(w: &mut World, q: &Query, b: Budget) -> Spent {
    let pin = w.ix.pin(&[(RANGE, ReadAt::Latest { barrier: 0 })], None).unwrap();
    let mut scan = match w.ix.scan_within(&pin, q, Box::new(RecallEstimate), b) {
        Ok(s) => s,
        Err(e) => return (Err(e), None, 0),
    };
    let mut ended = None;
    for _ in 0..1_000_000 {
        match w.ix.visit(&mut scan) {
            Ok(true) => {}
            Ok(false) => break,
            Err(e) => {
                ended = Some(e);
                break;
            }
        }
    }
    let (usage, visited) = (scan.usage(), scan.visited);
    if let Some(e) = ended {
        // After an exhaustion the scan stays ended: more steps change nothing, and finishing it
        // returns the same error and never a partial result.
        let again = w.ix.visit(&mut scan);
        let _ = again;
        let spent = scan.usage().unwrap();
        assert!(spent.cpu <= b.cpu && spent.io <= b.io && spent.peak <= b.memory, "a scan spent beyond its budget: {spent:?} of {b:?}");
        let fin = w.ix.finish(scan);
        assert_eq!(fin, Err(e.clone()), "finish after an exhaustion returned something else");
        return (Err(e), usage, visited);
    }
    (w.ix.finish(scan), usage, visited)
}

/// Whatever the budget, a query returns the whole result it would return unbudgeted, or
/// `OverBudget`; and the budget bites at the same point every time: with exactly what the query
/// used it passes, with one unit less of any resource it ends naming that resource.
#[test]
fn nr17_a_budget_gives_the_whole_result_or_an_error_and_bites_at_the_exact_point() {
    let runs = Runs { cases: Runs::default().cases / 4 + 2, ..Runs::default() };
    check_with(runs, "nr17", |g| {
        let dim = *g.pick(&[1usize, 3, DIM]);
        let n = g.below(60) + 1;
        let rows = rows_of(g, dim, n);
        for kind in kinds() {
            let mut w = world(kind, dim, &rows);
            for _ in 0..3 {
                let q = query(g, dim);
                let (full, used, _) = run(&mut w, &q, Budget::UNLIMITED);
                let (full, used) = (full.map_err(|e| e.to_string())?, used.ok_or("no usage")?);
                let again = run(&mut w, &q, Budget::UNLIMITED);
                if again.0.as_ref() != Ok(&full) || again.1 != Some(used) {
                    return Err(format!("{kind}: the same query spent differently the second time"));
                }
                let exactly = Budget { cpu: used.cpu, io: used.io, memory: used.peak };
                let (r, u, _) = run(&mut w, &q, exactly);
                if r.as_ref() != Ok(&full) || u != Some(used) {
                    return Err(format!("{kind}: with exactly what it used, the query changed: {r:?}"));
                }
                for (what, less) in [
                    ("cpu", Budget { cpu: used.cpu.wrapping_sub(1), ..exactly }),
                    ("io", Budget { io: used.io.wrapping_sub(1), ..exactly }),
                    ("memory", Budget { memory: used.peak.wrapping_sub(1), ..exactly }),
                ] {
                    let spent = match what {
                        "cpu" => used.cpu,
                        "io" => used.io,
                        _ => used.peak,
                    };
                    if spent == 0 {
                        continue;
                    }
                    let (r, _, _) = run(&mut w, &q, less);
                    if r != Err(Error::OverBudget(what)) {
                        return Err(format!("{kind}: one unit short of {what} gave {r:?}"));
                    }
                }
                // Any random budget: whole, or an error.
                let b = Budget { cpu: g.below(used.cpu + 5), io: g.below(used.io + 5), memory: g.below(used.peak + 5) };
                match run(&mut w, &q, b).0 {
                    Ok(hits) if hits == full => {}
                    Err(Error::OverBudget(_)) => {}
                    other => return Err(format!("{kind}: {b:?} gave {other:?}")),
                }
            }
        }
        Ok(())
    });
}

/// What a query spends is a function of the data and the query: equal across stores for CPU and
/// memory, and for I/O it is what the vectors it read cost on NVMe and nothing from RAM.
#[test]
fn nr18_budgets_are_counted_deterministically_across_stores() {
    let runs = Runs { cases: Runs::default().cases / 4 + 2, ..Runs::default() };
    check_with(runs, "nr18", |g| {
        let dim = *g.pick(&[1usize, 3, DIM]);
        let n = g.below(80) + 1;
        let rows = rows_of(g, dim, n);
        let q = query(g, dim);
        let mut seen: Vec<(&str, Spent)> = Vec::new();
        for kind in kinds() {
            let mut w = world(kind, dim, &rows);
            seen.push((kind, run(&mut w, &q, Budget::UNLIMITED)));
        }
        let (k0, base) = &seen[0];
        for (k, s) in &seen[1..] {
            let (a, b) = (base.1.ok_or("usage")?, s.1.ok_or("usage")?);
            if (a.cpu, a.peak, base.2) != (b.cpu, b.peak, s.2) {
                return Err(format!("{k} spent {b:?} (visited {}) where {k0} spent {a:?} (visited {})", s.2, base.2));
            }
            if base.0 != s.0 {
                return Err(format!("{k} returned another result than {k0}"));
            }
            let on_disk = *k == "pread" || *k == "uring";
            if !on_disk && b.io != 0 {
                return Err(format!("{k}: io {} charged for vectors that are in RAM", b.io));
            }
        }
        // The same again on another thread, and on a fresh index: the same spending.
        let rows2 = rows.clone();
        let q2 = q.clone();
        let again = std::thread::spawn(move || {
            let mut w = world("ram", dim, &rows2);
            run(&mut w, &q2, Budget::UNLIMITED)
        })
        .join()
        .unwrap();
        let ram = seen.iter().find(|(k, _)| *k == "ram").map(|(_, s)| s).ok_or("no ram")?;
        if again.1 != ram.1 || again.2 != ram.2 {
            return Err("the spending depends on the thread or the instance".into());
        }
        Ok(())
    });
}

/// The I/O budget bounds the reads of a query. A scan charges the vectors it scores, and then
/// `finish` reads the vector of every hit again, uncharged.
#[test]
#[ignore = "open bug VECTOR-budget-finish-reads-uncharged: finish reads the vector of every hit from the store without charging the I/O budget"]
fn nr19_the_io_budget_covers_every_vector_a_query_reads() {
    let f = files("iobudget", Backend::Pread, 3);
    let counting = Counting::new(Arc::new(NvmeVectors::on(f.disks.clone(), DIM).unwrap()));
    let mut ix = serving(ix_config(DIM, Some(counting.clone() as Arc<dyn Vectors>)));
    let rows: Vec<_> = (0..200u64)
        .map(|k| (k, Some((0..DIM).map(|i| ((k * 7 + i as u64) % 13) as f32 - 6.0).collect::<Vec<f32>>())))
        .collect();
    feed(&mut ix, &rows, 0).unwrap();
    let pin = latest(&mut ix);
    let q = exact(Op::L2, &[1.0, -2.0, 3.0, 0.0, 1.0, 2.0], 10);
    // The scan alone reads 200 vectors of 24 bytes. A budget of exactly that passes the scan.
    let io = 200 * 24;
    let before = counting.reads();
    let r = ix.search_within(&pin, &q, Budget { io, ..Budget::UNLIMITED });
    let read = (counting.reads() - before) * 24;
    assert!(r.is_ok(), "{r:?}");
    assert!(read <= io, "the query read {read} bytes of vectors within an I/O budget of {io}");
}

/// The result of a scan is the same whether vectors come from the rows, RAM or NVMe through
/// either backend: hits, scores, rows, and the units visited.
#[test]
fn nr20_scans_are_identical_from_rows_ram_and_nvme() {
    let runs = Runs { cases: Runs::default().cases / 4 + 2, ..Runs::default() };
    check_with(runs, "nr20", |g| {
        let dim = *g.pick(&[1usize, 3, DIM, 33]);
        let mut worlds: Vec<(&str, World)> = Vec::new();
        for k in kinds() {
            worlds.push((k, world(k, dim, &[])));
        }
        let mut at = 0;
        for _ in 0..g.below(5) + 1 {
            let changes: Vec<(u64, Option<Vec<f32>>)> =
                (0..g.below(25) + 1).map(|_| (g.below(40), (!g.chance(1, 6)).then(|| grid(g, dim)))).collect();
            let maintain = g.below(4);
            let mut next = at;
            for (_, w) in &mut worlds {
                next = feed(&mut w.ix, &changes, at).map_err(|e| e.to_string())?;
                for _ in 0..maintain {
                    w.ix.maintain(8);
                    w.ix.run();
                }
                w.ix.advance_low(RANGE, next.saturating_sub(4)).map_err(|e| e.to_string())?;
            }
            at = next;
            for _ in 0..3 {
                let q = query(g, dim);
                let mut results = Vec::new();
                for (k, w) in &mut worlds {
                    results.push((*k, run(w, &q, Budget::UNLIMITED)));
                }
                let (k0, first) = &results[0];
                for (k, r) in &results[1..] {
                    if r.0 != first.0 || r.2 != first.2 {
                        return Err(format!("{k} differs from {k0}: {:?} against {:?}", r.0, first.0));
                    }
                }
            }
        }
        Ok(())
    });
}

/// Hostile queries give the same refusal from every store, and never a panic.
#[test]
fn nr21_hostile_queries_are_refused_alike_and_never_panic() {
    let mut worlds: Vec<(&str, World)> = Vec::new();
    let mut g = Gen::from_seed(3);
    let rows = rows_of(&mut g, DIM, 40);
    for k in kinds() {
        worlds.push((k, world(k, DIM, &rows)));
    }
    let mk = |vec: Vec<f32>, limit: usize, recall: f64, op: Op| Query { op, vec, limit, filter: None, exact: false, recall };
    let queries = vec![
        mk(vec![f32::NAN; DIM], 5, 0.9, Op::L2),
        mk(vec![f32::INFINITY; DIM], 5, 0.9, Op::Dot),
        mk(vec![1.0; DIM - 1], 5, 0.9, Op::L2),
        mk(vec![1.0; DIM + 1], 5, 0.9, Op::L2),
        mk(vec![], 5, 0.9, Op::L2),
        mk(vec![1.0; DIM], 0, 0.9, Op::L2),
        mk(vec![1.0; DIM], usize::MAX, 0.9, Op::L2),
        mk(vec![1.0; DIM], 1001, 0.9, Op::L2),
        mk(vec![1.0; DIM], 5, f64::NAN, Op::L2),
        mk(vec![1.0; DIM], 5, 2.0, Op::L2),
        mk(vec![1.0; DIM], 5, -1.0, Op::L2),
        mk(vec![0.0; DIM], 5, 0.9, Op::Cosine),
        mk(vec![f32::MAX; DIM], 5, 0.9, Op::Dot),
        mk(vec![f32::MIN_POSITIVE / 4.0; DIM], 5, 0.9, Op::Cosine),
    ];
    let budgets = [Budget::UNLIMITED, Budget { cpu: 0, io: 0, memory: 0 }, Budget { cpu: 1, io: 1, memory: 1 }];
    for q in &queries {
        for b in budgets {
            let mut outs = Vec::new();
            for (k, w) in &mut worlds {
                let q = q.clone();
                let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(w, &q, b)));
                let r = r.unwrap_or_else(|_| panic!("{k}: a hostile query panicked: {q:?} {b:?}"));
                outs.push((*k, r.0));
            }
            if b != Budget::UNLIMITED {
                continue;
            }
            let (k0, first) = &outs[0];
            for (k, r) in &outs[1..] {
                match (first, r) {
                    (Ok(a), Ok(c)) => assert_eq!(a, c, "{k} against {k0}: {q:?}"),
                    (Err(a), Err(c)) => assert_eq!(a, c, "{k} against {k0}: {q:?}"),
                    (a, c) => panic!("{k} and {k0} disagree on {q:?}: {c:?} against {a:?}"),
                }
            }
        }
    }
}
