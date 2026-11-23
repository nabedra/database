//! Stepped services in simulation. Each test fails when the property it names is broken.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::sim::{NodeClock, ServiceInfo, ServiceState, SimEvent, World, in_node, run};
use crate::sync::Condvar;
use crate::task::{self, ServiceHandle, Step, Stepped};
use crate::time;

fn ms(value: u64) -> Duration {
    Duration::from_millis(value)
}

/// Counts down, one unit per step, then exits.
struct Counter {
    left: u32,
    bell: Condvar,
}

impl Stepped for Counter {
    fn step(&mut self) -> Step {
        if self.left == 0 {
            return Step::Exit;
        }
        self.left -= 1;
        Step::Worked
    }

    fn doorbell(&self) -> &Condvar {
        &self.bell
    }

    fn run(mut self) {
        while self.step() != Step::Exit {}
    }
}

fn counter(left: u32) -> Counter {
    Counter {
        left,
        bell: Condvar::new(),
    }
}

struct TestService<F> {
    step: F,
    bell: Condvar,
}

impl<F: FnMut() -> Step + Send + 'static> Stepped for TestService<F> {
    fn step(&mut self) -> Step {
        (self.step)()
    }

    fn doorbell(&self) -> &Condvar {
        &self.bell
    }

    fn run(self) {
        unreachable!("simulation only")
    }
}

#[test]
fn maximum_jitter_delays_each_step_once() {
    run(74, |world| async move {
        world.add_node(1, NodeClock::default());
        world.set_jitter(1_000, ms(1));
        let handle = spawn_in(1, "counter", counter(1));
        assert_eq!(world.service_steps(), 0);
        tokio::time::sleep(ms(10)).await;
        assert!(handle.is_finished());
        assert_eq!(world.service_steps(), 2);
    });
}

#[test]
fn inline_drive_rechecks_a_service_rung_late_in_an_idle_pass() {
    run(75, |world| async move {
        world.add_node(1, NodeClock::default());
        let queue = Arc::new(Queue::default());
        spawn_in(
            1,
            "consumer",
            Consumer {
                queue: queue.clone(),
            },
        );
        let mut notify = Some(queue.clone());
        spawn_in(
            1,
            "notifier",
            TestService {
                step: move || {
                    if let Some(queue) = notify.take() {
                        queue.push(99);
                    }
                    Step::Idle(None)
                },
                bell: Condvar::new(),
            },
        );
        let result = in_node(1, async {
            task::drive_until("late doorbell", || queue.done().pop())
        })
        .await;
        assert_eq!(result, Some(("consumer".to_owned(), 99)));
    });
}

#[test]
fn inline_exit_wakes_a_parked_driver() {
    run(78, |world| async move {
        world.add_node(1, NodeClock::default());
        let stop = Arc::new(AtomicBool::new(false));
        let should_stop = stop.clone();
        let handle = spawn_in(
            1,
            "idle",
            TestService {
                step: move || {
                    if should_stop.load(Ordering::SeqCst) {
                        Step::Exit
                    } else {
                        Step::Idle(None)
                    }
                },
                bell: Condvar::new(),
            },
        );
        tokio::time::sleep(ms(1)).await;
        assert_eq!(world.live_tasks(1), 1);
        stop.store(true, Ordering::SeqCst);
        handle.join().unwrap();
        tokio::time::sleep(ms(1)).await;
        assert_eq!(world.live_tasks(1), 0);
    });
}

#[test]
fn cross_node_join_runs_on_the_service_nodes_clock() {
    run(76, |world| async move {
        world.add_node(1, NodeClock::default());
        world.add_node(
            2,
            NodeClock {
                drift_ppm: 500_000,
                ..NodeClock::default()
            },
        );
        let mut deadline = None;
        let handle = spawn_in(
            2,
            "timer",
            TestService {
                step: move || {
                    assert_eq!(crate::sim::current_node(), Some(2));
                    let now = time::now_std();
                    let at = *deadline.get_or_insert(now + ms(300));
                    if now >= at {
                        Step::Exit
                    } else {
                        Step::Idle(Some(at))
                    }
                },
                bell: Condvar::new(),
            },
        );
        in_node(1, async {
            handle.join().unwrap();
            assert_eq!(crate::sim::current_node(), Some(1));
        })
        .await;
        assert_eq!(world.paused(), ms(200));
    });
}

/// A queue consumer: idle on its doorbell until an item or the close flag arrives.
#[derive(Default)]
struct Queue {
    items: Mutex<VecDeque<u32>>,
    done: Mutex<Vec<(String, u32)>>,
    closed: AtomicBool,
    bell: Condvar,
}

impl Queue {
    fn push(&self, item: u32) {
        self.items.lock().unwrap().push_back(item);
        self.bell.notify_all();
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.bell.notify_all();
    }

    fn done(&self) -> Vec<(String, u32)> {
        self.done.lock().unwrap().clone()
    }
}

struct Consumer {
    queue: Arc<Queue>,
}

impl Stepped for Consumer {
    fn step(&mut self) -> Step {
        let item = self.queue.items.lock().unwrap().pop_front();
        match item {
            Some(item) => {
                let name = task::current_service().unwrap_or_default();
                self.queue.done.lock().unwrap().push((name, item));
                Step::Worked
            }
            None if self.queue.closed.load(Ordering::SeqCst) => Step::Exit,
            None => Step::Idle(None),
        }
    }

    fn doorbell(&self) -> &Condvar {
        &self.queue.bell
    }

    fn run(mut self) {
        loop {
            match self.step() {
                Step::Exit => return,
                Step::Worked => {}
                Step::Idle(_) => {
                    let guard = self.queue.items.lock().unwrap();
                    let _guard = self
                        .queue
                        .bell
                        .wait_while(guard, |items| {
                            items.is_empty() && !self.queue.closed.load(Ordering::SeqCst)
                        })
                        .unwrap();
                }
            }
        }
    }
}

fn spawn_in(node: u64, name: &str, service: impl Stepped) -> ServiceHandle {
    let name = name.to_owned();
    // spawn_stepped reads the node from the task context, so call it inside the node.
    let (sender, receiver) = std::sync::mpsc::channel();
    let job = in_node(node, async move {
        sender
            .send(task::spawn_stepped(name, 64 * 1024, service).unwrap())
            .unwrap();
    });
    futures_lite_block(job);
    receiver.recv().unwrap()
}

/// Polls a future that completes without waiting (in_node around synchronous code).
fn futures_lite_block<F: std::future::Future<Output = ()>>(future: F) {
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    assert!(future.as_mut().poll(&mut cx).is_ready());
}

#[test]
fn a_stepped_service_runs_as_a_node_task_and_exits() {
    run(1, |world| async move {
        world.add_node(1, NodeClock::default());
        let handle = spawn_in(1, "counter", counter(5));
        assert_eq!(
            world.stepped_services(1),
            vec![ServiceInfo {
                name: "counter".into(),
                state: ServiceState::Ready
            }]
        );
        // The driver steps it without anyone waiting: 5 units of work and an exit.
        tokio::time::sleep(ms(1)).await;
        assert!(handle.is_finished());
        assert_eq!(world.service_steps(), 6);
        assert_eq!(world.stepped_services(1)[0].state, ServiceState::Exited);
        assert!(world.services(1).is_empty(), "no helper-thread service");
        handle.join().unwrap();
    });
}

#[test]
fn an_idle_service_waits_for_its_doorbell() {
    run(2, |world| async move {
        world.add_node(1, NodeClock::default());
        let queue = Arc::new(Queue::default());
        let handle = spawn_in(
            1,
            "consumer",
            Consumer {
                queue: Arc::clone(&queue),
            },
        );
        tokio::time::sleep(ms(10)).await;
        let idle_steps = world.service_steps();
        assert_eq!(idle_steps, 1, "one step found the queue empty");
        tokio::time::sleep(ms(10_000)).await;
        assert_eq!(
            world.service_steps(),
            idle_steps,
            "no busy polling while idle"
        );

        let producer = Arc::clone(&queue);
        world
            .spawn_node(1, async move {
                producer.push(7);
                time::sleep(ms(5)).await;
                producer.push(8);
            })
            .await
            .unwrap();
        tokio::time::sleep(ms(1)).await;
        assert_eq!(
            queue.done(),
            vec![("consumer".into(), 7), ("consumer".into(), 8)]
        );
        queue.close();
        handle.join().unwrap();
        assert_eq!(world.stepped_services(1)[0].state, ServiceState::Exited);
    });
}

#[test]
fn drive_until_steps_the_node_services_inline() {
    run(3, |world| async move {
        world.add_node(1, NodeClock::default());
        let queue = Arc::new(Queue::default());
        spawn_in(
            1,
            "consumer",
            Consumer {
                queue: Arc::clone(&queue),
            },
        );
        let caller = Arc::clone(&queue);
        let seen = in_node(1, async move {
            task::spawn_blocking(move || {
                caller.push(1);
                caller.push(2);
                // The consumer has not run yet: only an inline drive can finish the work.
                task::drive_until("test wait", || {
                    let done = caller.done();
                    (done.len() == 2).then_some(done)
                })
            })
            .await
            .unwrap()
        })
        .await;
        assert_eq!(
            seen,
            Some(vec![("consumer".into(), 1), ("consumer".into(), 2)])
        );
        // Outside a world it returns None without calling `ready`.
        let outside = std::thread::spawn(|| {
            task::drive_until::<()>("outside", || panic!("ready must not be called"))
        })
        .join()
        .unwrap();
        assert_eq!(outside, None);
    });
}

/// Idle until a deadline on the node clock, then exits.
struct Timer {
    until: Option<std::time::Instant>,
    after: Duration,
    bell: Condvar,
    fired_at: Arc<Mutex<Option<std::time::Instant>>>,
}

impl Stepped for Timer {
    fn step(&mut self) -> Step {
        let now = time::now_std();
        let until = *self.until.get_or_insert(now + self.after);
        if now < until {
            return Step::Idle(Some(until));
        }
        *self.fired_at.lock().unwrap() = Some(now);
        Step::Exit
    }

    fn doorbell(&self) -> &Condvar {
        &self.bell
    }

    fn run(self) {
        unreachable!("simulation only")
    }
}

#[test]
fn drive_until_pauses_the_world_to_the_earliest_timed_idle() {
    run(4, |world| async move {
        world.add_node(1, NodeClock::default());
        let fired_at = Arc::new(Mutex::new(None));
        let handle = spawn_in(
            1,
            "timer",
            Timer {
                until: None,
                after: ms(5_000),
                bell: Condvar::new(),
                fired_at: Arc::clone(&fired_at),
            },
        );
        let start = in_node(1, async { time::now_std() }).await;
        // Join drives inline: nothing but the deadline can make progress, so the world
        // pauses to it.
        in_node(1, async move { handle.join().unwrap() }).await;
        let fired = fired_at.lock().unwrap().unwrap();
        assert_eq!(fired.duration_since(start), ms(5_000));
        assert_eq!(world.paused(), ms(5_000));
        assert_eq!(world.elapsed(), ms(5_000), "the runtime clock did not move");
    });
}

#[test]
fn a_timed_idle_service_wakes_at_its_deadline_without_a_drive() {
    run(5, |world| async move {
        world.add_node(1, NodeClock::default());
        let fired_at = Arc::new(Mutex::new(None));
        let handle = spawn_in(
            1,
            "timer",
            Timer {
                until: None,
                after: ms(300),
                bell: Condvar::new(),
                fired_at: Arc::clone(&fired_at),
            },
        );
        tokio::time::sleep(ms(299)).await;
        assert!(!handle.is_finished());
        tokio::time::sleep(ms(2)).await;
        assert!(handle.is_finished());
        assert_eq!(world.paused(), Duration::ZERO);
    });
}

#[test]
#[should_panic(expected = "drive_until(stuck site): nothing can progress on node Some(1)")]
fn a_wait_nothing_can_satisfy_panics_with_the_site_and_states() {
    run(6, |world| async move {
        world.add_node(1, NodeClock::default());
        let queue = Arc::new(Queue::default());
        spawn_in(1, "consumer", Consumer { queue });
        in_node(1, async {
            task::drive_until::<()>("stuck site", || None);
        })
        .await;
    });
}

type NameAndNode = (Option<String>, Option<u64>);

#[test]
fn inside_a_step_the_service_is_current() {
    struct Probe {
        bell: Condvar,
        seen: Arc<Mutex<Option<NameAndNode>>>,
    }
    impl Stepped for Probe {
        fn step(&mut self) -> Step {
            *self.seen.lock().unwrap() =
                Some((task::current_service(), crate::sim::current_node()));
            Step::Exit
        }
        fn doorbell(&self) -> &Condvar {
            &self.bell
        }
        fn run(self) {}
    }
    run(7, |world| async move {
        world.add_node(4, NodeClock::default());
        let seen = Arc::new(Mutex::new(None));
        let handle = spawn_in(
            4,
            "probe",
            Probe {
                bell: Condvar::new(),
                seen: Arc::clone(&seen),
            },
        );
        assert!(!handle.is_current());
        tokio::time::sleep(ms(1)).await;
        assert_eq!(*seen.lock().unwrap(), Some((Some("probe".into()), Some(4))));
        assert_ne!(task::current_service().as_deref(), Some("probe"));
    });
}

#[test]
fn joining_a_service_from_its_own_step_is_a_reentrancy_panic() {
    struct Selfish {
        bell: Condvar,
        me: Arc<Mutex<Option<ServiceHandle>>>,
        message: Arc<Mutex<Option<String>>>,
    }
    impl Stepped for Selfish {
        fn step(&mut self) -> Step {
            let me = self.me.lock().unwrap().take();
            if let Some(me) = me {
                assert!(me.is_current());
                // Product code checks is_current first; joining anyway is a bug.
                let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| me.join()))
                    .unwrap_err();
                *self.message.lock().unwrap() = panic.downcast_ref::<String>().cloned();
            }
            Step::Exit
        }
        fn doorbell(&self) -> &Condvar {
            &self.bell
        }
        fn run(self) {}
    }
    run(8, |world| async move {
        world.add_node(1, NodeClock::default());
        let me = Arc::new(Mutex::new(None));
        let message = Arc::new(Mutex::new(None));
        let handle = spawn_in(
            1,
            "selfish",
            Selfish {
                bell: Condvar::new(),
                me: Arc::clone(&me),
                message: Arc::clone(&message),
            },
        );
        *me.lock().unwrap() = Some(handle);
        tokio::time::sleep(ms(1)).await;
        assert_eq!(
            message.lock().unwrap().as_deref(),
            Some("reentrancy: joining service selfish from inside its own step")
        );
    });
}

#[test]
fn a_panicking_step_finishes_the_service_and_join_returns_the_panic() {
    struct Bomb(Condvar);
    impl Stepped for Bomb {
        fn step(&mut self) -> Step {
            panic!("planted step panic")
        }
        fn doorbell(&self) -> &Condvar {
            &self.0
        }
        fn run(self) {}
    }
    run(9, |world| async move {
        world.add_node(1, NodeClock::default());
        let handle = spawn_in(1, "bomb", Bomb(Condvar::new()));
        let error = in_node(1, async move { handle.join().unwrap_err() }).await;
        assert_eq!(error.downcast_ref::<&str>(), Some(&"planted step panic"));
        assert_eq!(world.stepped_services(1)[0].state, ServiceState::Panicked);
    });
}

/// The states of node 1's stepped services, as seen from a destructor.
type Seen = Arc<Mutex<Vec<(&'static str, Option<u64>, Vec<ServiceState>)>>>;

fn record(seen: &Seen, who: &'static str) {
    let states = crate::sim::world()
        .unwrap()
        .stepped_services(1)
        .into_iter()
        .map(|info| info.state)
        .collect();
    seen.lock()
        .unwrap()
        .push((who, crate::sim::current_node(), states));
}

/// Records, when its state drops, what the crash had done by then.
struct Witness {
    bell: Condvar,
    seen: Seen,
}

impl Stepped for Witness {
    fn step(&mut self) -> Step {
        Step::Idle(None)
    }
    fn doorbell(&self) -> &Condvar {
        &self.bell
    }
    fn run(self) {}
}

impl Drop for Witness {
    fn drop(&mut self) {
        record(&self.seen, "service");
    }
}

struct TaskGuard(Seen);

impl Drop for TaskGuard {
    fn drop(&mut self) {
        record(&self.0, "task");
    }
}

#[test]
fn a_crash_kills_services_before_any_destructor_runs() {
    run(10, |world| async move {
        world.add_node(1, NodeClock::default());
        let seen: Seen = Arc::default();
        let witness = spawn_in(
            1,
            "witness",
            Witness {
                bell: Condvar::new(),
                seen: Arc::clone(&seen),
            },
        );
        let queue = Arc::new(Queue::default());
        let other = spawn_in(1, "consumer", Consumer { queue });
        let guard = TaskGuard(Arc::clone(&seen));
        let task = world.spawn_node(1, async move {
            let _guard = guard;
            std::future::pending::<()>().await
        });
        tokio::time::sleep(ms(1)).await;
        let steps = world.service_steps();
        world.crash(1);
        // The services' state drops inside the crash; the aborted task's future drops when
        // the runtime next runs. Both see every service dead.
        assert_eq!(
            *seen.lock().unwrap(),
            vec![(
                "service",
                Some(1),
                vec![ServiceState::Dead, ServiceState::Dead]
            )]
        );
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(
            seen.lock().unwrap()[1],
            (
                "task",
                Some(1),
                vec![ServiceState::Dead, ServiceState::Dead]
            )
        );
        // Dead services are never stepped again and join at once.
        tokio::time::sleep(ms(100)).await;
        assert_eq!(world.service_steps(), steps);
        witness.join().unwrap();
        other.join().unwrap();
        // A restart forgets them; a service started on the restarted node runs.
        world.restart(1);
        assert!(world.stepped_services(1).is_empty());
        let fresh = spawn_in(1, "fresh", counter(2));
        in_node(1, async move { fresh.join().unwrap() }).await;
    });
}

#[test]
fn a_service_started_on_a_crashed_node_is_dead() {
    run(11, |world| async move {
        world.add_node(1, NodeClock::default());
        world.crash(1);
        let handle = spawn_in(1, "late", counter(3));
        assert!(handle.is_finished());
        handle.join().unwrap();
        tokio::time::sleep(ms(1)).await;
        assert_eq!(world.service_steps(), 0);
    });
}

/// Logs its name on each of `left` steps.
struct Named {
    name: &'static str,
    left: u32,
    log: Arc<Mutex<Vec<&'static str>>>,
    bell: Condvar,
}

impl Stepped for Named {
    fn step(&mut self) -> Step {
        if self.left == 0 {
            return Step::Exit;
        }
        self.left -= 1;
        self.log.lock().unwrap().push(self.name);
        Step::Worked
    }
    fn doorbell(&self) -> &Condvar {
        &self.bell
    }
    fn run(self) {}
}

/// P3: an inline drive steps services in registration order, never in hash order.
#[test]
fn drive_steps_services_in_registration_order() {
    run(19, |world| async move {
        world.add_node(1, NodeClock::default());
        let log = Arc::new(Mutex::new(Vec::new()));
        let names = ["e", "b", "d", "a", "c", "f"];
        for name in names {
            spawn_in(
                1,
                name,
                Named {
                    name,
                    left: 2,
                    log: Arc::clone(&log),
                    bell: Condvar::new(),
                },
            );
        }
        let caller_log = Arc::clone(&log);
        in_node(1, async move {
            task::drive_until("order", || {
                (caller_log.lock().unwrap().len() == 12).then_some(())
            })
        })
        .await;
        let expected: Vec<&str> = names.iter().chain(names.iter()).copied().collect();
        assert_eq!(*log.lock().unwrap(), expected);
    });
}

/// Runs a seeded two-node workload with jitter and returns its event trace.
fn traced_run(seed: u64) -> Vec<String> {
    run(seed, |world| async move {
        world.set_jitter(300, ms(3));
        let trace = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&trace);
        world.observe(Arc::new(move |event: &SimEvent<'_>| {
            sink.lock().unwrap().push(format!("{event:?}"))
        }));
        let mut queues = Vec::new();
        let mut handles = Vec::new();
        for node in [1, 2] {
            world.add_node(node, NodeClock::default());
            for name in ["a", "b"] {
                let queue = Arc::new(Queue::default());
                handles.push(spawn_in(
                    node,
                    name,
                    Consumer {
                        queue: Arc::clone(&queue),
                    },
                ));
                queues.push((node, queue));
            }
        }
        for (index, (node, queue)) in queues.iter().enumerate() {
            let queue = Arc::clone(queue);
            world.spawn_node(*node, async move {
                for item in 0..20 {
                    queue.push(index as u32 * 100 + item);
                    time::sleep(Duration::from_micros(700)).await;
                }
            });
        }
        tokio::time::sleep(ms(100)).await;
        for (_, queue) in &queues {
            queue.close();
        }
        for handle in handles {
            handle.join().unwrap();
        }
        trace.lock().unwrap().clone()
    })
}

#[test]
fn same_seed_same_trace() {
    fn trace_hash(events: &[String]) -> u64 {
        events
            .iter()
            .flat_map(|event| event.bytes().chain([0]))
            .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
                (hash ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3)
            })
    }

    let mut hashes = std::collections::BTreeSet::new();
    for seed in 42..47 {
        let first = traced_run(seed);
        let replay = traced_run(seed);
        assert!(first.len() > 80, "seed {seed}: {} events", first.len());
        let hash = trace_hash(&first);
        assert_eq!(hash, trace_hash(&replay), "replay hash for seed {seed}");
        assert_eq!(first, replay, "replay events for seed {seed}");
        eprintln!(
            "seed {seed}: {} events, trace hash {hash:016x}",
            first.len()
        );
        hashes.insert(hash);
    }
    assert!(hashes.len() > 1, "jitter does not follow the seed");
}

#[test]
fn thread_sleep_pauses_the_whole_world() {
    run(12, |world| async move {
        world.add_node(1, NodeClock::default());
        world.add_node(2, NodeClock::default());
        let fired = Arc::new(AtomicU32::new(0));
        let timer = Arc::clone(&fired);
        // Armed before the pause: it fires late by the pause.
        world.spawn_node(2, async move {
            time::sleep(ms(1_000)).await;
            timer.store(1, Ordering::SeqCst);
        });
        tokio::task::yield_now().await;
        let before = in_node(2, async { time::now() }).await;
        in_node(1, async {
            task::spawn_blocking(|| time::thread_sleep(ms(2_000)))
                .await
                .unwrap()
        })
        .await;
        let after = in_node(2, async { time::now() }).await;
        assert_eq!(after - before, ms(2_000), "every node's clock moved");
        assert_eq!(fired.load(Ordering::SeqCst), 0);
        tokio::time::sleep(ms(999)).await;
        assert_eq!(fired.load(Ordering::SeqCst), 0);
        tokio::time::sleep(ms(2)).await;
        assert_eq!(fired.load(Ordering::SeqCst), 1);
        // A timer armed after the pause is mapped through it.
        let start = in_node(2, async { time::now() }).await;
        in_node(2, async { time::sleep_until(start + ms(50)).await }).await;
        assert_eq!(in_node(2, async { time::now() }).await - start, ms(50));
    });
}

#[test]
#[should_panic(expected = "would wait forever")]
fn a_condvar_wait_on_the_simulation_thread_panics() {
    run(13, |_world| async move {
        let lock = Mutex::new(());
        let condvar = Condvar::new();
        let _guard = condvar.wait(lock.lock().unwrap());
    });
}

/// Sends once on a tokio oneshot, then exits.
struct Sender {
    bell: Condvar,
    sender: Option<tokio::sync::oneshot::Sender<u32>>,
}

impl Stepped for Sender {
    fn step(&mut self) -> Step {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(99);
        }
        Step::Exit
    }
    fn doorbell(&self) -> &Condvar {
        &self.bell
    }
    fn run(self) {}
}

#[test]
fn block_on_inline_drives_services_until_the_future_completes() {
    run(14, |world| async move {
        world.add_node(1, NodeClock::default());
        let (sender, receiver) = tokio::sync::oneshot::channel();
        spawn_in(
            1,
            "sender",
            Sender {
                bell: Condvar::new(),
                sender: Some(sender),
            },
        );
        let value = in_node(1, async move {
            task::spawn_blocking(move || task::block_on_inline(receiver))
                .await
                .unwrap()
        })
        .await;
        assert_eq!(value.unwrap(), 99);
    });
}

#[test]
fn block_on_inline_drives_services_between_self_waking_polls() {
    run(77, |world| async move {
        world.add_node(1, NodeClock::default());
        let (sender, mut receiver) = tokio::sync::oneshot::channel();
        spawn_in(
            1,
            "sender",
            Sender {
                bell: Condvar::new(),
                sender: Some(sender),
            },
        );
        let mut polls = 0;
        let value = in_node(1, async move {
            task::block_on_inline(std::future::poll_fn(|cx| {
                polls += 1;
                assert!(polls <= 2, "the service must run between polls");
                match receiver.try_recv() {
                    Ok(value) => std::task::Poll::Ready(value),
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                        cx.waker().wake_by_ref();
                        std::task::Poll::Pending
                    }
                    Err(error) => panic!("{error}"),
                }
            }))
        })
        .await;
        assert_eq!(value, 99);
    });
}

#[test]
fn block_on_inline_refuses_a_future_that_needs_virtual_time() {
    run(15, |world| async move {
        world.add_node(1, NodeClock::default());
        let result = in_node(1, async {
            task::spawn_blocking(|| task::block_on_inline(time::sleep(ms(10)))).await
        })
        .await;
        let panic = result.unwrap_err().into_panic();
        let message = panic.downcast_ref::<String>().unwrap();
        assert!(
            message.contains("needs virtual time or another task"),
            "{message}"
        );
        assert!(message.contains("services/tests.rs"), "{message}");
    });
}

#[test]
fn spawn_oneshot_runs_after_the_current_step_in_its_node() {
    run(16, |world| async move {
        world.add_node(1, NodeClock::default());
        let log = Arc::new(Mutex::new(Vec::new()));
        let job_log = Arc::clone(&log);
        in_node(1, async {
            task::spawn_oneshot("oltp-damage", 64 * 1024, move || {
                job_log
                    .lock()
                    .unwrap()
                    .push((task::current_service(), crate::sim::current_node()));
            })
            .unwrap();
        })
        .await;
        assert!(log.lock().unwrap().is_empty(), "deferred, not inline");
        tokio::time::sleep(ms(1)).await;
        assert_eq!(
            *log.lock().unwrap(),
            vec![(Some("oltp-damage".to_owned()), Some(1))]
        );
        // A crash before it starts cancels it.
        let cancelled = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&cancelled);
        in_node(1, async {
            task::spawn_oneshot("late", 64 * 1024, move || {
                flag.store(true, Ordering::SeqCst)
            })
            .unwrap();
        })
        .await;
        world.crash(1);
        tokio::time::sleep(ms(1)).await;
        assert!(!cancelled.load(Ordering::SeqCst));
    });
}

#[test]
fn freeze_stops_a_node_without_aborting_it() {
    run(17, |world: Arc<World>| async move {
        world.add_node(1, NodeClock::default());
        world.add_node(2, NodeClock::default());
        let ticks = Arc::new(AtomicU32::new(0));
        let other = Arc::new(AtomicU32::new(0));
        for (node, counter) in [(1, Arc::clone(&ticks)), (2, Arc::clone(&other))] {
            world.spawn_node(node, async move {
                loop {
                    time::sleep(ms(10)).await;
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            });
        }
        tokio::time::sleep(ms(55)).await;
        assert_eq!(ticks.load(Ordering::SeqCst), 5);
        world.freeze(1);
        assert!(world.is_frozen(1));
        // Tasks spawned while frozen wait too.
        let late = Arc::clone(&ticks);
        world.spawn_node(1, async move {
            late.fetch_add(100, Ordering::SeqCst);
        });
        let clock_before = in_node(1, async { time::now() }).await;
        tokio::time::sleep(ms(100)).await;
        assert_eq!(
            ticks.load(Ordering::SeqCst),
            5,
            "a frozen node runs nothing"
        );
        assert_eq!(other.load(Ordering::SeqCst), 15, "other nodes keep running");
        assert_eq!(
            in_node(1, async { time::now() }).await - clock_before,
            ms(100),
            "the frozen node's clock keeps running"
        );
        assert_eq!(world.live_tasks(1), 2, "nothing was aborted");
        world.thaw(1);
        tokio::time::sleep(ms(1)).await;
        // The overdue sleep fires at once, and the late task runs.
        assert_eq!(ticks.load(Ordering::SeqCst), 106);
    });
}

#[test]
fn a_panicking_extension_callback_keeps_the_slot() {
    run(79, |world| async move {
        world.insert_ext(42_u32);
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            world.with_ext(|value: &mut u32| {
                *value += 1;
                panic!("extension callback");
            });
        }));
        assert!(panic.is_err());
        assert_eq!(world.with_ext(|value: &mut u32| *value), Some(43));
    });
}

#[test]
fn ext_slots_are_typed_and_reentrant_per_type() {
    #[derive(Debug, PartialEq)]
    struct Probes(Vec<u32>);
    struct Other(u32);
    run(18, |world| async move {
        assert_eq!(world.with_ext(|probes: &mut Probes| probes.0.len()), None);
        world.insert_ext(Probes(vec![1]));
        world.insert_ext(Other(5));
        let nested = world.with_ext(|probes: &mut Probes| {
            probes.0.push(2);
            let same = crate::sim::world().unwrap().with_ext(|_: &mut Probes| ());
            let other = crate::sim::world()
                .unwrap()
                .with_ext(|other: &mut Other| other.0);
            (same, other)
        });
        assert_eq!(nested, Some((None, Some(5))));
        assert_eq!(
            world.with_ext(|probes: &mut Probes| probes.0.clone()),
            Some(vec![1, 2])
        );
        world.insert_ext(Probes(vec![9]));
        assert_eq!(
            world.with_ext(|probes: &mut Probes| probes.0.clone()),
            Some(vec![9])
        );
    });
}
