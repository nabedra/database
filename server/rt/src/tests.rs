//! The seam outside a simulated world equals the direct calls. These tests run in a
//! product build and, unchanged, with the `sim` feature: outside an installed world the
//! simulation forwards to the product calls too.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{ids, task, time};

#[tokio::test(start_paused = true)]
async fn now_is_the_tokio_clock() {
    assert_eq!(time::now(), tokio::time::Instant::now());
    tokio::time::advance(Duration::from_millis(1_234)).await;
    assert_eq!(time::now(), tokio::time::Instant::now());
}

#[tokio::test(start_paused = true)]
async fn timers_take_the_requested_time() {
    let start = tokio::time::Instant::now();
    time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        tokio::time::Instant::now() - start,
        Duration::from_millis(300)
    );

    let deadline = start + Duration::from_millis(1_000);
    time::sleep_until(deadline).await;
    assert_eq!(tokio::time::Instant::now(), deadline);

    let elapsed = time::timeout(Duration::from_millis(50), std::future::pending::<()>()).await;
    assert!(elapsed.is_err());
    assert_eq!(
        tokio::time::Instant::now() - deadline,
        Duration::from_millis(50)
    );
    let at = tokio::time::Instant::now() + Duration::from_millis(20);
    assert!(
        time::timeout_at(at, std::future::pending::<()>())
            .await
            .is_err()
    );
    assert_eq!(tokio::time::Instant::now(), at);
    assert_eq!(
        time::timeout(Duration::from_secs(1), async { 7 })
            .await
            .unwrap(),
        7
    );

    let mut ticks = time::interval(Duration::from_millis(10));
    let first = ticks.tick().await;
    let second = ticks.tick().await;
    assert_eq!(second - first, Duration::from_millis(10));
    let mut later = time::interval_at(at + Duration::from_millis(100), Duration::from_millis(5));
    assert_eq!(later.tick().await, at + Duration::from_millis(100));
}

#[test]
fn std_and_wall_clocks_are_the_system_clocks() {
    let std_before = std::time::Instant::now();
    let wall_before = SystemTime::now();
    let std_now = time::now_std();
    let wall_now = time::wall_now();
    let ms = time::unix_ms();
    let micros = time::unix_micros();
    let nanos = time::unix_nanos();
    let std_after = std::time::Instant::now();
    let wall_after = SystemTime::now();
    assert!(std_before <= std_now && std_now <= std_after);
    assert!(wall_before <= wall_now && wall_now <= wall_after);
    let low = wall_before.duration_since(UNIX_EPOCH).unwrap();
    let high = wall_after.duration_since(UNIX_EPOCH).unwrap();
    assert!(low.as_millis() as u64 <= ms && ms <= high.as_millis() as u64);
    assert!(low.as_micros() as u64 <= micros && micros <= high.as_micros() as u64);
    assert!(low.as_nanos() <= nanos && nanos <= high.as_nanos());
}

#[test]
fn thread_sleep_sleeps() {
    let start = std::time::Instant::now();
    time::thread_sleep(Duration::from_millis(20));
    assert!(start.elapsed() >= Duration::from_millis(20));
}

#[tokio::test]
async fn spawned_work_runs_on_the_runtime() {
    assert_eq!(task::spawn(async { 3 }).await.unwrap(), 3);
    let handle = tokio::runtime::Handle::current();
    assert_eq!(task::spawn_on(&handle, async { 4 }).await.unwrap(), 4);

    // spawn_blocking runs on one of tokio's blocking threads, not inline.
    let caller = std::thread::current().id();
    let ran_on = task::spawn_blocking(|| std::thread::current().id())
        .await
        .unwrap();
    assert_ne!(ran_on, caller);
    assert!(
        task::spawn_blocking(|| -> u8 { panic!("planted") })
            .await
            .unwrap_err()
            .is_panic()
    );

    let mut set = task::JoinSet::new();
    task::spawn_in(&mut set, async { 5 });
    assert_eq!(set.join_next().await.unwrap().unwrap(), 5);
    let pending = task::spawn_in(&mut set, std::future::pending::<u8>());
    pending.abort();
    assert!(set.join_next().await.unwrap().unwrap_err().is_cancelled());
    assert!(set.is_empty());
}

#[test]
fn elapsed_reads_the_std_clock() {
    let start = time::now_std();
    time::thread_sleep(Duration::from_millis(20));
    let measured = time::elapsed(start);
    assert!(measured >= Duration::from_millis(20));
    assert!(measured <= start.elapsed());
    // A start in the future gives zero, as Instant::elapsed does.
    assert_eq!(
        time::elapsed(time::now_std() + Duration::from_secs(3_600)),
        Duration::ZERO
    );
}

#[tokio::test(start_paused = true)]
async fn elapsed_reads_the_tokio_clock() {
    let start = time::now();
    tokio::time::advance(Duration::from_millis(1_234)).await;
    assert_eq!(time::elapsed(start), Duration::from_millis(1_234));
    assert_eq!(time::elapsed(start), start.elapsed());
    assert_eq!(
        time::elapsed(time::now() + Duration::from_secs(1)),
        Duration::ZERO
    );
}

#[test]
fn spawn_blocking_on_uses_the_given_runtime() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .build()
        .unwrap();
    // The calling thread has no runtime entered when the work is submitted.
    let caller = std::thread::current().id();
    let target = runtime.handle();
    let ran = task::spawn_blocking_on(target, move || {
        (
            std::thread::current().id(),
            tokio::runtime::Handle::current().id(),
        )
    });
    let (thread, runtime_id) = runtime.block_on(ran).unwrap();
    assert_ne!(thread, caller);
    assert_eq!(runtime_id, target.id());
    let panicked = task::spawn_blocking_on(target, || -> u8 { panic!("planted") });
    assert!(runtime.block_on(panicked).unwrap_err().is_panic());
}

#[test]
fn spawn_on_uses_the_given_runtime_and_preserves_abort() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    // The calling thread has no runtime entered when the tasks are submitted.
    let target = runtime.handle();
    let ran = task::spawn_on(target, async { tokio::runtime::Handle::current().id() });
    let cancelled = task::spawn_on(target, async { panic!("aborted task was polled") });
    cancelled.abort();
    assert_eq!(runtime.block_on(ran).unwrap(), target.id());
    assert!(runtime.block_on(cancelled).unwrap_err().is_cancelled());
}

#[test]
fn services_are_named_threads() {
    let started = Arc::new(AtomicBool::new(false));
    let flag = started.clone();
    let service = task::spawn_service("nabedra-test-service", move || {
        flag.store(true, Ordering::SeqCst);
        std::thread::current().name().map(str::to_owned)
    })
    .unwrap();
    assert_eq!(
        service.join().unwrap().as_deref(),
        Some("nabedra-test-service")
    );
    assert!(started.load(Ordering::SeqCst));

    let caller = std::thread::current().id();
    let stacked = task::spawn_service_with_stack("nabedra-test-stack", 128 * 1024, move || {
        // Use part of the stack to show the requested size is usable.
        let buffer = [7u8; 32 * 1024];
        (
            std::thread::current().id(),
            buffer.iter().map(|&b| u32::from(b)).sum::<u32>(),
        )
    })
    .unwrap();
    let (id, sum) = stacked.join().unwrap();
    assert_ne!(id, caller);
    assert_eq!(sum, 7 * 32 * 1024);
}

#[test]
fn ids_are_random_version_4() {
    let first = ids::uuid_v4();
    let second = ids::uuid_v4();
    assert_ne!(first, second);
    assert_eq!(first.get_version_num(), 4);
    assert_eq!(first.get_variant(), uuid::Variant::RFC4122);
    // 64 random bits: 32 draws are distinct and set the top bits at some point.
    let draws: Vec<u64> = (0..32).map(|_| ids::random_u64()).collect();
    let mut unique = draws.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), draws.len());
    let union = draws.iter().fold(0, |acc, value| acc | value);
    assert_ne!(union >> 62, 0, "the top bits are never set");
}

/// The P7-BANS wrappers inside a simulated world: `spawn_in` and `spawn_blocking_on`
/// keep the node context and registry, and `elapsed` follows the node clock (O09).
#[cfg(feature = "sim")]
mod in_a_world {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use crate::sim::{NodeClock, current_node, in_node, run, runtime};
    use crate::{task, time};

    fn ms(value: u64) -> Duration {
        Duration::from_millis(value)
    }

    #[test]
    fn spawn_in_carries_the_node_and_joins_its_registry() {
        run(11, |world| async move {
            world.add_node(
                1,
                NodeClock {
                    mono_offset: ms(7_000),
                    ..NodeClock::default()
                },
            );
            world.add_node(2, NodeClock::default());
            let start = world.start();
            let mut set = task::JoinSet::new();
            let mut sleepers = task::JoinSet::new();
            in_node(1, async {
                // A session handler reads its node's clock, not the runtime's.
                task::spawn_in(&mut set, async move {
                    (current_node(), time::now().duration_since(start))
                });
                task::spawn_in(&mut sleepers, time::sleep(ms(10_000)));
            })
            .await;
            in_node(2, async {
                task::spawn_in(&mut sleepers, time::sleep(ms(50)));
            })
            .await;
            assert_eq!(
                set.join_next().await.unwrap().unwrap(),
                (Some(1), ms(7_000))
            );
            tokio::task::yield_now().await;
            assert_eq!(world.live_tasks(1), 1, "the sleeping task is registered");

            // A crash aborts node 1's JoinSet task; node 2's task in the same set runs on.
            world.crash(1);
            assert!(
                sleepers
                    .join_next()
                    .await
                    .unwrap()
                    .unwrap_err()
                    .is_cancelled()
            );
            sleepers.join_next().await.unwrap().unwrap();
            assert_eq!(world.live_tasks(1), 0);

            // Spawning into a down node aborts at once, as task::spawn does.
            in_node(1, async {
                task::spawn_in(&mut set, async { (None, Duration::ZERO) });
            })
            .await;
            assert!(set.join_next().await.unwrap().unwrap_err().is_cancelled());

            // Outside a node the task is an ordinary runtime task.
            task::spawn_in(&mut set, async { (current_node(), Duration::ZERO) });
            assert_eq!(
                set.join_next().await.unwrap().unwrap(),
                (None, Duration::ZERO)
            );
        });
    }

    #[test]
    fn spawn_blocking_on_runs_inline_in_the_node_context() {
        run(12, |world| async move {
            world.add_node(3, NodeClock::default());
            let handle = tokio::runtime::Handle::current();
            let ran = Arc::new(AtomicBool::new(false));
            let ran_in_job = ran.clone();
            let node = in_node(3, async move {
                let job = task::spawn_blocking_on(&handle, move || {
                    ran_in_job.store(true, Ordering::SeqCst);
                    current_node()
                });
                assert!(
                    ran.load(Ordering::SeqCst),
                    "spawn_blocking_on did not run inline"
                );
                job.await.unwrap()
            })
            .await;
            assert_eq!(node, Some(3));
            let handle = tokio::runtime::Handle::current();
            let panicked = task::spawn_blocking_on(&handle, || -> u8 { panic!("planted") });
            assert!(panicked.await.unwrap_err().is_panic());
        });
    }

    #[test]
    #[should_panic(expected = "a simulated world cannot spawn onto another runtime")]
    fn spawn_blocking_on_rejects_a_foreign_runtime() {
        let foreign = runtime().unwrap();
        let target = foreign.handle();
        run(13, |_world| async move {
            drop(task::spawn_blocking_on(target, || ()));
        });
    }

    #[test]
    fn elapsed_follows_the_node_clock_and_ignores_wall_jumps() {
        run(14, |world| async move {
            // 10% fast.
            world.add_node(
                4,
                NodeClock {
                    drift_ppm: 100_000,
                    ..NodeClock::default()
                },
            );
            let (tokio_start, std_start) =
                in_node(4, async { (time::now(), time::now_std()) }).await;
            tokio::time::advance(ms(1_000)).await;
            // O09: a wall clock jump moves neither measured duration.
            world.jump_wall_backward(4, Duration::from_secs(3_600));
            let (tokio_elapsed, std_elapsed) = in_node(4, async {
                (time::elapsed(tokio_start), time::elapsed(std_start))
            })
            .await;
            assert_eq!(tokio_elapsed, ms(1_100));
            assert_eq!(std_elapsed, ms(1_100));
            // Outside the node the runtime's virtual clock applies.
            assert_eq!(time::elapsed(world.start()), ms(1_000));
        });
    }
}

/// The product queue service: run waits on the doorbell where step would idle.
struct Echo {
    shared: Arc<(std::sync::Mutex<Vec<u32>>, crate::sync::Condvar)>,
    seen: std::sync::mpsc::Sender<(Option<String>, u32)>,
}

impl task::Stepped for Echo {
    fn step(&mut self) -> task::Step {
        match self.shared.0.lock().unwrap().pop() {
            Some(0) => task::Step::Exit,
            Some(value) => {
                self.seen.send((task::current_service(), value)).unwrap();
                task::Step::Worked
            }
            None => task::Step::Idle(None),
        }
    }

    fn doorbell(&self) -> &crate::sync::Condvar {
        &self.shared.1
    }

    fn run(mut self) {
        loop {
            match self.step() {
                task::Step::Exit => return,
                task::Step::Worked => {}
                task::Step::Idle(_) => {
                    let guard = self.shared.0.lock().unwrap();
                    let _guard = self.shared.1.wait_while(guard, |items| items.is_empty());
                }
            }
        }
    }
}

#[test]
fn stepped_services_are_named_threads_running_their_loop() {
    let shared = Arc::new((
        std::sync::Mutex::new(Vec::new()),
        crate::sync::Condvar::new(),
    ));
    let (seen, received) = std::sync::mpsc::channel();
    let handle = task::spawn_stepped(
        "kv-journal",
        256 * 1024,
        Echo {
            shared: Arc::clone(&shared),
            seen,
        },
    )
    .unwrap();
    assert!(!handle.is_current());
    shared.0.lock().unwrap().push(5);
    shared.1.notify_one();
    assert_eq!(received.recv().unwrap(), (Some("kv-journal".to_owned()), 5));
    assert!(!handle.is_finished());
    shared.0.lock().unwrap().push(0);
    shared.1.notify_all();
    handle.join().unwrap();
    assert_eq!(
        task::current_service(),
        std::thread::current().name().map(str::to_owned)
    );
}

#[test]
fn drive_until_is_none_outside_a_world() {
    let value = task::drive_until::<()>("product", || panic!("ready must not be called"));
    assert_eq!(value, None);
}

#[test]
fn spawn_oneshot_is_a_named_thread() {
    let (sender, receiver) = std::sync::mpsc::channel();
    task::spawn_oneshot("oltp-damage", 256 * 1024, move || {
        sender.send(task::current_service()).unwrap();
    })
    .unwrap();
    assert_eq!(receiver.recv().unwrap().as_deref(), Some("oltp-damage"));
}

#[test]
fn condvar_forwards_to_std() {
    let pair = Arc::new((std::sync::Mutex::new(false), crate::sync::Condvar::new()));
    let (lock, condvar) = &*pair;
    let guard = lock.lock().unwrap();
    let (guard, timeout) = condvar
        .wait_timeout(guard, Duration::from_millis(5))
        .unwrap();
    assert!(timeout.timed_out());
    drop(guard);
    let notifier = Arc::clone(&pair);
    let thread = std::thread::spawn(move || {
        *notifier.0.lock().unwrap() = true;
        notifier.1.notify_all();
    });
    let guard = condvar
        .wait_while(lock.lock().unwrap(), |ready| !*ready)
        .unwrap();
    assert!(*guard);
    drop(guard);
    thread.join().unwrap();
    #[cfg(not(feature = "sim"))]
    assert_eq!(
        std::mem::size_of::<crate::sync::Condvar>(),
        std::mem::size_of::<std::sync::Condvar>(),
        "repr(transparent) in product"
    );
}
