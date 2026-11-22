//! Simulation-mode tests. Each one fails when the property it names is broken.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use super::{NodeClock, World, current_node, in_node, run};
use crate::{ids, task, time};

fn ms(value: u64) -> Duration {
    Duration::from_millis(value)
}

/// The node clock's elapsed time since the world started.
fn local_elapsed(world: &World) -> Duration {
    time::now().duration_since(world.start())
}

fn wall_ms() -> i128 {
    match time::wall_now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(since) => since.as_millis() as i128,
        Err(before) => -(before.duration().as_millis() as i128),
    }
}

#[test]
fn monotonic_offset_is_applied() {
    run(1, |world| async move {
        world.add_node(
            1,
            NodeClock {
                mono_offset: ms(5_000),
                ..NodeClock::default()
            },
        );
        world.add_node(2, NodeClock::default());
        tokio::time::advance(ms(250)).await;
        let one = in_node(1, async { local_elapsed(&world) }).await;
        let two = in_node(2, async { local_elapsed(&world) }).await;
        assert_eq!(one, ms(5_250));
        assert_eq!(two, ms(250));
        // The std clock follows the same node clock.
        let std_gap = in_node(1, async { time::now_std() }).await
            - in_node(2, async { time::now_std() }).await;
        assert_eq!(std_gap, ms(5_000));
    });
}

#[test]
fn drift_scales_the_node_clock_and_its_timers() {
    run(2, |world| async move {
        // 10% fast and 50% slow.
        world.add_node(
            1,
            NodeClock {
                drift_ppm: 100_000,
                ..NodeClock::default()
            },
        );
        world.add_node(
            2,
            NodeClock {
                drift_ppm: -500_000,
                ..NodeClock::default()
            },
        );
        tokio::time::advance(ms(10_000)).await;
        assert_eq!(
            in_node(1, async { local_elapsed(&world) }).await,
            ms(11_000)
        );
        assert_eq!(in_node(2, async { local_elapsed(&world) }).await, ms(5_000));
        assert_eq!(
            in_node(1, async { time::unix_ms() }).await,
            super::EPOCH_MS + 11_000
        );

        // An 11 s sleep on the fast node takes 10 s of virtual time; on the slow node a
        // 1 s sleep takes 2 s.
        let virtual_start = tokio::time::Instant::now();
        in_node(1, async { time::sleep(ms(11_000)).await }).await;
        assert_eq!(tokio::time::Instant::now() - virtual_start, ms(10_000));
        let virtual_start = tokio::time::Instant::now();
        in_node(2, async { time::sleep(ms(1_000)).await }).await;
        assert_eq!(tokio::time::Instant::now() - virtual_start, ms(2_000));

        // A deadline on the node clock fires when the node clock reaches it, not earlier.
        let deadline = in_node(1, async { time::now() + ms(1_100) }).await;
        in_node(1, async { time::sleep_until(deadline).await }).await;
        let reached = in_node(1, async { time::now() }).await;
        assert!(
            reached >= deadline,
            "fired early: {reached:?} < {deadline:?}"
        );
        // tokio timers have millisecond ticks: at most one tick late on the node clock.
        assert!(
            reached - deadline < ms(2),
            "fired late: {:?}",
            reached - deadline
        );

        // A timeout measured on the slow node.
        let virtual_start = tokio::time::Instant::now();
        let outcome = in_node(2, async {
            time::timeout(ms(500), std::future::pending::<()>()).await
        })
        .await;
        assert!(outcome.is_err());
        assert_eq!(tokio::time::Instant::now() - virtual_start, ms(1_000));
    });
}

#[test]
fn drift_change_keeps_the_clock_continuous() {
    run(3, |world| async move {
        world.add_node(
            1,
            NodeClock {
                drift_ppm: 200_000,
                ..NodeClock::default()
            },
        );
        tokio::time::advance(ms(1_000)).await;
        let before = in_node(1, async { local_elapsed(&world) }).await;
        assert_eq!(before, ms(1_200));
        world.set_drift_ppm(1, -200_000);
        assert_eq!(in_node(1, async { local_elapsed(&world) }).await, before);
        tokio::time::advance(ms(1_000)).await;
        assert_eq!(in_node(1, async { local_elapsed(&world) }).await, ms(2_000));
    });
}

#[test]
fn wall_jumps_move_wall_now_but_never_now() {
    run(4, |world| async move {
        // A large monotonic offset: a jump that leaks into now() cannot hide behind saturation.
        world.add_node(
            1,
            NodeClock {
                mono_offset: Duration::from_secs(600),
                ..NodeClock::default()
            },
        );
        world.add_node(
            2,
            NodeClock {
                wall_offset_nanos: -3_000_000_000,
                ..NodeClock::default()
            },
        );
        tokio::time::advance(ms(100)).await;
        in_node(1, async {
            let mono_before = time::now();
            let wall_before = wall_ms();
            world.jump_wall_backward(1, ms(60_000));
            let mono_after = time::now();
            let wall_after = wall_ms();
            assert!(
                mono_after >= mono_before,
                "now() went backward on a wall jump"
            );
            assert_eq!(mono_after, mono_before, "a wall jump moved now()");
            assert_eq!(wall_after, wall_before - 60_000);

            world.jump_wall_forward(1, ms(90_000));
            assert_eq!(time::now(), mono_after, "a wall jump moved now()");
            assert_eq!(wall_ms(), wall_after + 90_000);

            // A backward jump past the epoch reads 0 from the epoch helpers.
            world.jump_wall_backward(1, Duration::from_secs(60 * 365 * 24 * 3600));
            assert_eq!(time::unix_ms(), 0);
            assert_eq!(time::unix_nanos(), 0);
        })
        .await;
        // Node 2 starts three seconds behind; the jumps on node 1 do not touch it.
        assert_eq!(
            in_node(2, async { wall_ms() }).await,
            i128::from(super::EPOCH_MS) + 100 - 3_000
        );

        // A forward monotonic step moves both clocks forward.
        let mono = in_node(2, async { time::now() }).await;
        world.advance_mono(2, ms(400));
        assert_eq!(in_node(2, async { time::now() }).await - mono, ms(400));
        assert_eq!(
            in_node(2, async { wall_ms() }).await,
            i128::from(super::EPOCH_MS) + 500 - 3_000
        );
    });
}

#[test]
fn wall_clock_preserves_time_before_the_epoch() {
    run(4, |world| async move {
        world.add_node(1, NodeClock::default());
        world.jump_wall_backward(1, ms(super::EPOCH_MS + 2_000));
        in_node(1, async {
            let wall = time::wall_now();
            assert_eq!(wall, SystemTime::UNIX_EPOCH - ms(2_000));
            assert_eq!(time::unix_ms(), 0);
            assert_eq!(time::unix_micros(), 0);
            assert_eq!(time::unix_nanos(), 0);
            world.jump_wall_forward(1, ms(2_500));
            assert_eq!(time::wall_now(), SystemTime::UNIX_EPOCH + ms(500));
        })
        .await;
    });
}

fn draws(seed: u64, node: u64) -> (Vec<uuid::Uuid>, Vec<u64>) {
    run(seed, move |world| async move {
        world.add_node(1, NodeClock::default());
        world.add_node(2, NodeClock::default());
        in_node(node, async {
            let ids = (0..4).map(|_| ids::uuid_v4()).collect();
            let numbers = (0..4).map(|_| ids::random_u64()).collect();
            (ids, numbers)
        })
        .await
    })
}

#[test]
fn ids_are_deterministic_per_seed_and_distinct_per_node() {
    let first = draws(7, 1);
    assert_eq!(first, draws(7, 1), "the same seed must give the same ids");
    assert_ne!(first, draws(7, 2), "two nodes must not share an id stream");
    assert_ne!(first, draws(8, 1), "another seed must give other ids");
    for id in &first.0 {
        assert_eq!(id.get_version_num(), 4);
        assert_eq!(id.get_variant(), uuid::Variant::RFC4122);
    }
    let mut unique = first.0.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), first.0.len());
}

#[test]
fn node_streams_are_independent_of_other_nodes_draws() {
    // Node 1's ids do not depend on how often node 2 drew.
    let interleaved = run(9, |world| async move {
        world.add_node(1, NodeClock::default());
        world.add_node(2, NodeClock::default());
        in_node(2, async {
            (0..5).for_each(|_| {
                let _ = ids::uuid_v4();
            })
        })
        .await;
        in_node(1, async { ids::uuid_v4() }).await
    });
    assert_eq!(interleaved, draws(9, 1).0[0]);
}

#[test]
fn crash_aborts_the_nodes_tasks_and_their_descendants() {
    run(5, |world| async move {
        world.add_node(1, NodeClock::default());
        world.add_node(2, NodeClock::default());
        let ticks = Arc::new(AtomicUsize::new(0));

        let survivor = {
            let ticks = ticks.clone();
            world.spawn_node(2, async move {
                time::sleep(ms(1_000)).await;
                ticks.fetch_add(1, Ordering::SeqCst);
                current_node()
            })
        };
        let root = world.spawn_node(1, async move {
            // A grandchild spawned through the seam joins node 1 as well.
            let child = task::spawn(async {
                let grandchild = task::spawn(time::sleep(ms(10_000)));
                (current_node(), grandchild.await)
            });
            time::sleep(ms(10_000)).await;
            child.await
        });
        tokio::time::advance(ms(10)).await;
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            world.live_tasks(1),
            3,
            "the root, child and grandchild are registered"
        );

        world.crash(1);
        let error = root.await.expect_err("the crash aborts the root task");
        assert!(error.is_cancelled());
        assert_eq!(world.live_tasks(1), 0);
        assert!(!world.is_up(1));

        // Spawning into a down node aborts at once; after a restart it runs again.
        let late = world.spawn_node(1, async { 1 });
        assert!(late.await.expect_err("node 1 is down").is_cancelled());
        world.restart(1);
        assert_eq!(
            world.spawn_node(1, async { current_node() }).await.unwrap(),
            Some(1)
        );

        assert_eq!(
            survivor.await.unwrap(),
            Some(2),
            "another node's task survives"
        );
        assert_eq!(ticks.load(Ordering::SeqCst), 1);
    });
}

#[test]
#[allow(clippy::async_yields_async)] // in_node returns a handle spawned inside the node.
fn spawn_on_registers_with_the_node() {
    run(6, |world| async move {
        world.add_node(1, NodeClock::default());
        let handle = tokio::runtime::Handle::current();
        let task = in_node(
            1,
            async move { task::spawn_on(&handle, time::sleep(ms(5_000))) },
        )
        .await;
        tokio::task::yield_now().await;
        world.crash(1);
        assert!(task.await.unwrap_err().is_cancelled());
    });
}

#[test]
#[should_panic(expected = "a simulated world cannot spawn onto another runtime")]
fn spawn_on_rejects_a_foreign_runtime_in_simulation() {
    let foreign = super::runtime().unwrap();
    let target = foreign.handle();
    run(6, |world| async move {
        world.add_node(1, NodeClock::default());
        in_node(1, async {
            drop(task::spawn_on(target, async {}));
        })
        .await;
    });
}

#[test]
fn run_enters_its_runtime_before_constructing_the_future() {
    run(6, |world| {
        assert_eq!(super::world().unwrap().seed(), world.seed());
        let sleep = time::sleep(ms(25));
        async move {
            sleep.await;
            assert_eq!(local_elapsed(&world), ms(25));
        }
    });
}

#[test]
fn spawn_blocking_runs_inline_in_the_node_context() {
    run(7, |world| async move {
        world.add_node(
            1,
            NodeClock {
                mono_offset: ms(2_000),
                ..NodeClock::default()
            },
        );
        tokio::time::advance(ms(30)).await;
        let world_for_job = world.clone();
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ran_in_job = ran.clone();
        let (node, elapsed, thread) = in_node(1, async move {
            let handle = task::spawn_blocking(move || {
                ran_in_job.store(true, Ordering::SeqCst);
                // Blocking sleeps do not stall the simulation or move virtual time.
                time::thread_sleep(Duration::from_secs(2));
                (
                    current_node(),
                    time::now().duration_since(world_for_job.start()),
                    std::thread::current().name().map(str::to_owned),
                )
            });
            // The work has already run when spawn_blocking returns, before any await.
            assert!(
                ran.load(Ordering::SeqCst),
                "spawn_blocking did not run inline"
            );
            handle.await.unwrap()
        })
        .await;
        assert_eq!(node, Some(1));
        assert_eq!(
            elapsed,
            ms(2_030),
            "virtual time stands still during blocking work"
        );
        assert_eq!(thread.as_deref(), Some("sim-blocking"));

        // tokio's blocking_recv works in blocking work, as on a product blocking thread.
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        sender.send(5).await.unwrap();
        let received = task::spawn_blocking(move || receiver.blocking_recv())
            .await
            .unwrap();
        assert_eq!(received, Some(5));

        // A panic becomes a JoinError, as with tokio's spawn_blocking.
        let panicked = task::spawn_blocking(|| -> u8 { panic!("planted") }).await;
        assert!(panicked.unwrap_err().is_panic());
    });
}

#[test]
#[allow(clippy::async_yields_async)] // in_node returns a handle spawned inside the node.
fn spawn_blocking_order_is_the_call_order() {
    let order = run(8, |world| async move {
        world.add_node(1, NodeClock::default());
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut handles = Vec::new();
        for index in 0..8 {
            let log = log.clone();
            handles.push(
                in_node(1, async move {
                    task::spawn_blocking(move || log.lock().unwrap().push(index))
                })
                .await,
            );
        }
        for handle in handles {
            handle.await.unwrap();
        }
        Arc::try_unwrap(log).unwrap().into_inner().unwrap()
    });
    assert_eq!(order, (0..8).collect::<Vec<_>>());
}

#[test]
fn services_get_their_name_stack_and_node_context() {
    run(10, |world| async move {
        world.add_node(
            3,
            NodeClock {
                mono_offset: ms(700),
                ..NodeClock::default()
            },
        );
        let world_for_service = world.clone();
        let service = in_node(3, async move {
            task::spawn_service_with_stack("journal", 256 * 1024, move || {
                (
                    std::thread::current().name().map(str::to_owned),
                    current_node(),
                    time::now().duration_since(world_for_service.start()) >= ms(700),
                )
            })
        })
        .await
        .unwrap();
        let (name, node, offset_applied) = service.join().unwrap();
        assert_eq!(name.as_deref(), Some("journal"));
        assert_eq!(node, Some(3));
        assert!(offset_applied, "the service reads its node's clock");
        assert_eq!(world.services(3), vec!["journal".to_owned()]);

        let plain = in_node(3, async { task::spawn_service("flush", current_node) })
            .await
            .unwrap();
        assert_eq!(plain.join().unwrap(), Some(3));
    });
}

#[test]
fn outside_a_node_the_world_clock_is_virtual() {
    run(11, |world| async move {
        tokio::time::advance(ms(1_500)).await;
        assert_eq!(local_elapsed(&world), ms(1_500));
        assert_eq!(time::unix_ms(), super::EPOCH_MS + 1_500);
        assert_eq!(time::unix_micros(), (super::EPOCH_MS + 1_500) * 1_000);
        assert_eq!(current_node(), None);
    });
}

#[test]
#[should_panic(expected = "stops or reverses the clock")]
fn a_drift_that_stops_the_clock_is_refused() {
    run(12, |world| async move {
        world.add_node(
            1,
            NodeClock {
                drift_ppm: -1_000_000,
                ..NodeClock::default()
            },
        );
    });
}
