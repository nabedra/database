//! The worker pool inside a `nabedra_rt::sim` world (check `worker-pool-sim`, a fault
//! model). Each test mirrors a product pool test or names the scenario it covers: C04
//! and C06 (slot count, overlap and refill), F01 and F02 (errors and panics leave the
//! pool usable), F03 (a dropped receiver never cancels, stop wakes waiters, usage returns
//! to baseline), and the phase 9 Q4 limit on `block_on`.

use super::sim::JobCost;
use super::*;
use nabedra_rt::sim::{NodeClock, World, in_node, run};
use std::collections::{BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const NODE: u64 = 1;

fn ms(value: u64) -> Duration {
    Duration::from_millis(value)
}

/// Runs `body` as part of node 1 of a fresh world seeded with `seed`.
fn simulate<F, Fut>(seed: u64, body: F) -> Fut::Output
where
    F: FnOnce(Arc<World>) -> Fut,
    Fut: Future,
{
    run(seed, |world| async move {
        world.add_node(NODE, NodeClock::default());
        in_node(NODE, body(world)).await
    })
}

fn pool(count: usize) -> (WorkerPool, WorkerThreads) {
    WorkerPool::start(NonZeroUsize::new(count).unwrap(), Handle::current()).unwrap()
}

/// Costs taken from `queue` in dispatch order, zero once it is empty.
fn queued_costs(costs: impl IntoIterator<Item = Duration>) -> JobCost {
    let mut queue: VecDeque<Duration> = costs.into_iter().collect();
    JobCost::from_fn(move |_| queue.pop_front().unwrap_or_default())
}

fn poll_once<F: Future + ?Sized>(future: std::pin::Pin<&mut F>) -> std::task::Poll<F::Output> {
    std::pin::pin!(tokio::task::unconstrained(future))
        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
}

/// A finished job as the test records it: worker, virtual time it ran, service name.
type Ran = (WorkerId, Duration, Option<String>);

/// Submits `jobs` jobs at once, each from its own task, and returns what each recorded,
/// in submission order.
async fn run_jobs(world: &Arc<World>, pool: &WorkerPool, jobs: usize) -> Vec<Ran> {
    let mut handles = Vec::new();
    for _ in 0..jobs {
        let pool = pool.clone();
        let world = world.clone();
        handles.push(nabedra_rt::task::spawn(async move {
            pool.submit(0, move |id, _| {
                Ok((id, world.elapsed(), nabedra_rt::task::current_service()))
            })
            .await
            .unwrap()
            .receive()
            .await
            .unwrap()
        }));
    }
    let mut ran = Vec::new();
    for handle in handles {
        ran.push(handle.await.unwrap());
    }
    ran
}

#[test]
fn c04_sim_pool_runs_count_worker_tasks_and_overlaps_count_jobs() {
    simulate(4, |world| async move {
        let cost = ms(10);
        world.insert_ext(JobCost::fixed(cost));
        let before = world.live_tasks(NODE);
        let (pool, threads) = pool(3);
        // One node task per worker, and no thread (P6 plants count + 1).
        assert_eq!(world.live_tasks(NODE) - before, 3);
        assert!(world.services(NODE).is_empty());
        let ran = run_jobs(&world, &pool, 12).await;
        let ids: BTreeSet<WorkerId> = ran.iter().map(|(id, _, _)| *id).collect();
        assert_eq!(
            ids,
            BTreeSet::from([0, 1, 2]),
            "worker ids stay in 0..count"
        );
        for (id, _, service) in &ran {
            assert_eq!(
                service.as_deref(),
                Some(format!("nabedra-worker-{id}").as_str())
            );
        }
        // Three slots hold a job each at once: 12 jobs of 10 ms finish in four waves.
        let mut waves = std::collections::BTreeMap::<Duration, usize>::new();
        for (_, at, _) in &ran {
            *waves.entry(*at).or_default() += 1;
        }
        assert_eq!(
            waves.into_iter().collect::<Vec<_>>(),
            vec![(cost, 3), (cost * 2, 3), (cost * 3, 3), (cost * 4, 3)]
        );
        assert_eq!(pool.scheduler().usage(), vec![(0, 0, 0)]);
        threads.shutdown().await.unwrap();
        assert_eq!(world.live_tasks(NODE), before, "every worker task ended");
    });
}

#[test]
fn c06_f01_f02_f03_sim_overlap_refill_errors_panic_and_receiver_drop() {
    simulate(6, |world| async move {
        // The first job dispatched holds its slot for 100 ms; the rest cost nothing.
        world.insert_ext(queued_costs([ms(100)]));
        let (pool, threads) = pool(2);
        let slow = pool
            .submit(0, |id, _| {
                Ok((id, nabedra_rt::sim::world().unwrap().elapsed()))
            })
            .await
            .unwrap();
        let fast = pool
            .submit(0, |id, _| Ok(id))
            .await
            .unwrap()
            .receive()
            .await
            .unwrap();
        assert_eq!(
            world.elapsed(),
            Duration::ZERO,
            "the free slot did not wait"
        );
        // The same partition index refills the free worker while the first is busy.
        let next = pool
            .submit(0, |id, _| Ok(id))
            .await
            .unwrap()
            .receive()
            .await
            .unwrap();
        assert_eq!(next, fast);
        for code in [
            ErrorCode::ValidationError,
            ErrorCode::StorageUnavailable,
            ErrorCode::InternalError,
            ErrorCode::LimitExceeded,
        ] {
            let failed: Job<()> = pool
                .submit(0, move |_, _| {
                    Err(ProtocolError::new(
                        code,
                        "injected ordinary task failure",
                        Outcome::NotApplied,
                    ))
                })
                .await
                .unwrap();
            let error = failed.receive().await.unwrap_err();
            assert_eq!(error.code, code);
            assert_eq!(error.outcome, Outcome::NotApplied);
            let subsequent = pool.submit(0, |id, _| Ok(id)).await.unwrap();
            assert_eq!(subsequent.receive().await.unwrap(), fast);
        }
        let panic: Job<()> = pool
            .submit(0, |_, _| panic!("injected unwind"))
            .await
            .unwrap();
        let error = panic.receive().await.unwrap_err();
        assert_eq!(error.code, ErrorCode::InternalError);
        let after_panic = pool.submit(0, |id, _| Ok(id)).await.unwrap();
        assert_eq!(after_panic.receive().await.unwrap(), fast);
        let done = Arc::new(AtomicBool::new(false));
        let abandoned = pool
            .submit(0, {
                let done = done.clone();
                move |_, _| {
                    done.store(true, Ordering::SeqCst);
                    Ok(())
                }
            })
            .await
            .unwrap();
        drop(abandoned);
        // The free worker takes jobs in order, so the abandoned job ran before this one.
        let witness = pool.submit(0, |_, _| Ok(())).await.unwrap();
        witness.receive().await.unwrap();
        assert!(
            done.load(Ordering::SeqCst),
            "a dropped receiver cancelled its job"
        );
        assert_eq!(world.elapsed(), Duration::ZERO);
        let (slow_id, slow_at) = slow.receive().await.unwrap();
        assert_ne!(slow_id, fast);
        assert_eq!(slow_at, ms(100));
        let final_job = pool.submit(0, |_, _| Ok(99)).await.unwrap();
        assert_eq!(final_job.receive().await.unwrap(), 99);
        threads.shutdown().await.unwrap();
        assert_eq!(pool.scheduler().usage(), vec![(0, 0, 0)]);
    });
}

#[test]
fn f03_sim_pool_stop_wakes_download_memory_waiter() {
    simulate(3, |_world| async move {
        let resources = WorkerResources {
            max_threads: 1,
            memory_limit_bytes: 1000,
            task_memory_limit_bytes: 900,
            control_memory_reserve_bytes: 100,
        };
        let (pool, threads) =
            WorkerPool::start_with_resources(resources, Handle::current()).unwrap();
        let held = pool.scheduler().reserve_memory(1, 0, 900).await.unwrap();
        let mut waiter = Box::pin(pool.scheduler().reserve_memory(2, 0, 100));
        assert!(poll_once(waiter.as_mut()).is_pending());
        let shutdown = threads.shutdown();
        assert!(waiter.await.is_err());
        drop(held);
        shutdown.await.unwrap();
        assert_eq!(pool.scheduler().usage(), vec![(0, 0, 0)]);
        assert!(pool.submit(0, |_, _| Ok(())).await.is_err());
    });
}

#[test]
fn m02_f03_sim_buffered_requests_keep_their_memory_until_released() {
    simulate(2, |_world| async move {
        let resources = WorkerResources {
            max_threads: 2,
            memory_limit_bytes: 1000,
            task_memory_limit_bytes: 800,
            control_memory_reserve_bytes: 100,
        };
        let (pool, threads) =
            WorkerPool::start_with_resources(resources, Handle::current()).unwrap();
        let mut retained = Vec::new();
        for request in 1..=2 {
            let charge = pool
                .scheduler()
                .reserve_memory(request, 0, 400)
                .await
                .unwrap();
            let job = pool
                .submit_buffered(request, 1, vec![charge], 400, |_, _| Ok(vec![0u8; 100]))
                .await
                .unwrap();
            retained.push(job.receive().await.unwrap());
        }
        assert_eq!(pool.scheduler().usage(), vec![(0, 0, 800)]);
        drop(retained);
        let subsequent = pool.submit_request(4, 1, 800, |_, _| Ok(42)).await.unwrap();
        assert_eq!(subsequent.receive().await.unwrap(), 42);
        threads.shutdown().await.unwrap();
        assert_eq!(pool.scheduler().usage(), vec![(0, 0, 0)]);
    });
}

#[test]
fn q4_sim_block_on_completes_ready_futures_and_contains_one_that_needs_time() {
    simulate(9, |_world| async move {
        let (pool, threads) = pool(1);
        let ready = pool
            .submit(0, |_, runtime| {
                Ok(runtime.block_on(async {
                    tokio::task::yield_now().await;
                    7
                }))
            })
            .await
            .unwrap();
        assert_eq!(ready.receive().await.unwrap(), 7);
        // Virtual time cannot pass while a job blocks the simulation (Q4): the job
        // panics, and the outer boundary turns that into a typed error.
        let timed: Job<()> = pool
            .submit(0, |_, runtime| {
                runtime.block_on(nabedra_rt::time::sleep(ms(1)));
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(
            timed.receive().await.unwrap_err().code,
            ErrorCode::InternalError
        );
        let usable = pool.submit(0, |id, _| Ok(id)).await.unwrap();
        assert_eq!(usable.receive().await.unwrap(), 0);
        threads.shutdown().await.unwrap();
    });
}

/// Sets a flag when dropped.
struct Witness(Arc<AtomicBool>);

impl Drop for Witness {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[test]
fn sim_shutdown_drains_accepted_jobs_and_aborts_tasks_jobs_spawned() {
    simulate(5, |world| async move {
        world.insert_ext(JobCost::fixed(ms(5)));
        let before = world.live_tasks(NODE);
        let (pool, threads) = pool(2);
        let dropped = Arc::new(AtomicBool::new(false));
        let spawned = pool
            .submit(0, {
                let dropped = dropped.clone();
                move |_, runtime| {
                    let witness = Witness(dropped);
                    drop(runtime.spawn(async move {
                        let _witness = witness;
                        std::future::pending::<()>().await;
                    }));
                    Ok(())
                }
            })
            .await
            .unwrap();
        spawned.receive().await.unwrap();
        let mut accepted = Vec::new();
        for value in 0..4 {
            accepted.push(pool.submit(0, move |_, _| Ok(value)).await.unwrap());
        }
        let shutdown = threads.shutdown();
        assert!(pool.submit(0, |_, _| Ok(0)).await.is_err());
        shutdown.await.unwrap();
        let mut values = Vec::new();
        for job in accepted {
            values.push(job.receive().await.unwrap());
        }
        assert_eq!(
            values,
            vec![0, 1, 2, 3],
            "shutdown drained every accepted job"
        );
        // Aborted tasks are dropped when the runtime next runs them.
        for _ in 0..10 {
            if dropped.load(Ordering::SeqCst) && world.live_tasks(NODE) == before {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            dropped.load(Ordering::SeqCst),
            "the job's task outlived its worker"
        );
        assert_eq!(world.live_tasks(NODE), before);
        assert_eq!(pool.scheduler().usage(), vec![(0, 0, 0)]);
    });
}

#[test]
fn sim_crash_fails_accepted_jobs_instead_of_hanging() {
    simulate(8, |world| async move {
        world.insert_ext(JobCost::fixed(ms(50)));
        let (pool, threads) = pool(1);
        let ran = Arc::new(AtomicBool::new(false));
        let running = pool
            .submit(0, {
                let ran = ran.clone();
                move |_, _| {
                    ran.store(true, Ordering::SeqCst);
                    Ok(1)
                }
            })
            .await
            .unwrap();
        // The only worker holds the job for 50 ms; the node crashes 10 ms in.
        nabedra_rt::time::sleep(ms(10)).await;
        world.crash(NODE);
        assert!(running.receive().await.is_err());
        assert!(!ran.load(Ordering::SeqCst), "a crashed worker ran its job");
        // Its reservation was released, and the dead pool refuses new work.
        assert!(pool.submit(0, |_, _| Ok(2)).await.is_err());
        assert_eq!(pool.scheduler().usage(), vec![(0, 0, 0)]);
        assert!(threads.shutdown().await.is_err());
        assert_eq!(world.live_tasks(NODE), 0);
    });
}

/// The schedule of one seeded run: each job's worker and the virtual time it ran.
fn schedule(seed: u64) -> Vec<(WorkerId, Duration)> {
    simulate(seed, |world| async move {
        world.insert_ext(JobCost::seeded(seed, ms(20)));
        let (pool, threads) = pool(3);
        let ran = run_jobs(&world, &pool, 24).await;
        threads.shutdown().await.unwrap();
        ran.into_iter().map(|(id, at, _)| (id, at)).collect()
    })
}

#[test]
fn sim_same_seed_same_schedule() {
    for seed in [11, 12, 13] {
        let first = schedule(seed);
        assert_eq!(first, schedule(seed), "seed {seed} replayed differently");
        let ids: BTreeSet<WorkerId> = first.iter().map(|(id, _)| *id).collect();
        assert!(ids.iter().all(|id| *id < 3));
    }
    assert_ne!(
        schedule(11),
        schedule(12),
        "the seed does not reach the schedule"
    );
}
