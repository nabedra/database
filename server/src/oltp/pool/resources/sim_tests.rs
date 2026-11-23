use super::super::{OltpPool, POOL_FORMAT_VERSION, PoolIdentity, tests::group_identity};
use super::*;
use nabedra_rt::{
    sim::{self, Cut, NodeClock, World},
    time::{sleep, timeout},
};
use nabedra_sim::trace::Trace;
use std::time::Duration;

const NODE: u64 = 1;
const DATA: &str = "/sim/native/data";
const TASK_BYTES: u64 = 1024;
type SharedTrace = Arc<Mutex<Trace>>;

fn open() -> OltpPool {
    let resources = WorkerSharedResources::new(
        NODE,
        WorkerResources::default(),
        SharedResourceConfig {
            query_threads: 2,
            control_threads: 1,
            ..Default::default()
        },
    )
    .unwrap();
    OltpPool::open_development_on(
        DATA,
        PoolIdentity {
            engine: "oltp".into(),
            pool_format_version: POOL_FORMAT_VERSION,
            cluster_id: Uuid::from_u128(1),
            worker_id: Uuid::from_u128(2),
            pool_id: Uuid::from_u128(3),
        },
        resources,
        nabedra_rt::fs::real(),
    )
    .unwrap()
}

fn replay(test: &str, run: impl AsyncFn(Arc<World>, SharedTrace)) {
    for seed in 41..46 {
        let run = &run;
        let once = || {
            sim::run(seed, |world| async move {
                world.add_node(NODE, NodeClock::default());
                world.mount(NODE, DATA);
                let trace = Arc::new(Mutex::new(Trace::new()));
                world.observe(Arc::new({
                    let trace = trace.clone();
                    move |event| trace.lock().unwrap().push(format!("{event:?}"))
                }));
                timeout(Duration::from_secs(5), run(world.clone(), trace.clone()))
                    .await
                    .expect("native executor fixture timed out");
                assert_eq!(world.unmounted_path_errors(), 0);
                let trace = trace.lock().unwrap();
                (trace.hash(), trace.events())
            })
        };
        let first = once();
        assert!(first.1 > 0);
        assert_eq!(first, once(), "{test}, seed {seed}");
        eprintln!(
            "fault model: {test}, seed {seed}, trace {:016x}, {} events, replay matched",
            first.0, first.1
        );
    }
}

async fn assert_node_stops(world: &World) {
    while world.live_tasks(NODE) != 0 {
        sleep(Duration::from_millis(1)).await;
    }
    assert!(world.services(NODE).is_empty());
}

#[test]
fn c04_c06_f01_f02_native_slots_refill_and_isolate_failures() {
    replay(
        "native slot refill and failure isolation",
        async |world, trace| {
            sim::in_node(NODE, async {
                let pool = open();
                let resources = pool.resources();
                assert!(world.services(NODE).is_empty(), "native OS threads started");
                let baseline = resources.scheduler().usage();
                let first = resources.prepare(ExecutionClass::Query).await.unwrap();
                let first_compute = first.reserve_compute(1, TASK_BYTES).await.unwrap();
                let second = resources.prepare(ExecutionClass::Query).await.unwrap();
                let second_compute = second.reserve_compute(2, TASK_BYTES).await.unwrap();
                assert_eq!(
                    resources.scheduler().usage(),
                    [(NODE, 2, baseline[0].2 + 2 * TASK_BYTES)]
                );
                let mut refill = std::pin::pin!(resources.prepare(ExecutionClass::Query));
                assert!(futures_util::poll!(&mut refill).is_pending());

                let control = resources.prepare(ExecutionClass::Control).await.unwrap();
                assert!(
                    futures_util::poll!(std::pin::pin!(resources.prepare(ExecutionClass::Control)))
                        .is_pending()
                );
                drop(control);

                // The actual pool path runs a sync journal commit on Control while
                // both Query slots are reserved and a third admission is waiting.
                let identity = group_identity(&pool);
                let group = pool.group(identity.clone()).await.unwrap();
                assert_eq!(group.identity(), &identity);
                drop(group);

                let ordinary = capacity();
                let (result, second_compute) = second
                    .run_with_guard(second_compute, {
                        let ordinary = ordinary.clone();
                        move || Err::<(), _>(ordinary)
                    })
                    .await
                    .unwrap();
                assert_eq!(result, Err(ordinary));
                drop(second_compute);

                let refilled = refill.await.unwrap();
                assert_eq!(
                    refilled
                        .execute(3, TASK_BYTES, || -> Result<(), ProtocolError> {
                            panic!("simulated native work panic");
                        })
                        .await,
                    Err(crate::oltp::recovery_unknown())
                );
                assert_eq!(
                    resources.scheduler().usage(),
                    [(NODE, 1, baseline[0].2 + TASK_BYTES)]
                );

                let (node, draw) = resources
                    .execute(ExecutionClass::Query, 4, TASK_BYTES, || {
                        Ok((sim::current_node(), nabedra_rt::ids::random_u64()))
                    })
                    .await
                    .unwrap();
                assert_eq!(node, Some(NODE));
                trace
                    .lock()
                    .unwrap()
                    .push(format!("query on {node:?}, draw={draw}"));
                drop((first_compute, first));
                assert_eq!(resources.scheduler().usage(), baseline);
                resources.drain().await;
            })
            .await;
            assert_node_stops(&world).await;
        },
    );
}

#[test]
fn f03_native_drain_keeps_cancelled_work_until_settlement() {
    replay("native orderly drain", async |world, trace| {
        sim::in_node(NODE, async {
            let pool = open();
            let resources = pool.resources();
            let baseline = resources.scheduler().usage();
            world.freeze(NODE);
            let ran = Arc::new(AtomicBool::new(false));
            let mut job = Box::pin(resources.execute(ExecutionClass::Query, 1, TASK_BYTES, {
                let ran = ran.clone();
                move || {
                    trace.lock().unwrap().push(format!(
                        "cancelled caller, job on {:?}",
                        sim::current_node()
                    ));
                    ran.store(true, Ordering::Relaxed);
                    Ok(())
                }
            }));
            assert!(futures_util::poll!(&mut job).is_pending());
            drop(job);
            assert_eq!(resources.query.slots.available_permits(), 1);
            assert_eq!(
                resources.scheduler().usage(),
                [(NODE, 1, baseline[0].2 + TASK_BYTES)]
            );

            let mut drain = std::pin::pin!(resources.drain());
            assert!(futures_util::poll!(&mut drain).is_pending());
            assert!(!ran.load(Ordering::Relaxed));
            assert!(resources.prepare(ExecutionClass::Control).await.is_err());
            world.thaw(NODE);
            drain.await;
            assert!(ran.load(Ordering::Relaxed));
            assert_eq!(resources.scheduler().usage(), baseline);
            assert_eq!(
                resources.query.slots.available_permits(),
                2,
                "cancelled job leaked a native slot"
            );
            assert_eq!(resources.control.slots.available_permits(), 1);
        })
        .await;
        assert_node_stops(&world).await;
    });
}

#[test]
fn f03_native_crash_drops_queued_work_and_releases_accounting() {
    replay("native crash and restart", async |world, trace| {
        let (pool, identity) = sim::in_node(NODE, async {
            let pool = open();
            let identity = group_identity(&pool);
            assert_eq!(
                pool.group(identity.clone()).await.unwrap().identity(),
                &identity
            );
            (pool, identity)
        })
        .await;
        let resources = pool.resources();
        let baseline = resources.scheduler().usage();

        // Submit from the harness so the result futures survive the worker
        // crash. The executor's receiving tasks must still belong to NODE.
        world.freeze(NODE);
        let ran = Arc::new(AtomicU64::new(0));
        let work = || {
            let ran = ran.clone();
            move || Ok(ran.fetch_add(1, Ordering::Relaxed))
        };
        {
            let mut dropped =
                Box::pin(resources.execute(ExecutionClass::Query, 1, TASK_BYTES, work()));
            let mut query =
                std::pin::pin!(resources.execute(ExecutionClass::Query, 2, TASK_BYTES, work()));
            let mut control =
                std::pin::pin!(resources.execute(ExecutionClass::Control, 0, 0, work()));
            assert!(futures_util::poll!(&mut dropped).is_pending());
            assert!(futures_util::poll!(&mut query).is_pending());
            assert!(futures_util::poll!(&mut control).is_pending());
            drop(dropped);
            assert_eq!(resources.query.slots.available_permits(), 0);
            assert_eq!(resources.control.slots.available_permits(), 0);
            assert_eq!(
                resources.scheduler().usage(),
                [(NODE, 2, baseline[0].2 + 2 * TASK_BYTES)]
            );
            world.crash(NODE);
            assert_eq!(query.await, Err(crate::oltp::recovery_unknown()));
            assert_eq!(control.await, Err(crate::oltp::recovery_unknown()));
        }

        resources.drain().await;
        assert_eq!(resources.scheduler().usage(), baseline);
        assert_eq!(resources.query.slots.available_permits(), 2);
        assert_eq!(resources.control.slots.available_permits(), 1);
        sim::in_node(NODE, async { drop(pool) }).await;

        world.restart_with(NODE, Cut::Crash);
        sim::in_node(NODE, async {
            let pool = open();
            let group = pool.retained_group(identity.clone()).await.unwrap();
            assert_eq!(group.identity(), &identity);
            trace
                .lock()
                .unwrap()
                .push("committed group present after restart".into());
        })
        .await;
        assert_node_stops(&world).await;
        assert_eq!(ran.load(Ordering::Relaxed), 0, "an outstanding job ran");
    });
}
