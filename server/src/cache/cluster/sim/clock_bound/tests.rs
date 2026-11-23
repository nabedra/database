use super::*;
use crate::cache::cluster::clock::{Clock, ClockMode};
use nabedra_rt::sim::{EPOCH_MS, NodeClock, in_node};

const NODE: u64 = 1;
const BASE: u64 = 1_000_000;
const DRIFT: u32 = 200;

fn update<T>(world: &World, f: impl FnOnce(&mut SimClockBound) -> T) -> T {
    world.with_ext(f).expect("the clock model is installed")
}

fn install(world: &World, nodes: &[(u64, i64)]) {
    let mut model = SimClockBound::default();
    for &(node, drift_ppm) in nodes {
        world.add_node(
            node,
            NodeClock {
                drift_ppm,
                wall_offset_nanos: 5_000_000,
                ..NodeClock::default()
            },
        );
        model.add_node(
            node,
            BoundSpec {
                base_error_ns: BASE,
                declared_drift_ppm: DRIFT,
            },
        );
    }
    world.insert_ext(model);
}

async fn sync(world: &World, node: u64, residual_ns: i64) {
    in_node(node, async {
        update(world, |model| model.resync(world, node, residual_ns)).unwrap();
    })
    .await;
}

#[test]
fn k03_clock_bound_encloses_truth_at_both_signed_drift_limits() {
    sim::run(1, |world| async move {
        let drift = i64::from(DRIFT);
        install(&world, &[(NODE, -drift), (2, drift)]);
        sync(&world, NODE, -(BASE as i64)).await;
        sync(&world, 2, BASE as i64).await;

        world.pause_all(Duration::from_nanos(3_000_000_001));
        let truth = u64::try_from(true_ns(world.elapsed())).unwrap();
        for node in [NODE, 2] {
            in_node(node, async {
                let (lower, upper) = update(&world, |model| model.sample(&world)).unwrap();
                assert!(
                    lower <= truth && truth <= upper,
                    "node {node} bound missed truth"
                );
                assert_eq!(upper - lower, 2 * (BASE + 600_001));
            })
            .await;
        }
    });
}

#[test]
fn k03_clock_bound_keeps_slow_time_covered_across_reanchors() {
    sim::run(5, |world| async move {
        let slow = -i64::from(DRIFT);
        install(&world, &[(NODE, slow)]);
        sync(&world, NODE, -(BASE as i64)).await;

        for ppm in [slow, 0, slow, 0, slow, 0] {
            world.pause_all(Duration::from_nanos(1));
            update(&world, |model| model.set_drift_ppm(&world, NODE, ppm)).unwrap();
        }
        in_node(NODE, async {
            let (lower, upper) = update(&world, |model| model.sample(&world)).unwrap();
            let truth = u64::try_from(true_ns(world.elapsed())).unwrap();
            assert!(
                lower <= truth && truth <= upper,
                "reanchored bound missed truth"
            );
        })
        .await;
    });
}

#[test]
fn k03_clock_bound_fences_authority_at_the_deadline() {
    sim::run(2, |world| async move {
        let drift = i64::from(DRIFT);
        install(&world, &[(NODE, -drift), (2, drift)]);
        sync(&world, NODE, -(BASE as i64)).await;
        sync(&world, 2, BASE as i64).await;
        let mut old = Clock::new(ClockMode::ClockBound).unwrap();
        let mut new = Clock::new(ClockMode::ClockBound).unwrap();
        let deadline = EPOCH_MS + 3_000;

        world.pause_all(Duration::from_millis(2_998));
        assert!(in_node(NODE, async { old.now().unwrap().allows(deadline) }).await);
        assert!(!in_node(2, async { new.now().unwrap().fenced(deadline) }).await);
        world.pause_all(Duration::from_millis(2));
        assert!(!in_node(NODE, async { old.now().unwrap().allows(deadline) }).await);
        assert!(in_node(2, async { new.now().unwrap().fenced(deadline) }).await);
    });
}

#[test]
fn k03_clock_bound_checks_sync_width_and_rollback_in_the_real_clock() {
    sim::run(3, |world| async move {
        install(&world, &[(NODE, -i64::from(DRIFT))]);
        in_node(NODE, async {
            let mut clock = Clock::new(ClockMode::ClockBound).unwrap();
            assert_eq!(
                clock.now().unwrap_err().to_string(),
                "ClockBound is not synchronized"
            );
            update(&world, |model| model.resync(&world, NODE, 0)).unwrap();
            assert!(clock.now().is_ok());

            world.pause_all(Duration::from_secs(20));
            assert!(clock.now().is_ok(), "10 ms is allowed");
            world.pause_all(Duration::from_nanos(1));
            assert_eq!(
                clock.now().unwrap_err().to_string(),
                "cache clock uncertainty exceeds 10 ms"
            );

            update(&world, |model| model.set_synchronized(NODE, false)).unwrap();
            assert_eq!(
                clock.now().unwrap_err().to_string(),
                "ClockBound is not synchronized"
            );
            assert!(update(&world, |model| model.resync(&world, NODE, BASE as i64 + 1)).is_err());
            assert_eq!(
                clock.now().unwrap_err().to_string(),
                "ClockBound is not synchronized"
            );
            update(&world, |model| model.resync(&world, NODE, -100)).unwrap();
            let (lower, upper) = update(&world, |model| model.sample(&world)).unwrap();
            let truth = u64::try_from(true_ns(world.elapsed())).unwrap();
            assert_eq!(lower, truth - 100 - BASE);
            assert_eq!(upper, truth - 100 + BASE);
            assert!(clock.now().is_ok());

            world.jump_wall_backward(NODE, Duration::from_secs(1));
            assert_eq!(
                clock.now().unwrap_err().to_string(),
                "cache clock moved backwards"
            );
        })
        .await;
    });
}

#[test]
fn k03_clock_bound_exposes_out_of_bound_drift() {
    sim::run(4, |world| async move {
        install(&world, &[(NODE, -10 * i64::from(DRIFT))]);
        sync(&world, NODE, 0).await;
        world.pause_all(Duration::from_secs(3));

        in_node(NODE, async {
            let (_lower, upper) = update(&world, |model| model.sample(&world)).unwrap();
            let truth = u64::try_from(true_ns(world.elapsed())).unwrap();
            assert!(
                upper < truth,
                "the model hid the clock-assumption violation"
            );
            let mut clock = Clock::new(ClockMode::ClockBound).unwrap();
            assert!(clock.now().unwrap().allows(EPOCH_MS + 3_000));
        })
        .await;
    });
}
