//! A controller-state/worker-actor K03 fault model for a paced authority handoff.

use std::time::Duration;

use nabedra_rt::sim::{self, EPOCH_MS, NodeClock, in_node};

use super::super::{
    clock::{Clock, ClockMode},
    controller::tests::LeaseControllerFixture,
    protocol::LEASE_MS,
    worker::LeaseActorFixture,
};
use super::clock_bound::{BoundSpec, SimClockBound};
use crate::cache::{command::key_slot, wire::Response};

const CONTROLLER: u64 = 99;
const KEY: &[u8] = b"{k03-lease}";
const ERROR_NS: u64 = 1_000_000;
const DECLARED_PPM: u32 = 200;

#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    old_clock_covered_truth: bool,
    published_epochs: Vec<u64>,
}

fn handoff(old_drift_ppm: i64) -> Outcome {
    sim::run(41, |world| async move {
        let mut controller = LeaseControllerFixture::new();
        let slot = key_slot(KEY);
        let index = usize::from(slot);
        let initial = controller.topology().slots[index];
        let old_id = initial.primary.expect("initial primary");
        let next_id = initial.replica.expect("initial replica");
        let fast = i64::from(DECLARED_PPM);
        let clocks = [
            (CONTROLLER, fast, ERROR_NS as i64),
            (old_id.node, old_drift_ppm, -(ERROR_NS as i64)),
            (next_id.node, fast, ERROR_NS as i64),
        ];
        let mut model = SimClockBound::default();
        for (node, ppm, _) in clocks {
            world.add_node(
                node,
                NodeClock {
                    drift_ppm: ppm,
                    ..NodeClock::default()
                },
            );
            model.add_node(
                node,
                BoundSpec {
                    base_error_ns: ERROR_NS,
                    declared_drift_ppm: DECLARED_PPM,
                },
            );
        }
        world.insert_ext(model);
        for (node, _, residual) in clocks {
            in_node(node, async {
                world
                    .with_ext::<SimClockBound, _>(|model| model.resync(&world, node, residual))
                    .unwrap()
                    .unwrap();
            })
            .await;
        }

        let mut authority = Clock::new(ClockMode::ClockBound).unwrap();
        let mut old = in_node(old_id.node, async { LeaseActorFixture::new(old_id) }).await;
        let mut next = in_node(next_id.node, async { LeaseActorFixture::new(next_id) }).await;
        let initial_time = in_node(CONTROLLER, async { authority.now().unwrap() }).await;
        let deadline = initial_time.lower_ms + LEASE_MS;
        let grant = controller.renew(old_id, initial_time);
        assert_eq!(
            controller.topology().slots[index].lease.unwrap().expires_ms,
            deadline
        );
        let old_progress = in_node(old_id.node, async {
            old.install_update(grant);
            assert!(matches!(old.write(KEY, b"before handoff", initial.epoch), Response::Simple(ref value) if &value[..] == b"OK"));
            old.progress(slot)
        }).await;
        assert!(old_progress.serving_primary);
        assert_eq!(old_progress.sequence, 1);
        controller.record(old_id, initial_time, old_progress);
        in_node(next_id.node, async {
            next.install_topology(controller.topology().clone());
            next.copy_first_write_from(&old, slot, initial.epoch);
        })
        .await;

        // Do not run the old actor or refresh its heartbeat during the pause.
        world.pause_all(Duration::from_millis(LEASE_MS - 1));
        let before = in_node(CONTROLLER, async { authority.now().unwrap() }).await;
        assert!(before.lower_ms < deadline);
        let report = in_node(next_id.node, async { next.progress(slot) }).await;
        controller.record(next_id, before, report);
        controller.reconcile(before);
        assert_eq!(controller.topology().slots[index].primary, Some(old_id));
        assert_eq!(controller.topology().slots[index].epoch, initial.epoch);

        world.pause_all(Duration::from_millis(1));
        let now = in_node(CONTROLLER, async { authority.now().unwrap() }).await;
        assert_eq!(now.lower_ms, deadline);
        let report = in_node(next_id.node, async { next.progress(slot) }).await;
        controller.record(next_id, now, report);
        controller.reconcile(now);
        let promoted = controller.topology().slots[index];
        assert_eq!(promoted.primary, Some(next_id));
        assert_eq!(promoted.epoch, initial.epoch + 1);
        assert_eq!(promoted.lease.unwrap().not_before_ms, deadline);

        let handoff_instant = world.elapsed();
        let grant = controller.renew(next_id, now);
        in_node(next_id.node, async {
            next.install_update(grant);
            assert!(next.progress(slot).baseline);
            assert!(matches!(next.write(KEY, b"successor write", promoted.epoch), Response::Simple(ref value) if &value[..] == b"OK"));
            assert_eq!(next.progress(slot).sequence, 1);
        }).await;
        let mut published_epochs = vec![promoted.epoch];

        // Resume a stale primary which still holds the exact originally committed lease.
        // Both writes are attempted at the same true instant, without installing a newer
        // topology on the stale actor that would fence it before the clock is checked.
        let true_time =
            u64::try_from(u128::from(EPOCH_MS) * 1_000_000 + handoff_instant.as_nanos()).unwrap();
        let (clock_covered, old_reply, old_sequence) = in_node(old_id.node, async {
            let (lower, upper) = world
                .with_ext::<SimClockBound, _>(|model| model.sample(&world))
                .unwrap()
                .unwrap();
            let reply = old.write(KEY, b"stale primary write", initial.epoch);
            (
                lower <= true_time && true_time <= upper,
                reply,
                old.progress(slot).sequence,
            )
        })
        .await;
        assert_eq!(world.elapsed(), handoff_instant);
        match old_reply {
            Response::Simple(value) => {
                assert_eq!(&value[..], b"OK");
                assert_eq!(old_sequence, 2);
                published_epochs.push(initial.epoch);
            }
            Response::Error(message) => {
                assert_eq!(
                    &message[..],
                    b"CLUSTERDOWN cache slot has no valid authority"
                );
                assert_eq!(old_sequence, 1, "the expired primary did not publish");
            }
            _ => panic!("unexpected reply from the old primary"),
        }

        Outcome {
            old_clock_covered_truth: clock_covered,
            published_epochs,
        }
    })
}

#[test]
fn k03_actor_handoff_fences_the_paused_primary_with_in_bound_drift() {
    let outcome = handoff(-i64::from(DECLARED_PPM));
    assert!(outcome.old_clock_covered_truth);
    assert_eq!(
        outcome.published_epochs.len(),
        1,
        "hand-off overlap: {outcome:?}"
    );
}

#[test]
fn k03_actor_handoff_exposes_excessive_slow_drift() {
    let outcome = handoff(-100 * i64::from(DECLARED_PPM));
    assert!(!outcome.old_clock_covered_truth);
    assert_eq!(
        outcome.published_epochs.len(),
        2,
        "out-of-bound drift must hit both actors: {outcome:?}"
    );
    assert_eq!(outcome.published_epochs[0], outcome.published_epochs[1] + 1);
}
