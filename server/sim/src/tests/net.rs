use std::collections::BTreeSet;

use nabedra_check::Gen;

use crate::net::{Filters, NetPlan, Plan, draw_partition};

#[test]
fn a_healthy_network_delivers_everything_after_the_base_delay() {
    let mut net = NetPlan::new(5);
    for _ in 0..100 {
        assert_eq!(
            net.plan(1, 2),
            Plan::Deliver {
                delay_ms: 1,
                duplicate: false
            }
        );
    }
    assert_eq!(net.messages(), 100);
}

#[test]
fn blocks_are_directed_and_isolation_and_partitions_are_symmetric() {
    let mut net = NetPlan::new(1);
    net.block(1, 2);
    assert_eq!(net.plan(1, 2), Plan::Drop);
    assert_ne!(net.plan(2, 1), Plan::Drop);
    assert!(!net.connected(1, 2));
    net.unblock(1, 2);
    assert!(net.connected(1, 2));

    net.isolate(2, [1, 2, 3]);
    for other in [1, 3] {
        assert!(net.is_blocked(2, other) && net.is_blocked(other, 2));
    }
    assert!(net.connected(1, 3));
    assert!(!net.is_blocked(2, 2));
    net.heal_links();
    assert_eq!(net.blocked().count(), 0);

    let sides = [BTreeSet::from([1, 2]), BTreeSet::from([3, 4, 5])];
    net.partition(&sides);
    assert_eq!(net.blocked().count(), 2 * 2 * 3);
    assert!(net.connected(1, 2) && net.connected(3, 5));
    assert!(!net.connected(2, 4));
}

#[test]
fn quorum_counts_only_reachable_usable_unpaused_nodes() {
    let nodes = [1, 2, 3, 4, 5];
    let mut net = NetPlan::new(1);
    assert!(net.has_quorum(1, &nodes, |_| true));
    net.partition(&[BTreeSet::from([1, 2]), BTreeSet::from([3, 4, 5])]);
    assert!(!net.has_quorum(1, &nodes, |_| true));
    assert!(net.has_quorum(3, &nodes, |_| true));
    assert!(!net.has_quorum(3, &nodes, |id| id != 5));
    net.pause(4);
    assert!(net.is_paused(4));
    assert!(!net.has_quorum(3, &nodes, |_| true));
    assert!(!net.has_quorum(4, &nodes, |_| true));
    net.resume(4);
    assert!(net.has_quorum(3, &nodes, |_| true));
}

#[test]
fn every_change_bumps_the_epoch_and_heal_all_clears_everything() {
    let mut net = NetPlan::new(1);
    let mut epoch = net.epoch();
    let mut changed = |net: &NetPlan| {
        assert!(net.epoch() > epoch);
        epoch = net.epoch();
    };
    net.block(1, 2);
    changed(&net);
    net.isolate(3, [1, 2, 3]);
    changed(&net);
    net.pause(2);
    changed(&net);
    net.bump_epoch();
    changed(&net);
    net.set_filters(Filters {
        drop_per_mille: 50,
        dup_per_mille: 10,
        delay_max_ms: 5,
    });
    net.heal_all();
    changed(&net);
    assert_eq!(net.blocked().count(), 0);
    assert!(!net.is_paused(2));
    assert!(net.filters().is_clear());
}

#[test]
fn drops_duplicates_and_delays_follow_the_filters() {
    let mut net = NetPlan::new(9);
    net.set_filters(Filters {
        drop_per_mille: 150,
        dup_per_mille: 50,
        delay_max_ms: 30,
    });
    let (mut drops, mut dups, mut max_delay) = (0, 0, 0);
    for _ in 0..10_000 {
        match net.plan(1, 2) {
            Plan::Drop => drops += 1,
            Plan::Deliver {
                delay_ms,
                duplicate,
            } => {
                assert!((1..=31).contains(&delay_ms));
                max_delay = max_delay.max(delay_ms);
                dups += u32::from(duplicate);
            }
        }
    }
    assert!((1_200..1_800).contains(&drops), "{drops} drops");
    assert!((300..600).contains(&dups), "{dups} duplicates");
    assert_eq!(max_delay, 31);
}

#[test]
fn drawn_filters_and_partitions_come_from_the_tape_and_shrink() {
    let mut g = Gen::from_seed(3);
    let mut seen = BTreeSet::new();
    for _ in 0..200 {
        seen.insert(format!("{:?}", Filters::draw(&mut g)));
    }
    assert!(seen.len() > 20);
    // The minimal tape gives no faults and isolates the first node.
    let mut minimal = Gen::from_tape(Vec::new());
    assert!(Filters::draw(&mut minimal).is_clear());
    assert_eq!(
        draw_partition(&mut minimal, &[1, 2, 3]),
        vec![BTreeSet::from([2, 3]), BTreeSet::from([1])]
    );
    for seed in 0..200 {
        let mut g = Gen::from_seed(seed);
        let sides = draw_partition(&mut g, &[1, 2, 3, 4, 5]);
        assert_eq!(sides.len(), 2);
        assert!(sides.iter().all(|side| !side.is_empty()));
        assert_eq!(sides.iter().map(BTreeSet::len).sum::<usize>(), 5);
        // Replaying the tape gives the same partition.
        let tape = g.into_tape();
        assert_eq!(
            draw_partition(&mut Gen::from_tape(tape), &[1, 2, 3, 4, 5]),
            sides
        );
    }
}
