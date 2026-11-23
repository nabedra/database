use std::collections::BTreeMap;

use nabedra_check::Gen;

use crate::nemesis::{Nemesis, Profile, Timeline};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Act {
    Calm,
    Crash,
    Partition,
    Restart(u64),
}

fn profile() -> Profile<Act> {
    Profile::new("mixed", (100, 400))
        .with(Act::Calm, 2)
        .with(Act::Crash, 1)
        .with(Act::Partition, 1)
        .with(Act::Restart(0), 0)
}

#[test]
fn timeline_returns_due_actions_in_time_then_insertion_order() {
    let mut timeline = Timeline::new();
    timeline.push(30, "c");
    timeline.push(10, "a");
    timeline.push(10, "b");
    timeline.push(50, "d");
    assert_eq!(timeline.next_at(), Some(10));
    assert_eq!(timeline.due(9), Vec::<&str>::new());
    assert_eq!(timeline.due(30), ["a", "b", "c"]);
    assert_eq!(timeline.len(), 1);
    timeline.push(u64::MAX, "e");
    assert_eq!(timeline.due(u64::MAX), ["d", "e"]);
    assert!(timeline.is_empty());
    timeline.push(5, "f");
    assert_eq!(timeline.drain(), ["f"]);
}

#[test]
fn picks_follow_the_weights_and_zero_weights_are_left_out() {
    let profile = profile();
    assert_eq!(profile.total_weight(), 4);
    assert_eq!(profile.actions().count(), 3);
    let mut g = Gen::from_seed(11);
    let mut counts = BTreeMap::new();
    for _ in 0..8_000 {
        *counts.entry(profile.pick(&mut g)).or_insert(0) += 1;
    }
    assert_eq!(counts.len(), 3);
    assert!((3_700..4_300).contains(&counts[&Act::Calm]), "{counts:?}");
    assert!((1_800..2_200).contains(&counts[&Act::Crash]), "{counts:?}");
    // The minimal choice is the first, mildest action, and the shortest gap.
    let mut minimal = Gen::from_tape(Vec::new());
    assert_eq!(profile.pick(&mut minimal), Act::Calm);
    assert_eq!(profile.gap(&mut minimal), 100);
}

#[test]
fn profiles_cycle_through_seeds() {
    let profiles = [
        Profile::new("a", (1, 1)).with(Act::Calm, 1),
        Profile::new("b", (1, 1)).with(Act::Crash, 1),
    ];
    let names: Vec<_> = (0..4)
        .map(|seed| Profile::round_robin(&profiles, seed).name)
        .collect();
    assert_eq!(names, ["a", "b", "a", "b"]);
}

/// Drives a nemesis for `ms` virtual milliseconds in 10 ms steps.
fn drive(g: &mut Gen, ms: u64) -> Vec<(u64, Act)> {
    let mut nemesis = Nemesis::new(profile(), 0, g);
    let mut log = Vec::new();
    let mut now = 0;
    while now < ms {
        now += 10;
        for action in nemesis.poll(now, g) {
            if action == Act::Crash {
                nemesis.after(now + 250, Act::Restart(now));
            }
            log.push((now, action));
        }
    }
    for action in nemesis.stop() {
        log.push((now, action));
    }
    assert!(nemesis.is_stopped());
    assert_eq!(nemesis.pending_follow_ups(), 0);
    assert!(nemesis.poll(now + 10_000, g).is_empty());
    log
}

#[test]
fn the_schedule_is_a_function_of_the_tape() {
    let mut a = Gen::from_seed(5);
    let mut b = Gen::from_seed(5);
    let first = drive(&mut a, 10_000);
    assert_eq!(first, drive(&mut b, 10_000));
    // Between 25 and 100 picks in 10 s with gaps of 100 to 400 ms.
    let picked = first
        .iter()
        .filter(|(_, act)| !matches!(act, Act::Restart(_)))
        .count();
    assert!((25..=100).contains(&picked), "{picked}");
    // Every crash is followed by its restart, at the scheduled time or at the stop.
    let crashes: Vec<u64> = first
        .iter()
        .filter(|(_, act)| *act == Act::Crash)
        .map(|(at, _)| *at)
        .collect();
    let restarts: Vec<u64> = first
        .iter()
        .filter_map(|(_, act)| match act {
            Act::Restart(at) => Some(*at),
            _ => None,
        })
        .collect();
    assert_eq!(crashes, restarts);
    // Replaying the recorded tape gives the same schedule.
    let tape = a.into_tape();
    assert_eq!(drive(&mut Gen::from_tape(tape), 10_000), first);
    assert_ne!(drive(&mut Gen::from_seed(6), 10_000), first);
}
