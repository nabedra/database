use std::collections::BTreeSet;

use nabedra_check::Gen;

use crate::shrink::{kinds, read_tape, shrink_tape, violation_kind, write_tape};

#[test]
fn violation_kinds_drop_the_time_prefix_and_the_detail() {
    assert_eq!(
        violation_kind("t=120ms MonotonicEpoch: n1 reports"),
        "MonotonicEpoch"
    );
    assert_eq!(violation_kind("harness: panic: boom"), "harness");
    assert_eq!(violation_kind("Liveness"), "Liveness");
    assert_eq!(violation_kind("t=oops: x"), "t=oops");
    assert_eq!(
        kinds(&["t=1ms A: x", "t=2ms B: y", "t=3ms A: z"]),
        BTreeSet::from(["A".to_owned(), "B".to_owned()])
    );
}

/// A "run" that fails when it draws a value of at least 900 in its first 20 draws, and
/// otherwise reports an unrelated violation when the tape starts with 7.
fn run(g: &mut Gen) -> Vec<String> {
    let draws: Vec<u64> = (0..20).map(|_| g.below(1000)).collect();
    if draws.iter().any(|d| *d >= 900) {
        vec![format!("t=5ms Big: {draws:?}")]
    } else if draws[0] == 7 {
        vec!["t=5ms Other: seven".into()]
    } else {
        Vec::new()
    }
}

#[test]
fn shrinking_keeps_the_original_kind_and_minimises_the_tape() {
    let mut seed = 0;
    let (tape, violations) = loop {
        let mut g = Gen::from_seed(seed);
        let violations = run(&mut g);
        if !violations.is_empty() && violation_kind(&violations[0]) == "Big" {
            break (g.into_tape(), violations);
        }
        seed += 1;
    };
    let shrunk = shrink_tape(tape.clone(), &kinds(&violations), 200, run);
    assert!(shrunk.steps <= 200);
    assert!(shrunk.message.contains("Big"), "{}", shrunk.message);
    // The smallest failing tape: nineteen zeros and one 900, or a shorter prefix of it.
    assert!(
        shrunk.tape.iter().filter(|&&c| c != 0).count() == 1,
        "{:?}",
        shrunk.tape
    );
    assert!(shrunk.tape.contains(&900), "{:?}", shrunk.tape);
    assert!(run(&mut Gen::from_tape(shrunk.tape.clone()))[0].contains("Big"));

    // A candidate that only shows another kind is not kept.
    let only_other = shrink_tape(vec![7, 0, 0], &BTreeSet::from(["Big".to_owned()]), 50, run);
    assert_eq!(only_other.tape, vec![7, 0, 0]);
}

#[test]
fn tapes_round_trip_through_files() {
    let path = std::env::temp_dir().join(format!("nabedra-sim-unit-{}.tape", std::process::id()));
    let tape = vec![0, 1, u64::MAX, 42];
    write_tape(&path, &tape).unwrap();
    assert_eq!(read_tape(&path).unwrap(), tape);
    std::fs::write(&path, b"not json").unwrap();
    assert!(read_tape(&path).is_err());
    let _ = std::fs::remove_file(&path);
}
