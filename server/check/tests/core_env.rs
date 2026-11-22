//! `NABEDRA_SEED` and `NABEDRA_CASES` replay. One test per binary, because it sets process
//! environment variables.

use std::panic::{self, AssertUnwindSafe};

use nabedra_check::Gen;
use nabedra_check::prop::{self, Config};

fn property(g: &mut Gen) -> Result<(), String> {
    let v = g.vec(8, |g| g.below(50));
    if v.iter().filter(|&&x| x > 40).count() >= 2 { Err(format!("two values over 40: {v:?}")) } else { Ok(()) }
}

fn failure(f: impl FnOnce()) -> String {
    let p = panic::catch_unwind(AssertUnwindSafe(f)).expect_err("the property fails");
    p.downcast_ref::<String>().cloned().unwrap()
}

fn field<'a>(msg: &'a str, key: &str) -> &'a str {
    msg.lines().find_map(|l| l.trim().strip_prefix(key)).unwrap()
}

#[test]
fn env_seed_replays_the_reported_failure() {
    // SAFETY: this binary has one test, so no other thread reads the environment concurrently.
    unsafe {
        std::env::remove_var("NABEDRA_SEED");
        std::env::set_var("NABEDRA_CASES", "300");
    }
    let defaults = Config::default();
    assert_eq!((defaults.cases, defaults.seed), (300, None));

    // Default base seed: the failure is found at some case and reported with its own seed.
    let first = failure(|| prop::check("env-replay", property));
    let replay = field(&first, "replay: ");
    let seed = replay.strip_prefix("NABEDRA_SEED=").unwrap().strip_suffix(" NABEDRA_CASES=1").unwrap();
    assert!(!field(&first, "case: ").starts_with("0 "), "pick a property that does not fail on case 0:\n{first}");

    // Following the replay line runs exactly that case first and reproduces the same failure.
    unsafe {
        std::env::set_var("NABEDRA_SEED", seed);
        std::env::set_var("NABEDRA_CASES", "1");
    }
    let again = failure(|| prop::check("env-replay", property));
    assert_eq!(field(&again, "case: "), "0 of 1", "{again}");
    assert_eq!(field(&again, "seed: "), field(&first, "seed: "));
    assert_eq!(field(&again, "failure: "), field(&first, "failure: "));
    assert_eq!(field(&again, "shrunk tape length: "), field(&first, "shrunk tape length: "));

    // Hex works too, and a different seed does not reproduce it as case 0.
    let hex = format!("{:#x}", seed.parse::<u64>().unwrap());
    unsafe { std::env::set_var("NABEDRA_SEED", &hex) };
    assert_eq!(field(&failure(|| prop::check("env-replay", property)), "seed: "), field(&first, "seed: "));

    unsafe { std::env::set_var("NABEDRA_SEED", "not-a-seed") };
    let bad = panic::catch_unwind(Config::default).expect_err("an unparsable seed panics");
    assert!(bad.downcast_ref::<String>().unwrap().contains("NABEDRA_SEED"));
    unsafe {
        std::env::remove_var("NABEDRA_SEED");
        std::env::remove_var("NABEDRA_CASES");
    }
}
