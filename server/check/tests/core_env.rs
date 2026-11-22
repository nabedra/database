//! `NABEDRA_SEED` and `NABEDRA_CASES` replay in a fresh process for each environment.

use std::process::Command;

use nabedra_check::Gen;
use nabedra_check::prop;

fn property(g: &mut Gen) -> Result<(), String> {
    let v = g.vec(8, |g| g.below(50));
    if v.iter().filter(|&&x| x > 40).count() >= 2 { Err(format!("two values over 40: {v:?}")) } else { Ok(()) }
}

fn failure(seed: Option<&str>, cases: &str) -> String {
    let mut child = Command::new(std::env::current_exe().unwrap());
    child.args(["--exact", "env_seed_replays_the_reported_failure", "--nocapture"])
        .env("NABEDRA_CHECK_REPLAY_CHILD", "1")
        .env("NABEDRA_CASES", cases);
    match seed {
        Some(seed) => child.env("NABEDRA_SEED", seed),
        None => child.env_remove("NABEDRA_SEED"),
    };
    let out = child.output().unwrap();
    assert_eq!(out.status.code(), Some(101), "{out:?}");
    String::from_utf8(out.stderr).unwrap()
}

fn field<'a>(msg: &'a str, key: &str) -> &'a str {
    msg.lines().find_map(|l| l.trim().strip_prefix(key)).unwrap()
}

#[test]
fn env_seed_replays_the_reported_failure() {
    if std::env::var_os("NABEDRA_CHECK_REPLAY_CHILD").is_some() {
        prop::check("env-replay", property);
        return;
    }

    // Default base seed: the failure is found at some case and reported with its own seed.
    let first = failure(None, "300");
    let replay = field(&first, "replay: ");
    let seed = replay.strip_prefix("NABEDRA_SEED=").unwrap().strip_suffix(" NABEDRA_CASES=1").unwrap();
    assert!(!field(&first, "case: ").starts_with("0 "), "pick a property that does not fail on case 0:\n{first}");
    assert!(field(&first, "case: ").ends_with(" of 300"), "{first}");

    // Following the replay line runs exactly that case first and reproduces the same failure.
    let again = failure(Some(seed), "1");
    assert_eq!(field(&again, "case: "), "0 of 1", "{again}");
    assert_eq!(field(&again, "seed: "), field(&first, "seed: "));
    assert_eq!(field(&again, "failure: "), field(&first, "failure: "));
    assert_eq!(field(&again, "shrunk tape length: "), field(&first, "shrunk tape length: "));

    // The same seed also replays in hex.
    let hex = format!("{:#x}", seed.parse::<u64>().unwrap());
    assert_eq!(field(&failure(Some(&hex), "1"), "seed: "), field(&first, "seed: "));
    assert!(failure(Some("not-a-seed"), "1").contains("NABEDRA_SEED=\"not-a-seed\" is not a u64"));
}
