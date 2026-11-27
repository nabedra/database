//! `NABEDRA_SEED` and `NABEDRA_CASES` replay in a fresh process for each environment.

use std::process::Command;

use nabedra_check::Gen;
use nabedra_check::evidence::Record;
use nabedra_check::prop;

fn property(g: &mut Gen) -> Result<(), String> {
    let v = g.vec(8, |g| g.below(50));
    if v.iter().filter(|&&x| x > 40).count() >= 2 {
        Err(format!("two values over 40: {v:?}"))
    } else {
        Ok(())
    }
}

fn failure(seed: Option<&str>, cases: &str) -> String {
    let mut child = Command::new(std::env::current_exe().unwrap());
    child
        .args([
            "--exact",
            "env_seed_replays_the_reported_failure",
            "--nocapture",
        ])
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
    msg.lines()
        .find_map(|l| l.trim().strip_prefix(key))
        .unwrap()
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
    let seed = replay
        .strip_prefix("NABEDRA_SEED=")
        .unwrap()
        .strip_suffix(" NABEDRA_CASES=1")
        .unwrap();
    assert!(
        !field(&first, "case: ").starts_with("0 "),
        "pick a property that does not fail on case 0:\n{first}"
    );
    assert!(field(&first, "case: ").ends_with(" of 300"), "{first}");

    // Following the replay line runs exactly that case first and reproduces the same failure.
    let again = failure(Some(seed), "1");
    assert_eq!(field(&again, "case: "), "0 of 1", "{again}");
    assert_eq!(field(&again, "seed: "), field(&first, "seed: "));
    assert_eq!(field(&again, "failure: "), field(&first, "failure: "));
    assert_eq!(
        field(&again, "shrunk tape length: "),
        field(&first, "shrunk tape length: ")
    );

    // The same seed also replays in hex.
    let hex = format!("{:#x}", seed.parse::<u64>().unwrap());
    assert_eq!(
        field(&failure(Some(&hex), "1"), "seed: "),
        field(&first, "seed: ")
    );
    assert!(failure(Some("not-a-seed"), "1").contains("NABEDRA_SEED=\"not-a-seed\" is not a u64"));
}

#[test]
fn evidence_uses_the_tested_revision() {
    if let Some(expected) = std::env::var_os("NABEDRA_CHECK_EXPECTED_SHA") {
        assert_eq!(
            Record::new("check-core", "U").fill_from_env().sha,
            expected.to_str().unwrap()
        );
        return;
    }

    let head = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    assert!(head.status.success());
    let head = String::from_utf8(head.stdout).unwrap();
    let checkout = std::env::current_dir().unwrap();
    let outside_checkout = std::env::temp_dir();
    let caller_sha = "a".repeat(40);
    let tested_sha = "b".repeat(40);
    for (cwd, pinned, expected) in [
        (&checkout, Some(tested_sha.as_str()), tested_sha.as_str()),
        (&checkout, None, head.trim()),
        (&outside_checkout, None, caller_sha.as_str()),
    ] {
        let mut child = Command::new(std::env::current_exe().unwrap());
        child
            .args(["--exact", "evidence_uses_the_tested_revision"])
            .current_dir(cwd)
            .env("GITHUB_SHA", &caller_sha)
            .env("NABEDRA_CHECK_EXPECTED_SHA", expected)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE");
        match pinned {
            Some(sha) => child.env("TESTED_SHA", sha),
            None => child.env_remove("TESTED_SHA"),
        };
        let out = child.output().unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stdout)
        );
    }
}
