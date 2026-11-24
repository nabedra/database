//! What the writer writes, what it refuses, and what is left on disk when it refuses.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use nabedra_rt::sim::{Cut, NodeClock, SimEvent, StepOutcome, Wipe};
use serde_json::json;

use super::*;
use crate::net::{Filters, NetPlan};
use crate::runtime;

/// What a test makes fail, on its own thread.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Fail {
    /// Every write, as on a full disk.
    FullDisk,
    /// The first write that reaches the file after the header.
    OneWrite,
    SyncDir,
    /// The run panics between the last look and the moment the trace has its name.
    PanicWhileNaming,
}

thread_local! {
    static FAIL: Cell<Option<Fail>> = const { Cell::new(None) };
}

pub(super) fn fails(what: Fail) -> bool {
    let fails = FAIL.get() == Some(what);
    if fails && what == Fail::OneWrite {
        FAIL.set(None);
    }
    fails
}

fn clean() -> Commit {
    Commit {
        sha: SHA.to_owned(),
        unclean: None,
    }
}

const SHA: &str = "0123456789abcdef0123456789abcdef01234567";

/// A directory of its own, removed at the end.
struct Dir(PathBuf);

impl Dir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("nabedra-tla-{}-{n}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    /// The file names, without the part that tells two writers apart.
    fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(&self.0)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .map(|name| match name.split_once(".ndjson.") {
                Some((base, rest)) => {
                    format!("{base}.ndjson.{}", rest.rsplit('.').next().unwrap())
                }
                None => name,
            })
            .collect();
        names.sort();
        names
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn builder() -> Builder {
    TlaTrace::builder("Spec", "unit")
        .params(json!({"Nodes": ["a", "b"]}))
        .node(1, "a")
        .action(Action::new("Vote", &["term"]).msg(&["to"]))
        .action(Action::new("Apply", &["log"]))
        .action(Action::unconstrained(
            "Tick",
            "the spec's timer has no state",
        ))
}

fn open(builder: Builder, world: &Arc<World>, dir: &Dir) -> io::Result<TlaTrace> {
    builder.subscribe(world, &dir.0, &clean())
}

/// Runs `body` with an open trace on a world with nodes 1 and 2, and returns what
/// `finish` said.
fn record(
    dir: &Dir,
    builder: Builder,
    body: impl FnOnce(&Arc<World>, &TlaSteps),
) -> io::Result<PathBuf> {
    runtime::run(7, |world| async move {
        world.add_node(1, NodeClock::default());
        world.add_node(2, NodeClock::default());
        let trace = open(builder, &world, dir)?;
        body(&world, &trace.steps());
        trace.finish()
    })
}

fn good(steps: &TlaSteps) {
    steps.step(1, "Vote", json!({"term": 3}), Some(json!({"to": "b"})));
}

/// `body` after one good step must make `finish` fail with `why`, and publish nothing.
#[track_caller]
fn refused(why: &str, body: impl FnOnce(&Arc<World>, &TlaSteps)) {
    let dir = Dir::new();
    let error = record(&dir, builder(), |world, steps| {
        good(steps);
        body(world, steps);
        // A later good step must not hide the problem.
        good(steps);
    })
    .expect_err(why)
    .to_string();
    assert!(error.contains(why), "{error}");
    assert_eq!(dir.names(), ["Spec-7.ndjson.partial"]);
}

#[track_caller]
fn not_installed(why: &str, builder: Builder) {
    let dir = Dir::new();
    let error = record(&dir, builder, |_, _| ()).expect_err(why).to_string();
    assert!(error.contains(why), "{error}");
    assert!(dir.names().is_empty());
}

fn lines(path: &Path) -> Vec<String> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect()
}

#[test]
fn lines_have_the_format_in_a_fixed_key_order() {
    let dir = Dir::new();
    let path = record(&dir, builder().wide("commitTs"), |world, steps| {
        good(steps);
        world.pause_all(Duration::from_secs(3));
        steps.step(
            2,
            "Apply",
            json!({"log": {"z": 1, "commitTs": [5, {"n": u64::MAX}], "a": set([3, 1, 2])}}),
            None,
        );
        steps.step("client-1", "Tick", json!({}), None);
    })
    .unwrap();
    assert_eq!(dir.names(), ["Spec-7.ndjson"]);
    assert_eq!(
        lines(&path),
        [
            format!(
                r#"{{"trace":"nabedra.trace/1","spec":"Spec","seed":7,"sha":"{SHA}","workload":"unit","params":{{"Nodes":["a","b"]}}}}"#
            ),
            r#"{"seq":0,"t":0,"node":"a","action":"Vote","vars":{"term":3},"msg":{"to":"b"}}"#.into(),
            r#"{"seq":1,"t":3000000000,"node":"world","action":"Pause","vars":{},"msg":{"nanos":"3000000000"}}"#.into(),
            r#"{"seq":2,"t":3000000000,"node":"2","action":"Apply","vars":{"log":{"a":[1,2,3],"commitTs":["5",{"n":"18446744073709551615"}],"z":1}}}"#.into(),
            r#"{"seq":3,"t":3000000000,"node":"client-1","action":"Tick","vars":{}}"#.into(),
        ]
    );
}

#[test]
fn nothing_is_installed_without_a_directory() {
    assert_eq!(trace_dir(None), None);
    assert_eq!(trace_dir(Some("".into())), None);
    assert_eq!(trace_dir(Some("/t".into())), Some("/t".into()));
    let dir = Dir::new();
    runtime::run(7, |world| {
        // Whatever the environment says, a shrinking sweep installs nothing.
        let trace = suppressed(|| builder().install(&world)).unwrap();
        assert!(trace.is_none());
        std::future::ready(())
    });
    assert!(dir.names().is_empty());
    assert!(!SUPPRESSED.get());
}

#[test]
fn a_step_without_a_declared_variable_is_refused() {
    refused("required variables are missing: [\"term\"]", |_, steps| {
        steps.step(1, "Vote", json!({}), Some(json!({"to": "b"})));
    });
}

#[test]
fn a_misspelt_variable_is_refused_even_beside_the_right_one() {
    refused("required variables are missing", |_, steps| {
        steps.step(1, "Vote", json!({"trem": 3}), Some(json!({"to": "b"})));
    });
    refused("undeclared variables: [\"trem\"]", |_, steps| {
        steps.step(
            1,
            "Vote",
            json!({"term": 3, "trem": 3}),
            Some(json!({"to": "b"})),
        );
    });
}

#[test]
fn undeclared_actions_and_messages_are_refused() {
    refused("action Commit is not declared", |_, steps| {
        steps.step(1, "Commit", json!({"term": 1}), None);
    });
    refused("the required message is missing", |_, steps| {
        steps.step(1, "Vote", json!({"term": 1}), None);
    });
    refused("message fields are missing: [\"to\"]", |_, steps| {
        steps.step(1, "Vote", json!({"term": 1}), Some(json!({"from": "b"})));
    });
    refused("undeclared message field \"re\"", |_, steps| {
        let msg = json!({"to": "b", "re": 1});
        steps.step(1, "Vote", json!({"term": 1}), Some(msg));
    });
    refused("carries an undeclared message", |_, steps| {
        steps.step(1, "Apply", json!({"log": []}), Some(json!({})));
    });
    refused("vars is not an object", |_, steps| {
        steps.step(1, "Tick", json!([]), None);
    });
}

#[test]
fn a_message_of_the_wrong_kind_is_refused() {
    let send = |msg: Value| {
        let dir = Dir::new();
        let builder = builder().action(
            Action::new("Send", &["sent"])
                .msg_value("type", "Commit")
                .msg_optional("hint"),
        );
        record(&dir, builder, |_, steps| {
            steps.step(1, "Send", json!({"sent": true}), Some(msg));
        })
    };
    send(json!({"type": "Commit"})).unwrap();
    send(json!({"type": "Commit", "hint": 1})).unwrap();
    for (msg, why) in [
        (json!({"type": "Abort"}), "message type is \"Abort\", not"),
        (json!({"hint": 1}), "message fields are missing"),
        (json!({}), "message fields are missing"),
    ] {
        let error = send(msg).unwrap_err().to_string();
        assert!(error.contains(why), "{error}");
    }
}

#[test]
fn values_tlc_would_misread_are_refused() {
    for (value, why) in [
        (json!(2_147_483_648_i64), "32 bits"),
        (json!(-2_147_483_649_i64), "32 bits"),
        (json!(u64::MAX), "32 bits"),
        (json!({"deep": [4_294_967_297_i64]}), "deep: 4294967297"),
        (json!(1.5), "32 bits"),
        (json!(null), "null"),
    ] {
        refused(why, |_, steps| {
            steps.step(1, "Apply", json!({"log": value}), None);
        });
    }
    refused("to: 2147483648", |_, steps| {
        steps.step(
            1,
            "Vote",
            json!({"term": 1}),
            Some(json!({"to": 2_147_483_648_i64})),
        );
    });
    not_installed(
        "params: MaxTs: 2147483648",
        builder().params(json!({"MaxTs": 2_147_483_648_i64})),
    );
    let dir = Dir::new();
    let edges = json!({"log": [i32::MIN, i32::MAX]});
    record(&dir, builder(), |_, steps| {
        steps.step(1, "Apply", edges, None)
    })
    .unwrap();
    // A wide field holds integers only as strings, on every line, whatever their size.
    let dir = Dir::new();
    let wide = builder().wide("log");
    let path = record(&dir, wide, |_, steps| {
        steps.step(1, "Apply", json!({"log": 1}), None);
        steps.step(1, "Apply", json!({"log": i64::MIN}), None);
    })
    .unwrap();
    let written = fs::read_to_string(path).unwrap();
    assert!(written.contains(r#""vars":{"log":"1"}"#));
    assert!(written.contains(r#""vars":{"log":"-9223372036854775808"}"#));
}

#[test]
fn a_trace_that_could_show_nothing_is_refused() {
    not_installed("params must name", builder().params(json!({})));
    not_installed("params must name", builder().params(json!(null)));
    not_installed(
        "declares no variables",
        builder().action(Action::new("Bare", &[])),
    );
    not_installed(
        "gives no reason",
        builder().action(Action::unconstrained("Bare", " ")),
    );
    not_installed(
        "declared twice",
        builder().action(Action::new("Vote", &["term"])),
    );
    not_installed(
        "not an action name",
        builder().action(Action::new("No-Name", &["x"])),
    );
    let bare = TlaTrace::builder("Spec", "unit")
        .params(json!({"N": 1}))
        .action(Action::unconstrained("Tick", "no state"));
    not_installed("no declared action carries a variable", bare);
    // Declared, but the run only logged names.
    for body in [true, false] {
        let dir = Dir::new();
        let error = record(&dir, builder(), |_, steps| {
            if body {
                steps.step(1, "Tick", json!({}), None);
            }
        })
        .unwrap_err()
        .to_string();
        assert!(error.contains("carries a variable"), "{error}");
        assert_eq!(dir.names(), ["Spec-7.ndjson.partial"]);
    }
}

#[test]
fn a_step_of_a_node_that_is_down_is_refused() {
    refused("node 2 is not up", |world, steps| {
        world.crash(2);
        steps.step(2, "Apply", json!({"log": []}), None);
    });
    // Naming it does not help, by its spec name or its id.
    refused("node 1 is not up", |world, steps| {
        world.crash(1);
        steps.step("a", "Apply", json!({"log": []}), None);
    });
    refused("node 2 is not up", |world, steps| {
        world.crash(2);
        steps.step("2", "Apply", json!({"log": []}), None);
    });
    refused("node 9 is not up", |_, steps| {
        steps.step(9, "Apply", json!({"log": []}), None);
    });
}

#[test]
fn a_node_name_names_one_node() {
    for (id, name) in [(2, "a"), (2, "3"), (2, " "), (2, "world")] {
        not_installed("cannot be named", builder().node(id, name));
    }
    let dir = Dir::new();
    let named = builder().node(2, "2");
    record(&dir, named, |_, steps| {
        steps.step("a", "Apply", json!({"log": []}), None);
    })
    .unwrap();
}

#[test]
fn a_run_that_never_finishes_leaves_no_trace() {
    let dir = Dir::new();
    let panicked = std::panic::catch_unwind(|| {
        record(&dir, builder(), |_, steps| {
            good(steps);
            panic!("the workload failed");
        })
    });
    assert!(panicked.is_err());
    assert_eq!(dir.names(), ["Spec-7.ndjson.partial"]);
    // Its steps are kept for debugging, under a name no checker reads.
    let partial = fs::read_dir(&dir.0).unwrap().next().unwrap().unwrap();
    assert_eq!(lines(&partial.path()).len(), 2);

    let dropped = std::panic::catch_unwind(|| {
        runtime::run(7, |world| {
            world.add_node(1, NodeClock::default());
            let trace = open(builder(), &world, &dir).unwrap();
            good(&trace.steps());
            drop(trace);
            std::future::ready(())
        })
    })
    .unwrap_err();
    let text = dropped.downcast_ref::<String>().unwrap();
    assert!(text.contains("dropped without finish()"), "{text}");
    assert_eq!(dir.names(), ["Spec-7.ndjson.partial"; 2]);
    // The name is free again.
    record(&dir, builder(), |_, steps| good(steps)).unwrap();
    assert!(dir.0.join("Spec-7.ndjson").exists());
}

#[test]
fn a_run_cut_short_hides_the_earlier_trace_of_its_seed() {
    let dir = Dir::new();
    record(&dir, builder(), |_, steps| good(steps)).unwrap();
    refused_in(&dir, "not declared");
    assert_eq!(
        dir.names(),
        ["Spec-7.ndjson.partial", "Spec-7.ndjson.previous"]
    );
}

#[track_caller]
fn refused_in(dir: &Dir, why: &str) {
    let error = record(dir, builder(), |_, steps| {
        good(steps);
        steps.step(1, "Nope", json!({}), None);
    })
    .unwrap_err()
    .to_string();
    assert!(error.contains(why), "{error}");
}

#[test]
fn a_replay_must_write_the_same_bytes() {
    let dir = Dir::new();
    let path = record(&dir, builder(), |_, steps| good(steps)).unwrap();
    let first = fs::read(&path).unwrap();
    record(&dir, builder(), |_, steps| good(steps)).unwrap();
    assert_eq!(fs::read(&path).unwrap(), first);
    assert_eq!(dir.names(), ["Spec-7.ndjson"]);
    let error = record(&dir, builder(), |_, steps| {
        good(steps);
        good(steps);
    })
    .unwrap_err()
    .to_string();
    assert!(error.contains("not deterministic"), "{error}");
    assert_eq!(
        dir.names(),
        ["Spec-7.ndjson.partial", "Spec-7.ndjson.previous"]
    );
}

#[test]
fn a_step_after_finish_withdraws_the_trace() {
    let dir = Dir::new();
    let late = std::panic::catch_unwind(|| {
        runtime::run(7, |world| {
            world.add_node(1, NodeClock::default());
            let trace = open(builder(), &world, &dir).unwrap();
            let steps = trace.steps();
            good(&steps);
            trace.finish().unwrap();
            assert_eq!(dir.names(), ["Spec-7.ndjson"]);
            // Teardown faults are not part of the trace.
            world.crash(1);
            world.restart(1);
            good(&steps);
            std::future::ready(())
        })
    });
    assert!(late.is_err());
    assert_eq!(dir.names(), ["Spec-7.ndjson.withdrawn"]);
}

#[test]
fn one_file_has_one_writer_at_a_time() {
    let dir = Dir::new();
    runtime::run(7, |world| {
        world.add_node(1, NodeClock::default());
        let trace = open(builder(), &world, &dir).unwrap();
        let error = open(builder(), &world, &dir).unwrap_err().to_string();
        assert!(error.contains("already being written"), "{error}");
        good(&trace.steps());
        trace.finish().unwrap();
        std::future::ready(())
    });
}

/// `/dev/full` fails every write that reaches it, so a full disk fails the run whether it
/// shows up at a step or only at the final flush.
#[cfg(target_os = "linux")]
#[test]
fn a_write_error_fails_the_run() {
    for steps_written in [1, 5_000] {
        let dir = Dir::new();
        FAIL.set(Some(Fail::FullDisk));
        let result = record(&dir, builder(), |_, steps| {
            for _ in 0..steps_written {
                good(steps);
            }
        });
        FAIL.set(None);
        let error = result.unwrap_err().to_string();
        assert!(error.contains("No space left"), "{error}");
        assert!(dir.names().is_empty());
    }
    let dir = Dir::new();
    fs::write(dir.0.join("file"), "").unwrap();
    let blocked = Dir(dir.0.join("file"));
    assert!(record(&blocked, builder(), |_, steps| good(steps)).is_err());
}

/// The buffer keeps what a failed write did not take and writes it later, so the file can
/// come out whole. The disk still failed under the run.
#[test]
fn a_write_error_is_not_forgotten_when_a_later_write_succeeds() {
    let dir = Dir::new();
    let error = record(&dir, builder(), |_, steps| {
        good(steps);
        FAIL.set(Some(Fail::OneWrite));
        for _ in 0..5_000 {
            good(steps);
        }
        assert_eq!(FAIL.get(), None, "no write reached the file");
    })
    .unwrap_err()
    .to_string();
    assert!(error.contains("injected write error"), "{error}");
    assert_eq!(dir.names(), ["Spec-7.ndjson.partial"]);
}

#[test]
fn a_directory_that_cannot_be_synced_leaves_no_trace() {
    let dir = Dir::new();
    FAIL.set(Some(Fail::SyncDir));
    let result = record(&dir, builder(), |_, steps| good(steps));
    FAIL.set(None);
    let error = result.unwrap_err().to_string();
    assert!(error.contains("directory sync"), "{error}");
    assert_eq!(
        dir.names(),
        ["Spec-7.ndjson.partial", "Spec-7.ndjson.withdrawn"]
    );
}

#[test]
fn a_panic_while_the_trace_gets_its_name_takes_the_name_away() {
    let dir = Dir::new();
    FAIL.set(Some(Fail::PanicWhileNaming));
    let result = record(&dir, builder(), |_, steps| good(steps));
    FAIL.set(None);
    let error = result.unwrap_err().to_string();
    assert!(error.contains("panicked at"), "{error}");
    assert_eq!(
        dir.names(),
        ["Spec-7.ndjson.partial", "Spec-7.ndjson.withdrawn"]
    );
}

/// The hook sees the panic too, unless other code silences it on that thread.
#[test]
fn a_service_step_that_panicked_is_refused() {
    refused("service journal panicked", |_, steps| {
        steps.observe(&SimEvent::Step {
            node: Some(1),
            service: "journal",
            outcome: StepOutcome::Panicked,
        });
    });
}

#[test]
fn time_that_goes_backward_is_refused() {
    refused("is before the last step's", |_, steps| {
        lock(&steps.inner).last_t = 1;
    });
}

#[test]
fn a_step_outside_the_runtime_is_refused() {
    let dir = Dir::new();
    let runtime = runtime::runtime_seeded(7).unwrap();
    let world = runtime.block_on(async { World::new(7) });
    world.add_node(1, NodeClock::default());
    let trace = open(builder(), &world, &dir).unwrap();
    good(&trace.steps());
    let error = trace.finish().unwrap_err().to_string();
    assert!(error.contains("no virtual time"), "{error}");
}

/// A repository with one commit that tracks `server/Cargo.toml`.
struct Repo(Dir);

impl Repo {
    fn new() -> Self {
        let repo = Self(Dir::new());
        repo.git(&["init", "-q"]);
        fs::create_dir(repo.0.0.join("server")).unwrap();
        fs::write(repo.0.0.join("server/Cargo.toml"), "one").unwrap();
        fs::write(repo.0.0.join("README"), "one").unwrap();
        repo.commit(1_700_000_000);
        repo
    }

    fn git(&self, args: &[&str]) -> String {
        self.git_at(1_700_000_000, args)
    }

    fn git_at(&self, at: u64, args: &[&str]) -> String {
        let date = format!("{at} +0000");
        let out = std::process::Command::new("git")
            .args(["-c", "user.name=t", "-c", "user.email=t@example.invalid"])
            .args(["-c", "commit.gpgsign=false"])
            .args(args)
            .env("GIT_COMMITTER_DATE", &date)
            .env("GIT_AUTHOR_DATE", &date)
            .current_dir(&self.0.0)
            .output()
            .unwrap();
        assert!(out.status.success(), "{args:?}: {out:?}");
        String::from_utf8(out.stdout).unwrap().trim().to_owned()
    }

    fn commit(&self, at: u64) -> String {
        self.git_at(at, &["add", "-A"]);
        self.git_at(at, &["commit", "-qm", "c"]);
        self.git(&["rev-parse", "HEAD"])
    }

    fn of(&self, given: Option<&str>, built: u64) -> Result<Commit, String> {
        commit::of(&self.0.0.join("server"), given, Some(built))
    }

    /// Why the sources are unclean, for an executable built at `built`.
    fn unclean(&self, built: u64) -> String {
        self.of(None, built).unwrap().unclean.unwrap_or_default()
    }
}

#[test]
fn the_commit_is_the_one_that_tracks_the_sources() {
    let repo = Repo::new();
    let head = repo.git(&["rev-parse", "HEAD"]);
    let now = 1_700_000_500;
    let commit = repo.of(None, now).unwrap();
    assert_eq!(
        (commit.sha.as_str(), &commit.unclean),
        (head.as_str(), &None)
    );
    assert_eq!(repo.of(Some(&head), now).unwrap(), commit);
    for given in [
        "",
        SHA,
        "0000000000000000000000000000000000000000",
        &head[..7],
    ] {
        let error = repo.of(Some(given), now).unwrap_err();
        assert!(error.contains("but the source tree is at"), "{error}");
    }
    // Sources that the repository around them does not track, or with no repository.
    let inside = repo.0.0.join("copied");
    fs::create_dir(&inside).unwrap();
    fs::write(inside.join("Cargo.toml"), "").unwrap();
    let error = commit::of(&inside, None, Some(now)).unwrap_err();
    assert!(error.contains("is not tracked"), "{error}");
    let nowhere = Dir::new();
    let error = commit::of(&nowhere.0, Some(SHA), Some(now)).unwrap_err();
    assert!(error.contains("no commit SHA"), "{error}");
}

#[test]
fn uncommitted_sources_are_unclean() {
    let repo = Repo::new();
    let now = 1_700_000_500;
    assert_eq!(repo.unclean(now), "");
    // Outside the sources nothing matters.
    fs::write(repo.0.0.join("README"), "two").unwrap();
    assert_eq!(repo.unclean(now), "");
    fs::write(repo.0.0.join("server/Cargo.toml"), "two").unwrap();
    assert_eq!(
        repo.unclean(now),
        "1 uncommitted files, such as M server/Cargo.toml"
    );
    repo.git(&["checkout", "-q", "server"]);
    fs::write(repo.0.0.join("server/new.rs"), "").unwrap();
    assert!(repo.unclean(now).contains("?? server/new.rs"));
    repo.git(&["add", "server/new.rs"]);
    assert!(repo.unclean(now).contains("A  server/new.rs"));
}

#[test]
fn an_executable_of_an_earlier_head_is_unclean() {
    let repo = Repo::new();
    let first = repo.git(&["rev-parse", "HEAD"]);
    fs::write(repo.0.0.join("README"), "two").unwrap();
    repo.commit(1_700_001_000);
    // HEAD moved, but not the sources.
    assert_eq!(repo.unclean(1_700_000_500), "");
    fs::write(repo.0.0.join("server/Cargo.toml"), "two").unwrap();
    repo.commit(1_700_002_000);
    assert_eq!(
        repo.unclean(1_700_000_500),
        format!("the executable was built at {first}")
    );
    assert_eq!(repo.unclean(1_700_002_500), "");
    let built = commit::of(&repo.0.0.join("server"), None, None).unwrap();
    assert!(built.unclean.unwrap().contains("build time"));
    fs::remove_dir_all(repo.0.0.join(".git/logs")).unwrap();
    assert!(repo.unclean(1_700_002_500).contains("git has no record"));
}

#[test]
fn unclean_sources_are_refused_or_marked() {
    let unclean = Commit {
        unclean: Some("1 uncommitted files".into()),
        ..clean()
    };
    for mode in [None, Some(""), Some("1"), Some("MARK")] {
        let error = unclean.clone().strict(mode).unwrap_err();
        assert!(error.contains("1 uncommitted files"), "{error}");
        assert_eq!(clean().strict(mode).unwrap(), clean());
    }
    assert_eq!(unclean.clone().strict(Some("mark")).unwrap(), unclean);
    assert_eq!(clean().workload("w"), "w");
    let dir = Dir::new();
    let path = runtime::run(7, |world| {
        world.add_node(1, NodeClock::default());
        let trace = builder().subscribe(&world, &dir.0, &unclean).unwrap();
        good(&trace.steps());
        std::future::ready(trace.finish().unwrap())
    });
    assert!(lines(&path)[0].contains(r#""workload":"unit [unclean sources]""#));
}

/// The steps of a trace, without `seq` and `t`.
fn faults(path: &Path) -> Vec<String> {
    lines(path)[1..]
        .iter()
        .map(|line| line[line.find("\"node\"").unwrap()..].to_owned())
        .collect()
}

#[test]
fn world_faults_are_logged_in_order() {
    let dir = Dir::new();
    let path = record(&dir, builder(), |world, steps| {
        world.mount(2, "/data");
        good(steps);
        world.crash(2);
        world.wipe(2, Path::new("/data"), Wipe::Reformat);
        world.restart(2);
        // Crashes the node itself.
        world.restart_with(2, Cut::PowerLoss { seed: u64::MAX });
        world.restart(1);
        steps.fault(Fault::SyncError {
            node: 1,
            mount: "/data".into(),
            path: "/data/wal".into(),
            op: "SyncData".into(),
        });
        steps.fault(Fault::WallJump {
            node: 1,
            nanos: -3_000_000_000,
        });
    })
    .unwrap();
    assert_eq!(
        faults(&path)[1..],
        [
            r#""node":"2","action":"Crash","vars":{},"msg":{"kind":"unspecified"}}"#,
            r#""node":"2","action":"DiskLoss","vars":{},"msg":{"how":"reformat","mount":"/data"}}"#,
            r#""node":"2","action":"Restart","vars":{},"msg":{"cut":"none","incarnation":1}}"#,
            r#""node":"2","action":"Crash","vars":{},"msg":{"kind":"unspecified"}}"#,
            r#""node":"2","action":"Restart","vars":{},"msg":{"cut":"power512","incarnation":2}}"#,
            // Node 1 was never down, and the line says so.
            r#""node":"a","action":"Restart","vars":{},"msg":{"cut":"none","incarnation":0}}"#,
            r#""node":"a","action":"SyncError","vars":{},"msg":{"mount":"/data","op":"SyncData","path":"/data/wal"}}"#,
            r#""node":"a","action":"WallJump","vars":{},"msg":{"nanos":"-3000000000"}}"#,
        ]
    );
}

#[test]
fn net_plan_faults_are_logged_in_order() {
    let dir = Dir::new();
    let run = |tapped: bool| {
        let mut net = NetPlan::new(11);
        runtime::run(7, |world| {
            world.add_node(1, NodeClock::default());
            world.add_node(2, NodeClock::default());
            let trace = tapped.then(|| open(builder().delays(true), &world, &dir).unwrap());
            if let Some(trace) = &trace {
                trace.tap_net(&mut net);
                good(&trace.steps());
            }
            net.block(1, 2);
            net.block(1, 2);
            let mut plans = vec![net.plan(1, 2)];
            net.heal_links();
            net.heal_links();
            net.isolate(1, [1, 2]);
            net.unblock(1, 2);
            net.unblock(1, 2);
            net.pause(2);
            net.heal_all();
            net.set_filters(Filters {
                drop_per_mille: 300,
                dup_per_mille: 300,
                delay_max_ms: 2,
            });
            plans.extend((0..40).map(|_| net.plan_about(2, 1, || json!({"type": "Ack"}))));
            let path = trace.map(|trace| trace.finish().unwrap());
            std::future::ready((plans, format!("{net:?}"), path))
        })
    };
    let (plans, after, path) = run(true);
    // The tap never changes a plan, a draw, or the plan's own text.
    let (untapped, untapped_after, _) = run(false);
    assert_eq!((&plans, &after), (&untapped, &untapped_after));
    assert!(!after.contains("tap"));

    let mut expected: Vec<String> = [
        r#""node":"a","action":"Partition","vars":{},"msg":{"to":"2"}}"#,
        r#""node":"a","action":"DropMessage","vars":{},"msg":{"cause":"blocked","to":"2"}}"#,
        r#""node":"world","action":"Heal","vars":{},"msg":{"all":true}}"#,
        r#""node":"a","action":"Partition","vars":{},"msg":{"to":"2"}}"#,
        r#""node":"2","action":"Partition","vars":{},"msg":{"to":"a"}}"#,
        r#""node":"a","action":"Heal","vars":{},"msg":{"to":"2"}}"#,
        r#""node":"2","action":"HoldMessages","vars":{}}"#,
        r#""node":"world","action":"Heal","vars":{},"msg":{"all":true}}"#,
        r#""node":"2","action":"ReleaseMessages","vars":{}}"#,
    ]
    .map(str::to_owned)
    .into();
    let mut kinds = BTreeSet::new();
    for plan in &plans[1..] {
        match *plan {
            crate::net::Plan::Drop => {
                kinds.insert("drop");
                expected.push(
                    r#""node":"2","action":"DropMessage","vars":{},"msg":{"cause":"plan","to":"a"}}"#
                        .into(),
                );
            }
            crate::net::Plan::Deliver {
                delay_ms,
                duplicate,
            } => {
                if delay_ms > 1 {
                    kinds.insert("delay");
                    expected.push(format!(
                        r#""node":"2","action":"DelayMessage","vars":{{}},"msg":{{"delay_ns":"{delay_ms}000000","to":"a"}}}}"#
                    ));
                }
                if duplicate {
                    kinds.insert("duplicate");
                    expected.push(
                        r#""node":"2","action":"DuplicateMessage","vars":{},"msg":{"to":"a"}}"#
                            .into(),
                    );
                }
            }
        }
    }
    assert_eq!(kinds.len(), 3, "the plans covered too little: {plans:?}");
    assert_eq!(faults(&path.unwrap())[1..], expected);
}

#[test]
fn a_mapped_fault_must_meet_its_declaration() {
    let crash = |map: fn(FaultLine) -> Option<FaultLine>| {
        let dir = Dir::new();
        let builder = builder()
            .action(Action::new("Stop", &["up"]))
            .faults(move |line| {
                if line.action == "Crash" {
                    map(line)
                } else {
                    Some(line)
                }
            });
        let result = record(&dir, builder, |world, steps| {
            good(steps);
            world.crash(2);
        });
        result.map(|path| faults(&path)[1..].to_vec())
    };
    assert_eq!(
        crash(|mut line| {
            assert_eq!(line.fault, Fault::Crash { node: 2 });
            line.action = "Stop".into();
            line.vars = json!({"up": false});
            line.msg = None;
            Some(line)
        })
        .unwrap(),
        [r#""node":"2","action":"Stop","vars":{"up":false}}"#]
    );
    assert_eq!(crash(|_| None).unwrap(), [""; 0]);
    for (why, map) in [
        (
            "required variables are missing",
            (|mut line| {
                line.action = "Stop".into();
                line.msg = None;
                Some(line)
            }) as fn(FaultLine) -> Option<FaultLine>,
        ),
        ("is not declared", |mut line| {
            line.action = "Halt".into();
            Some(line)
        }),
        ("is not declared", |mut line| {
            line.vars = json!({"up": false});
            Some(line)
        }),
    ] {
        let error = crash(map).unwrap_err().to_string();
        assert!(error.contains(why), "{error}");
    }
}
