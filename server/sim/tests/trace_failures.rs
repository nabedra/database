//! Runs that go wrong must not leave a file that looks like a complete trace. Each test here
//! uses only the writer's public interface, and looks at what is on disk afterwards.
//!
//! The tests that need another process, or a process with other environment variables, run
//! this test binary again with [`child`] as the only test.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard};
use std::time::Duration;

use nabedra_rt::fs::{FsOp, OpenMode};
use nabedra_rt::sim::{ClockRead, NodeClock, StoragePause, World};
use nabedra_rt::sync::Condvar;
use nabedra_rt::task::{Step, Stepped};
use nabedra_sim::sweep::{SeedRun, SweepConfig, sweep};
use nabedra_sim::tla_trace::{Action, Builder, attributed};
use nabedra_sim::{TlaSteps, TlaTrace, runtime};
use serde_json::{Value, json};

/// A panic on a thread that belongs to no run counts for every run that is open, so the test
/// that plants one runs apart from the others.
static APART: RwLock<()> = RwLock::new(());

fn together() -> RwLockReadGuard<'static, ()> {
    APART.read().unwrap_or_else(PoisonError::into_inner)
}

struct Scratch(PathBuf);

impl Scratch {
    fn new(label: &str) -> Self {
        let name = format!("nabedra-trace-failures-{label}-{}", std::process::id());
        let dir = std::env::temp_dir().join(name);
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    /// Traces that had their name for a while.
    fn withdrawn(&self) -> usize {
        fs::read_dir(&self.0)
            .unwrap()
            .filter(|entry| {
                let path = entry.as_ref().unwrap().path();
                path.extension().is_some_and(|ext| ext == "withdrawn")
            })
            .count()
    }

    /// The files that a checker would read.
    fn traces(&self) -> Vec<PathBuf> {
        let mut files: Vec<PathBuf> = fs::read_dir(&self.0)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "ndjson"))
            .collect();
        files.sort();
        files
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn builder() -> Builder {
    TlaTrace::builder("Spec", "failures")
        .params(json!({"N": 1}))
        .action(Action::new("Apply", &["n"]))
}

fn apply(steps: &TlaSteps, n: u64) {
    steps.step("client", "Apply", json!({"n": n}), None);
}

/// Runs `body` with node 1 and a trace, then finishes it.
fn traced<F: Future<Output = ()>>(
    dir: &Path,
    body: impl FnOnce(Arc<World>) -> F,
) -> io::Result<PathBuf> {
    runtime::run(1, |world| {
        world.add_node(1, NodeClock::default());
        let trace = builder().install_in(&world, dir).unwrap();
        let body = body(world);
        async move {
            apply(&trace.steps(), 0);
            body.await;
            apply(&trace.steps(), 1);
            trace.finish()
        }
    })
}

async fn settle() {
    nabedra_rt::time::sleep(Duration::from_millis(50)).await;
}

/// A service that panics at its first step.
struct Faulty(Condvar);

impl Stepped for Faulty {
    fn step(&mut self) -> Step {
        panic!("planted service panic");
    }
    fn doorbell(&self) -> &Condvar {
        &self.0
    }
    fn run(self) {}
}

/// One that unwinds without running the panic hook.
struct Resumes(Condvar);

impl Stepped for Resumes {
    fn step(&mut self) -> Step {
        std::panic::resume_unwind(Box::new("planted"));
    }
    fn doorbell(&self) -> &Condvar {
        &self.0
    }
    fn run(self) {}
}

#[track_caller]
fn refuses_panic(label: &str, dir: &Scratch, result: io::Result<PathBuf>) {
    assert!(dir.traces().is_empty(), "{label}: {:?}", dir.traces());
    assert_eq!(dir.withdrawn(), 0, "{label}: it never had its name");
    let error = result.expect_err(label).to_string();
    assert!(error.contains("panicked"), "{label}: {error}");
}

/// Wherever it happens, and whether or not anybody looks at it.
#[test]
fn a_panic_anywhere_in_the_run_leaves_no_trace() {
    let _apart = APART.write().unwrap_or_else(PoisonError::into_inner);
    type Planted = fn(Arc<World>);
    let planted: [(&str, Planted); 11] = [
        ("node task", |world| {
            world.spawn_node(1, async { panic!("planted") });
        }),
        ("task outside any node", |_| {
            nabedra_rt::task::spawn(async { panic!("planted") });
        }),
        ("task whose handle is read", |_| {
            let task = nabedra_rt::task::spawn(async { panic!("planted") });
            nabedra_rt::task::spawn(async { assert!(task.await.is_err()) });
        }),
        ("descendant", |world| {
            world.spawn_node(1, async {
                nabedra_rt::task::spawn(async { panic!("planted") });
            });
        }),
        ("blocking job", |_| {
            drop(nabedra_rt::task::spawn_blocking(|| panic!("planted")));
        }),
        ("tokio's own blocking pool", |_| {
            #[allow(clippy::disallowed_methods)] // a thread the simulation does not know
            let job = tokio::task::spawn_blocking(|| panic!("planted"));
            while !job.is_finished() {
                std::thread::yield_now();
            }
        }),
        ("thread of its own", |_| {
            #[allow(clippy::disallowed_methods)] // a thread the simulation does not know
            let thread = std::thread::spawn(|| panic!("planted"));
            assert!(thread.join().is_err());
        }),
        ("stepped service", |world| {
            world.spawn_node(1, async {
                let faulty = Faulty(Condvar::new());
                drop(nabedra_rt::task::spawn_stepped("faulty", 1 << 20, faulty).unwrap());
            });
        }),
        ("stepped service that resumes an unwinding", |world| {
            world.spawn_node(1, async {
                let faulty = Resumes(Condvar::new());
                drop(nabedra_rt::task::spawn_stepped("faulty", 1 << 20, faulty).unwrap());
            });
        }),
        ("property", |_| {
            let caught = nabedra_check::prop::run(
                &|_| panic!("planted"),
                &mut nabedra_check::Gen::from_tape(Vec::new()),
            );
            assert!(caught.is_err());
        }),
        ("caught", |_| {
            let caught = std::panic::catch_unwind(|| panic!("planted"));
            assert!(caught.is_err());
        }),
    ];
    for (label, plant) in planted {
        let dir = Scratch::new("panic");
        let result = traced(&dir.0, |world| async move {
            plant(world);
            settle().await;
        });
        refuses_panic(label, &dir, result);
    }
    // The clean twin: the same run without a panic.
    let dir = Scratch::new("no-panic");
    traced(&dir.0, |world| async move {
        world.spawn_node(1, async {});
        nabedra_rt::task::spawn_blocking(|| ()).await.unwrap();
        settle().await;
    })
    .unwrap();
    assert_eq!(dir.traces().len(), 1);
}

#[test]
fn a_panic_after_finish_withdraws_the_trace_while_the_run_lives() {
    let _together = together();
    let dir = Scratch::new("after-finish");
    let late = |panic_in_task: bool| {
        let dir = dir.0.clone();
        std::panic::catch_unwind(move || {
            runtime::run(1, |world| async move {
                let trace = builder().install_in(&world, &dir).unwrap();
                apply(&trace.steps(), 0);
                let path = trace.finish().unwrap();
                assert!(path.exists());
                if panic_in_task {
                    nabedra_rt::task::spawn(async { panic!("planted") });
                    settle().await;
                } else {
                    panic!("planted");
                }
            });
        })
    };
    assert!(late(true).is_ok());
    assert!(dir.traces().is_empty(), "{:?}", dir.traces());
    assert!(late(false).is_err());
    assert!(dir.traces().is_empty(), "{:?}", dir.traces());
}

/// A destructor that finishes its trace while the run unwinds must not publish it.
#[test]
fn finishing_while_the_run_unwinds_leaves_no_trace() {
    struct Finishes(Option<TlaTrace>);
    impl Drop for Finishes {
        fn drop(&mut self) {
            let error = self.0.take().unwrap().finish().unwrap_err().to_string();
            assert!(error.contains("the run is panicking"), "{error}");
        }
    }
    let _together = together();
    let dir = Scratch::new("unwinds");
    let path = dir.0.clone();
    let unwound = std::panic::catch_unwind(move || {
        runtime::run(1, |world| async move {
            let trace = builder().install_in(&world, &path).unwrap();
            apply(&trace.steps(), 0);
            let _finishes = Finishes(Some(trace));
            panic!("planted");
        })
    });
    assert!(unwound.is_err());
    assert!(dir.traces().is_empty());
}

/// A sweep runs seeds side by side. A panic in one says nothing about the others.
#[test]
fn a_panic_in_one_run_leaves_the_traces_of_other_runs() {
    let _together = together();
    let dir = Scratch::new("other-run");
    let outcome = traced(&dir.0, |_| async {
        let other = std::thread::scope(|scope| {
            let other = scope.spawn(|| {
                runtime::run(2, |world| async move {
                    world.add_node(1, NodeClock::default());
                    let task = world.spawn_node(1, async { panic!("planted") });
                    assert!(task.await.is_err());
                });
                attributed(|| std::panic::catch_unwind(|| panic!("before any world")))
            });
            other.join().unwrap()
        });
        assert!(other.is_err());
    });
    outcome.unwrap();
    assert_eq!(dir.traces().len(), 1);
}

#[test]
fn a_replaced_panic_hook_refuses_the_trace() {
    // The hook is the process's, so this happens in a process of its own.
    let dir = Scratch::new("hook");
    let said = child("replaced-hook", &dir.0, &[]);
    assert!(said.contains("the panic hook was replaced"), "{said}");
    assert!(dir.traces().is_empty());
    // Another trace that puts it in place again does not help the first one.
    let said = child("restored-hook", &dir.0, &[]);
    assert!(said.contains("replaced during the run"), "{said}");
    assert!(dir.traces().is_empty());
}

/// The hook stands in front of the one it found, and passes panics on to it.
#[test]
fn the_panic_hook_passes_panics_on() {
    let dir = Scratch::new("passes-on");
    let said = child("hook-before", &dir.0, &[]);
    assert!(said.contains("seen by the earlier hook: planted"), "{said}");
    assert!(said.contains("panicked at"), "{said}");
    assert!(dir.traces().is_empty());
}

/// A run never writes into a file that is already there, whoever made it.
#[test]
fn a_partial_file_that_exists_is_not_taken_over() {
    let dir = Scratch::new("taken");
    let said = child("taken-partial", &dir.0, &[]);
    assert!(said.contains("installed: Err"), "{said}");
    assert!(said.contains("left: \"of somebody else\""), "{said}");
    assert!(dir.traces().is_empty());
}

/// Every thread opens its first trace, and so installs the hook, at the same time.
#[test]
fn many_threads_may_open_their_first_trace_at_once() {
    let dir = Scratch::new("first-traces");
    let said = child("first-traces", &dir.0, &[]);
    assert!(said.contains("finished: 16"), "{said}");
}

/// A directory that can be written and searched but not read cannot be synced.
#[cfg(unix)]
#[test]
fn a_directory_that_cannot_be_opened_leaves_no_trace() {
    let _together = together();
    use std::os::unix::fs::PermissionsExt;
    let dir = Scratch::new("unreadable");
    let mode = |mode| fs::set_permissions(&dir.0, fs::Permissions::from_mode(mode)).unwrap();
    let result = runtime::run(1, |world| {
        let trace = builder().install_in(&world, &dir.0).unwrap();
        apply(&trace.steps(), 0);
        mode(0o333);
        std::future::ready(trace.finish())
    });
    let readable = fs::File::open(&dir.0).is_ok();
    mode(0o755);
    if readable {
        eprintln!("not checked: this user reads every directory");
        return;
    }
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
    assert!(dir.traces().is_empty(), "{:?}", dir.traces());
    assert_eq!(dir.withdrawn(), 0, "it never had its name");
}

/// Two processes with one directory and seed, whose lines differ.
#[test]
fn another_process_cannot_write_into_a_published_trace() {
    let dir = Scratch::new("two-processes");
    let start = |name: &str| spawn(name, &dir.0, &[]);
    let (short, long) = (start("short-run"), start("long-run"));
    let said = |child: std::process::Child| {
        let out = child.wait_with_output().unwrap();
        String::from_utf8_lossy(&out.stdout).into_owned()
    };
    let short = said(short);
    assert!(short.contains("finished: Ok"), "{short}");
    let long = said(long);
    assert!(long.contains("finished: Err"), "{long}");
    let traces = dir.traces();
    assert_eq!(traces.len(), 1);
    let text = fs::read_to_string(&traces[0]).unwrap();
    assert_eq!(text.lines().count(), 1 + 20, "only the short run's lines");
    let numbered = text.lines().skip(1).enumerate();
    for (n, line) in numbered {
        let line: Value = serde_json::from_str(line).unwrap();
        assert_eq!((&line["seq"], &line["vars"]["n"]), (&json!(n), &json!(n)));
    }
}

fn wait_for(path: &Path) {
    for _ in 0..3_000 {
        if path.exists() {
            return;
        }
        #[allow(clippy::disallowed_methods)] // a real wait for another process
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("{} never appeared", path.display());
}

/// A run that panics before it has a World leaves the traces of the seeds next to it.
#[test]
fn a_seed_that_panics_before_its_world_leaves_the_other_seeds_traces() {
    let _together = together();
    let dir = Scratch::new("sweep-panic");
    let config = SweepConfig {
        artifacts: dir.0.join("artifacts"),
        ..SweepConfig::new("panics", 1, 1)
    };
    traced(&dir.0, |_| async {
        let report = std::thread::scope(|scope| {
            let other = scope.spawn(|| sweep(&config, |_, _| panic!("planted")));
            other.join().unwrap()
        });
        assert!(!report.passed());
    })
    .unwrap();
    assert_eq!(dir.traces().len(), 1);
}

/// Storage pauses and clock freezes are faults of the World like any other.
#[test]
fn storage_pauses_and_clock_freezes_are_logged() {
    let _together = together();
    let dir = Scratch::new("suspend");
    let path = traced(&dir.0, |world| async move {
        world.mount(1, "/data");
        world.pause_storage(1, StoragePause::before(FsOp::Write).hold(ms(5)));
        world.pause_storage(1, StoragePause::after(FsOp::SyncData).hold(ms(5)));
        let job = world.spawn_node(1, async {
            nabedra_rt::task::spawn_blocking(|| {
                let disk = nabedra_rt::fs::real();
                let file = disk.open(Path::new("/data/log"), OpenMode::CreateNew);
                let file = file.unwrap();
                file.write_all_at(b"x", 0).unwrap();
                file.sync_data().unwrap();
            })
            .await
            .unwrap();
        });
        job.await.unwrap();
        world.freeze_after_clock_read(1, ClockRead::nth(1));
        let read = world.spawn_node(1, async {
            let _ = nabedra_rt::time::now();
        });
        settle().await;
        world.thaw(1);
        read.await.unwrap();
    })
    .unwrap();
    let text = fs::read_to_string(path).unwrap();
    let faults: Vec<&str> = text
        .lines()
        .filter_map(|line| line.split_once("\"node\":\"1\",").map(|(_, rest)| rest))
        .collect();
    assert_eq!(
        faults,
        [
            r#""action":"StoragePaused","vars":{},"msg":{"op":"Write","path":"/data/log","side":"before"}}"#,
            r#""action":"StorageResumed","vars":{}}"#,
            r#""action":"StoragePaused","vars":{},"msg":{"op":"SyncData","path":"/data/log","side":"after"}}"#,
            r#""action":"StorageResumed","vars":{}}"#,
            r#""action":"ClockFrozen","vars":{}}"#,
        ]
    );
}

fn ms(ms: u64) -> Duration {
    Duration::from_millis(ms)
}

/// Where the sources are, not where `GIT_DIR` points.
#[test]
fn the_environment_does_not_choose_the_repository() {
    let other = Scratch::new("other-repository");
    let git = |args: &[&str]| {
        let out = Command::new("git")
            .args(["-c", "user.name=t", "-c", "user.email=t@example.invalid"])
            .args(["-c", "commit.gpgsign=false"])
            .args(args)
            .current_dir(&other.0)
            .output()
            .unwrap();
        assert!(out.status.success(), "{args:?}: {out:?}");
        String::from_utf8(out.stdout).unwrap().trim().to_owned()
    };
    git(&["init", "-q"]);
    git(&["commit", "-q", "--allow-empty", "-m", "other"]);
    let wrong = git(&["rev-parse", "HEAD"]);
    let dir = Scratch::new("git-dir");
    let git_dir = other.0.join(".git");
    let said = child(
        "plain-run",
        &dir.0,
        &[("GIT_DIR", git_dir.to_str().unwrap())],
    );
    assert!(said.contains("finished: Ok"), "{said}");
    let text = fs::read_to_string(&dir.traces()[0]).unwrap();
    assert!(!text.contains(&wrong), "the trace names another repository");
}

/// A shrinking sweep reruns a failing seed with other tapes. They must not touch its trace.
#[test]
fn shrinking_leaves_the_trace_of_the_failing_seed() {
    let dir = Scratch::new("shrink");
    let said = child(
        "shrinking-sweep",
        &dir.0,
        &[
            ("NABEDRA_TRACE_DIR", dir.0.to_str().unwrap()),
            ("NABEDRA_TRACE_UNCLEAN", "mark"),
        ],
    );
    assert!(said.contains("shrunk: true"), "{said}");
    let traces = dir.traces();
    assert_eq!(traces.len(), 1, "{said}");
    let text = fs::read_to_string(&traces[0]).unwrap();
    let drawn = said
        .lines()
        .find_map(|line| line.strip_prefix("drawn: "))
        .unwrap();
    assert!(
        text.lines()
            .last()
            .unwrap()
            .contains(&format!("{{\"n\":{drawn}}}")),
        "the trace is not that of the first tape: {text}"
    );
}

const CHILD: &str = "NABEDRA_TRACE_FAILURES_CHILD";
const CHILD_DIR: &str = "NABEDRA_TRACE_FAILURES_DIR";

fn spawn(name: &str, dir: &Path, vars: &[(&str, &str)]) -> std::process::Child {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_process", "--nocapture"])
        .env_remove("NABEDRA_TRACE_DIR")
        .env(CHILD, name)
        .env(CHILD_DIR, dir)
        .envs(vars.iter().copied())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap()
}

/// What the child process called `name` printed.
fn child(name: &str, dir: &Path, vars: &[(&str, &str)]) -> String {
    let out = spawn(name, dir, vars).wait_with_output().unwrap();
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// The other process of a test above, and nothing on its own.
#[test]
fn child_process() {
    let Ok(name) = std::env::var(CHILD) else {
        return;
    };
    let dir = PathBuf::from(std::env::var_os(CHILD_DIR).unwrap());
    let opened = |name: &str| dir.join(format!("{name}.opened"));
    let published = dir.join("Spec-1.ndjson");
    match name.as_str() {
        "replaced-hook" => {
            let result = traced(&dir, |_| async {
                std::panic::set_hook(Box::new(|_| {}));
            });
            println!("finished: {result:?}");
        }
        "hook-before" => {
            std::panic::set_hook(Box::new(|info| {
                println!(
                    "seen by the earlier hook: {}",
                    info.payload_as_str().unwrap_or("?")
                );
            }));
            let result = traced(&dir, |_| async {
                nabedra_rt::task::spawn(async { panic!("planted") });
                settle().await;
            });
            println!("finished: {result:?}");
        }
        "taken-partial" => runtime::run(1, |world| {
            // The name of this process's first partial file.
            let name = format!("Spec-1.ndjson.{}-0.partial", std::process::id());
            fs::write(dir.join(&name), "of somebody else").unwrap();
            let installed = builder().install_in(&world, &dir);
            println!("installed: {:?}", installed.map(|_| "opened"));
            println!("left: {:?}", fs::read_to_string(dir.join(&name)).unwrap());
            std::future::ready(())
        }),
        "restored-hook" => {
            let result = traced(&dir, |_| async {
                std::panic::set_hook(Box::new(|_| {}));
                let other = std::thread::scope(|scope| {
                    let other = scope.spawn(|| traced(&dir.join("other"), |_| async {}));
                    other.join().unwrap()
                });
                assert!(other.is_ok());
            });
            println!("finished: {result:?}");
        }
        "first-traces" => {
            let barrier = std::sync::Barrier::new(16);
            let finished = std::thread::scope(|scope| {
                let runs: Vec<_> = (0..16)
                    .map(|n| {
                        let dir = dir.join(n.to_string());
                        let barrier = &barrier;
                        scope.spawn(move || {
                            barrier.wait();
                            traced(&dir, |_| async {}).is_ok()
                        })
                    })
                    .collect();
                runs.into_iter()
                    .filter_map(|run| run.join().ok())
                    .filter(|ok| *ok)
                    .count()
            });
            println!("finished: {finished}");
        }
        "plain-run" => {
            let result = traced(&dir, |_| async {});
            println!("finished: {result:?}");
        }
        "short-run" | "long-run" => runtime::run(1, |world| {
            let trace = builder().install_in(&world, &dir).unwrap();
            fs::write(opened(&name), "").unwrap();
            let lines = if name == "short-run" { 20 } else { 2_000 };
            // Both are open before either finishes, and the long one finishes last.
            wait_for(&opened("short-run"));
            wait_for(&opened("long-run"));
            for n in 0..lines {
                apply(&trace.steps(), n);
            }
            if name == "long-run" {
                wait_for(&published);
            }
            println!("finished: {:?}", trace.finish());
            std::future::ready(())
        }),
        "shrinking-sweep" => {
            let mut config = SweepConfig::new("shrink", 1, 1);
            config.shrink = true;
            config.artifacts = dir.join("artifacts");
            let first = std::sync::Mutex::new(None);
            let runs = std::sync::atomic::AtomicU64::new(0);
            let report = sweep(&config, |seed, g| {
                runs.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let drawn = 10 + g.below(1_000);
                first.lock().unwrap().get_or_insert(drawn);
                runtime::run(seed, |world| {
                    if let Some(trace) = builder().install(&world).unwrap() {
                        apply(&trace.steps(), drawn);
                        trace.finish().unwrap();
                    }
                    std::future::ready(())
                });
                SeedRun {
                    seed,
                    violations: vec!["Planted: always".into()],
                    ..SeedRun::default()
                }
            });
            assert!(!report.passed());
            let runs = runs.into_inner();
            println!("\ndrawn: {}", first.into_inner().unwrap().unwrap());
            // Once, once again as a failing seed, and then with other tapes.
            println!("shrunk: {}", runs > 2);
        }
        other => panic!("no child called {other}"),
    }
}
