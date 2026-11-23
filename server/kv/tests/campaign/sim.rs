//! Enumerated NS04 and sampled NS05 cuts on stepped LSM services.
//! The recovery and history oracle are shared with the threaded campaign.

use std::fmt::Debug;
use std::slice;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use nabedra_rt::sim::{self, NodeClock, SimEvent, in_node};

use crate::common::Rng;
use crate::harness::{Config, Exec, Report};
use crate::workload::{CutKind, Mix, Step, generate};
use crate::{enumerated_cut, env_u64, summary};

const NODE: u64 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Digest {
    hash: u64,
    count: u64,
}

impl Default for Digest {
    fn default() -> Self {
        Self {
            hash: 0xcbf2_9ce4_8422_2325,
            count: 0,
        }
    }
}

impl Digest {
    fn add(&mut self, event: &impl Debug) {
        self.count += 1;
        for byte in format!("{event:?}\n").bytes() {
            self.hash = (self.hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3);
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Boundary {
    Armed { after: u64, kind: CutKind },
    End(CutKind),
}

#[derive(Debug, PartialEq, Eq)]
struct Run {
    report: Report,
    service_trace: Digest,
    fs_log: Digest,
    startup_ops: u64,
    workload_ops: u64,
}

fn run_once(cfg: &Config, steps: &[Step], cut: Option<Boundary>) -> Run {
    sim::run(cfg.seed, |world| async move {
        world.add_node(NODE, NodeClock::default());
        let service_trace = Arc::new(Mutex::new(Digest::default()));
        let trace = service_trace.clone();
        world.observe(Arc::new(move |event| {
            if let SimEvent::Step { node, .. } = event {
                assert_eq!(*node, Some(NODE));
                trace.lock().unwrap().add(event);
            }
        }));
        let fs_log = Arc::new(Mutex::new(Digest::default()));
        let log = fs_log.clone();
        let (report, startup_ops, workload_ops) = in_node(NODE, async move {
            let mut exec = Exec::new(cfg, None);
            exec.fs_observer = Some(Arc::new(move |call| log.lock().unwrap().add(call)));
            exec.start()?;
            let startup_ops = exec.fs.mutating_ops();
            if let Some(Boundary::Armed { after, kind }) = cut {
                exec.steps(&[Step::ArmCrash { after, kind }])?;
            }
            let mut yields = Rng(cfg.seed);
            for step in steps {
                exec.steps(slice::from_ref(step))?;
                if yields.chance(50) {
                    tokio::task::yield_now().await;
                }
            }
            // Only a reference run stays on one image for the whole workload.
            let workload_ops = if !cfg.mix.cuts && cut.is_none() {
                exec.fs.mutating_ops() - startup_ops
            } else {
                0
            };
            if let Some(Boundary::End(kind)) = cut {
                exec.steps(&[Step::Cut(kind)])?;
            }
            exec.finish()?;
            Ok::<_, String>((std::mem::take(&mut exec.report), startup_ops, workload_ops))
        })
        .await
        .unwrap_or_else(|error| panic!("LSM01-sim seed {} {cut:?}: {error}", cfg.seed));

        let helpers = world.services(NODE);
        assert!(helpers.is_empty(), "KV used helper threads: {helpers:?}");
        let services = world.stepped_services(NODE);
        for name in ["kv-journal", "kv-journal-prep", "kv-flush", "kv-compact"] {
            assert!(
                services.iter().any(|service| service.name == name),
                "KV did not register the stepped service {name}"
            );
        }
        let service_trace = service_trace.lock().unwrap().clone();
        let fs_log = fs_log.lock().unwrap().clone();
        assert!(service_trace.count > 0, "no stepped services ran");
        assert!(fs_log.count > 0, "no filesystem calls were traced");
        Run {
            report,
            service_trace,
            fs_log,
            startup_ops,
            workload_ops,
        }
    })
}

fn replay(cfg: &Config, steps: &[Step], cut: Option<Boundary>) -> Run {
    let first = run_once(cfg, steps, cut);
    let second = run_once(cfg, steps, cut);
    assert_eq!(first, second, "LSM01-sim replay seed {} {cut:?}", cfg.seed);
    first
}

#[test]
fn lsm01_sim_ns04_enumerated_seed_replays() {
    let seed = 100;
    let mix = Mix {
        cuts: false,
        faults: false,
        ..Mix::full()
    };
    let cfg = Config::new(seed, mix);
    let steps = generate(seed, 30, &mix);
    let reference = replay(&cfg, &steps, None);
    assert!(reference.workload_ops > 0);
    let mut total = Report::default();
    // After opening the machine, enumerate every boundary in the workload.
    // At the end boundary there is no later workload call to trip an armed cut.
    for after in 0..=reference.workload_ops {
        let kind = enumerated_cut(after);
        let boundary = if after == reference.workload_ops {
            Boundary::End(kind)
        } else {
            Boundary::Armed { after, kind }
        };
        let run = replay(&cfg, &steps, Some(boundary));
        assert_eq!(run.report.cuts, 1, "NS04 seed {seed}, boundary {after}");
        assert_eq!(
            run.report.first_cut_at,
            Some(reference.startup_ops + after),
            "NS04 seed {seed} missed filesystem boundary {after}"
        );
        total.add(&run.report);
    }
    assert_eq!(total.cuts, reference.workload_ops + 1);
    eprintln!(
        "LSM01-sim NS04 seed {seed}: {} enumerated cuts replayed ({} crash, {} kill, {} power loss); reference trace {:016x}/{} events, fs {:016x}/{} calls",
        total.cuts,
        total.crash_cuts,
        total.kill_cuts,
        total.power_loss_cuts,
        reference.service_trace.hash,
        reference.service_trace.count,
        reference.fs_log.hash,
        reference.fs_log.count
    );
}

#[test]
fn lsm01_sim_ns05_sampled_seed_replays() {
    let seed = 6;
    let mix = Mix::full();
    let cfg = Config::new(seed, mix);
    let steps = generate(seed, 150, &mix);
    let run = replay(&cfg, &steps, None);
    assert!(
        run.report.power_loss_cuts > 0,
        "NS05 seed {seed} cut no power"
    );
    eprintln!(
        "LSM01-sim NS05 seed {seed}: {} steps, {} cuts ({} power loss), trace {:016x}/{} events, fs {:016x}/{} calls",
        run.report.steps,
        run.report.cuts,
        run.report.power_loss_cuts,
        run.service_trace.hash,
        run.service_trace.count,
        run.fs_log.hash,
        run.fs_log.count
    );
}

/// Run the general NS10 workload twice per seed against the serial oracle, with
/// stepped services on both runs. The NS04 smoke case separately enumerates all
/// write boundaries for one seed.
#[test]
fn lsm01_sampled_replay_sweep() {
    let first = env_u64("NS10_SEED_BASE", 0);
    let seeds = env_u64("NS10_SEEDS", 20);
    let steps = usize::try_from(env_u64("NS10_STEPS", 400)).expect("NS10_STEPS fits usize");
    assert!(
        seeds > 0 && steps > 0,
        "the replay sweep must execute a workload"
    );
    let end = first.checked_add(seeds).expect("seed range overflows u64");
    let mix = Mix::full();
    let started = Instant::now();
    let mut total = Report::default();
    let mut digest = Digest::default();

    for seed in first..end {
        let cfg = Config::new(seed, mix);
        let workload = generate(seed, steps, &mix);
        let run = replay(&cfg, &workload, None);
        assert_eq!(run.report.steps, steps as u64, "LSM01-sim seed {seed}");
        digest.add(&(seed, &run.service_trace, &run.fs_log));
        eprintln!(
            "LSM01-sim seed {seed}: {} steps, {} cuts ({} power loss), trace {:016x}/{} events, fs {:016x}/{} calls",
            run.report.steps,
            run.report.cuts,
            run.report.power_loss_cuts,
            run.service_trace.hash,
            run.service_trace.count,
            run.fs_log.hash,
            run.fs_log.count
        );
        total.add(&run.report);
    }

    assert_eq!(total.runs, seeds);
    assert_eq!(digest.count, seeds);
    assert!(total.power_loss_cuts > 0, "NS05 cut no power");
    assert!(total.crash_cuts > 0, "the workload executed no crash cuts");
    assert!(total.faults > 0 && total.releases > 0);
    summary("LSM01-sim sampled fault model", seeds, started, &total);
    eprintln!(
        "LSM01-sim seeds {first}..{end}: {seeds} primary runs and {seeds} matching replays, {:016x}/{} paired trace digests",
        digest.hash, digest.count
    );
}
