//! Unit tests of the simulation core. Run with `cargo test -p nabedra-sim --lib`, and for
//! the seeded runtime also with `RUSTFLAGS="--cfg tokio_unstable"`.
//!
//! `compat` pins the values the Events simulator produces today (copied reference code and
//! literal constants), so the Events migration can tell whether its trace hashes change.
//! `toy` is a small cluster on the real `nabedra_rt::sim::World` that exercises the whole
//! core: net plan, nemesis, trace, observer, sweep, shrink and planted search.

mod compat;
mod nemesis;
mod net;
mod raft;
mod shrink;
mod sweep;
mod toy;

use std::time::Duration;

use crate::runtime;

#[test]
fn run_installs_a_seeded_world_on_a_paused_runtime() {
    let elapsed = runtime::run(5, |world| async move {
        assert_eq!(world.seed(), 5);
        assert!(nabedra_rt::sim::world().is_some());
        // An hour of virtual time passes at once on the paused clock.
        nabedra_rt::time::sleep(Duration::from_secs(3_600)).await;
        runtime::virtual_ms(&world)
    });
    assert_eq!(elapsed, 3_600_000);
    assert!(
        nabedra_rt::sim::world().is_none(),
        "the world is uninstalled"
    );
    let flavor = runtime::runtime_seeded(1)
        .unwrap()
        .handle()
        .runtime_flavor();
    assert_eq!(flavor, tokio::runtime::RuntimeFlavor::CurrentThread);
    assert_eq!(runtime::SEEDED, cfg!(tokio_unstable));
}

/// Node streams come from the world seed through `nabedra-rt`, so two worlds with one seed
/// draw the same ids and two seeds draw different ones.
#[test]
fn node_streams_follow_the_world_seed() {
    let draw = |seed| {
        runtime::run(seed, |world| async move {
            world.add_node(1, nabedra_rt::sim::NodeClock::default());
            nabedra_rt::sim::in_node(1, async {
                (nabedra_rt::ids::random_u64(), nabedra_rt::ids::uuid_v4())
            })
            .await
        })
    };
    assert_eq!(draw(3), draw(3));
    assert_ne!(draw(3), draw(4));
}

#[test]
fn runtime_is_entered_when_the_body_constructs_its_future() {
    runtime::run(5, |_| nabedra_rt::time::sleep(Duration::from_millis(1)));
}

#[cfg(tokio_unstable)]
#[test]
fn select_branch_order_follows_the_runtime_seed() {
    let select_order = |seed| {
        runtime::run(seed, |_| async {
            let mut branches = Vec::new();
            for _ in 0..64 {
                branches.push(tokio::select! {
                    _ = std::future::ready(()) => 0,
                    _ = std::future::ready(()) => 1,
                });
            }
            branches
        })
    };
    assert_eq!(select_order(5), select_order(5));
    assert_ne!(select_order(5), select_order(6));
}
