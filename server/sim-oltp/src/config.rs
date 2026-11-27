use std::{collections::BTreeMap, time::Duration};

use nabedra_check::{
    Gen,
    config::{Knob, sample},
};
use nabedra_database_server::oltp_runtime::sim::Limits;
use nabedra_rt::sim::{NodeClock, World};
use nabedra_sim::{fnv, rng::stream_seed};
use nabedra_transport::SimLimits;
use serde_json::{Value, json};

use crate::bootstrap::{BOOTSTRAP_CLIENT, CLIENTS, GATEWAYS, WORKERS};

/// Worker and gateway drift may change during a run within this bound.
pub(crate) const CLOCK_DRIFT_BOUND_PPM: i64 = 200;

const CLOCKS: &[(u64, &str, &str)] = &[
    (1, "n1.drift_ppm", "n1.wall_jump_ms"),
    (2, "n2.drift_ppm", "n2.wall_jump_ms"),
    (3, "n3.drift_ppm", "n3.wall_jump_ms"),
    (101, "n101.drift_ppm", "n101.wall_jump_ms"),
    (102, "n102.drift_ppm", "n102.wall_jump_ms"),
    (1001, "n1001.drift_ppm", "n1001.wall_jump_ms"),
    (1002, "n1002.drift_ppm", "n1002.wall_jump_ms"),
    (1003, "n1003.drift_ppm", "n1003.wall_jump_ms"),
];

/// The largest fixed Data apply reply budget, `META.output_bytes` in
/// `oltp-replication`'s `hosted.rs`.
pub(crate) const MIN_APPLY_REPLY_BYTES: i64 = 128 << 10;

pub(crate) struct Config {
    knobs: BTreeMap<String, Value>,
}

impl Config {
    pub fn sample(seed: u64) -> Self {
        let mut knobs = vec![
            Knob::Bool("data.direct.batch"),
            Knob::IntRange("request_deadline_ms", 250, 10_000),
            Knob::IntRange("executor.max_in_flight", 4, 256),
            Knob::IntRange("transport.session_slots", 4, 256),
            // Production fixes this arena at 1 MiB. A Data apply reserves up to
            // 128 KiB of it at once, and a larger request than the arena can never
            // be granted, so smaller samples only stall worker startup.
            Knob::IntRange("data.apply_reply_bytes", MIN_APPLY_REPLY_BYTES, 1 << 20),
        ];
        for &(_, drift, jump) in CLOCKS {
            knobs.push(Knob::IntRange(
                drift,
                -CLOCK_DRIFT_BOUND_PPM,
                CLOCK_DRIFT_BOUND_PPM,
            ));
            knobs.push(Knob::IntRange(jump, -2_000, 2_000));
        }
        let mut generator = Gen::from_seed(stream_seed(seed, "oltp-config"));
        let mut knobs = sample(&mut generator, &knobs);
        knobs.insert("direct_oltp".into(), json!(true));
        Self { knobs }
    }

    /// Override one sampled knob, as a seed-search focus token (`batch`) or the `resources`
    /// overlay asks.
    pub fn force(&mut self, knob: &str, value: Value) {
        assert!(self.knobs.contains_key(knob), "unknown knob {knob}");
        self.knobs.insert(knob.into(), value);
    }

    pub fn deadline(&self) -> Duration {
        Duration::from_millis(self.knobs["request_deadline_ms"].as_u64().unwrap())
    }

    pub fn setup(&self, world: &World) {
        let nodes = WORKERS
            .into_iter()
            .chain(GATEWAYS)
            .chain([BOOTSTRAP_CLIENT])
            .chain(CLIENTS);
        assert!(nodes.eq(CLOCKS.iter().map(|clock| clock.0)));
        for &(node, drift, _) in CLOCKS {
            world.add_node(
                node,
                NodeClock {
                    drift_ppm: self.knobs[drift].as_i64().unwrap(),
                    ..Default::default()
                },
            );
        }
        // Only these clocks can slow down after arming a timer.
        for node in WORKERS.into_iter().chain(GATEWAYS) {
            world.set_timer_floor_ppm(node, Some(-CLOCK_DRIFT_BOUND_PPM));
        }
        let limit = |key| self.knobs[key].as_u64().unwrap() as usize;
        world.insert_ext(Limits {
            direct_deadline: self.deadline(),
            direct_max_in_flight: limit("executor.max_in_flight"),
            direct_batch: self.knobs["data.direct.batch"].as_bool().unwrap(),
            data_apply_reply_bytes: limit("data.apply_reply_bytes"),
        });
        world.insert_ext(SimLimits {
            session_slots: limit("transport.session_slots"),
            direct_handler_slots: limit("executor.max_in_flight"),
        });
    }

    pub fn jump_clocks(&self, world: &World) {
        for &(node, _, jump) in CLOCKS {
            let ms = self.knobs[jump].as_i64().unwrap();
            let delta = Duration::from_millis(ms.unsigned_abs());
            if ms < 0 {
                world.jump_wall_backward(node, delta);
            } else {
                world.jump_wall_forward(node, delta);
            }
        }
    }

    pub fn report(&self) -> Value {
        let bytes = serde_json::to_vec(&self.knobs).expect("sampled configuration");
        let mut config = json!(self.knobs);
        config["hash"] = json!(format!("{:016x}", fnv(&bytes)));
        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nabedra_rt::{sim, time};
    use tokio::sync::oneshot;

    fn timer_earliness(node: u64, clear_floor: bool) -> Duration {
        sim::run(1, move |world| async move {
            Config::sample(1).setup(&world);
            world.set_drift_ppm(node, 0);
            if clear_floor {
                world.set_timer_floor_ppm(node, None);
            }

            let (armed, wait_armed) = oneshot::channel();
            let timer = world.spawn_node(node, async move {
                let deadline = time::now() + Duration::from_secs(10);
                let sleep = time::sleep_until(deadline);
                armed.send(()).unwrap();
                sleep.await;
                deadline.saturating_duration_since(time::now())
            });
            wait_armed.await.unwrap();
            world.set_drift_ppm(node, -CLOCK_DRIFT_BOUND_PPM);
            timer.await.unwrap()
        })
    }

    #[test]
    fn setup_keeps_timers_from_firing_before_their_node_deadline() {
        for node in WORKERS.into_iter().chain(GATEWAYS) {
            assert_eq!(timer_earliness(node, false), Duration::ZERO, "node {node}");
        }

        // Revert only the setup floor. The same slowdown now fires early.
        assert!(timer_earliness(WORKERS[0], true) > Duration::ZERO);
        // Client clocks are fixed during the workload. The forced slowdown
        // demonstrates that setup did not install a timer floor on them.
        for node in [BOOTSTRAP_CLIENT].into_iter().chain(CLIENTS) {
            assert!(timer_earliness(node, false) > Duration::ZERO, "node {node}");
        }
    }

    /// The production range is not raised to the number of peers (lead decision 12.4): the
    /// product fixes 256 slots and lets automatic growth reach 4,096 workers, so a worker can
    /// have fewer slots than peers. Seed 66 samples four slots for five peers and stays red.
    #[test]
    fn session_slots_keep_the_sampled_value() {
        let config = Config::sample(66);
        assert_eq!(config.knobs["transport.session_slots"], json!(4));
    }

    /// Seed 22 sampled a 124,761-byte arena and no worker ever bound HTTP.
    #[test]
    fn sampled_reply_arena_fits_the_largest_apply_budget() {
        for seed in 1..=2_000 {
            let config = Config::sample(seed);
            let bytes = config.knobs["data.apply_reply_bytes"].as_i64().unwrap();
            assert!(
                (MIN_APPLY_REPLY_BYTES..=1 << 20).contains(&bytes),
                "seed {seed}: {bytes}"
            );
        }
    }
}
