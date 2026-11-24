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
const MIN_APPLY_REPLY_BYTES: i64 = 128 << 10;

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
            knobs.push(Knob::IntRange(drift, -200, 200));
            knobs.push(Knob::IntRange(jump, -2_000, 2_000));
        }
        let mut generator = Gen::from_seed(stream_seed(seed, "oltp-config"));
        let mut knobs = sample(&mut generator, &knobs);
        knobs.insert("direct_oltp".into(), json!(true));
        Self { knobs }
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
