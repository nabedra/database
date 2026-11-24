pub(crate) use nabedra_resources::{perf_stage, stage_timing::*};

/// Inside a simulation world, every simulated process shares these
/// process-wide histograms, which also time host work. Report none, so a
/// seed's public stats, and the HTTP bytes that carry them, depend only on
/// its own world.
#[cfg(feature = "sim")]
pub(crate) fn snapshot() -> serde_json::Value {
    if nabedra_rt::sim::world().is_some() {
        return serde_json::Value::Object(serde_json::Map::new());
    }
    nabedra_resources::stage_timing::snapshot()
}

#[cfg(all(test, feature = "sim"))]
mod sim_tests {
    /// A seed's stats must not reveal stages that an earlier world in this
    /// process recorded (oltp-sim seed 2 replayed differently when cold).
    #[test]
    fn sim_world_stats_omit_process_stage_timing() {
        stage("test.sim.process_stage").record(std::time::Duration::from_micros(3));
        assert!(
            super::snapshot()
                .as_object()
                .is_some_and(|stages| stages.contains_key("test.sim.process_stage"))
        );
        nabedra_rt::sim::run(1, |_| async {
            assert_eq!(super::snapshot(), serde_json::json!({}));
        });
    }

    use super::stage;
}
