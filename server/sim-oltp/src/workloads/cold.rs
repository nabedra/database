//! The `cold` workload: `direct` load, then every worker stops (`kill` or `power`) with
//! requests in flight, and the workers reopen their retained volumes with start delays of
//! 0 to 3 s each (O15, D09, ACC3). The cold start must finish within 120 s of virtual time
//! after the last start. Acknowledged writes survive and dedup records replay the original
//! results; the final reads and the history checks verify both.

use nabedra_check::Gen;

use super::clients::Ctx;
use crate::{
    disk::DiskCut,
    nemesis::{
        apply::{COLD_LIMIT, Chaos},
        window,
    },
    node::Cluster,
};

/// The drawn full stop.
pub(crate) struct Stop {
    power: bool,
    delays: [std::time::Duration; 3],
}

impl Stop {
    pub(crate) fn draw(g: &mut Gen) -> Self {
        Self {
            power: g.bool(),
            delays: [(); 3].map(|()| window(g, 0, 3_000)),
        }
    }

    pub(crate) async fn run(
        &self,
        cluster: &mut Cluster,
        ctx: &Ctx,
        chaos: Option<&mut Chaos>,
    ) -> Result<(), String> {
        // Restarts the nemesis still owes would only race the full stop.
        if let Some(chaos) = chaos {
            chaos.heal(cluster, ctx).await?;
            chaos.abandon_split();
        }
        let cut = if self.power {
            DiskCut::Power
        } else {
            DiskCut::Kill
        };
        cluster.record(format!(
            "cold stop {cut:?}, starts after {:?}",
            self.delays
        ));
        ctx.history.lock().unwrap().marker(
            &cluster.world,
            "cold-stop",
            serde_json::json!({
                "power": self.power,
                "delays_ms": self.delays.map(|delay| delay.as_millis() as u64),
            }),
        );
        cluster
            .cold_start_staggered("cold start", cut, self.delays, COLD_LIMIT)
            .await
    }
}
