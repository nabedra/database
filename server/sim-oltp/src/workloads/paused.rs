//! D05 as its contract row and `suite_d05` in `tests/oltp_direct_acceptance.py` run it: on a
//! converged cluster, at the 10 s request deadline, pause one follower and then both.
//!
//! One paused: every write succeeds within the deadline. The survivors drop the paused worker,
//! and it joins the ready set again after it resumes, without a restart. Two paused: no success,
//! and a `not_applied` answer leaves the row unchanged. After an unknown outcome the row holds
//! the old or the new value, and the same ID and body then returns one result.

use std::time::Duration;

use nabedra_rt::{ids::uuid_v4, sim::in_node, time};
use nabedra_sim::{Draw, rng::stream};
use serde_json::{Value, json};

use super::{
    Workload,
    clients::{self, Ctx, Request},
};
use crate::{
    bootstrap::{CLIENTS, GATEWAYS, WORKERS},
    config::Config,
    history::{Client, Completion},
    node::Cluster,
};

/// The deadline of the contract's runner, and the product's own.
pub(crate) const DEADLINE: Duration = Duration::from_secs(10);
/// `suite_d05` allows half a second more than the deadline.
const WRITE_LIMIT: Duration = Duration::from_millis(10_500);
const DROP_LIMIT: Duration = Duration::from_secs(90);
const REJOIN_LIMIT: Duration = Duration::from_secs(120);
const RECOVER_LIMIT: Duration = Duration::from_secs(180);
const WRITES: i64 = 5;
const KEY: i64 = 1;

/// Whether this seed samples the D05 deadline. One `direct` seed in four does.
pub(crate) fn samples(seed: u64, workload: Workload) -> bool {
    workload == Workload::Direct && stream(seed, "oltp-d05").chance(250)
}

pub(crate) fn selected(config: &Config, workload: Workload) -> bool {
    workload == Workload::Direct && config.deadline() == DEADLINE
}

struct Run<'a> {
    cluster: &'a mut Cluster,
    ctx: &'a Ctx,
    client: Client,
    value: i64,
}

impl Run<'_> {
    async fn write(&mut self, gateway: u64) -> (Request, Completion, Duration) {
        let id = in_node(CLIENTS[0], async { uuid_v4() }).await;
        self.value += 1;
        let request = Request::row("table.upsert", &self.ctx.table, KEY, self.value, id);
        let started = self.cluster.world.elapsed();
        let answer = self.send(gateway, &request).await;
        (request, answer, self.cluster.world.elapsed() - started)
    }

    async fn send(&mut self, gateway: u64, request: &Request) -> Completion {
        clients::attempt(
            self.ctx,
            &mut self.client,
            CLIENTS[0],
            gateway,
            request,
            false,
        )
        .await
    }

    async fn read(&mut self, gateway: u64) -> Completion {
        let id = in_node(CLIENTS[0], async { uuid_v4() }).await;
        let request = Request::get(&self.ctx.table, KEY, id);
        self.send(gateway, &request).await
    }

    fn violate(&self, invariant: &str, detail: String) {
        self.cluster
            .checks
            .violate(&self.cluster.world, invariant, format!("D05: {detail}"));
    }

    /// The elected leader of the range, once a write succeeds.
    async fn leader(&mut self) -> Option<u64> {
        let (_, answer, _) = self.write(GATEWAYS[0]).await;
        if let Some(why) = clients::failure(&answer) {
            self.violate(
                "Liveness",
                format!("the healthy cluster refused a write: {why}"),
            );
            return None;
        }
        let leader = self.cluster.leader();
        if leader.is_none() {
            self.violate("Liveness", "no elected leader runs after a write".into());
        }
        leader
    }

    /// Poll a read until it succeeds.
    async fn recovered(&mut self) -> Option<Value> {
        let deadline = self.cluster.world.elapsed() + RECOVER_LIMIT;
        loop {
            let answer = self.read(GATEWAYS[0]).await;
            match answer {
                Completion::Ok(row) => return Some(row),
                _ if self.cluster.world.elapsed() >= deadline => {
                    let why = clients::failure(&answer);
                    self.violate("Liveness", format!("reads did not recover: {why:?}"));
                    return None;
                }
                _ => time::sleep(Duration::from_secs(1)).await,
            }
        }
    }

    async fn one_paused(&mut self, clauses: &mut Vec<&'static str>) -> Option<()> {
        let leader = self.leader().await?;
        let followers: Vec<u64> = others(leader);
        let paused = followers[(self.ctx.world.seed() % 2) as usize];
        let survivors = others(paused);
        self.cluster.record(format!(
            "D05 one paused: leader n{leader}, paused n{paused}"
        ));
        self.cluster.freeze(paused);
        for index in 0..WRITES {
            let gateway = GATEWAYS[(index % 2) as usize];
            let (request, answer, took) = self.write(gateway).await;
            let failed = match clients::failure(&answer) {
                Some(why) => Some(why),
                None => (took >= WRITE_LIMIT).then(|| format!("succeeded after {took:?}")),
            };
            if let Some(why) = failed {
                self.violate(
                    "OnePausedProgress",
                    format!(
                        "write {} through n{gateway} with only n{paused} paused: {why}",
                        request.id
                    ),
                );
            }
        }
        clauses.push("D05:one-paused");
        // Hold the pause until the owner has removed the paused worker, so the resume rejoins
        // it.
        let dropped = self
            .cluster
            .wait_ready_within(&survivors, true, survivors.len(), DROP_LIMIT)
            .await;
        self.cluster.thaw(paused);
        if let Err(error) = dropped {
            self.violate(
                "Liveness",
                format!("the paused n{paused} was not dropped: {error}"),
            );
        }
        let rejoined = self
            .cluster
            .wait_ready_within(&WORKERS, true, WORKERS.len(), REJOIN_LIMIT)
            .await;
        clauses.push("D05:rejoin");
        match rejoined {
            Ok(()) => Some(()),
            Err(error) => {
                self.violate("Liveness", format!("n{paused} did not rejoin: {error}"));
                None
            }
        }
    }

    async fn two_paused(&mut self, clauses: &mut Vec<&'static str>) -> Option<()> {
        self.recovered().await?;
        let leader = self.leader().await?;
        let before = json!({"id": KEY, "value": self.value});
        let followers = others(leader);
        self.cluster.record(format!(
            "D05 two paused: leader n{leader}, paused {followers:?}, row {before}"
        ));
        for &node in &followers {
            self.cluster.freeze(node);
        }
        let (request, answer, _) = self.write(GATEWAYS[0]).await;
        let written = json!({"id": KEY, "value": self.value});
        for &node in &followers {
            self.cluster.thaw(node);
        }
        clauses.push("D05:two-paused");
        let answered = clients::failure(&answer);
        if answered.is_none() {
            self.violate(
                "NoQuorumNoSuccess",
                format!("write {} succeeded with both followers paused", request.id),
            );
        }
        let after = self.recovered().await?;
        let definite = !clients::unknown(&answer);
        if after != before && (definite || after != written) {
            self.violate(
                "RowUnchanged",
                format!(
                    "write {} was answered {answered:?} with both followers paused, and the row went from {before} to {after}",
                    request.id
                ),
            );
        }
        // The same ID and body returns the one result, twice.
        let deadline = self.cluster.world.elapsed() + RECOVER_LIMIT;
        let retry = loop {
            let retry = self.send(GATEWAYS[1], &request).await;
            if clients::failure(&retry).is_none() || self.cluster.world.elapsed() >= deadline {
                break retry;
            }
            time::sleep(Duration::from_secs(1)).await;
        };
        let again = self.send(GATEWAYS[0], &request).await;
        match (&retry, &again) {
            (Completion::Ok(first), Completion::Ok(second)) if first == second => {}
            _ => self.violate(
                "DedupUnique",
                format!(
                    "the retry of {} answered {:?} and its repetition {:?}",
                    request.id,
                    clients::failure(&retry),
                    clients::failure(&again)
                ),
            ),
        }
        let row = self.read(GATEWAYS[1]).await;
        if !matches!(&row, Completion::Ok(row) if *row == written) {
            self.violate(
                "RowUnchanged",
                format!(
                    "after the retry of {} the row is not {written} ({:?})",
                    request.id,
                    clients::failure(&row)
                ),
            );
        }
        Some(())
    }
}

fn others(node: u64) -> Vec<u64> {
    WORKERS.into_iter().filter(|other| *other != node).collect()
}

/// Run the schedule and return the clauses whose assertions ran.
pub(crate) async fn run(cluster: &mut Cluster, ctx: &Ctx, deadline: Duration) -> Vec<&'static str> {
    assert_eq!(deadline, DEADLINE, "D05 runs only at its own deadline");
    let mut clauses = Vec::new();
    let client = ctx.new_client();
    let mut run = Run {
        cluster,
        ctx,
        client,
        value: 800_000_000,
    };
    if run.one_paused(&mut clauses).await.is_some() {
        run.two_paused(&mut clauses).await;
    }
    for node in WORKERS {
        run.cluster.thaw(node);
    }
    clauses
}
