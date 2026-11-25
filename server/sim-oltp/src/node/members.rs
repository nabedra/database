//! Opt-in process operations of the membership workload: volumes split per worker, a restart
//! under the same identity on an empty or an older data volume, and the replacement of a
//! permanently lost worker by a spare one (`docs/oltp-recovery.md`). No other workload calls
//! them, and a run that calls none keeps its schedule.

use nabedra_rt::{
    fs::{FaultFs, FsOp},
    sim::{NodeClock, StoragePause, Wipe},
};

use super::*;
use crate::{
    bootstrap::{Candidate, ROOT, SPARES},
    disk::DataVolume,
};

/// The data volume a worker finds when it starts again under the same identity. Its key volume
/// and its bootstrap file survive in every case.
#[derive(Clone, Copy)]
pub(crate) enum Disk<'a> {
    /// The volume's contents are lost.
    Empty(Wipe, Marker),
    /// A snapshot of the volume was put back.
    Stale(&'a FaultFs),
}

/// The volume marker that an empty volume carries when the worker starts on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Marker {
    /// The marker the worker's bootstrap file names: the volume kept its identity and lost its
    /// contents, or an operator copied the old marker.
    Kept,
    /// Another marker, as a provisioner writes it for a new volume.
    Fresh,
    /// No marker.
    Absent,
}

/// What a started process reached within its limit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Started {
    Ready,
    /// The process runs and does not report ready.
    Running,
    /// The process ended with this result.
    Exited(String),
}

impl Cluster {
    /// Give every worker a key volume of its own. Call it before any process starts.
    pub fn split_volumes(&mut self) {
        assert!(
            self.processes.is_empty() && !self.config.is_split(),
            "split the volumes once, before any process starts"
        );
        self.config.split_volumes();
        for node in self.config.nodes() {
            disk::mount_keys(&self.world, node);
        }
    }

    /// Start the cluster with a delay before each worker, while `observe` runs once after each
    /// readiness poll.
    pub async fn start_staggered(
        &mut self,
        delays: [Duration; 3],
        mut observe: impl AsyncFnMut(&Cluster),
    ) -> Result<(), String> {
        let nodes = self.config.nodes();
        let began = self.world.elapsed();
        let mut order: Vec<usize> = (0..nodes.len()).collect();
        order.sort_by_key(|&index| (delays[index], index));
        self.checks
            .expect_recovery(&self.world, "initial bootstrap", {
                RECOVERY_LIMIT + delays.into_iter().max().unwrap_or_default()
            });
        for index in order {
            while self.world.elapsed() < began + delays[index] {
                observe(self).await;
                time::sleep(READINESS_POLL).await;
            }
            self.start_worker(index).await?;
        }
        let deadline = self.world.elapsed() + RECOVERY_LIMIT;
        loop {
            observe(self).await;
            let mut waiting = Vec::new();
            for node in nodes {
                self.check_running(node).await?;
                if !self.is_ready(node, nodes.len()).await {
                    waiting.push(node);
                }
            }
            if waiting.is_empty() {
                break;
            }
            if self.world.elapsed() >= deadline {
                return Err(format!("readiness deadline for {waiting:?}"));
            }
            time::sleep(READINESS_POLL).await;
        }
        let connection = self.admin("cluster.connection", json!({})).await?;
        self.validate_connection(&connection)?;
        for (index, node) in GATEWAYS.into_iter().enumerate() {
            self.admin(
                "api.enroll",
                json!({"expected": null, "next": self.config.api_enrollment(index)}),
            )
            .await?;
            self.start_gateway(index, &connection).await?;
            self.record(format!("gateway n{node} activated"));
        }
        self.wait_ready(&GATEWAYS, false).await?;
        self.checks.recovered(&self.world, "initial bootstrap");
        Ok(())
    }

    /// Whether `node` reports the current run with `ready` workers.
    pub async fn is_ready(&self, node: u64, ready: usize) -> bool {
        request(
            BOOTSTRAP_CLIENT,
            node,
            "/health/ready",
            &self.config.data_token,
            None,
            HTTP_TIMEOUT,
        )
        .await
        .is_ok_and(|reply| {
            reply.status == 200
                && reply.body["cluster_id"] == json!(self.config.cluster_id)
                && reply.body["run_id"] == json!(self.config.run_id)
                && reply.body["ready_workers"] == json!(ready)
        })
    }

    /// Start a stopped worker with the bootstrap file it holds, on `disk`. Nothing is prepared
    /// through the survivors here: call [`Cluster::prepare_restart`] first to start the same
    /// worker under its next incarnation.
    pub async fn restart_same_identity(
        &mut self,
        node: u64,
        how: DiskCut,
        disk: Disk<'_>,
        limit: Duration,
    ) -> Result<Started, String> {
        assert!(
            self.config.is_split(),
            "the keys must survive the data volume: split the volumes"
        );
        assert!(!self.processes.contains_key(&node), "n{node} has a process");
        let index = self.config.index_of(node).expect("known worker");
        let (volume, marker) = match disk {
            Disk::Empty(wipe, marker) => {
                self.record(format!(
                    "n{node} restarts on an empty volume, {marker:?} marker"
                ));
                (DataVolume::Empty(wipe), marker)
            }
            Disk::Stale(image) => {
                self.record(format!("n{node} restarts on an earlier volume image"));
                (DataVolume::Stale(image), Marker::Absent)
            }
        };
        disk::reopen_data(&self.world, &self.checks, node, how, volume);
        in_node(node, async {
            match marker {
                Marker::Kept => self.config.write_kept_marker(index)?,
                Marker::Fresh => self.config.write_fresh_marker(index)?,
                Marker::Absent => {}
            }
            self.config.write_bootstrap_file(index)
        })
        .await
        .map_err(|error| format!("write n{node} restart files: {error}"))?;
        self.spawn_worker(node);
        Ok(self.watch_start(node, limit).await)
    }

    /// Crash `node` with a journal sync stopped just before or just after it ran, so the cut
    /// separates durability from the acknowledgment that waits for it. Returns whether a sync
    /// was stopped within `limit`. The crash happens either way.
    pub async fn crash_at_sync(
        &mut self,
        node: u64,
        how: DiskCut,
        after: bool,
        limit: Duration,
    ) -> bool {
        let pause = if after {
            StoragePause::after(FsOp::SyncData)
        } else {
            StoragePause::before(FsOp::SyncData)
        };
        // The hold only keeps a rule that outlives this process from stopping the next one.
        self.world
            .pause_storage(node, pause.path_prefix(ROOT).hold(limit));
        let deadline = self.world.elapsed() + limit;
        while self.world.paused_storage(node).is_empty() && self.world.elapsed() < deadline {
            time::sleep(Duration::from_millis(1)).await;
        }
        let stopped = !self.world.paused_storage(node).is_empty();
        self.record(format!(
            "n{node} stopped at a sync: {stopped}, after {after}"
        ));
        self.crash(node, how).await;
        stopped
    }

    /// Follow a start until the worker is ready, its process ended, or `limit` passed. A
    /// process that ended leaves its node down.
    pub async fn watch_start(&mut self, node: u64, limit: Duration) -> Started {
        let deadline = self.world.elapsed() + limit;
        loop {
            if let Some(result) = self.ended(node).await {
                return Started::Exited(result);
            }
            if self.is_ready(node, self.config.nodes().len()).await {
                self.prepared.remove(&node);
                return Started::Ready;
            }
            if self.world.elapsed() >= deadline {
                return Started::Running;
            }
            time::sleep(READINESS_POLL).await;
        }
    }

    /// The result of `node`'s process if it ended on its own. Its node is then down.
    pub async fn ended(&mut self, node: u64) -> Option<String> {
        let process = self.processes.get(&node)?;
        if !process.task.is_finished() {
            return None;
        }
        let process = self.processes.remove(&node).expect("finished process");
        let result = format!("{:?}", process.task.await);
        self.record(format!("n{node} ended: {result}"));
        self.abort_start(node).await;
        Some(result)
    }

    /// Put a spare worker's volumes, certificate and clock in place.
    pub fn add_spare(&mut self, spare: u64) -> Result<(), String> {
        assert!(SPARES.contains(&spare), "n{spare} is no spare worker");
        if !self.spares.insert(spare) {
            return Ok(());
        }
        let digest = Bootstrap::spare_digest(spare)
            .map_err(|error| format!("n{spare} certificate: {error}"))?;
        self.world.add_node(spare, NodeClock::default());
        self.net.add_node(spare);
        disk::mount(&self.world, &self.checks, spare);
        if self.config.is_split() {
            disk::mount_keys(&self.world, spare);
        }
        self.world.set_certificate(spare, digest);
        Ok(())
    }

    /// Provision `spare` as the replacement of the lost worker in slot `index`.
    pub async fn provision_replacement(
        &mut self,
        index: usize,
        spare: u64,
    ) -> Result<Candidate, String> {
        self.add_spare(spare)?;
        let connection = self.admin("cluster.connection", json!({})).await?;
        let id = self.split_id().await;
        in_node(spare, async {
            self.config
                .provision_replacement(index, spare, id, &connection["control_peers"])
        })
        .await
        .map_err(|error| format!("provision n{spare}: {error}"))
    }

    /// Start the replacement with the record Control prepared for it.
    pub async fn start_candidate(
        &mut self,
        candidate: &mut Candidate,
        record: &Value,
    ) -> Result<(), String> {
        let spare = candidate.spare;
        in_node(spare, async {
            self.config.write_candidate(candidate, record)
        })
        .await
        .map_err(|error| format!("write n{spare} bootstrap: {error}"))?;
        self.spawn_worker(spare);
        Ok(())
    }

    /// Start the replacement's process again after it stopped, on its own volumes.
    pub fn restart_candidate(&mut self, spare: u64, how: DiskCut) {
        disk::reopen(&self.world, &self.checks, spare, how);
        self.spawn_worker(spare);
    }

    /// Start the process of a worker that the cluster removed, on the disk and the keys it had.
    pub fn restart_removed(&mut self, node: u64) {
        assert!(self.config.index_of(node).is_none(), "n{node} is a member");
        self.record(format!("removed n{node} starts again"));
        disk::reopen(&self.world, &self.checks, node, DiskCut::Kill);
        self.spawn_worker(node);
    }

    /// The replacement took over: the lost worker is no longer part of the cluster.
    pub fn adopt(&mut self, candidate: Candidate, groups: &Value) {
        let lost = self.config.node(candidate.index);
        self.spares.insert(candidate.spare);
        self.outage_ended(lost);
        self.config.adopt(candidate, groups);
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use nabedra_rt::fs::os;

    use super::*;
    use crate::{bootstrap::KEYS, config::Config};

    async fn cluster(world: &Arc<World>) -> Cluster {
        Config::sample(1).setup(world);
        let mut cluster = Cluster::new(
            world.clone(),
            Invariants::new(),
            1,
            Arc::new(|_, _| {}),
            |_, _, _| {},
        )
        .await
        .unwrap();
        cluster.split_volumes();
        cluster
    }

    /// The process comes up with its keys and its bootstrap file on the key volume, and only
    /// the pool is on the data volume.
    #[test]
    fn a_cluster_with_split_volumes_starts_and_keeps_its_keys() {
        nabedra_sim::runtime::run(1, |world| async move {
            let mut cluster = cluster(&world).await;
            cluster.start().await.unwrap();
            let keys = world.volume(1, Path::new(KEYS)).unwrap();
            let data = world.volume(1, Path::new(ROOT)).unwrap();
            for name in [
                "node.key",
                "node.pem",
                "ca.pem",
                "auth.json",
                "bootstrap.json",
            ] {
                assert!(keys.contents(&Path::new(KEYS).join(name)).is_ok(), "{name}");
                assert!(
                    data.contents(&Path::new(ROOT).join(name)).is_err(),
                    "{name}"
                );
            }
            assert!(
                data.contents(Path::new("/sim-oltp/pool/kv/CURRENT"))
                    .is_ok()
            );
            cluster.teardown().await;
            assert_eq!(cluster.checks.violations(), [""; 0]);
            assert_eq!(world.unmounted_path_errors(), 0);
        });
    }

    /// An empty volume that does not carry the marker the bootstrap file names is never
    /// opened: the process ends and writes nothing.
    #[test]
    fn a_worker_refuses_an_empty_volume_without_its_marker() {
        for marker in [Marker::Fresh, Marker::Absent] {
            nabedra_sim::runtime::run(1, |world| async move {
                let mut cluster = cluster(&world).await;
                cluster.start().await.unwrap();
                cluster.crash(2, DiskCut::Kill).await;
                let started = cluster
                    .restart_same_identity(
                        2,
                        DiskCut::Kill,
                        Disk::Empty(Wipe::Replace, marker),
                        Duration::from_secs(10),
                    )
                    .await
                    .unwrap();
                assert!(matches!(started, Started::Exited(_)), "{started:?}");
                let data = world.volume(2, Path::new(ROOT)).unwrap();
                assert!(
                    data.contents(Path::new("/sim-oltp/pool/kv/CURRENT"))
                        .is_err()
                );
                let keys = world.volume(2, Path::new(KEYS)).unwrap();
                assert!(keys.contents(&Path::new(KEYS).join("node.key")).is_ok());
                cluster.teardown().await;
                assert_eq!(cluster.checks.violations(), [""; 0]);
            });
        }
    }

    /// A crash stopped at a sync cuts where the sync was, and the worker restarts.
    #[test]
    fn a_crash_lands_at_a_stopped_sync() {
        nabedra_sim::runtime::run(1, |world| async move {
            let mut cluster = cluster(&world).await;
            cluster.start().await.unwrap();
            let stopped = cluster
                .crash_at_sync(3, DiskCut::Power, false, Duration::from_secs(5))
                .await;
            assert!(stopped, "an idle cluster still syncs heartbeats and votes");
            assert!(!world.is_up(3) && world.paused_storage(3).is_empty());
            cluster.restart_worker(3, DiskCut::Power).await.unwrap();
            cluster.teardown().await;
            assert_eq!(cluster.checks.violations(), [""; 0]);
        });
    }

    /// A worker that starts again on the state it stopped with, put back from a snapshot, opens
    /// it: the rollback itself does not make the volume foreign.
    #[test]
    fn a_worker_opens_its_own_volume_after_a_rollback_to_the_current_state() {
        nabedra_sim::runtime::run(1, |world| async move {
            let mut cluster = cluster(&world).await;
            cluster.start().await.unwrap();
            cluster.crash(2, DiskCut::Power).await;
            let image = disk::snapshot(&world, 2);
            cluster.prepare_restart(2).await.unwrap();
            let started = cluster
                .restart_same_identity(
                    2,
                    DiskCut::Power,
                    Disk::Stale(&image),
                    Duration::from_secs(30),
                )
                .await
                .unwrap();
            assert_eq!(started, Started::Ready);
            // Both volumes answer as the running incarnation's.
            in_node(2, async {
                assert!(os::read(format!("{KEYS}/bootstrap.json")).is_ok());
                assert!(os::read(format!("{ROOT}/pool/kv/CURRENT")).is_ok());
            })
            .await;
            cluster.teardown().await;
            assert_eq!(cluster.checks.violations(), [""; 0]);
        });
    }
}
