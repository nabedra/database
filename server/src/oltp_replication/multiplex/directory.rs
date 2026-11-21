//! Transport authentication uses locally installed Control enrollment. Refresh
//! happens outside Raft handlers, so authenticating Control cannot recursively
//! require a Control quorum call through the same saturated listener.
use super::*;
use crate::oltp::{
    mvcc::json_decode_budget,
    pool::{ExecutionClass, WorkerSharedResources},
};
use crate::oltp_replication::{
    authority::control::{ApiEnrollment, ControlClient, InventoryPage, ReplacementRecord},
    types::{Digest, GroupKind, WorkerEnrollment},
};
use serde::de::DeserializeOwned;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

enum Role {
    Worker,
    Api(super::super::types::Text<128>),
    BootstrapControl(GroupIdentity),
    PreparedControl,
}
struct Entry {
    enrollment: WorkerEnrollment,
    role: Role,
    seen: u64,
    _charge: Arc<ScratchLease>,
}
struct SystemTrust {
    operators: BTreeMap<Uuid, WorkerEnrollment>,
    revoked_workers: BTreeSet<Uuid>,
    revoked_certificates: BTreeSet<Digest>,
    candidate: Option<(ReplacementRecord, bool)>,
    _charge: ScratchLease,
}

pub(crate) struct AppliedPeerDirectory {
    cluster: Uuid,
    control: GroupIdentity,
    shared: Arc<WorkerSharedResources>,
    entries: RwLock<BTreeMap<Uuid, Entry>>,
    prepared: RwLock<Option<Entry>>,
    system: RwLock<Option<SystemTrust>>,
    learner_groups: RwLock<BTreeMap<Uuid, BTreeSet<Uuid>>>,
    revision: AtomicU64,
    epoch: AtomicU64,
    refresh: tokio::sync::Mutex<()>,
}
impl AppliedPeerDirectory {
    /// Bootstrap pins are an explicit operator-supplied Control peer set. They
    /// grant consensus/enrollment reads only, never application or queue access.
    /// Retained node reuse still requires the manager's actual exclusion proof.
    pub(crate) fn from_bootstrap(
        control: GroupIdentity,
        peers: Vec<WorkerEnrollment>,
        shared: Arc<WorkerSharedResources>,
    ) -> Result<Arc<Self>, ProtocolError> {
        control.validate().map_err(|_| invalid())?;
        if control.kind != GroupKind::Control || peers.is_empty() || peers.len() > 128 {
            return Err(invalid());
        }
        let directory = Arc::new(Self {
            cluster: control.cluster_id,
            control: control.clone(),
            shared,
            entries: RwLock::new(BTreeMap::new()),
            prepared: RwLock::new(None),
            system: RwLock::new(None),
            learner_groups: RwLock::new(BTreeMap::new()),
            revision: AtomicU64::new(0),
            epoch: AtomicU64::new(1),
            refresh: tokio::sync::Mutex::new(()),
        });
        for peer in peers {
            if directory
                .entries
                .read()
                .map_err(|_| unavailable())?
                .contains_key(&peer.worker_id)
            {
                return Err(invalid());
            }
            directory.install(peer, Role::BootstrapControl(control.clone()), 0)?;
        }
        Ok(directory)
    }
    pub(crate) fn allow_replacement_bootstrap(
        &self,
        record: &ReplacementRecord,
    ) -> Result<(), ProtocolError> {
        let mut groups = self.learner_groups.write().map_err(|_| unavailable())?;
        for plan in &record.groups {
            for member in &plan.old {
                if member.worker_id != record.intent.previous.worker_id {
                    groups.entry(member.worker_id).or_default().insert(plan.expected.identity.group_id);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn install_system_trust(
        &self,
        _revision: u64,
        operators: Vec<WorkerEnrollment>,
        revoked: Vec<WorkerIdentity>,
        candidate: Option<(ReplacementRecord, bool)>,
    ) -> Result<(), ProtocolError> {
        let charge = self.shared.reserve_scratch(crate::oltp::pool::ByteClass::Progress,
            4096 + revoked.len() * 128 + candidate.as_ref().map_or(0, |(record, _)| record.groups.len() * 2048))?;
        *self.system.write().map_err(|_| unavailable())? = Some(SystemTrust {
            operators: operators.into_iter().map(|entry| (entry.worker_id, entry)).collect(),
            revoked_workers: revoked.iter().map(|worker| worker.worker_id).collect(),
            revoked_certificates: revoked.iter().map(|worker| worker.certificate_sha256).collect(),
            candidate,
            _charge: charge,
        });
        Ok(())
    }

    pub(crate) fn has_mounted_replacement(&self) -> bool {
        self.system.read().is_ok_and(|system| system.as_ref().is_some_and(|state|
            state.candidate.as_ref().is_some_and(|(_, mounted)| *mounted)))
    }

    pub(crate) fn system_placement_changed(&self) -> bool {
        self.system.read().is_ok_and(|system| system.as_ref().is_some_and(|state|
            !state.revoked_workers.is_empty()))
    }

    pub(crate) fn replacement_capability_active(&self) -> bool {
        self.system.read().is_ok_and(|system| system.is_some())
    }

    pub(crate) fn current_operator(&self, identity: WorkerIdentity) -> Option<bool> {
        match self.system.read() {
            Ok(state) => state.as_ref().map(|state| state.operators.get(&identity.worker_id)
                .is_some_and(|entry| WorkerIdentity::from(entry) == identity)),
            Err(_) => Some(false),
        }
    }

    fn denied(state: &SystemTrust, worker: Uuid, certificate: Option<Digest>) -> bool {
        state.revoked_workers.contains(&worker)
            || certificate.is_some_and(|digest| state.revoked_certificates.contains(&digest))
    }

    fn install(
        &self,
        enrollment: WorkerEnrollment,
        role: Role,
        seen: u64,
    ) -> Result<(), ProtocolError> {
        enrollment.validate().map_err(|_| invalid())?;
        let bytes = std::mem::size_of::<Entry>()
            + 256
            + enrollment.endpoint.capacity_bytes()
            + enrollment.server_name.capacity_bytes()
            + match &role {
                Role::Worker | Role::PreparedControl => 0,
                Role::Api(database) => database.capacity_bytes(),
                Role::BootstrapControl(group) => group.database.capacity_bytes(),
            };
        let mut entries = self.entries.write().map_err(|_| unavailable())?;
        if let Some(old) = entries.get_mut(&enrollment.worker_id) {
            if matches!(
                (&old.role, &role),
                (Role::Worker, Role::Api(_))
                    | (Role::Api(_), Role::Worker)
                    | (Role::BootstrapControl(_), Role::Api(_))
            ) {
                return Err(invalid());
            }
            if old.enrollment.enrollment_revision > enrollment.enrollment_revision {
                old.seen = seen;
                return Ok(());
            }
            if old.enrollment.enrollment_revision == enrollment.enrollment_revision {
                if old.enrollment != enrollment {
                    return Err(invalid());
                }
                if !matches!(old.role, Role::BootstrapControl(_)) {
                    old.seen = seen;
                    return Ok(());
                }
            }
        }
        // Reserve the replacement before publishing it. A failed refresh keeps
        // the prior verified entry; it never relabels an API as a worker.
        let charge = Arc::new(
            self.shared
                .reserve_scratch(crate::oltp::pool::ByteClass::Progress, bytes)?,
        );
        entries.insert(
            enrollment.worker_id,
            Entry {
                enrollment,
                role,
                seen,
                _charge: charge,
            },
        );
        Ok(())
    }
    pub(crate) fn revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }
    pub(crate) async fn refresh(&self, control: &ControlClient) -> Result<u64, ProtocolError> {
        let _order = self.refresh.lock().await;
        let revision = control.enrollment_revision().await?;
        if revision != 0 && revision == self.revision() {
            return Ok(revision);
        }
        let epoch = self
            .epoch
            .load(Ordering::Relaxed)
            .checked_add(1)
            .ok_or_else(unavailable)?;
        self.epoch.store(epoch, Ordering::Relaxed);
        let mut after = None;
        loop {
            let response = control.workers(revision, after).await?;
            let (page, _owner) = self.decode::<WorkerEnrollment>(response).await?;
            check_page(&page, revision, after)?;
            for worker in page.items.iter() {
                if after.is_some_and(|old| worker.worker_id <= old) {
                    return Err(invalid());
                }
                after = Some(worker.worker_id);
                self.install(worker.clone(), Role::Worker, epoch)?;
            }
            if page.after != after {
                return Err(invalid());
            }
            if page.done {
                break;
            }
        }
        after = None;
        loop {
            let response = control.apis(revision, after).await?;
            let (page, _owner) = self.decode::<ApiEnrollment>(response).await?;
            check_page(&page, revision, after)?;
            for api in page.items.iter() {
                if after.is_some_and(|old| api.enrollment.worker_id <= old) {
                    return Err(invalid());
                }
                after = Some(api.enrollment.worker_id);
                self.install(
                    api.enrollment.clone(),
                    Role::Api(api.database.clone()),
                    epoch,
                )?;
            }
            if page.after != after {
                return Err(invalid());
            }
            if page.done {
                break;
            }
        }
        let prepared = control.restart_preparation(revision).await?;
        let restart = prepared
            .filter(|prepared| !prepared.enrolled)
            .map(|prepared| {
                prepared.intent.validate()?;
                Ok::<_, ProtocolError>(prepared.intent.next)
            }).transpose()?;
        let data = control.prepared_data_worker(revision).await?;
        if restart.is_some() && data.is_some() {
            return Err(invalid());
        }
        let candidate = restart.or(data).map(|enrollment| {
                enrollment.validate().map_err(|_| invalid())?;
                let bytes = std::mem::size_of::<Entry>() + 256
                    + enrollment.endpoint.capacity_bytes()
                    + enrollment.server_name.capacity_bytes();
                let charge = self.shared.reserve_scratch(crate::oltp::pool::ByteClass::Progress, bytes)?;
                Ok::<_, ProtocolError>(Entry {
                    enrollment,
                    role: Role::PreparedControl,
                    seen: epoch,
                    _charge: Arc::new(charge),
                })
            }).transpose()?;
        *self.prepared.write().map_err(|_| unavailable())? = candidate;
        self.entries
            .write()
            .map_err(|_| unavailable())?
            .retain(|_, entry| entry.seen == epoch);
        self.revision.store(revision, Ordering::Release);
        Ok(revision)
    }
    /// The worker owns and joins this task. Refresh is independent of query and
    /// Raft handlers, and stopping it does not infer exclusion of any process.
    pub(crate) async fn run_refresh(
        self: Arc<Self>,
        control: Arc<ControlClient>,
        mut stop: tokio::sync::watch::Receiver<bool>,
    ) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            if *stop.borrow() {
                return;
            }
            tokio::select! {
                changed=stop.changed()=>{if changed.is_err()||*stop.borrow(){return}},
                _=interval.tick()=>{
                    let result=tokio::select! {
                        changed=stop.changed()=>{if changed.is_err()||*stop.borrow(){return}continue},
                        result=self.refresh(&control)=>result,
                    };
                    if let Err(error)=result{tracing::warn!(code=?error.code,outcome=?error.outcome,"Control enrollment refresh incomplete; prior verified peers retained");}
                }
            }
        }
    }
    async fn decode<T: DeserializeOwned + Send + 'static>(
        &self,
        reply: ReceivedReply,
    ) -> Result<(InventoryPage<T>, Arc<ScratchLease>), ProtocolError> {
        let raw = reply.sections[0].as_ref().ok_or_else(invalid)?.clone();
        let bytes = self
            .shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                json_decode_budget(raw.as_slice())
            })
            .await?;
        let reply = reply.reserve_reply_decode(&self.shared, bytes).await?;
        self.shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                let raw = reply.sections[0].as_ref().ok_or_else(invalid)?;
                let page = serde_json::from_slice(raw.as_slice()).map_err(|_| invalid())?;
                Ok((page, reply.scratch.clone()))
            })
            .await
    }
}
impl PeerDirectory for AppliedPeerDirectory {
    fn applied_revision(&self) -> Option<u64> {
        Some(self.revision())
    }
    fn revoked(&self, peer: WorkerIdentity) -> bool {
        self.system.read().map_or(true, |state| state.as_ref().is_some_and(|state|
            Self::denied(state, peer.worker_id, Some(peer.certificate_sha256))))
    }
    fn is_api(&self, peer: WorkerIdentity) -> bool {
        self.entries.read().is_ok_and(|entries| {
            entries.get(&peer.worker_id).is_some_and(|entry| {
                matches!(entry.role, Role::Api(_))
                    && WorkerIdentity::from(&entry.enrollment) == peer
            })
        })
    }
    fn resolve(&self, worker: Uuid) -> Result<WorkerEnrollment, ProtocolError> {
        if let Some(state) = self.system.read().map_err(|_| unavailable())?.as_ref() {
            if Self::denied(state, worker, None) {
                return Err(unavailable());
            }
            if let Some((record, _)) = &state.candidate
                && record.intent.next.worker_id == worker
            {
                return Ok(record.intent.next.clone());
            }
            // During a retained restart the prepared entry must override the
            // previous operator, so applied operators are a later fallback.
        }
        if let Some(candidate) = self.prepared.read().map_err(|_| unavailable())?
            .as_ref().filter(|candidate| candidate.enrollment.worker_id == worker)
        {
            return Ok(candidate.enrollment.clone());
        }
        if let Some(enrollment) = self.system.read().map_err(|_| unavailable())?
            .as_ref().and_then(|state| state.operators.get(&worker))
        {
            return Ok(enrollment.clone());
        }
        self.entries
            .read()
            .map_err(|_| unavailable())?
            .get(&worker)
            .map(|entry| entry.enrollment.clone())
            .ok_or_else(unavailable)
    }
    fn verify(
        &self,
        peer: WorkerIdentity,
        group: &GroupIdentity,
        purpose: Purpose,
    ) -> Result<TransportTrust, ProtocolError> {
        if group.cluster_id != self.cluster {
            return Err(invalid());
        }
        if let Some(state) = self.system.read().map_err(|_| unavailable())?.as_ref() {
            if Self::denied(state, peer.worker_id, Some(peer.certificate_sha256)) {
                return Err(invalid());
            }
            if let Some((record, mounted)) = &state.candidate
                && record.intent.next.worker_id == peer.worker_id
            {
                if WorkerIdentity::from(&record.intent.next) != peer {
                    return Err(invalid());
                }
                let admitted = (group == &self.control && purpose == Purpose::SystemRecovery)
                    || (*mounted && matches!(purpose, Purpose::Raft | Purpose::Snapshot | Purpose::RaftProgress)
                        && record.groups.iter().any(|plan| plan.expected.identity == *group))
                    || (*mounted && state.operators.get(&peer.worker_id)
                        .is_some_and(|entry| WorkerIdentity::from(entry) == peer) && group == &self.control
                        && matches!(purpose, Purpose::Authority | Purpose::ControlDelegation));
                return if admitted { Ok(TransportTrust::AppliedEnrollment) } else { Err(unavailable()) };
            }
        }
        if let Some(candidate) = self
            .prepared
            .read()
            .map_err(|_| unavailable())?
            .as_ref()
            .filter(|candidate| candidate.enrollment.worker_id == peer.worker_id)
        {
            if WorkerIdentity::from(&candidate.enrollment) != peer {
                return Err(invalid());
            }
            return if group == &self.control && purpose == Purpose::ControlDelegation {
                Ok(TransportTrust::PreparedControl)
            } else if group == &self.control && purpose == Purpose::SystemRecovery {
                Ok(TransportTrust::AppliedEnrollment)
            } else {
                // The worker's voter placements still point here during a
                // retained restart. Until enrollment, route callers must
                // continue to the surviving replicas.
                Err(unavailable())
            };
        }
        if let Some(operator) = self.system.read().map_err(|_| unavailable())?
            .as_ref().and_then(|state| state.operators.get(&peer.worker_id))
        {
            return if WorkerIdentity::from(operator) == peer {
                Ok(TransportTrust::AppliedEnrollment)
            } else {
                Err(invalid())
            };
        }
        let entries = self.entries.read().map_err(|_| unavailable())?;
        let entry = entries.get(&peer.worker_id).ok_or_else(unavailable)?;
        if WorkerIdentity::from(&entry.enrollment) != peer {
            return Err(invalid());
        }
        match &entry.role {
            Role::Worker => Ok(TransportTrust::AppliedEnrollment),
            Role::Api(database)
                if group.kind == GroupKind::Control
                    && group.database == *database
                    && matches!(
                        purpose,
                        Purpose::Authority | Purpose::ApiIngress | Purpose::ApiObservation
                    ) =>
            {
                Ok(TransportTrust::AppliedEnrollment)
            }
            // An API gateway sends `Method::Direct` / `DirectForward` to the range
            // leader of its own database's Data groups (T5/T6). Only `ApiIngress`;
            // the receiver still checks the method and the Control grant (T9 finding:
            // without this arm every API gateway point operation ended unknown).
            Role::Api(database)
                if matches!(group.kind, GroupKind::Data { .. })
                    && group.database == *database
                    && purpose == Purpose::ApiIngress =>
            {
                Ok(TransportTrust::AppliedEnrollment)
            }
            Role::BootstrapControl(_)
                if (matches!(purpose, Purpose::Raft | Purpose::Snapshot | Purpose::RaftProgress)
                    || (group == &self.control && purpose == Purpose::SystemRecovery))
                    && self.learner_groups.read().map_err(|_| unavailable())?
                        .get(&peer.worker_id).is_some_and(|groups| groups.contains(&group.group_id)) =>
            {
                Ok(TransportTrust::AppliedEnrollment)
            }
            Role::BootstrapControl(control)
                if group == control
                    && matches!(
                        purpose,
                        Purpose::Raft | Purpose::Authority | Purpose::ControlDelegation | Purpose::SystemRecovery
                    ) =>
            {
                Ok(TransportTrust::BootstrapControl)
            }
            _ => Err(invalid()),
        }
    }
}
fn check_page<T>(
    page: &InventoryPage<T>,
    revision: u64,
    after: Option<Uuid>,
) -> Result<(), ProtocolError> {
    if page.revision != revision
        || (!page.done && page.items.is_empty())
        || page
            .after
            .is_some_and(|cursor| after.is_some_and(|old| cursor <= old))
            && !page.items.is_empty()
    {
        return Err(invalid());
    }
    Ok(())
}
fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Unauthorized,
        "peer identity or service role differs from applied Control enrollment",
        Outcome::NotApplied,
    )
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "peer enrollment view is unavailable",
        Outcome::Unknown,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::WorkerResources,
        oltp::pool::SharedResourceConfig,
        oltp_replication::types::{Digest, Text},
    };
    fn enrollment() -> WorkerEnrollment {
        WorkerEnrollment {
            worker_id: Uuid::new_v4(),
            incarnation: Uuid::new_v4(),
            certificate_sha256: Digest::from_bytes([3; 32]),
            endpoint: Text::try_from("127.0.0.1:1234").unwrap(),
            server_name: Text::try_from("localhost").unwrap(),
            enrollment_revision: 1,
        }
    }

    #[test]
    fn r03_prepared_restart_routes_past_the_unavailable_voter() {
        let shared = WorkerSharedResources::new(
            1,
            WorkerResources::default(),
            SharedResourceConfig::default(),
        )
        .unwrap();
        let control = GroupIdentity {
            cluster_id: Uuid::new_v4(),
            database: Text::try_from("tenant").unwrap(),
            group_id: Uuid::new_v4(),
            kind: GroupKind::Control,
            group_format_version: 1,
        };
        let old = enrollment();
        let directory = AppliedPeerDirectory::from_bootstrap(
            control.clone(),
            vec![old.clone()],
            shared.clone(),
        )
        .unwrap();
        let next = WorkerEnrollment {
            incarnation: Uuid::new_v4(),
            enrollment_revision: 2,
            ..old.clone()
        };
        *directory.prepared.write().unwrap() = Some(Entry {
            enrollment: next.clone(),
            role: Role::PreparedControl,
            seen: 1,
            _charge: Arc::new(
                shared
                    .reserve_scratch(crate::oltp::pool::ByteClass::Progress, 1024)
                    .unwrap(),
            ),
        });
        let target = WorkerIdentity::from(&directory.resolve(old.worker_id).unwrap());
        assert_eq!(target, WorkerIdentity::from(&next));
        assert_eq!(
            directory.verify(target, &control, Purpose::ControlDelegation),
            Ok(TransportTrust::PreparedControl)
        );
        assert_eq!(
            directory.verify(target, &control, Purpose::Raft),
            Err(unavailable())
        );
        assert_eq!(
            directory.verify(
                WorkerIdentity::from(&old),
                &control,
                Purpose::ControlDelegation
            ),
            Err(invalid())
        );
        let data = GroupIdentity {
            group_id: Uuid::new_v4(),
            kind: GroupKind::Data {
                table_id: Uuid::new_v4(),
                shard_id: Uuid::new_v4(),
            },
            ..control
        };
        for purpose in [Purpose::Raft, Purpose::Authority, Purpose::ServiceMutation] {
            assert_eq!(directory.verify(target, &data, purpose), Err(unavailable()));
        }
    }

    #[test]
    fn o15_api_transport_role_cannot_become_a_voter_or_queue_copy_source() {
        let shared = WorkerSharedResources::new(
            1,
            WorkerResources::default(),
            SharedResourceConfig::default(),
        )
        .unwrap();
        let baseline = shared.control_bytes_used();
        let control = GroupIdentity {
            cluster_id: Uuid::new_v4(),
            database: Text::try_from("tenant").unwrap(),
            group_id: Uuid::new_v4(),
            kind: GroupKind::Control,
            group_format_version: 1,
        };
        let worker = enrollment();
        let directory = AppliedPeerDirectory::from_bootstrap(
            control.clone(),
            vec![worker.clone()],
            shared.clone(),
        )
        .unwrap();
        let identity = WorkerIdentity::from(&worker);
        assert_eq!(
            directory.verify(identity, &control, Purpose::Raft).unwrap(),
            TransportTrust::BootstrapControl
        );
        assert!(
            directory
                .verify(identity, &control, Purpose::ApiIngress)
                .is_err()
        );
        let mut data = control.clone();
        data.kind = GroupKind::Data {
            table_id: Uuid::new_v4(),
            shard_id: Uuid::new_v4(),
        };
        data.group_id = Uuid::new_v4();
        assert!(directory.verify(identity, &data, Purpose::Raft).is_err());
        // This unit checks the cache's defense in depth. Production upgrades
        // come only from refresh's actual Control ReadIndex inventory pages.
        directory.install(worker, Role::Worker, 1).unwrap();
        assert_eq!(
            directory.verify(identity, &data, Purpose::Raft).unwrap(),
            TransportTrust::AppliedEnrollment
        );
        let mut api = enrollment();
        directory
            .install(api.clone(), Role::Api(control.database.clone()), 1)
            .unwrap();
        let old = WorkerIdentity::from(&api);
        for purpose in [
            Purpose::Raft,
            Purpose::Snapshot,
            Purpose::Storage,
            Purpose::QueueControl,
            Purpose::QueueData,
        ] {
            assert!(directory.verify(old, &control, purpose).is_err());
        }
        for purpose in [
            Purpose::Authority,
            Purpose::ApiIngress,
            Purpose::ApiObservation,
        ] {
            assert_eq!(
                directory.verify(old, &control, purpose).unwrap(),
                TransportTrust::AppliedEnrollment
            );
        }
        // T9: an API reaches its database's Data groups with `ApiIngress` only.
        assert_eq!(
            directory.verify(old, &data, Purpose::ApiIngress).unwrap(),
            TransportTrust::AppliedEnrollment
        );
        for purpose in [
            Purpose::Raft,
            Purpose::Snapshot,
            Purpose::Storage,
            Purpose::Authority,
            Purpose::ApiObservation,
            Purpose::Settlement,
        ] {
            assert!(directory.verify(old, &data, purpose).is_err());
        }
        let mut foreign_data = data.clone();
        foreign_data.database = Text::try_from("other").unwrap();
        assert!(
            directory
                .verify(old, &foreign_data, Purpose::ApiIngress)
                .is_err()
        );
        let mut catalog = data.clone();
        catalog.kind = GroupKind::Catalog;
        assert!(
            directory
                .verify(old, &catalog, Purpose::ApiIngress)
                .is_err()
        );
        let mut foreign = control.clone();
        foreign.database = Text::try_from("other").unwrap();
        assert!(
            directory
                .verify(old, &foreign, Purpose::ApiIngress)
                .is_err()
        );
        assert!(directory.install(api.clone(), Role::Worker, 2).is_err());
        api.incarnation = Uuid::new_v4();
        api.enrollment_revision = 2;
        api.certificate_sha256 = Digest::from_bytes([4; 32]);
        directory
            .install(api.clone(), Role::Api(control.database.clone()), 2)
            .unwrap();
        assert!(
            directory
                .verify(old, &control, Purpose::ApiIngress)
                .is_err()
        );
        assert!(
            directory
                .verify(WorkerIdentity::from(&api), &control, Purpose::ApiIngress)
                .is_ok()
        );
        api.endpoint = Text::try_from("127.0.0.1:2345").unwrap();
        assert!(
            directory
                .install(api, Role::Api(control.database.clone()), 2)
                .is_err()
        );
        assert!(shared.control_bytes_used() > baseline);
        drop(directory);
        assert_eq!(shared.control_bytes_used(), baseline);
    }
}
