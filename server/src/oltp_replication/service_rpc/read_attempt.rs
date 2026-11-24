pub(crate) use crate::oltp::read_attempt::{ScheduledReadError, UnsettledRead};
use crate::oltp::{
    mvcc::store::consensus::ConsensusFuture, pool::WorkerSharedResources,
    read_attempt::ReadGrantSettlement,
};
use crate::protocol::ProtocolError;
pub use nabedra_oltp_types::authority::{ReadGrant, ReadScope};
use nabedra_oltp_types::types::{Authority, WorkerIdentity};
use nabedra_resources::memory::{ChargedBytes, capacity};
use std::sync::{Arc, Weak};
use uuid::Uuid;

#[cfg(test)]
pub(crate) mod fixture;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oltp::{
        pool::WorkerSharedResources,
        read_attempt::{ATTEMPT_BYTES, UNUSED_LIFETIME},
    };
    use nabedra_oltp_types::types::{Authority, Digest, ReadTs, RequestBinding, WorkerIdentity};
    use nabedra_resources::config::WorkerResources;
    use uuid::Uuid;

    #[tokio::test(start_paused = true)]
    async fn o22_f01_f03_running_tickets_survive_timeout_and_unused_tickets_are_fenced() {
        let shared =
            WorkerSharedResources::new(1, WorkerResources::default(), Default::default()).unwrap();
        let book = &shared.read_attempts;
        let base = shared.scheduler().usage();
        let hash = Digest::from_bytes([7; 32]);
        let worker = WorkerIdentity {
            worker_id: nabedra_rt::ids::uuid_v4(),
            incarnation: nabedra_rt::ids::uuid_v4(),
            certificate_sha256: hash,
            enrollment_revision: 1,
        };
        let run_id = nabedra_rt::ids::uuid_v4();
        let scope = ReadScope {
            request: RequestBinding {
                run_id,
                request_id: nabedra_rt::ids::uuid_v4(),
                digest: hash,
                accepted_context: hash,
            },
            authority: Authority {
                run_id,
                owner_term: 1,
            },
            read_ts: ReadTs::new(13).unwrap(),
        };
        let group = nabedra_rt::ids::uuid_v4();
        let grant = ReadGrant {
            scope,
            owner: worker,
            worker,
            group,
            node: 7,
            ticket: Uuid::nil(),
            input: hash,
        };
        let unused = book.open(&shared, grant).unwrap();
        book.settle(unused).unwrap();
        assert!(
            book.claim(unused.ticket, worker, group, grant.node)
                .is_err()
        );
        let live = book.open(&shared, grant).unwrap();
        let stale = WorkerIdentity {
            incarnation: nabedra_rt::ids::uuid_v4(),
            ..worker
        };
        assert!(book.claim(live.ticket, stale, group, grant.node).is_err());
        assert!(
            book.claim(live.ticket, worker, group, grant.node + 1)
                .is_err()
        );
        assert!(
            book.settle(ReadGrant {
                owner: stale,
                ..live
            })
            .is_err()
        );
        let (original, work) = book.claim(live.ticket, worker, group, grant.node).unwrap();
        assert_eq!(original, live);
        assert!(book.claim(live.ticket, worker, group, grant.node).is_err());
        let aged = book.open(&shared, grant).unwrap();
        tokio::time::advance(UNUSED_LIFETIME * 2).await;
        let another = book.open(&shared, grant).unwrap();
        assert!(book.claim(aged.ticket, worker, group, grant.node).is_err());
        book.settle(another).unwrap();
        assert!(book.settle(live).is_err());
        assert_eq!(shared.scheduler().usage()[0].1, 0);
        assert_eq!(shared.scheduler().usage()[0].2, base[0].2 + ATTEMPT_BYTES);
        drop(work);
        book.settle(live).unwrap();
        assert!(book.claim(live.ticket, worker, group, grant.node).is_err());
        assert_eq!(shared.scheduler().usage(), base);
    }
}

/// The worker-local view used to verify and route scheduled participant reads.
pub(crate) trait ReplicaReadSource: Send + Sync + 'static {
    fn scan_context(
        &self,
        scope: ReadScope,
        owner: WorkerIdentity,
        target: WorkerIdentity,
    ) -> Result<ChargedBytes, ProtocolError>;

    fn knows_read_grant(&self, grant: ReadGrant) -> Result<bool, ProtocolError>;

    fn read_member(
        &self,
        authority: Authority,
        worker: Uuid,
    ) -> Result<Option<WorkerIdentity>, ProtocolError>;

    fn local_identity(&self) -> Result<WorkerIdentity, ProtocolError>;
}

impl<T: ReplicaReadSource> ReplicaReadSource for std::sync::Mutex<T> {
    fn scan_context(
        &self,
        scope: ReadScope,
        owner: WorkerIdentity,
        target: WorkerIdentity,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.lock()
            .map_err(|_| super::unavailable())?
            .scan_context(scope, owner, target)
    }

    fn knows_read_grant(&self, grant: ReadGrant) -> Result<bool, ProtocolError> {
        self.lock()
            .map_err(|_| super::unavailable())?
            .knows_read_grant(grant)
    }

    fn read_member(
        &self,
        authority: Authority,
        worker: Uuid,
    ) -> Result<Option<WorkerIdentity>, ProtocolError> {
        self.lock()
            .map_err(|_| super::unavailable())?
            .read_member(authority, worker)
    }

    fn local_identity(&self) -> Result<WorkerIdentity, ProtocolError> {
        self.lock()
            .map_err(|_| super::unavailable())?
            .local_identity()
    }
}

/// The owner retains the read grant across retries until all copies settle.
pub(crate) trait QueueReadSource: ReadGrantSettlement + 'static {
    fn resume_read_continuation(&self, scope: ReadScope) -> ConsensusFuture<'_, ()>;
    fn open_read_grant(&self, grant: ReadGrant) -> ConsensusFuture<'_, ()>;
    fn finish_read_grant(&self, grant: ReadGrant) -> ConsensusFuture<'_, ()>;
}

// Storage keeps weak, type-erased bindings. Each runtime owner retains its
// sized slot, which points weakly back to the owner through the read interface.
pub(crate) type ReplicaReadSlot = Weak<dyn ReplicaReadSource>;
pub(crate) type QueueReadSlot = Weak<dyn QueueReadSource>;

pub(super) fn read_replica(
    shared: &WorkerSharedResources,
) -> Result<Arc<dyn ReplicaReadSource>, ProtocolError> {
    shared
        .read_replica::<ReplicaReadSlot>()?
        .upgrade()
        .ok_or_else(capacity)
}

pub(super) fn read_queue(
    shared: &WorkerSharedResources,
) -> Result<Arc<dyn QueueReadSource>, ProtocolError> {
    shared
        .read_queue::<QueueReadSlot>()?
        .upgrade()
        .ok_or_else(capacity)
}
