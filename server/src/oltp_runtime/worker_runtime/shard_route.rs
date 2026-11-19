//! Queue placement suggestions verified against the pinned catalog and Control.
//! The scope is isolated until queue ownership and takeover can be fenced.
use super::*;
use crate::{
    oltp::mvcc::{
        catalog::{PinnedCatalog, ShardRoute},
        planner::PointRoute,
    },
    oltp_replication::authority::{control::GroupRecord, timestamp::split::SplitSourcePlacement},
};

mod tests;

pub(in crate::oltp_runtime) struct ScopedPointPlacement {
    point: PointRoute,
    record: GroupRecord,
    members: [WorkerIdentity; 3],
    preferred_worker: WorkerIdentity,
    _owner: ScratchLease,
}

impl ScopedPointPlacement {
    fn from_control(
        point: PointRoute,
        placement: SplitSourcePlacement,
        preferred_node: Option<u64>,
        shared: &WorkerSharedResources,
    ) -> Result<Self, ProtocolError> {
        placement.validate(&point.shard().group)?;
        let [first, second, third] = &placement.voters[..] else {
            return Err(stale());
        };
        let preferred_worker = placement
            .voters
            .iter()
            .find(|voter| Some(voter.member.node_id) == preferred_node)
            .unwrap_or(first)
            .worker;
        let owner = shared.reserve_scratch(
            ByteClass::Data,
            std::mem::size_of::<Self>() + placement.record.identity.database.capacity_bytes(),
        )?;
        Ok(Self {
            point,
            record: placement.record,
            members: [first.worker, second.worker, third.worker],
            preferred_worker,
            _owner: owner,
        })
    }

    pub(in crate::oltp_runtime) fn route(&self) -> &ShardRoute {
        self.point.shard()
    }

    pub(in crate::oltp_runtime) fn generation(&self) -> Uuid {
        self.record.generation
    }

    pub(in crate::oltp_runtime) fn members(&self) -> [WorkerIdentity; 3] {
        self.members
    }

    /// A routing preference has no queue authority or execution rights.
    pub(in crate::oltp_runtime) fn preferred_worker(&self) -> WorkerIdentity {
        self.preferred_worker
    }

    fn verify_target(
        &self,
        current: SplitSourcePlacement,
        target: WorkerIdentity,
    ) -> Result<(), ProtocolError> {
        current.validate(&self.point.shard().group)?;
        if current.record != self.record
            || !current
                .voters
                .iter()
                .map(|voter| voter.worker)
                .eq(self.members)
            || !self.members.contains(&target)
        {
            return Err(stale());
        }
        Ok(())
    }
}

impl WorkerRuntime {
    pub(in crate::oltp_runtime) async fn scoped_point_placement(
        &self,
        pin: &PinnedCatalog,
        operation: &protocol::Operation,
    ) -> Result<Option<ScopedPointPlacement>, PlanError> {
        let Some(point) = self.planner.point_route(pin, operation).await? else {
            return Ok(None);
        };
        let group = &point.shard().group;
        let control = &self.services.control_client;
        let reply = control.group(group.group_id).await?;
        let decoded = reply.decode_section::<Option<GroupRecord>>(0)?;
        let record = decoded
            .as_ref()
            .filter(|record| record.identity == *group)
            .cloned()
            .ok_or_else(stale)?;
        drop(decoded);
        drop(reply);
        if record.replica_count != 3 {
            return Ok(None);
        }
        let preferred_node = control
            .cached_replica_routes(group)
            .filter(|routes| routes.record == record)
            .and_then(|routes| routes.last_successful_node());
        // The existing split-source Control read verifies the exact Data
        // generation and ready process incarnations at the same authority cut.
        let placement = control.split_source(&record, point.authority()).await?;
        Ok(Some(ScopedPointPlacement::from_control(
            point,
            placement,
            preferred_node,
            &self.services.shared,
        )?))
    }

    /// Recheck the placement and exact local process before considering queue
    /// work. Queue term and the accepted request binding are separate checks.
    pub(in crate::oltp_runtime) async fn verify_scoped_point_target(
        &self,
        placement: &ScopedPointPlacement,
        pin: &PinnedCatalog,
    ) -> Result<(), ProtocolError> {
        if pin.catalog != *self.services.catalog.participant().identity()
            || pin.read_ts != placement.point.read_ts()
            || pin.authority != placement.point.authority()
        {
            return Err(stale());
        }
        let current = self
            .services
            .control_client
            .split_source(&placement.record, pin.authority)
            .await?;
        placement.verify_target(current, self.services.local)
    }
}

fn stale() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "scoped point placement changed or the receiver is outside it",
        Outcome::NotApplied,
    )
}
