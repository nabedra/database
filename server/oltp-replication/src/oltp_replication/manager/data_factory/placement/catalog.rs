use super::*;
use crate::{
    oltp::mvcc::{
        catalog::{CatalogHandle, CatalogRead, OwnedRecord},
        store::{
            ParticipantReply,
            lifecycle::{GuardMode, LifecycleReply, VerifiedLifecycleGuard},
        },
    },
    oltp_replication::authority::{
        control::ReplicaMoveLineage,
        status::{IdentityLookup, RequestKind, StatusReply, StatusService},
    },
};

#[doc(hidden)]
pub struct PlacementCatalog<'a> {
    pub catalog: &'a CatalogHandle,
    pub status: &'a dyn StatusService,
}

impl PlacementCatalog<'_> {
    pub(super) async fn observe(
        &self,
        group: &GroupIdentity,
        authority: Authority,
    ) -> Result<ReplicaMoveLineage, ProtocolError> {
        let GroupKind::Data { table_id, .. } = group.kind else {
            return Err(invalid());
        };
        let latest = ReadTs::new(u64::MAX).map_err(|_| invalid())?;
        let pin = self.catalog.snapshot(latest, authority)?;
        let (table_version, table) = record(self.catalog.describe(&pin, table_id).await?)?;
        let (root_version, root) = record(self.catalog.routing_root(&pin, table_id).await?)?;
        if table.schema_epoch != root.schema_epoch || table.routing_epoch != root.routing_epoch {
            return Err(unavailable());
        }
        let mut cursor = root.first_page;
        let mut route_digest = None;
        let deadline = nabedra_rt::time::now() + Duration::from_secs(30);
        while let Some(page_id) = cursor {
            let (_, page) = record(
                nabedra_rt::time::timeout_at(
                    deadline,
                    self.catalog.routing_page(&pin, table_id, page_id),
                )
                .await
                .map_err(|_| unavailable())??,
            )?;
            if let Some(route) = page.shards.iter().find(|route| route.group == *group) {
                if route.schema_epoch != root.schema_epoch
                    || route.routing_epoch > root.routing_epoch
                {
                    return Err(unavailable());
                }
                route_digest = Some(crate::oltp_replication::owned::serialized_digest(route)?);
                break;
            }
            cursor = page.next_page;
        }
        let (current_root, _) = record(self.catalog.routing_root(&pin, table_id).await?)?;
        if current_root != root_version {
            return Err(unavailable());
        }
        Ok(ReplicaMoveLineage {
            catalog: self.catalog.participant().identity().group_id,
            table: table_id,
            table_version,
            schema_epoch: table.schema_epoch,
            route_digest: route_digest.ok_or_else(not_active)?,
        })
    }

    pub(super) async fn acquire(
        &self,
        move_: &ReplicaMove,
    ) -> Result<VerifiedLifecycleGuard, ProtocolError> {
        let binding = move_.lifecycle()?;
        let reservation = self
            .status
            .reserve_identity(
                move_.authority,
                binding.request.request_id,
                binding.request.digest,
                RequestKind::Mutation,
            )
            .await?;
        if !matches!(
            reservation.reply,
            StatusReply::Identity(IdentityLookup::Reserved(_))
        ) {
            return Err(conflict());
        }
        self.catalog
            .participant()
            .acquire_lifecycle(binding, GuardMode::Shared)
            .await?
            .ok_or_else(unavailable)
    }

    pub(super) async fn verify(
        &self,
        move_: &ReplicaMove,
    ) -> Result<VerifiedLifecycleGuard, ProtocolError> {
        let guard = self
            .catalog
            .participant()
            .lifecycle_guard(&move_.lifecycle()?)
            .await?
            .ok_or_else(unavailable)?;
        if move_.guard.as_ref() != Some(&guard.clone().into_sealed())
            || self.observe(&move_.request.group, move_.authority).await? != move_.lineage
        {
            return Err(not_active());
        }
        Ok(guard)
    }

    pub(super) async fn release(&self, move_: &ReplicaMove) -> Result<(), ProtocolError> {
        let binding = move_.lifecycle()?;
        let participant = self.catalog.participant();
        let Some(guard) = participant.lifecycle_guard(&binding).await? else {
            return Ok(());
        };
        if guard.mode() != GuardMode::Shared
            || move_
                .guard
                .as_ref()
                .is_some_and(|prior| prior != &guard.clone().into_sealed())
        {
            return Err(unavailable());
        }
        // This guard owns neither writes nor a queue record. Status settles its
        // exact identity before release from the Catalog's shared holder set.
        self.status
            .terminal_failure(
                move_.authority,
                binding.request,
                ProtocolError::new(
                    ErrorCode::Conflict,
                    "placement lifecycle has no transaction writes",
                    Outcome::NotApplied,
                ),
            )
            .await?;
        let settled = self
            .status
            .failure_settled_proof(binding.txn, binding.request)
            .await?;
        let applied = participant
            .release_lifecycle(move_.authority, guard, settled)
            .await?;
        match applied.reply {
            ParticipantReply::Lifecycle(LifecycleReply::Released) => Ok(()),
            _ => Err(unavailable()),
        }
    }
}

fn record<T>(read: CatalogRead<T>) -> Result<(CommitTs, OwnedRecord<T>), ProtocolError> {
    match read {
        CatalogRead::Value {
            version: Some(version),
            record: Some(record),
        } => Ok((version, record)),
        CatalogRead::Value { .. } => Err(not_active()),
        CatalogRead::Blocked(_) => Err(unavailable()),
    }
}

fn not_active() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Conflict,
        "replica move requires the exact committed Catalog shard route",
        Outcome::NotApplied,
    )
}
