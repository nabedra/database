use super::*;
use crate::oltp::mvcc::{
    MvccKey,
    catalog::CatalogRead,
    index::{IndexPhase, IndexRecord},
    store::{ParticipantCommand, ParticipantReply, index_reclaim::ReclaimIndex},
};
use crate::oltp_replication::{
    authority::control::{INDEX_RECLAIM_FEATURE, IdlePasses},
    owned::OwnedCommand,
};
use std::time::Duration;

const INTERVAL: Duration = Duration::from_millis(250);
const STEP_TIMEOUT: Duration = Duration::from_secs(5);

struct Range {
    group: GroupIdentity,
    after: Option<Uuid>,
}

pub(super) async fn reclaim_indexes(runtime: Weak<WorkerRuntime>) {
    let mut sweep = automatic_split::Sweep::default();
    let mut range = None;
    let mut idle = IdlePasses::new(Duration::from_secs(2));
    loop {
        nabedra_rt::time::sleep(INTERVAL).await;
        let Some(runtime) = runtime.upgrade() else {
            break;
        };
        if !runtime.accepting.load(Ordering::Acquire) {
            break;
        }
        // CAP1: no reclamation call or proposal before Control enabled
        // `data.index.reclaim` (an older build decodes neither); the
        // feature watch sets the flag.
        if !runtime.index_reclaim_enabled() {
            continue;
        }
        let started = nabedra_rt::time::now();
        let applied = runtime.services.control.applied_index();
        if !idle.due(applied, &(), started) {
            continue;
        }
        let result = nabedra_rt::time::timeout(STEP_TIMEOUT, async {
            let authority = runtime.index_authority(None).await?;
            if range.is_none() {
                let record = match sweep.next(&runtime).await {
                    Ok(record) => record,
                    Err(error) => {
                        sweep = automatic_split::Sweep::default();
                        return Err(error);
                    }
                };
                let Some(record) = record else {
                    return Ok(());
                };
                range = Some(Range {
                    group: record.identity,
                    after: None,
                });
            }
            let current = range.as_mut().expect("selected range");
            if !runtime.reclaim_index_step(authority, current).await? {
                range = None;
            }
            Ok::<_, ProtocolError>(())
        })
        .await;
        if let Ok(Err(error)) = &result {
            tracing::debug!(code=?error.code, message=error.message, "index reclamation step interrupted");
        }
        let succeeded = matches!(result, Ok(Ok(())));
        let complete = succeeded && range.is_none() && sweep.is_exhausted();
        if complete {
            sweep = automatic_split::Sweep::default();
        } else if !succeeded {
            range = None;
        }
        // Catalog retirement and Timestamp pin release can occur without a Control entry.
        idle.record(complete, applied, (), started);
    }
}

impl WorkerRuntime {
    /// CAP1: Control enabled `data.index.reclaim`, as this worker observed.
    pub(super) fn index_reclaim_enabled(&self) -> bool {
        self.services
            .manager
            .cluster_features()
            .enabled(INDEX_RECLAIM_FEATURE)
    }

    async fn reclaim_index_step(
        &self,
        authority: Authority,
        range: &mut Range,
    ) -> Result<bool, ProtocolError> {
        let GroupKind::Data { table_id, .. } = range.group.kind else {
            return Err(unknown());
        };
        let data = self.services.resolver.participant(&range.group).await?;
        let Some(index_id) = data.next_stored_index(range.after).await? else {
            return Ok(false);
        };
        let previous = range.after.replace(index_id);
        let pin = self
            .services
            .catalog
            .snapshot(ReadTs::new(u64::MAX).map_err(|_| unknown())?, authority)?;
        let CatalogRead::Value {
            version: Some(retired_at),
            record: Some(record),
        } = self
            .services
            .catalog
            .read::<IndexRecord>(&pin, MvccKey::CatalogIndex { table_id, index_id })
            .await?
        else {
            return Ok(true);
        };
        if !matches!(record.phase, IndexPhase::Retired { .. })
            || record.definition.table_id != table_id
            || record.definition.index_id != index_id
        {
            return Ok(true);
        }
        let routing_version = match self.services.catalog.routing_root(&pin, table_id).await? {
            CatalogRead::Value { version, .. } => version,
            CatalogRead::Blocked(_) => return Ok(true),
        };
        let safe_ts = self
            .services
            .timestamps
            .advance_floor(authority, Some(retired_at))
            .await?;
        if safe_ts <= retired_at.get() {
            return Ok(true);
        }
        let command = OwnedCommand::new_owned(
            ParticipantCommand::ReclaimIndex(ReclaimIndex {
                authority,
                catalog_group_id: pin.catalog.group_id,
                table_id,
                index_id,
                retired_at,
                safe_ts: ReadTs::new(safe_ts).map_err(|_| unknown())?,
                routing_version,
            }),
            &self.services.shared,
            ByteClass::Progress,
        )?;
        match data.propose(command).await?.reply {
            ParticipantReply::IndexReclaimed {
                complete: false, ..
            } => {
                range.after = previous;
                #[cfg(test)]
                crate::oltp_runtime::recovery_tests::index_holds::checkpoint(
                    index_id,
                    crate::oltp_runtime::recovery_tests::index_holds::Checkpoint::Reclaim,
                    || None,
                )
                .await;
            }
            ParticipantReply::IndexReclaimed { complete: true, .. }
            | ParticipantReply::Rejected(_) => {}
            _ => return Err(unknown()),
        }
        Ok(true)
    }
}
