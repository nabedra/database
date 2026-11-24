use super::OperatorPolicy;
use crate::oltp_deployment::inventory::{SecondaryPoolConfig, WorkerPools};
use crate::{
    oltp::pool::mount::{PoolMountProfile, VerifiedMountAnchor},
    oltp_replication::authority::control::{ControlHandle, LocalOperatorPermit},
    protocol::{ErrorCode, Outcome, ProtocolError},
};

pub(super) fn empty() -> crate::oltp_replication::types::Page<SecondaryPoolConfig> {
    crate::oltp_replication::types::Page::new(Vec::new()).expect("an empty pool page is bounded")
}

pub(super) async fn open_configured(
    pools: &WorkerPools,
    config: &mut Vec<(SecondaryPoolConfig, VerifiedMountAnchor)>,
    control: &ControlHandle,
    operator: &LocalOperatorPermit,
    policy: OperatorPolicy,
) -> Result<(), ProtocolError> {
    for (entry, _) in config.iter() {
        if !control
            .pool_drain_allows_retained_open(entry.mount.identity.pool_id)
            .await?
        {
            return Err(ProtocolError::new(
                ErrorCode::Conflict,
                "secondary pool generation is authoritatively closing or closed",
                Outcome::NotApplied,
            ));
        }
    }
    let worker = pools.worker_identity();
    for (entry, anchor) in std::mem::take(config) {
        let previous = anchor.retained_worker_if_initialized()?;
        let exclusion = match (previous, entry.mount.profile) {
            (Some(old), PoolMountProfile::Docker { .. }) if old != worker => {
                Some(operator.exclude_previous_before_open(&policy, nabedra_rt::ids::uuid_v4(), old)?)
            }
            (Some(old), PoolMountProfile::Ebs) if old != worker => {
                return Err(ProtocolError::new(
                    ErrorCode::UnsupportedOperation,
                    "EBS pool reopen requires an external previous-owner exclusion proof",
                    Outcome::NotApplied,
                ));
            }
            _ => None,
        };
        // Section 3 rule 6 (O26): a secondary pool that cannot open (damage
        // refused by R1 to R6, EIO, a bad mount) takes the failed-mount path.
        // The worker stays up and its other pools serve; the pool's replicas
        // are reported lost when the retained groups are rehosted.
        let id = entry.mount.identity.pool_id;
        if let Err(error) = pools.open_configured(&entry, anchor, exclusion.as_ref()) {
            // Rule 6 keeps the worker up, but the refusal stays visible: this
            // log, `refused_pools` in `/v1/runtime/stats` (with the same class)
            // and a `pool_refused` report for each of the pool's replicas.
            let class = crate::oltp_replication::authority::control::PoolRefusal::classify(&error);
            tracing::error!(target: "nabedra_database_server::oltp_replication::manager::local::registered::multi_pool",
                pool = %id,
                ?class,
                code = ?error.code,
                %error,
                "secondary OLTP pool refused at open; the worker keeps serving its other pools"
            );
            eprintln!("nabedra-db: secondary OLTP pool {id} refused ({class:?}): {error}");
            pools.mark_refused(id, error);
        }
    }
    Ok(())
}
