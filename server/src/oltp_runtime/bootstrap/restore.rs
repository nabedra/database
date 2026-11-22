//! Activate only offline-imported native seeds under the new target authority.
//! Every group still obtains real local consensus/membership registration.
use super::*;
use crate::oltp::pool::TransactionBudget;
use crate::oltp_replication::{manager::RegisteredWorker, service_rpc::ParticipantEndpoint};
#[derive(serde::Deserialize)]
pub(super) struct Page {
    pub(super) items: Vec<GroupIdentity>,
    pub(super) after: Option<Uuid>,
    pub(super) exhausted: bool,
}
pub(super) async fn group_page(
    pool: &crate::oltp::pool::OltpPool,
    after: Option<Uuid>,
) -> Result<(Page, crate::oltp::pool::ScratchLease), ProtocolError> {
    let raw = pool
        .list_groups(
            after,
            64,
            TransactionBudget {
                scratch_bytes: 128 << 10,
                batch_bytes: 0,
                output_bytes: 64 << 10,
            },
        )
        .await?;
    let owner = pool.resources().reserve_scratch(
        ByteClass::Data,
        crate::oltp::mvcc::json_decode_budget(raw.as_slice())?,
    )?;
    let page: Page = serde_json::from_slice(raw.as_slice()).map_err(|_| unavailable())?;
    if !page.exhausted && page.after <= after {
        return Err(unavailable());
    }
    Ok((page, owner))
}

pub(super) async fn activate(resolver: &LocalParticipants) -> Result<(), ProtocolError> {
    let mut after = None;
    loop {
        let (page, _owner) = group_page(&resolver.pool, after).await?;
        for group in page
            .items
            .iter()
            .filter(|group| matches!(group.kind, GroupKind::Data { .. }))
        {
            let store = resolver.pool.group(group.clone()).await?;
            let machine = ParticipantMachine::new(store.clone())?;
            let Some(seed) = machine.restore_seed().await? else {
                continue;
            };
            let participant = resolver.native_participant(group).await?;
            let authority = *resolver.authority.read().map_err(|_| unavailable())?;
            if participant.activation().await?.is_none() {
                participant
                    .fence_initial(
                        id(group.group_id, b"initial-fence"),
                        authority,
                        seed.catalog_group_id,
                        seed.schema_epoch,
                        seed.routing_epoch,
                        seed.span,
                    )
                    .await?;
            }
            if resolver.control.group(group.group_id).await?.is_none() {
                let running = resolver
                    .manager
                    .start_data(
                        store,
                        resolver.member.clone(),
                        resolver.peers.clone(),
                        raft_config(),
                    )
                    .await?;
                register(&resolver.control, &resolver.operator, &running, authority).await?;
            }
        }
        if page.exhausted {
            break;
        }
        after = page.after;
    }
    Ok(())
}

pub(super) async fn activate_registered(
    worker: &RegisteredWorker,
    control: &Arc<ControlHandle>,
    operator: &LocalOperatorPermit,
    rpc: &Arc<ControlClient>,
    authority: Authority,
    deadline: tokio::time::Instant,
) -> Result<(), ProtocolError> {
    let Some(genesis) = &worker.restored else {
        return Ok(());
    };
    let mut after = None;
    loop {
        let (page, _owner) = group_page(&worker.pool, after).await?;
        for group in page
            .items
            .iter()
            .filter(|group| matches!(group.kind, GroupKind::Data { .. }))
        {
            let machine = ParticipantMachine::new(worker.pool.group(group.clone()).await?)?;
            let Some(seed) = machine.restore_seed().await? else {
                continue;
            };
            if seed.restore_id != genesis.statement().restore_id {
                return Err(unavailable());
            }
            let running = worker
                .start_restored_data(group.clone(), registered_raft_config())
                .await?;
            let participant = Arc::new(ParticipantHandle::new(machine, running.clone()));
            let node = running.state.resident().node_id();
            worker.manager.bind_service(
                group,
                node,
                ParticipantEndpoint::new(
                    participant.clone(),
                    node,
                    rpc.clone(),
                    worker.manager.client(),
                    Some(worker.placements.status_topology()?),
                )?,
            )?;
            loop {
                let attempt = async {
                    if running.ensure_linearizable().await.is_ok() {
                        if participant.activation().await?.is_none() {
                            participant
                                .fence_initial(
                                    id(group.group_id, b"initial-fence"),
                                    authority,
                                    seed.catalog_group_id,
                                    seed.schema_epoch,
                                    seed.routing_epoch,
                                    seed.span.clone(),
                                )
                                .await?;
                        }
                        register(control, operator, &running, authority).await?;
                    }
                    let published = control.group(group.group_id).await?;
                    Ok::<_, ProtocolError>(published.is_some_and(|published| {
                        published.identity == *group
                            && published.replica_count == worker.required_voters as u64
                    }))
                }
                .await;
                match attempt {
                    Ok(true) => break,
                    Ok(false) => {}
                    Err(error) if super::registered::bootstrap_transient(&error) => {}
                    Err(error) => return Err(error),
                }
                if nabedra_rt::time::now() >= deadline {
                    return Err(unavailable());
                }
                nabedra_rt::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
        if page.exhausted {
            return Ok(());
        }
        after = page.after;
    }
}
