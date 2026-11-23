//! A data-only process joins a live run through an exact provisioned claim.
//! Only the system coordinators host durable authority and schedule requests.
use super::{
    authority_fence::transition_id,
    bootstrap::{
        queue_limits_for_formats,
        registered::{startup_read, wait_startup},
        registered_raft_config,
    },
    replica::{QueueAuthority, QueueScope, WorkerReplica},
    scoped_worker::{ScopedParticipants, ScopedWorker, ScopedWorkerServices},
    worker_endpoint::{AuthorityReader, QueueEndpoint, QueuePeerClient},
    worker_runtime::retirement,
};
use crate::{
    config::{ExecutionConfig, WorkerResources},
    executor::Executor,
    oltp::{mvcc::{catalog::CatalogHandle, planner::ParticipantResolver}, pool::{
        ByteClass, ExecutionClass, OltpPool, SharedResourceConfig, WorkerSharedResources,
        mount::VerifiedPoolMount,
    }},
    oltp_deployment::{
        inventory::WorkerPoolInventory,
        live::{InventoryPin, UsageSample, process_counters},
    },
    oltp_replication::{
        AppliedPeerDirectory, TlsMaterial,
        authority::{
            control::{
                ControlClient, ControlReply, DataWorkerClient, DataWorkerIntent,
                EksReplacementPermit, Fault, GroupRecord, InventoryPage, PublishedControlRoutes,
                ServiceAction, WorkerDrainPhase,
            },
            status::{PartitionedStatus, StatusService, StatusTopology},
        },
        manager::{GroupPlacement, HostedGroupManager, RemotePlacementTarget, shard_split::SplitEndpoint},
        multiplex::{EndpointRegistry, Purpose, TransportConfig, WorkerClient, WorkerTransport},
        service_rpc::{StatusClient, TimestampClient},
        types::*,
    },
    protocol::{ErrorCode, Outcome, ProtocolError},
    request_queue::QueueError,
    response,
};
use axum::{
    Router,
    body::{Bytes, to_bytes},
    extract::{Request, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, value::RawValue};
use std::{
    io::Read,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, watch},
};
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    version: u32,
    bind: SocketAddr,
    auth_file: PathBuf,
    node: Node,
    resources: WorkerResources,
    execution: ExecutionConfig,
    #[serde(default)]
    scoped_oltp: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Node {
    intent: DataWorkerIntent,
    #[serde(default)]
    eks_replacement_drain: Option<Uuid>,
    bind: SocketAddr,
    certificate: PathBuf,
    private_key: PathBuf,
    authority_ca: PathBuf,
    scheduler_node_id: u64,
    control: GroupPlacement,
    control_enrollments: Page<WorkerEnrollment>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Auth {
    data_token: String,
    #[serde(default)]
    admin_token: Option<String>,
}

fn valid_token(token: &str) -> bool {
    (43..=4096).contains(&token.len()) && token.bytes().all(|byte| (33..=126).contains(&byte))
}

struct Joined {
    scope: QueueScope,
    intent: DataWorkerIntent,
    shared: Arc<WorkerSharedResources>,
    replica: Arc<Mutex<WorkerReplica>>,
    control: Arc<ControlClient>,
    joining: DataWorkerClient,
    status: Arc<PartitionedStatus>,
    placement: Arc<RemotePlacementTarget>,
    auth: Auth,
    sample_slot: Semaphore,
    scoped: Option<(Arc<ScopedWorker>, crate::scheduling::MemoryCharge)>,
    /// `data.direct.v1`: this worker hosts Data replicas, so it advertises.
    direct: Arc<super::worker_runtime::direct_capability::DirectSwitch>,
}

pub async fn run(path: PathBuf) -> Result<(), &'static str> {
    let config: Config =
        super::read_config(&path).map_err(|_| "data worker configuration invalid")?;
    let auth: Auth = super::api_process::protected_json(&config.auth_file)
        .map_err(|_| "data worker authentication unreadable")?;
    if config.version != 1
        || config.bind.port() == 0
        || !valid_token(&auth.data_token)
        || auth
            .admin_token
            .as_ref()
            .is_some_and(|token| !valid_token(token) || token == &auth.data_token)
    {
        return Err("data worker version, readiness bind or data authentication invalid");
    }
    start(config, auth).await.map_err(|error| {
        eprintln!(
            "nabedra-db: data worker {:?}: {}",
            error.code, error.message
        );
        "data worker failed"
    })
}

async fn start(config: Config, auth: Auth) -> Result<(), ProtocolError> {
    let deadline = nabedra_rt::time::now() + Duration::from_secs(60);
    let (stop, mut stopped) = watch::channel(false);
    let mut node = config.node;
    let group = node.control.identity.clone();
    let local = WorkerIdentity::from(&node.intent.enrollment);
    if node.eks_replacement_drain.is_none() {
        node.intent.validate(&group)?;
    } else {
        node.intent.enrollment.validate().map_err(|_| invalid())?;
    }
    config
        .resources
        .validate_execution(&config.execution)
        .map_err(|_| invalid())?;
    if group.kind != GroupKind::Control
        || group.validate().is_err()
        || node.bind.port() == 0
        || node.scheduler_node_id == 0
        || !matches!(node.control.voters.len(), 1 | 3 | 5)
        || node.control.voters.len() != node.control_enrollments.len()
    {
        return Err(invalid());
    }
    validate_voters(&node.control.voters).map_err(|_| invalid())?;
    if node
        .control
        .voters
        .iter()
        .zip(node.control.voters.iter().skip(1))
        .any(|(previous, next)| previous.node_id >= next.node_id)
    {
        return Err(invalid());
    }
    let mut pinned = Vec::new();
    for member in node.control.voters.iter() {
        let enrollment = node
            .control_enrollments
            .iter()
            .find(|peer| peer.worker_id == member.worker_id)
            .ok_or_else(invalid)?;
        if member.worker_id == local.worker_id
            || member.pool_id == node.intent.mount.identity.pool_id
            || enrollment.certificate_sha256 == local.certificate_sha256
        {
            return Err(invalid());
        }
        pinned.push((member.node_id, WorkerIdentity::from(enrollment)));
    }
    let tls = tls_material(&node)?;
    let shared = WorkerSharedResources::new(
        node.scheduler_node_id,
        config.resources,
        SharedResourceConfig {
            query_threads: config.resources.max_threads as usize,
            ..Default::default()
        },
    )?;
    shared.bind_worker(local.worker_id)?;
    let directory = AppliedPeerDirectory::from_bootstrap(
        group.clone(),
        node.control_enrollments.into_inner(),
        shared.clone(),
    )?;
    let registry = Arc::new(EndpointRegistry::new(shared.clone()));
    registry.reserve(group.clone())?.bind(
        Purpose::SystemRecovery,
        Arc::new(crate::oltp_replication::manager::system_recovery::ReplacementCapabilityEndpoint {
            group: group.clone(),
            shared: shared.clone(),
        }),
    )?;
    let transport = Arc::new(WorkerTransport::new(
        &node.intent.enrollment,
        &tls,
        shared.clone(),
        directory.clone(),
        registry.clone(),
        TransportConfig::default(),
    )?);
    let client = transport.client();
    let control_routes =
        PublishedControlRoutes::new(pinned.iter().map(|(_, peer)| peer.worker_id).collect())?;
    let control = ControlClient::new(
        client.clone(),
        shared.clone(),
        group.clone(),
        control_routes.clone(),
    )?;
    let bootstrap_peers: Vec<_> = pinned.iter().map(|(_, peer)| *peer).collect();
    let joining = DataWorkerClient::new(client.clone(), shared.clone(), group.clone(), pinned);
    let last_error = Mutex::new(None);
    let replacement = if let Some(drain) = node.eks_replacement_drain {
        let permit: EksReplacementPermit = startup_read(
            deadline, &last_error, "read exact EKS replacement mount permit",
            || async {
                let token = replacement_token(&shared).await?;
                joining.replacement_permit(&token).await.map_err(limited_enrollment_retry)
            },
        ).await?;
        if node.intent.native_target.is_none() {
            node.intent.native_target = permit.next.native_target.clone();
        }
        if permit.drain_id != drain || permit.next != node.intent {
            return Err(refused("Control EKS replacement permit differs from the protected bootstrap"));
        }
        node.intent.validate(&group)?;
        Some(permit)
    } else {
        None
    };
    let listener = nabedra_transport::backend::current().bind(node.bind)
        .await
        .map_err(|_| unavailable("data worker transport bind failed"))?;
    let http = TcpListener::bind(config.bind)
        .await
        .map_err(|_| unavailable("data worker readiness bind failed"))?;
    let expectation = node.intent.mount.clone();
    let mount_resources = shared.clone();
    let mount_permit = replacement.clone();
    let (pool, key, added_pool) = nabedra_rt::time::timeout_at(
        deadline,
        shared.execute(ExecutionClass::Control, 0, 0, move || {
            let added = mount_permit.as_ref().map(|permit| {
                let expected = &permit.added.mount;
                let native = VerifiedPoolMount::verify(expected.clone(), None)?;
                OltpPool::open_registered(native, expected.identity.clone(), mount_resources.clone())
            }).transpose()?;
            let native = match &mount_permit {
                Some(permit) => VerifiedPoolMount::reopen_eks(permit)?,
                None => VerifiedPoolMount::verify(expectation.clone(), None)?,
            };
            let key = native.data_worker_key()?;
            let pool = OltpPool::open_registered(native, expectation.identity, mount_resources)?;
            Ok((pool, key, added))
        }),
    )
    .await
    .map_err(|_| bootstrap_timeout())??;
    let mounted = observe(pool.clone()).await?;
    let added_mount = match &added_pool {
        Some(pool) => Some(observe(pool.clone()).await?),
        None => None,
    };
    let scope = QueueScope {
        cluster_id: group.cluster_id,
        database: group.database.clone(),
    };
    let transport_task = nabedra_rt::task::spawn(async move {
        transport
            .run(listener, async move {
                while !*stopped.borrow_and_update() {
                    if stopped.changed().await.is_err() {
                        break;
                    }
                }
            })
            .await
    });

    let revocation_refresh = Mutex::new(None);
    let result = nabedra_rt::time::timeout_at(deadline, async {
        let enroll = if let (Some(permit), Some(added)) = (&replacement, &added_mount) {
            transition_id("eks-replacement-enroll", &(permit.drain_id, local, mounted.total_bytes, added.total_bytes))?
        } else {
            transition_id("data-worker-enroll", &(&node.intent, mounted.total_bytes))?
        };
        startup_read(
            deadline,
            &last_error,
            "complete data worker enrollment",
            || async {
                let reply = match (&replacement, &added_mount) {
                    (Some(permit), Some(added)) => {
                        let token = replacement_token(&shared).await?;
                        joining.complete_replacement(enroll, permit, &mounted, added, &key, &token).await?
                    }
                    _ => joining.complete(enroll, &node.intent, &mounted, &key).await?,
                };
                match reply {
                    ControlReply::Enrolled(worker) if worker == local => Ok(()),
                    ControlReply::Rejected(Fault::Busy | Fault::NotReady) => {
                        Err(bootstrap_timeout())
                    }
                    _ => Err(refused("Control refused data worker mount enrollment")),
                }
            },
        )
        .await?;
        startup_read(
            deadline, &last_error, "exclude replaced Control bootstrap peers", || async {
                directory.exclude_replaced_bootstrap_peers(&control, &bootstrap_peers).await
                    .map_err(limited_enrollment_retry)
            },
        ).await?;
        // REP6: Control-majority revocation view, refreshed once per second
        // by its own task. Inbound exchanges check it in memory only.
        startup_read(deadline, &last_error, "seed Data-only revocation view", || async {
            directory
                .seed_remote_revocations(&*control)
                .await
                .map_err(limited_enrollment_retry)
        })
        .await?;
        let refresh = directory
            .clone()
            .run_revocation_refresh(control.clone(), stop.subscribe());
        *revocation_refresh
            .lock()
            .map_err(|_| unavailable("data worker revocation refresh state"))? =
            Some(nabedra_rt::task::spawn(refresh));
        startup_read(
            deadline,
            &last_error,
            "apply data worker peer directory",
            || async {
                directory
                    .refresh(&control)
                    .await
                    .map_err(limited_enrollment_retry)
            },
        )
        .await?;
        let routes = startup_read(
            deadline,
            &last_error,
            "verify pinned Control voters",
            || control.replica_routes(&group),
        )
        .await?;
        if routes.members.as_slice() != &*node.control.voters {
            return Err(refused(
                "durable Control voter set differs from the bootstrap pins",
            ));
        }
        let (status, timestamps, catalog) = startup_read(deadline, &last_error, "read data worker system topology", || {
            retirement_status(&control, &shared, &client)
        })
        .await?;
        if catalog.replica_count != node.control.voters.len() as u64 {
            return Err(refused("durable Catalog placement differs from the system topology"));
        }
        let catalog_routes = startup_read(deadline, &last_error, "read data worker Catalog placement", || {
            control.replica_routes(&catalog.identity)
        }).await?;
        if catalog_routes.record != catalog {
            return Err(refused("durable Catalog placement changed during data worker startup"));
        }
        let system_voters = Page::new(catalog_routes.members.clone()).map_err(|_| invalid())?;
        drop(catalog_routes);
        let (source, members, ticket) = loop {
            let current = startup_read(
                deadline,
                &last_error,
                "read data worker queue authority",
                || control.current(&scope),
            )
            .await?;
            if current.ready.view.run_id != node.intent.run_id || current.owner == local {
                return Err(refused("data worker is outside the prepared surviving run"));
            }
            let members = startup_read(
                deadline,
                &last_error,
                "read data worker queue membership",
                || read_members(&control, &shared, current.ready),
            )
            .await?;
            if members
                .iter()
                .any(|worker| worker.worker_id == local.worker_id)
            {
                return Err(refused(
                    "data worker had already joined and lost its RAM queue",
                ));
            }
            let operation = transition_id(
                "data-worker-queue-join",
                &(node.intent.claim_id, current.ready),
            )?;
            // Once `data.direct.v1` is active, Control admits only joiners
            // that advertised it. This build decodes every direct entry; an
            // older Control (which rejects the request) has nothing active.
            let _ = control
                .change_direct_capability(
                    nabedra_rt::ids::uuid_v4(),
                    crate::oltp_replication::authority::control::DirectCapabilityOperation::Advertise {
                        version: crate::oltp_replication::authority::control::DIRECT_VERSION,
                    },
                )
                .await;
            // Every registered cluster feature (CAP1's registry).
            for feature in crate::oltp_replication::authority::control::CLUSTER_FEATURES
                .map(|feature| feature.name)
            {
                #[cfg(test)]
                if feature == crate::oltp_replication::authority::control::DIRECT_BATCH_FEATURE
                    && super::recovery_tests::take_fault("lose-batch-advert", local.worker_id)
                        .is_some()
                {
                    continue;
                }
                control.advertise_direct_feature(feature, local).await;
            }
            match startup_read(
                deadline,
                &last_error,
                "begin data worker queue join",
                || async {
                    let token = if replacement.is_some() { Some(replacement_token(&shared).await?) } else { None };
                    joining.begin(operation, &node.intent, current.ready, token.as_deref()).await
                },
            )
            .await?
            {
                ControlReply::JoinBegun {
                    ticket,
                    bootstrap: false,
                } => break (current, members, ticket),
                ControlReply::Rejected(Fault::Busy | Fault::Stale | Fault::NotReady) => {
                    wait_startup(deadline).await?
                }
                ControlReply::Rejected(Fault::DirectCapabilityRequired) => {
                    // A capability advert can miss the accepting leader
                    // while voters refresh this worker's enrollment. The
                    // rejected join has no receipt. Re-advertise this exact
                    // incarnation and retry against the current ready set.
                    tracing::debug!(
                        fault = ?Fault::DirectCapabilityRequired,
                        "retrying data worker queue join after incomplete capability advertisement"
                    );
                    wait_startup(deadline).await?
                }
                ControlReply::Rejected(fault) => {
                    tracing::warn!(?fault, "Control rejected data worker queue join");
                    return Err(refused("Control refused data worker queue join"));
                }
                _ => return Err(refused("Control refused data worker queue join")),
            }
        };
        let (legacy_limits, scoped_limits) = queue_limits_for_formats(
            &config.execution, config.scoped_oltp,
        );
        let mut replica = WorkerReplica::new_joining(
                shared.clone(),
                scope.clone(),
                local,
                source,
                members,
                legacy_limits,
                ticket,
            ).map_err(QueueError::protocol)?;
        if let Some(limits) = scoped_limits {
            replica.install_scoped_queues(limits).map_err(QueueError::protocol)?;
        }
        let replica = Arc::new(Mutex::new(replica));
        shared.bind_read_replica(&replica)?;
        let endpoint = QueueEndpoint::new(
            replica.clone(),
            shared.clone(),
            scope.clone(),
            control.clone(),
            config.resources.task_memory_limit_bytes,
        )?;
        match (&added_pool, &added_mount) {
            (Some(added), Some(observed)) => endpoint.require_join_mounts([
                Some((pool.clone(), mounted.total_bytes)), Some((added.clone(), observed.total_bytes)),
            ])?,
            _ => endpoint.require_join_mount(pool.clone(), mounted.total_bytes)?,
        }
        endpoint.install_retirement_reader(status.clone())?;
        let registration = registry.reserve(group.clone())?;
        registration.bind(Purpose::QueueControl, endpoint.clone())?;
        registration.bind(Purpose::QueueData, endpoint.clone())?;
        loop {
            let current =
                startup_read(deadline, &last_error, "await data worker queue cut", || {
                    control.current(&scope)
                })
                .await?;
            if current != source {
                if current.ready.view.run_id != node.intent.run_id || current.owner == local {
                    return Err(refused(
                        "queue authority left the data worker's authorized run",
                    ));
                }
                advance(&control, &shared, &replica, source, current).await?;
                break;
            }
            wait_startup(deadline).await?;
        }
        let current_mount = observe(pool.clone()).await?;
        if current_mount.total_bytes < mounted.total_bytes {
            return Err(refused(
                "data worker mount capacity changed during queue copy",
            ));
        }
        if let (Some(added), Some(observed)) = (&added_pool, &added_mount) {
            if observe(added.clone()).await?.total_bytes < observed.total_bytes {
                return Err(refused("added EKS pool capacity changed during queue copy"));
            }
        }
        endpoint.release_join_mount()?;
        let manager = HostedGroupManager::new(shared.clone(), client.clone(), registry.clone());
        let cluster_features = manager.cluster_features().clone();
        let (direct_catalog, direct_timestamps) = (catalog.identity.clone(), timestamps.clone());
        let scoped = if let Some(limits) = scoped_limits {
            let resolver = ScopedParticipants::new(shared.clone(), client.clone(), control.clone());
            let catalog_handle = CatalogHandle::new(resolver.participant(&catalog.identity).await?)?;
            let timestamps_client = TimestampClient::new(
                timestamps.clone(), shared.clone(), client.clone(), control.clone(), ServiceAction::Owner,
            )?;
            let peer = Arc::new(QueuePeerClient::new(client.clone(), shared.clone(), group.clone())?);
            endpoint.install_scoped_payload_readers(
                group.clone(), control.clone(), catalog_handle.clone(), timestamps_client.clone(),
            )?;
            endpoint.install_scoped_queue_peer(peer.clone())?;
            let charge = shared.scheduler().charge_memory(
                shared.scheduler_node_id(),
                WorkerSharedResources::request_actor_bytes(&config.execution).ok_or_else(invalid)?,
            )?;
            let worker = ScopedWorker::new(
                ScopedWorkerServices {
                    local, control: control.clone(), timestamps: timestamps_client,
                    status: status.clone(), catalog: catalog_handle, resolver, replica: replica.clone(),
                    endpoint: endpoint.clone(), peer, operator: false,
                    max_result_bytes: limits.max_retained_result_bytes_per_worker,
                },
                group.clone(),
                &auth.data_token,
                config.execution.max_workers_per_request,
                config.execution.result_retention_ms,
                Executor::new(2, 16)?,
                Executor::new(config.execution.max_pending_requests as usize, 0)?,
            )?;
            let application = super::application::ApplicationEndpoint::scoped(&worker);
            registration.bind(Purpose::ApiIngress, application.clone())?;
            registration.bind(Purpose::ApiObservation, application)?;
            let operation = transition_id("scoped-queue-capability-v2", &local.incarnation)?;
            startup_read(deadline, &last_error, "advertise data worker queue capability", || {
                control.advertise_scoped_queue(operation)
            }).await?;
            worker.start_maintenance()?;
            Some((worker, charge))
        } else {
            None
        };
        let placement = RemotePlacementTarget::new(
            manager.clone(),
            control.clone(),
            group.clone(),
            pool,
            node.intent.clone(),
            mounted.total_bytes,
            added_pool.zip(added_mount),
            key,
            status.topology().clone(),
            registered_raft_config(),
        )?;
        registration.bind(Purpose::ReplicaPlacement, placement.clone())?;
        let split = SplitEndpoint::for_data_worker(
            placement.clone(),
            manager,
            group.clone(),
            system_voters,
            control.clone(),
            timestamps,
            catalog.identity,
            status.topology().clone(),
        )?;
        registration.bind(Purpose::ShardSplit, split.clone())?;
        registration.bind(Purpose::ShardSplitProof, split.clone())?;
        registration.bind(Purpose::ShardSplitImage, split)?;
        let direct = super::worker_runtime::direct_capability::DirectSwitch::new(
            control.clone(),
            local,
            config.execution.direct_oltp,
        );
        // This worker leads Data ranges: serve `Method::Direct` once active.
        direct.serve_direct_when_active(
            super::worker_runtime::direct_capability::DirectLeaderServices {
                shared: shared.clone(),
                transport: client.clone(),
                catalog: direct_catalog,
                timestamps: direct_timestamps,
                local_timestamps: None,
            },
        );
        // CAP1: this worker's hosted leaders gate `ReclaimIndex` on it.
        let feature_control = control.clone();
        direct.on_active(move || {
            crate::oltp_replication::authority::control::watch_cluster_features(
                &feature_control,
                local,
                &[crate::oltp_replication::authority::control::INDEX_RECLAIM_FEATURE],
                &cluster_features,
            );
        });
        direct.start();
        Ok(Arc::new(Joined {
            scope,
            intent: node.intent,
            shared: shared.clone(),
            replica,
            control: control.clone(),
            joining,
            status,
            placement,
            auth,
            sample_slot: Semaphore::new(1),
            scoped,
            direct,
        }))
    })
    .await
    .unwrap_or_else(|_| {
        let failure = last_error
            .lock()
            .ok()
            .and_then(|error| error.clone())
            .unwrap_or_else(bootstrap_timeout);
        Err(failure)
    });

    let result = match result {
        Ok(joined) => {
            let voter_refresh =
                nabedra_rt::task::spawn(control_routes.run_refresh(control.clone(), stop.subscribe()));
            let refresh = nabedra_rt::task::spawn(directory.run_refresh(control, stop.subscribe()));
            let membership = nabedra_rt::task::spawn(follow_membership(joined.clone(), stop.subscribe()));
            let retirement = nabedra_rt::task::spawn(prune_retired(joined.clone(), stop.subscribe()));
            let (drain_stop, draining) = watch::channel(false);
            let drain = nabedra_rt::task::spawn(follow_drain(joined.clone(), draining));
            let (settle_stop, settling) = watch::channel(false);
            let placement = joined.placement.clone();
            let settlement = nabedra_rt::task::spawn(async move {
                placement.settle(settling).await;
                placement.shutdown().await
            });
            let result = serve(http, joined.clone()).await;
            joined.direct.stop().await;
            if let Some((scoped, _)) = &joined.scoped {
                scoped.drain().await;
            }
            let _ = drain_stop.send(true);
            let _ = drain.await;
            let _ = settle_stop.send(true);
            let settled = settlement.await.map_err(|_| unavailable("data placement settlement task failed"));
            let _ = stop.send(true);
            let _ = refresh.await;
            let _ = voter_refresh.await;
            let _ = membership.await;
            let _ = retirement.await;
            result.and(settled.and_then(|result| result))
        }
        Err(error) => Err(error),
    };
    let _ = stop.send(true);
    if let Some(refresh) = revocation_refresh.lock().ok().and_then(|mut task| task.take()) {
        let _ = refresh.await;
    }
    let transport = transport_task
        .await
        .map_err(|_| unavailable("data worker transport task failed"))?;
    result.and(transport)
}

async fn retirement_status(
    control: &Arc<ControlClient>,
    shared: &Arc<WorkerSharedResources>,
    client: &WorkerClient,
) -> Result<(Arc<PartitionedStatus>, GroupIdentity, GroupRecord), ProtocolError> {
    let revision = control.group_inventory_revision().await?;
    let mut groups = Vec::new();
    let mut timestamps = None;
    let mut catalog = None;
    let mut after = None;
    loop {
        let reply = control.groups(revision, after).await?;
        let bytes = reply.sections[0].as_ref().ok_or_else(invalid)?;
        let decode = crate::oltp::mvcc::json_decode_budget(bytes.as_slice())?;
        let reply = reply.reserve_reply_decode(shared, decode).await?;
        let page = reply.decode_section::<InventoryPage<GroupRecord>>(0)?;
        if page.revision != revision || !page.done && page.after <= after {
            return Err(invalid());
        }
        for group in page.items.iter() {
            if after.is_some_and(|previous| previous >= group.identity.group_id) {
                return Err(invalid());
            }
            after = Some(group.identity.group_id);
            match group.identity.kind {
                GroupKind::Status { bucket } => {
                    if groups.len() == 16 {
                        return Err(invalid());
                    }
                    groups.push((bucket, group.identity.clone()));
                }
                GroupKind::Timestamps => {
                    if timestamps.replace(group.identity.clone()).is_some() {
                        return Err(invalid());
                    }
                }
                GroupKind::Catalog => {
                    if catalog.replace(group.clone()).is_some() {
                        return Err(invalid());
                    }
                }
                _ => {}
            }
        }
        if after != page.after {
            return Err(invalid());
        }
        if page.done {
            break;
        }
    }
    groups.sort_unstable_by_key(|(bucket, _)| *bucket);
    let topology = StatusTopology::new(groups.into_iter().map(|(_, group)| group).collect())?;
    let clients = topology
        .groups()
        .iter()
        .map(|group| {
            StatusClient::new(
                group.clone(),
                shared.clone(),
                client.clone(),
                control.clone(),
                ServiceAction::QueueRetirement,
            )
            .map(|client| -> Arc<dyn StatusService> { client })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        PartitionedStatus::new(topology, clients)?,
        timestamps.ok_or_else(invalid)?,
        catalog.ok_or_else(invalid)?,
    ))
}

async fn prune_retired(joined: Arc<Joined>, mut stop: watch::Receiver<bool>) {
    let mut interval = nabedra_rt::time::interval(Duration::from_millis(250));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let cursor = Mutex::new(None);
    while !*stop.borrow() {
        tokio::select! {
            _ = stop.changed() => break,
            _ = interval.tick() => {}
        }
        let update = async {
            retirement::prune_replica(
                &joined.replica,
                &cursor,
                joined.control.as_ref(),
                &joined.scope,
                joined.status.as_ref(),
            )
            .await
        };
        tokio::select! {
            _ = stop.changed() => break,
            _ = nabedra_rt::time::timeout(Duration::from_secs(2), update) => {}
        }
    }
}

async fn read_members(
    control: &ControlClient,
    shared: &Arc<WorkerSharedResources>,
    expected: ReadyMembership,
) -> Result<Vec<WorkerIdentity>, ProtocolError> {
    let count = usize::try_from(expected.ready_count).map_err(|_| invalid())?;
    let size = count
        .checked_mul(2 * std::mem::size_of::<WorkerIdentity>())
        .ok_or_else(invalid)?;
    let _working = shared.reserve_scratch(crate::oltp::pool::ByteClass::Progress, size)?;
    let mut result = Vec::new();
    result.try_reserve_exact(count).map_err(|_| invalid())?;
    let mut after = None;
    loop {
        let reply = control.ready_page(expected, after).await?;
        let bytes = reply.sections[0].as_ref().ok_or_else(invalid)?;
        let decode = crate::oltp::mvcc::json_decode_budget(bytes.as_slice())?;
        let reply = reply.reserve_reply_decode(shared, decode).await?;
        let page = reply.decode_section::<InventoryPage<WorkerIdentity>>(0)?;
        if page.revision != expected.view.membership_revision
            || (!page.done && page.items.is_empty())
        {
            return Err(invalid());
        }
        for worker in page.items.iter() {
            if after.is_some_and(|old| worker.worker_id <= old) || result.len() >= count {
                return Err(invalid());
            }
            after = Some(worker.worker_id);
            result.push(*worker);
        }
        if page.after != after {
            return Err(invalid());
        }
        if page.done {
            return if result.len() == count {
                Ok(result)
            } else {
                Err(invalid())
            };
        }
    }
}

async fn advance(
    control: &ControlClient,
    shared: &Arc<WorkerSharedResources>,
    replica: &Mutex<WorkerReplica>,
    previous: QueueAuthority,
    current: QueueAuthority,
) -> Result<(), ProtocolError> {
    let members = read_members(control, shared, current.ready).await?;
    replica
        .lock()
        .map_err(|_| bootstrap_timeout())?
        .fenced_takeover(previous, current, members)
        .map_err(QueueError::protocol)
}

async fn follow_membership(joined: Arc<Joined>, mut stop: watch::Receiver<bool>) {
    let mut interval = nabedra_rt::time::interval(Duration::from_millis(250));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    while !*stop.borrow() {
        tokio::select! {
            _ = stop.changed() => continue,
            _ = interval.tick() => {}
        }
        let update = async {
            let previous = joined
                .replica
                .lock()
                .map_err(|_| bootstrap_timeout())?
                .authority();
            let current = joined.control.current(&joined.scope).await?;
            if current != previous {
                advance(
                    &joined.control,
                    &joined.shared,
                    &joined.replica,
                    previous,
                    current,
                )
                .await?;
            }
            Ok::<_, ProtocolError>(())
        };
        tokio::select! {
            _ = stop.changed() => {}
            result = update => if let Err(error) = result {
                tracing::debug!(code=?error.code, message=error.message, "data worker waits for current queue membership");
            }
        }
    }
}

async fn follow_drain(joined: Arc<Joined>, mut stop: watch::Receiver<bool>) {
    let mut interval = nabedra_rt::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = stop.changed() => return,
            _ = interval.tick() => {}
        }
        let result = async {
            let Some(record) = joined.control.own_worker_drain().await? else { return Ok(()); };
            if !matches!(record.phase, WorkerDrainPhase::QueueRemoved { .. }) { return Ok(()); }
            let proof = joined.placement.close_drained_worker(&record).await?;
            #[cfg(test)]
            super::recovery_tests::pause("worker_native_closed", record.id).await;
            let id = transition_id("data-worker-native-closed", &record.id)?;
            match joined.joining.close_worker(id, proof).await? {
                ControlReply::WorkerDrain(record) if matches!(record.phase, WorkerDrainPhase::Closed { .. }) => Ok(()),
                _ => Err(bootstrap_timeout()),
            }
        }.await;
        if let Err(error) = result {
            tracing::debug!(code=?error.code, message=error.message, "data worker waits for drain settlement");
        }
    }
}

async fn serve(listener: TcpListener, joined: Arc<Joined>) -> Result<(), ProtocolError> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|_| unavailable("data worker termination signal failed"))?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|_| unavailable("data worker interrupt signal failed"))?;
    let router = Router::new().route("/health/ready", get(readiness));
    let router = if joined.auth.admin_token.is_some() {
        router.route("/v1/cluster/operations", post(growth_admin))
    } else {
        router
    };
    crate::network::serve_router(listener, router.with_state(joined), None, async move {
        tokio::select! { _ = terminate.recv() => {}, _ = interrupt.recv() => {} }
    })
    .await
    .map_err(|_| unavailable("data worker readiness server failed"))
}

async fn readiness(
    State(joined): State<Arc<Joined>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, StatusCode> {
    if !super::http::authorize_database(
        &headers,
        &joined.auth.data_token,
        joined.scope.database.as_ref(),
    ) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let check = async {
        let mount = joined.placement.readiness_mount().await?;
        let added_mount = joined.placement.readiness_added_mount().await?;
        let current = joined.control.current(&joined.scope).await?;
        let queue = {
            let replica = joined.replica.lock().map_err(|_| bootstrap_timeout())?;
            if current != replica.authority() {
                return Err(bootstrap_timeout());
            }
            #[cfg(test)]
            super::recovery_tests::observe_record(&replica);
            replica.stats()
        };
        let mut body = json!({
            "role": "data_worker",
            "run_id": current.ready.view.run_id,
            "worker": WorkerIdentity::from(&joined.intent.enrollment),
            "growth_claim": joined.intent.claim_id,
            "ready_workers": current.ready.ready_count,
            "queue_owner": current.owner,
            "queue": queue,
            "block_cache": joined.shared.block_cache_usage(),
            "mount": mount,
            "system_groups_hosted": 0,
        });
        if let Some(added) = added_mount {
            body["added_mount"] = json!(added);
        }
        Ok((
            [(header::CONTENT_TYPE, "application/json")],
            body.to_string(),
        ))
    };
    nabedra_rt::time::timeout(Duration::from_secs(3), check)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}

const SAMPLE_BODY_LIMIT: usize = 4 << 10;
const SAMPLE_DEADLINE: Duration = Duration::from_secs(8);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AdminRequest {
    request_id: Uuid,
    operation: String,
    payload: Box<RawValue>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SampleRequest {
    pin: InventoryPin,
    worker: WorkerIdentity,
}

#[derive(Serialize)]
struct GrowthSample {
    pin: InventoryPin,
    inventory: WorkerPoolInventory,
    usage: UsageSample,
}

async fn growth_admin(State(joined): State<Arc<Joined>>, request: Request) -> Response {
    let mut request_id = None;
    let result = async {
        if !joined.auth.admin_token.as_ref().is_some_and(|token| {
            super::http::authorize_database(
                request.headers(),
                token,
                joined.scope.database.as_ref(),
            )
        }) {
            return Err(refused("unauthorized"));
        }
        let mut content_types = request.headers().get_all(header::CONTENT_TYPE).iter();
        let json = content_types
            .next()
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<mime::Mime>().ok())
            .is_some_and(|value| {
                value.type_() == mime::APPLICATION
                    && value.subtype() == mime::JSON
                    && value.suffix().is_none()
            })
            && content_types.next().is_none();
        if request.uri().query().is_some() || !json {
            return Err(invalid_sample());
        }
        let _slot = joined.sample_slot.try_acquire().map_err(|_| {
            ProtocolError::new(
                ErrorCode::Overloaded,
                "worker growth sample busy",
                Outcome::NotApplied,
            )
        })?;
        let _request = joined
            .shared
            .reserve_scratch(ByteClass::Progress, 32 << 10)?;
        nabedra_rt::time::timeout(SAMPLE_DEADLINE, async {
            let bytes = to_bytes(request.into_body(), SAMPLE_BODY_LIMIT)
                .await
                .map_err(|_| invalid_sample())?;
            crate::protocol::json_input(&bytes, SAMPLE_BODY_LIMIT).map_err(|_| invalid_sample())?;
            let envelope: AdminRequest =
                serde_json::from_slice(&bytes).map_err(|_| invalid_sample())?;
            request_id = Some(envelope.request_id);
            if envelope.request_id.is_nil() {
                return Err(invalid_sample());
            }
            if envelope.operation != "storage.growth.sample" {
                return Err(ProtocolError::new(
                    ErrorCode::UnsupportedOperation,
                    "data worker administration supports growth sampling only",
                    Outcome::NotApplied,
                ));
            }
            let sample =
                serde_json::from_str(envelope.payload.get()).map_err(|_| invalid_sample())?;
            let sample = joined.growth_sample(sample).await?;
            let encoded = response::encode_success(envelope.request_id, &sample, false);
            let bytes = joined
                .shared
                .copy_bytes_in(ByteClass::Progress, &encoded.body)?;
            Ok((encoded.status, Bytes::from_owner(bytes)))
        })
        .await
        .map_err(|_| sample_unavailable())?
    }
    .await;
    let (status, bytes) = result.unwrap_or_else(|error| {
        let encoded = response::encode_error(request_id, error);
        (encoded.status, Bytes::from(encoded.body))
    });
    (
        StatusCode::from_u16(status).expect("protocol HTTP status"),
        [(header::CONTENT_TYPE, "application/json")],
        bytes,
    )
        .into_response()
}

impl Joined {
    async fn growth_sample(&self, request: SampleRequest) -> Result<GrowthSample, ProtocolError> {
        let pin = request.pin;
        if request.worker != WorkerIdentity::from(&self.intent.enrollment)
            || pin.ready.view.run_id != self.intent.run_id
            || self.intent.mount.identity.cluster_id != self.scope.cluster_id
        {
            return Err(sample_unavailable());
        }
        let proof = self
            .control
            .own_growth_sample(pin, self.intent.claim_id)
            .await?;
        let primary = self.placement.readiness_mount().await?;
        let added = self.placement.readiness_added_mount().await?;
        if primary.expected != self.intent.mount
            || added
                .as_ref()
                .is_some_and(|mount| mount.expected.identity.cluster_id != self.scope.cluster_id)
        {
            return Err(sample_unavailable());
        }
        let (monotonic_ns, cpu_time_ns, read_bytes, write_bytes) = self
            .shared
            .execute(ExecutionClass::Control, 0, 0, || {
                process_counters().ok_or_else(sample_unavailable)
            })
            .await?;
        let (allocated_query_threads, charged_memory_bytes, limits) = self
            .shared
            .scheduler()
            .oltp_capacity(self.shared.scheduler_node_id())
            .ok_or_else(sample_unavailable)?;
        let (resident_group_count, resident_group_limit) = self.shared.resident_group_usage();
        let current = self
            .control
            .own_growth_sample(pin, self.intent.claim_id)
            .await?;
        if *current != *proof {
            return Err(sample_unavailable());
        }
        let (queued_count, oldest_queued) = {
            let replica = self.replica.lock().map_err(|_| sample_unavailable())?;
            if replica.authority() != current.authority {
                return Err(sample_unavailable());
            }
            replica
                .growth_queue_observation()
                .ok_or_else(sample_unavailable)?
        };
        let inventory = WorkerPoolInventory::for_joined_data_worker(
            &self.shared,
            &current,
            primary,
            added,
            charged_memory_bytes,
        )?;
        let usage = UsageSample {
            monotonic_ns,
            cpu_time_ns,
            read_bytes,
            write_bytes,
            allocated_query_threads,
            query_thread_limit: limits.max_threads,
            charged_memory_bytes,
            memory_limit_bytes: limits.memory_limit_bytes,
            resident_group_count,
            resident_group_limit,
            queued_count,
            oldest_queued,
        };
        Ok(GrowthSample {
            pin,
            inventory,
            usage,
        })
    }
}

fn invalid_sample() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid growth sample request",
        Outcome::NotApplied,
    )
}

fn sample_unavailable() -> ProtocolError {
    unavailable("automatic growth inventory unavailable")
}

async fn observe(
    pool: OltpPool,
) -> Result<crate::oltp::pool::mount::MountedPoolObservation, ProtocolError> {
    let shared = pool.resources().clone();
    shared
        .execute(ExecutionClass::Control, 0, 0, move || {
            pool.observe_registered_mount()
        })
        .await
}

fn tls_material(node: &Node) -> Result<TlsMaterial, ProtocolError> {
    fn pem(path: &Path) -> Result<Vec<u8>, ProtocolError> {
        let file = std::fs::File::open(path).map_err(|_| invalid())?;
        let metadata = file.metadata().map_err(|_| invalid())?;
        if !metadata.is_file() || !(1..=(64 << 10)).contains(&metadata.len()) {
            return Err(invalid());
        }
        let mut bytes = Vec::new();
        file.take((64 << 10) + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| invalid())?;
        if bytes.len() > 64 << 10 {
            return Err(invalid());
        }
        Ok(bytes)
    }
    let tls = TlsMaterial::from_pem(
        &pem(&node.certificate)?,
        &pem(&node.private_key)?,
        &pem(&node.authority_ca)?,
    )
    .map_err(|_| invalid())?;
    if Digest::parse(tls.certificate_sha256()).map_err(|_| invalid())?
        != node.intent.enrollment.certificate_sha256
    {
        return Err(refused(
            "data worker TLS certificate does not match its enrollment",
        ));
    }
    Ok(tls)
}

fn limited_enrollment_retry(error: ProtocolError) -> ProtocolError {
    if error.code == ErrorCode::Unauthorized {
        bootstrap_timeout()
    } else {
        error
    }
}

async fn replacement_token(shared: &Arc<WorkerSharedResources>) -> Result<String, ProtocolError> {
    shared.execute(ExecutionClass::Control, 0, 0, || {
        const MAX: usize = 8192;
        let mut token = String::new();
        std::fs::File::open("/var/run/nabedra-replacement/token")
            .map_err(|_| bootstrap_timeout())?.take((MAX + 1) as u64)
            .read_to_string(&mut token).map_err(|_| invalid())?;
        if !(32..=MAX).contains(&token.len())
            || !token.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(invalid());
        }
        Ok(token)
    }).await
}

fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid data worker bootstrap",
        Outcome::NotApplied,
    )
}

fn refused(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::Unauthorized, message, Outcome::NotApplied)
}

fn unavailable(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::StorageUnavailable, message, Outcome::NotApplied)
}

fn bootstrap_timeout() -> ProtocolError {
    unavailable("data worker is waiting for the durable enrollment or complete queue cut")
}
