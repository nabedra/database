//! Fixed, explicitly configured worker processes share the production queue
//! and transaction path. A static voter list grants no queue readiness.
use super::super::worker_endpoint::AuthorityReader;
use super::*;
use crate::oltp_replication::{
    authority::{
        control::{ControlMachine, ForwardedControl, HostedControlEndpoint, ReadyPhase},
        status::PartitionedStatus,
    },
    manager::{
        DamageReplacementEndpoint, DamageScheduler, DataGroupClient, DataGroupFactory,
        DataGroupSpec, FactoryDamageHost, RegisteredConfig, RegisteredWorker, ReplicaPlacement,
        shard_split::{SplitClient, SplitEndpoint},
    },
    service_rpc::{
        ParticipantClient, ParticipantEndpoint, StatusClient, StatusEndpoint, TimestampClient,
        TimestampEndpoint,
    },
};

mod cold;
mod data_worker;
mod restart;
mod system_replacement;

impl LocalServices {
    pub(crate) async fn open_registered(
        config: RegisteredConfig,
        resources: WorkerResources,
        native: SharedResourceConfig,
        execution: &ExecutionConfig,
    ) -> Result<Self, ProtocolError> {
        let scoped_oltp = config.scoped_oltp && config.required_voters == 3;
        let enrollments = config.control_enrollments.to_vec();
        let operator_pins = config
            .operator_pins
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let repository_path = config.archive_repository.clone();
        let repository_s3 = config.archive_repository_s3.clone();
        if repository_s3.is_some() && repository_path.is_none() {
            return Err(ProtocolError::new(
                ErrorCode::ValidationError,
                "S3 archives require a private archive_repository staging directory",
                Outcome::NotApplied,
            ));
        }
        if repository_s3.is_some() && !cfg!(feature = "oltp-backup-s3") {
            return Err(ProtocolError::new(
                ErrorCode::UnsupportedOperation,
                "S3 archives require the oltp-backup-s3 feature",
                Outcome::NotApplied,
            ));
        }
        let archive_keys =
            crate::oltp_backup_repository::ArchiveKeys::load(config.archive_encryption.as_ref())?;
        if archive_keys.writes_encrypted() && repository_path.is_none() {
            return Err(ProtocolError::new(
                ErrorCode::ValidationError,
                "encrypted archive writes require an archive repository",
                Outcome::NotApplied,
            ));
        }
        let mut worker = RegisteredWorker::open_development::<
            crate::oltp_runtime::worker_runtime::archive::VerifiedNativeRestore,
        >(
            config,
            resources,
            SharedResourceConfig {
                query_threads: resources.max_threads as usize,
                ..native
            },
            execution,
            archive_keys.clone(),
        )
        .await?;
        let local = WorkerIdentity::from(&worker.enrollment);
        let replacing = worker.system_replacement.is_some();
        let scoped_operator = replacing || operator_pins.contains(&local.certificate_sha256);
        let placements = worker.placements.clone();
        let status_topology = placements.status_topology()?;
        let seed = placements.control.voters[0].worker_id;
        let replication_factor = worker.required_voters;
        let rolling = worker.rolling.is_some();
        let manager = worker.manager.clone();
        let shared = worker.shared.clone();
        let archive_repository = match repository_path {
            Some(root) => Some(
                crate::oltp_runtime::worker_runtime::archive::NativeBackupPublication::open(
                    root,
                    repository_s3,
                    placements.control.identity.cluster_id,
                    shared.clone(),
                    archive_keys,
                )
                .await?,
            ),
            None => None,
        };
        let directory = worker.directory.clone();
        let pools = worker.pools.clone();
        eprintln!(
            "nabedra-db: registered consensus heartbeat_ms=500 election_min_ms=1500 election_max_ms=3000"
        );
        let deadline = nabedra_rt::time::now() + std::time::Duration::from_secs(if replacing { 240 } else if rolling { 120 } else { 60 });
        let last_error = Arc::new(Mutex::new(None::<ProtocolError>));
        let error_trace = last_error.clone();
        let scope = QueueScope {
            cluster_id: placements.control.identity.cluster_id,
            database: placements.control.identity.database.clone(),
        };
        let (stop, mut stopped) = watch::channel(false);
        let transport = worker.transport.clone();
        let listener = worker.listener.take().ok_or_else(unavailable)?;
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
        let result = nabedra_rt::time::timeout_at(deadline, async {
            let groups = worker.start_system_groups(registered_raft_config()).await?;
            let raw_control = groups.control.ok_or_else(unavailable)?;
            let raw_timestamps = groups.timestamps;
            let mut status_groups = Vec::new();
            for running in groups.status {
                let handle = StatusHandle::new(StatusMachine::new(running.state.store().clone())?, running.clone());
                status_groups.push((running, handle));
            }
            let raw_catalog = groups.catalog;
            let pins = operator_pins;
            let policy = worker.bootstrap_policy;
            let control = ControlHandle::new(
                ControlMachine::new(raw_control.state.store().clone())?,
                ForwardedControl::registered(manager.clone(), raw_control.clone(), pins.clone(), directory.clone())?,
                OperatorPolicy {
                    policy,
                    certificate_pins: pins.clone(),
                },
            );
            control.use_registered_directory(directory.clone())?;
            let registration = manager
                .registry()
                .reserve(placements.control.identity.clone())?;
            registration.bind(
                Purpose::SystemRecovery,
                Arc::new(crate::oltp_replication::manager::system_recovery::QuorumEndpoint {
                    manager: manager.clone(),
                    control: control.clone(),
                    probe_queue_peer: super::super::worker_endpoint::probe_queue_peer,
                }),
            )?;
            let control_endpoint = ControlEndpoint::new(control.clone(), shared.clone())?;
            let hosted_control = HostedControlEndpoint::registered(
                control_endpoint.clone(),
                raw_control.clone(),
                shared.clone(),
                pins.clone(),
                manager.clone(),
            )?;
            registration.bind(Purpose::Authority, hosted_control.clone())?;
            registration.bind(Purpose::ControlDelegation, hosted_control)?;
            let routes = Arc::new(LocalControlRoutes {
                manager: manager.clone(),
                group: placements.control.identity.group_id,
            });
            let rpc = ControlClient::new(
                manager.client(),
                shared.clone(),
                placements.control.identity.clone(),
                routes,
            )?;
            groups.damage.connect(&rpc);
            // NS14b: GroupDamaged and the replacement commands are proposed only
            // once every Control member decodes them (`data.direct.v1` active).
            control.install_group_damage_gate(
                crate::oltp_replication::authority::control::DirectCapabilityDamageGate::watch(
                    rpc.clone(),
                    std::time::Duration::from_secs(5),
                ),
            );
            let native_timestamps = raw_timestamps.as_ref().map(|running| {
                Ok::<_, ProtocolError>(TimestampHandle::new(
                    TimestampMachine::new(running.state.store().clone())?, running.clone(),
                ))
            }).transpose()?;
            let native_catalog = raw_catalog.as_ref().map(|running| {
                Ok::<_, ProtocolError>(Arc::new(ParticipantHandle::new(
                    ParticipantMachine::new(running.state.store().clone())?, running.clone(),
                )))
            }).transpose()?;
            let peer = Arc::new(QueuePeerClient::new(
                manager.client(),
                shared.clone(),
                placements.control.identity.clone(),
            )?);
            if let Some(native_timestamps) = &native_timestamps {
                native_timestamps.install_release_reader(rpc.clone(), peer.clone())?;
                // T3's ReserveWindow predicate (T7): data.direct.v1 is active.
                native_timestamps.install_direct_gate(
                    super::super::worker_runtime::direct_capability::ControlReserveWindowGate::new(
                        rpc.clone(),
                    ),
                );
                // PERF1: keep a window reserved after a queued allocation discards it.
                native_timestamps.spawn_direct_prefill();
            }
            for (running, handle) in &status_groups {
                let node = running.state.resident().node_id();
                let endpoint = StatusEndpoint::new(
                    handle.clone(),
                    node,
                    rpc.clone(),
                    manager.client(),
                    placements.timestamps.identity.clone(),
                    status_topology.clone(),
                )?;
                endpoint.install_admission_reader(peer.clone())?;
                manager.bind_service(handle.identity(), node, endpoint)?;
            }
            if let (Some(raw_timestamps), Some(native_timestamps)) = (&raw_timestamps, &native_timestamps) {
                let timestamp_endpoint = TimestampEndpoint::new(
                    native_timestamps.clone(),
                    raw_timestamps.state.resident().node_id(),
                    rpc.clone(),
                    manager.client(),
                    status_topology.clone(),
                )?;
                if let Some(repository) = &archive_repository {
                    timestamp_endpoint.install_backup_reader(repository.clone())?;
                }
                manager.bind_service(
                    &placements.timestamps.identity,
                    raw_timestamps.state.resident().node_id(),
                    timestamp_endpoint,
                )?;
            }
            if let (Some(raw_catalog), Some(native_catalog)) = (&raw_catalog, &native_catalog) {
                manager.bind_service(
                    &placements.catalog.identity,
                    raw_catalog.state.resident().node_id(),
                    ParticipantEndpoint::new(
                        native_catalog.clone(),
                        raw_catalog.state.resident().node_id(),
                        rpc.clone(),
                        manager.client(),
                        Some(status_topology.clone()),
                    )?,
                )?;
            }
            let operator = &worker.operator;
            let recovery = if replacing {
                None
            } else if worker.cold.is_some() {
                Some(cold::begin(&worker, &control, seed, deadline, &error_trace).await?)
            } else if rolling {
                restart::enroll(&worker, &control, deadline, &error_trace).await?;
                None
            } else {
                loop {
                    match startup_read(deadline, &error_trace, "initial Control state", || control.inspect()).await? {
                        Some(state) if state.ready.view.run_id == worker.initial_run && state.policy == policy => break,
                        Some(state) if worker.restored.is_some() && state.ready.view.run_id == worker.initial_run => {
                            return Err(ProtocolError::new(
                                ErrorCode::Unauthorized,
                                "mounted restore policy differs from committed Control policy",
                                Outcome::NotApplied,
                            ));
                        }
                        Some(_) if worker.restored.is_some() => return Err(unavailable()),
                        Some(_) => return Err(failure("registered initial run or topology policy differs")),
                        None if local.worker_id == seed => {
                            accepted(
                                control
                                    .initialize_local(
                                        operator,
                                        id(worker.bootstrap_id, b"initialize"),
                                        worker.initial_run,
                                    )
                                    .await?
                                    .reply,
                            )?;
                        }
                        _ => {}
                    }
                    wait_startup(deadline).await?;
                }
                let old = startup_read(deadline, &error_trace, "local enrollment", || control.worker(local.worker_id)).await?;
                if old.as_ref() != Some(&worker.enrollment) {
                    if old.is_some() {
                        return Err(failure("registered worker enrollment differs"));
                    }
                    accepted(
                        control
                            .enroll_local(
                                operator,
                                id(local.incarnation, b"enroll"),
                                None,
                                worker.enrollment.clone(),
                                None,
                            )
                            .await?
                            .reply,
                    )?;
                }
                None
            };
            if !replacing {
                worker.open_secondary_pools(&control, pins).await?;
            }
            let operator = &worker.operator;
            // A rolling restart reads the directory below instead of waiting for every enrollment.
            if !rolling && !replacing {
                loop {
                    let mut complete = true;
                    for enrollment in &enrollments {
                        complete &=
                            startup_read(deadline, &error_trace, "peer enrollment", || control.worker(enrollment.worker_id)).await?.as_ref() == Some(enrollment);
                    }
                    if complete {
                        break;
                    }
                    wait_startup(deadline).await?;
                }
            }
            if !replacing {
                startup_read(deadline, &error_trace, "peer directory", || async {
                    worker.directory.refresh(&rpc).await.map_err(|error| {
                        if rolling && error.code == ErrorCode::Unauthorized { unavailable() } else { error }
                    })
                }).await?;
                eprintln!("nabedra-db: registered {} enrolled", local.worker_id);
            }
            if replacing {
                // The fresh system replicas opened as learners. Their
                // membership is established together with the Data learners.
            } else if recovery.is_some() {
                startup_read(deadline, &error_trace, "fixed cold worker inventory", || {
                    cold::verify_workers(&worker, &control)
                })
                .await?;
                cold::verify_system_groups(&worker, &control, &rpc, deadline, &error_trace).await?;
            } else if rolling {
                cold::verify_system_groups(&worker, &control, &rpc, deadline, &error_trace).await?;
            } else {
                let state = startup_read(deadline, &error_trace, "Control inspect", || control.inspect()).await?.ok_or_else(unavailable)?;
                let authority = state.ready.view.authority();
                // Each real leader fences and registers its own group. Leadership
                // can differ across groups and across worker processes.
                loop {
                    let progress = async {
                    if raw_control.ensure_linearizable().await.is_ok() {
                        register(&control, operator, &raw_control, authority).await?;
                    }
                    if let (Some(raw_timestamps), Some(native_timestamps)) = (&raw_timestamps, &native_timestamps)
                        && raw_timestamps.ensure_linearizable().await.is_ok()
                    {
                        if native_timestamps.current_authority().await?.is_none() {
                            native_timestamps
                                .fence(
                                    id(placements.timestamps.identity.group_id, b"initial-fence"),
                                    None,
                                    authority,
                                    state.ready,
                                )
                                .await?;
                        }
                        register(&control, operator, raw_timestamps, authority).await?;
                    }
                    for (running, handle) in &status_groups {
                        if running.ensure_linearizable().await.is_ok() {
                            if handle.current_authority().await?.is_none() {
                                handle.fence(
                                    id(handle.identity().group_id, b"initial-fence"),
                                    None,
                                    authority,
                                ).await?;
                            }
                            register(&control, operator, running, authority).await?;
                        }
                    }
                    if let (Some(raw_catalog), Some(native_catalog)) = (&raw_catalog, &native_catalog)
                        && raw_catalog.ensure_linearizable().await.is_ok()
                    {
                        if native_catalog.activation().await?.is_none() {
                            let seed = ParticipantMachine::new(raw_catalog.state.store().clone())?
                                .restore_seed()
                                .await?;
                            if worker.restored.is_some() != seed.is_some() {
                                return Err(failure("registered Catalog restore genesis differs"));
                            }
                            let (schema_epoch, routing_epoch, span) = seed.map_or(
                                (1, 1, KeySpan { start: None, end: None }),
                                |seed| (seed.schema_epoch, seed.routing_epoch, seed.span),
                            );
                            native_catalog
                                .fence_initial(
                                    id(placements.catalog.identity.group_id, b"initial-fence"),
                                    authority,
                                    placements.catalog.identity.group_id,
                                    schema_epoch,
                                    routing_epoch,
                                    span,
                                )
                                .await?;
                        }
                        register(&control, operator, raw_catalog, authority).await?;
                    }
                    let mut complete = true;
                    for placement in placements.all() {
                        complete &= control.group(placement.identity.group_id).await?
                            .is_some_and(|record| record.identity == placement.identity
                                && record.replica_count == placement.voters.len() as u64);
                    }
                    Ok::<_, ProtocolError>(complete)
                    }.await;
                    match progress {
                        Ok(true) => break,
                        Ok(false) => {},
                        Err(error) if bootstrap_transient(&error) => {
                            tracing::debug!(worker=%local.worker_id,code=?error.code,outcome=?error.outcome,message=error.message,"system group bootstrap retry");
                            *error_trace.lock().map_err(|_|unavailable())?=Some(error);
                        },
                        Err(error) => return Err(error),
                    }
                    wait_startup(deadline).await?;
                }
                eprintln!(
                    "nabedra-db: registered {} system groups fenced and registered",
                    local.worker_id
                );
                super::restore::activate_registered(&worker, &control, operator, &rpc, authority, deadline).await?;
            }
            let timestamps = TimestampClient::new(
                placements.timestamps.identity.clone(),
                shared.clone(),
                manager.client(),
                rpc.clone(),
                crate::oltp_replication::authority::control::ServiceAction::Owner,
            )?;
            let mut status = Vec::new();
            for group in status_topology.groups() {
                status.push(StatusClient::new(
                    group.clone(),
                    shared.clone(),
                    manager.client(),
                    rpc.clone(),
                    crate::oltp_replication::authority::control::ServiceAction::Owner,
                )?);
            }
            if scoped_oltp {
                control_endpoint.install_scoped_queue_fence_readers(
                    status_topology.clone(),
                    status.clone(),
                    timestamps.clone(),
                    placements.catalog.identity.clone(),
                    manager.client(),
                    rpc.clone(),
                )?;
            }
            let services = status.iter().cloned()
                .map(|client| -> Arc<dyn StatusService> { client })
                .collect();
            let routed_status = PartitionedStatus::new(status_topology.clone(), services)?;
            let data = DataGroupClient::new(
                manager.clone(),
                placements.control.identity.clone(),
                placements.catalog.voters.clone(),
            )?;
            let data_bootstrap = if placements.catalog.local(local.worker_id).is_some() {
                &placements.catalog
            } else {
                &placements.control
            };
            let data_raft = registered_raft_config();
            #[cfg(test)]
            let data_raft = crate::oltp_growth_acceptance::snapshot_test_config(data_raft);
            let factory = DataGroupFactory::new(
                    manager.clone(),
                    worker.pool.clone(),
                    data_bootstrap.local(local.worker_id).ok_or_else(unavailable)?,
                    data_bootstrap.voters.clone(),
                    placements.catalog.identity.clone(),
                    control.clone(),
                    rpc.clone(),
                    status_topology.clone(),
                    data_raft,
                )?;
            if let Some(record) = &worker.system_replacement {
                factory.host_replacement_data(record).await?;
                system_replacement::join(&worker, &control, &raw_control, &enrollments, deadline).await?;
                startup_read(deadline, &error_trace, "replacement peer directory", || async {
                    directory.refresh(&rpc).await.map_err(|error|
                        if error.code == ErrorCode::Unauthorized { unavailable() } else { error })
                }).await?;
                directory.set_cold_bootstrap(false);
            } else if rolling {
                startup_read(deadline, &error_trace, "rolling retained Data groups", || factory.rehost_retained(&pools)).await?;
            } else if worker.restored.is_none() {
                if worker.restart_mount.is_some() {
                    startup_read(deadline, &error_trace, "retained Data group mounts", || factory.rehost_retained(&pools)).await?;
                } else {
                    startup_read(deadline, &error_trace, "retained Data groups", || factory.start_retained(recovery.is_some())).await?;
                }
            }
            registration.bind(Purpose::GroupLifecycle, factory.clone())?;
            let split = SplitClient::new(data.clone(), rpc.clone(),
                placements.timestamps.identity.clone(), placements.catalog.identity.clone());
            let split_endpoint = SplitEndpoint::new(factory.clone(), pools.clone(), split.clone(), native_timestamps.clone(), native_catalog.clone(), status_topology.clone())?;
            registration.bind(Purpose::ShardSplit, split_endpoint.clone())?;
            registration.bind(Purpose::ShardSplitProof, split_endpoint.clone())?;
            registration.bind(Purpose::ShardSplitImage, split_endpoint)?;
            if let Some(recovery) = &recovery {
                cold::finish(recovery, &worker, &control, &rpc, seed, deadline, &error_trace)
                    .await?;
                directory.set_cold_bootstrap(false);
            }
            if !rolling {
                restart::register_mount(&worker, &control, deadline, &error_trace).await?;
            }
            worker.seal_replacement_identity()?;
            let restart_mount = worker
                .restart_mount
                .take()
                .map(|mount| (mount.generation, mount.key));
            let damage_endpoint = DamageReplacementEndpoint::new(
                FactoryDamageHost::new(factory.clone(), pools.clone()),
                placements.control.identity.clone(),
            );
            registration.bind(Purpose::GroupDamage, damage_endpoint.clone())?;
            let placement = ReplicaPlacement::new(factory, pools.clone(), restart_mount);
            registration.bind(Purpose::ReplicaPlacement, placement.clone())?;
            #[cfg(test)]
            if let Some(restart) = &worker.rolling {
                super::super::recovery_tests::pause("rolling_before_join", restart.intent.id).await;
            }
            let operator = Arc::new(worker.operator);
            system_replacement::spawn(
                manager.clone(),
                control.clone(),
                raw_control.clone(),
                directory.clone(),
                operator.clone(),
                stop.subscribe(),
            );
            // NS14b: Control replaces damaged Data replicas on its own, from the
            // process whose Control replica leads.
            DamageScheduler::new(
                control.clone(),
                raw_control.clone(),
                operator.clone(),
                damage_endpoint,
                std::time::Duration::from_secs(2),
            )
            .spawn(stop.subscribe());
            let resolver = Arc::new(RegisteredParticipants {
                shared: shared.clone(),
                manager: manager.clone(),
                rpc: rpc.clone(),
                control: control.clone(),
                operator: operator.clone(),
                data,
                catalog: placements.catalog.identity.clone(),
            });
            let catalog =
                CatalogHandle::new(startup_read(deadline, &error_trace, "Catalog participant", || resolver.participant(&placements.catalog.identity)).await?)?;
            if replacing {
                use crate::oltp_replication::authority::control::{DIRECT_VERSION, DirectCapabilityOperation};
                startup_read(deadline, &error_trace, "replacement direct capability", || async {
                    rpc.change_direct_capability(nabedra_rt::ids::uuid_v4(), DirectCapabilityOperation::Advertise {
                        version: DIRECT_VERSION,
                    }).await.map(|_| ())
                }).await?;
            }
            let (state, ticket, bootstrap) = restart::begin_queue_join(
                &control, &operator, local, seed, deadline, &error_trace,
            ).await?;
            let ticket_id = ticket.ticket_id;
            let mut members = startup_read(deadline, &error_trace, "ready membership", || ready_members(&control, state.ready)).await?;
            eprintln!(
                "nabedra-db: registered {} queue join begun",
                local.worker_id
            );
            let (legacy_limits, scoped_limits) = queue_limits_for_formats(execution, scoped_oltp);
            let mut replica = if bootstrap {
                WorkerReplica::new_empty_bootstrap(
                    shared.clone(),
                    scope.clone(),
                    local,
                    ticket,
                    legacy_limits,
                )
            } else {
                WorkerReplica::new_joining(
                    shared.clone(),
                    scope.clone(),
                    local,
                    startup_read(deadline, &error_trace, "queue authority", || control.current(&scope)).await?,
                    members.clone(),
                    legacy_limits,
                    ticket,
                )
            }
            .map_err(QueueError::protocol)?;
            if let Some(limits) = scoped_limits {
                replica.install_scoped_queues(limits).map_err(QueueError::protocol)?;
            }
            let replica = replica.bind_for_reads(&shared)?;
            let issuer = super::super::admission_tickets::AdmissionTickets::new(
                shared.clone(),
                local,
                scope.clone(),
                control.clone(),
                replica.clone(),
            );
            let endpoint = QueueEndpoint::new(
                replica.clone(),
                shared.clone(),
                scope.clone(),
                control.clone(),
                resources.task_memory_limit_bytes,
            )?;
            endpoint.install_admission_tickets(issuer.clone())?;
            endpoint.install_retirement_reader(routed_status.clone())?;
            if scoped_oltp {
                endpoint.install_scoped_payload_readers(
                    placements.control.identity.clone(), rpc.clone(), catalog.clone(), timestamps.clone(),
                )?;
                endpoint.install_scoped_queue_peer(peer.clone())?;
            }
            let scoped = scoped_limits.map(|limits| super::super::scoped_worker::ScopedWorkerServices {
                local,
                control: rpc.clone(),
                timestamps: timestamps.clone(),
                status: routed_status.clone(),
                catalog: catalog.clone(),
                resolver: resolver.clone(),
                replica: replica.clone(),
                endpoint: endpoint.clone(),
                peer: peer.clone(),
                operator: scoped_operator,
                max_result_bytes: limits.max_retained_result_bytes_per_worker,
            });
            registration.bind(Purpose::QueueControl, endpoint.clone())?;
            registration.bind(Purpose::QueueData, endpoint)?;
            members.push(local);
            members.sort_unstable_by_key(|worker| worker.worker_id);
            for page in members.chunks(128) {
                accepted(
                    control
                        .ready_page_local(
                            &operator,
                            nabedra_rt::ids::uuid_v4(),
                            ticket_id,
                            Page::new(page.to_vec()).map_err(|_| unavailable())?,
                        )
                        .await?
                        .reply,
                )?;
            }
            let ControlReply::ReadySealed { expected, next, .. } = control
                .seal_ready_local(&operator, nabedra_rt::ids::uuid_v4(), ticket_id)
                .await?
                .reply
            else {
                return Err(unavailable());
            };
            eprintln!(
                "nabedra-db: registered {} queue ready revision sealed",
                local.worker_id
            );
            let fence = TimestampClient::new(
                placements.timestamps.identity.clone(),
                shared.clone(),
                manager.client(),
                rpc.clone(),
                crate::oltp_replication::authority::control::ServiceAction::ControlFence {
                    transition_id: ticket_id,
                },
            )?;
            // The same transition returns the original receipt if the write committed.
            // A Timestamp election can outlast one service call during a rolling join.
            let receipt = startup_read(deadline, &error_trace, "timestamp ready fence", || {
                fence.fence_ready(ticket_id, expected, next)
            })
            .await?;
            eprintln!(
                "nabedra-db: registered {} timestamp ready fence committed",
                local.worker_id
            );
            accepted(
                control
                    .timestamp_fenced(nabedra_rt::ids::uuid_v4(), receipt)
                    .await?
                    .reply,
            )?;
            if bootstrap {
                let proof = {
                    let replica = replica.lock().map_err(|_| unavailable())?;
                    VerifiedQueueCopy::from_empty_worker(ticket, &*replica)?
                };
                accepted(control.publish_ready(nabedra_rt::ids::uuid_v4(), proof).await?.reply)?;
                let current = startup_read(deadline, &error_trace, "queue authority", || control.current(&scope)).await?;
                replica
                    .lock()
                    .map_err(|_| unavailable())?
                    .activate_empty_bootstrap(current)
                    .map_err(QueueError::protocol)?;
            } else {
                loop {
                    if startup_read(deadline, &error_trace, "Control inspect", || control.inspect()).await?.ok_or_else(unavailable)?.ready == next {
                        break;
                    }
                    wait_startup(deadline).await?;
                }
                let current = startup_read(deadline, &error_trace, "queue authority", || control.current(&scope)).await?;
                replica
                    .lock()
                    .map_err(|_| unavailable())?
                    .fenced_takeover(
                        QueueAuthority {
                            ready: state.ready,
                            owner: state.owner.ok_or_else(unavailable)?,
                        },
                        current,
                        members,
                    )
                    .map_err(QueueError::protocol)?;
            }
            let queue = QueueCoordinator::new(replica.clone(), control.clone(), peer.clone());
            queue.bind_for_reads(&shared)?;
            eprintln!("nabedra-db: registered {} queue active", local.worker_id);
            let membership = spawn_membership(
                control.clone(),
                scope.clone(),
                local,
                replica.clone(),
                queue.clone(),
                peer,
                data_worker::JoinOwner {
                    operator: operator.clone(),
                    timestamps: placements.timestamps.identity.clone(),
                    manager: manager.clone(),
                    rpc: rpc.clone(),
                },
                Rejoin {
                    limits: legacy_limits,
                },
                stop.subscribe(),
            );
            if let Some(genesis) = worker.restored.take() {
                loop {
                    let state = startup_read(deadline, &error_trace, "restored ready set", || control.inspect())
                        .await?
                        .ok_or_else(unavailable)?;
                    if state.ready.ready_count == replication_factor as u64 && state.pending.is_none() {
                        break;
                    }
                    wait_startup(deadline).await?;
                }
                if replica.lock().map_err(|_| unavailable())?.stats().accepted_records != 0 {
                    return Err(failure("restored worker queue is not empty"));
                }
                genesis.publish()?;
            }
            Ok((
                control, operator, timestamps, status, routed_status, catalog, resolver, replica, queue, rpc,
                issuer, membership, split, placement, scoped, native_timestamps,
            ))
        }).await.unwrap_or_else(|_| {
            let mut error=last_error.lock().ok().and_then(|error|error.clone()).unwrap_or_else(unavailable);
            error.message=format!("registered bootstrap deadline expired: {}",error.message);
            Err(error)
        });
        match result {
            Ok((
                control,
                operator,
                timestamps,
                status,
                routed_status,
                catalog,
                resolver,
                replica,
                queue,
                control_client,
                issuer,
                membership,
                split,
                placement,
                scoped,
                native_timestamps,
            )) => {
                let refresh = nabedra_rt::task::spawn(
                    directory
                        .clone()
                        .run_refresh(control_client.clone(), stop.subscribe()),
                );
                Ok(Self {
                    shared,
                    pools,
                    scope,
                    local,
                    replication_factor,
                    direct_timestamps: native_timestamps.map(|handle| {
                        handle as Arc<
                        dyn crate::oltp_replication::authority::timestamp::DirectTimestampSource,
                    >
                    }),
                    cold_history: Some(
                        super::super::worker_runtime::cold_history::Sources::Registered {
                            timestamps: timestamps.clone(),
                        },
                    ),
                    control,
                    operator,
                    timestamps,
                    status: routed_status,
                    catalog,
                    resolver,
                    control_client,
                    admission_issuer: Some(issuer),
                    remote_status: status,
                    archive_repository,
                    split: Some(split),
                    placement: Some(placement),
                    replica,
                    queue,
                    scoped,
                    manager,
                    directory,
                    refresh: Mutex::new(Some(refresh)),
                    membership: Mutex::new(Some(membership)),
                    stop,
                    transport: Mutex::new(Some(transport_task)),
                })
            }
            Err(error) => {
                let _ = manager.shutdown().await;
                let _ = stop.send(true);
                let _ = transport_task.await;
                Err(error)
            }
        }
    }
}

pub(in crate::oltp_runtime) async fn startup_read<T, F, Fut>(
    deadline: tokio::time::Instant,
    last_error: &Mutex<Option<ProtocolError>>,
    stage: &'static str,
    mut read: F,
) -> Result<T, ProtocolError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, ProtocolError>>,
{
    loop {
        match nabedra_rt::time::timeout_at(deadline, read()).await {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(error)) if bootstrap_transient(&error) => {
                tracing::debug!(stage, code=?error.code, outcome=?error.outcome, message=error.message,
                    "registered authoritative read retry");
                *last_error.lock().map_err(|_| unavailable())? = Some(error);
            }
            Ok(Err(error)) => return Err(error),
            Err(_) => return Err(unavailable()),
        }
        wait_startup(deadline).await?;
    }
}
pub(in crate::oltp_runtime) async fn wait_startup(deadline: tokio::time::Instant) -> Result<(), ProtocolError> {
    if nabedra_rt::time::now() >= deadline {
        return Err(unavailable());
    }
    nabedra_rt::time::sleep_until(
        (nabedra_rt::time::now() + std::time::Duration::from_millis(100)).min(deadline),
    )
    .await;
    Ok(())
}
pub(in crate::oltp_runtime) async fn ready_members(
    control: &ControlHandle,
    ready: ReadyMembership,
) -> Result<Vec<WorkerIdentity>, ProtocolError> {
    let mut result = Vec::new();
    let mut after = None;
    loop {
        let bytes = control.ready_page_read(ready, after).await?;
        let page: crate::oltp_replication::authority::control::InventoryPage<WorkerIdentity> =
            serde_json::from_slice(bytes.as_ref()).map_err(|_| unavailable())?;
        result.extend(page.items.iter().copied());
        if page.done {
            return Ok(result);
        }
        after = page.after;
    }
}
/// What a live worker needs to join the ready set again after the owner
/// removed it (it was paused or partitioned, and is back with the same
/// incarnation and its RAM queue now stale).
struct Rejoin {
    limits: QueueLimits,
}
impl Rejoin {
    /// Begins (or adopts) this worker's own Join of the current ready set and
    /// replaces its stale replica with an empty joining one for that ticket.
    /// Returns the ticket once the replica waits for the owner's queue copy.
    #[allow(clippy::too_many_arguments)]
    async fn begin(
        &self,
        control: &ControlHandle,
        operator: &LocalOperatorPermit,
        local: WorkerIdentity,
        replica: &Mutex<WorkerReplica>,
        current: QueueAuthority,
        members: Vec<WorkerIdentity>,
    ) -> Result<Option<Uuid>, ProtocolError> {
        // A positively excluded process never serves the queue again; only a
        // new incarnation may join.
        if control.worker_replaced(local).await? {
            return Ok(None);
        }
        let state = control.inspect().await?.ok_or_else(unavailable)?;
        if state.ready != current.ready {
            return Ok(None);
        }
        let ticket = match &state.pending {
            None => {
                let operation = crate::oltp_runtime::authority_fence::transition_id(
                    "registered-queue-rejoin",
                    &(local, state.ready),
                )?;
                match control
                    .begin_join_local(operator, operation, state.ready.view, local)
                    .await?
                    .reply
                {
                    ControlReply::JoinBegun { ticket, .. } => ticket,
                    _ => return Ok(None),
                }
            }
            // The Join applied but its reply was lost: adopt it before any page.
            Some(Pending::Ready(pending))
                if pending.change
                    == crate::oltp_replication::authority::control::ReadyChange::Join
                    && pending.target == local
                    && pending.expected == current.ready
                    && pending.phase == ReadyPhase::Building
                    && pending.source_after.is_none()
                    && !pending.target_seen =>
            {
                JoinTicket {
                    ticket_id: pending.transition_id,
                    target: local,
                    source_ready: pending.expected,
                }
            }
            _ => return Ok(None),
        };
        replica
            .lock()
            .map_err(|_| unavailable())?
            .rejoin(current, members, self.limits, ticket)
            .map_err(QueueError::protocol)?;
        eprintln!(
            "nabedra-db: registered {} queue rejoin begun",
            local.worker_id
        );
        Ok(Some(ticket.ticket_id))
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_membership(
    control: Arc<ControlHandle>,
    scope: QueueScope,
    local: WorkerIdentity,
    replica: Arc<Mutex<WorkerReplica>>,
    queue: Arc<QueueCoordinator>,
    peer: Arc<QueuePeerClient>,
    join_owner: data_worker::JoinOwner,
    rejoin: Rejoin,
    mut stop: watch::Receiver<bool>,
) -> JoinHandle<()> {
    nabedra_rt::task::spawn(async move {
        // The ticket of this worker's own Join while it rejoins the ready set.
        let mut rejoining: Option<Uuid> = None;
        // At idle, every worker repeated two linearizable Control reads
        // each 250 ms (PERF2c); quiet passes now wait for a Control change.
        let mut idle = crate::oltp_replication::authority::control::IdlePasses::new(
            std::time::Duration::from_secs(2),
        );
        loop {
            if *stop.borrow_and_update() {
                break;
            }
            let started = nabedra_rt::time::now();
            let applied = control.applied_index();
            let held = replica.lock().ok().map(|replica| replica.authority());
            if !idle.due(applied, &held, started) {
                tokio::select! {_=nabedra_rt::time::sleep(std::time::Duration::from_millis(250))=>{},_=stop.changed()=>{}}
                continue;
            }
            // Ok(true) when the pass found nothing to do.
            let progress = async {
                let current = control.current(&scope).await?;
                let previous = replica.lock().map_err(|_| unavailable())?.authority();
                if current != previous {
                    #[cfg(test)]
                    if current.ready.ready_count == 3
                        && super::super::recovery_tests::hold_o21_ready_view()
                    {
                        return Ok(false);
                    }
                    let members = ready_members(&control, current.ready).await?;
                    if members.contains(&local) {
                        replica
                            .lock()
                            .map_err(|_| unavailable())?
                            .fenced_takeover(previous, current, members)
                            .map_err(QueueError::protocol)?;
                        if rejoining.take().is_some() {
                            eprintln!("nabedra-db: registered {} queue rejoined", local.worker_id);
                        }
                    } else {
                        // Removed while unresponsive (D05/LIVE1): join again.
                        rejoining = rejoin
                            .begin(
                                &control,
                                &join_owner.operator,
                                local,
                                &replica,
                                current,
                                members,
                            )
                            .await?;
                        return Ok(false);
                    }
                }
                if rejoining.is_none() && current.owner != local {
                    return Ok(current == previous);
                }
                let state = control.inspect().await?.ok_or_else(unavailable)?;
                let quiet = current == previous && rejoining.is_none() && state.pending.is_none();
                if let (Some(ticket), Some(Pending::Ready(pending))) = (rejoining, &state.pending)
                    && pending.transition_id == ticket
                    && pending.target == local
                    && pending.phase != ReadyPhase::AwaitingQueueProof
                {
                    // The Building pages and the Timestamp fence of its own Join;
                    // the owner copies the queue and publishes.
                    join_owner.advance(&control, pending).await?;
                }
                if current.owner == local
                    && let Some(Pending::Ready(pending)) = state.pending
                {
                    if pending.change != crate::oltp_replication::authority::control::ReadyChange::Join {
                        return Ok(false);
                    }
                    if pending.phase == ReadyPhase::AwaitingQueueProof {
                        let ticket = JoinTicket {
                            ticket_id: pending.transition_id,
                            target: pending.target,
                            source_ready: pending.expected,
                        };
                        let proof = queue.copy_join(ticket, &peer).await?;
                        accepted(control.publish_ready(nabedra_rt::ids::uuid_v4(), proof).await?.reply)?;
                    } else if control.data_worker(pending.target).await?.is_some() {
                        join_owner.advance(&control, &pending).await?;
                    }
                }
                Ok::<_, ProtocolError>(quiet)
            };
            tokio::select! {result=progress=>{
                let quiet = match result {
                    Ok(quiet) => quiet,
                    Err(error) => { tracing::debug!(worker=%local.worker_id, code=?error.code, outcome=?error.outcome, message=error.message, "queue membership progress waits"); false }
                };
                idle.record(quiet, applied, held, started);
            },_=stop.changed()=>{}}
            tokio::select! {_=nabedra_rt::time::sleep(std::time::Duration::from_millis(250))=>{},_=stop.changed()=>{}}
        }
    })
}
struct RegisteredParticipants {
    shared: Arc<WorkerSharedResources>,
    manager: Arc<HostedGroupManager>,
    rpc: Arc<ControlClient>,
    control: Arc<ControlHandle>,
    operator: Arc<LocalOperatorPermit>,
    data: Arc<DataGroupClient>,
    catalog: GroupIdentity,
}
impl ParticipantResolver for RegisteredParticipants {
    fn participant<'a>(
        &'a self,
        group: &'a GroupIdentity,
    ) -> ResolveFuture<'a, Arc<ParticipantHandle>> {
        Box::pin(async move {
            Ok(Arc::new(ParticipantHandle::remote(
                group.clone(),
                self.shared.clone(),
                ParticipantClient::new(
                    group.clone(),
                    self.shared.clone(),
                    self.manager.client(),
                    self.rpc.clone(),
                    crate::oltp_replication::authority::control::ServiceAction::Owner,
                )?,
            )?))
        })
    }
    fn create_data<'a>(
        &'a self,
        table: &'a TableDescriptor,
        pin: &'a PinnedCatalog,
    ) -> ResolveFuture<'a, ShardRoute> {
        Box::pin(async move {
            let shard_id = id(table.id, b"initial-shard");
            let group = GroupIdentity {
                cluster_id: self.catalog.cluster_id,
                database: self.catalog.database.clone(),
                group_id: id(shard_id, b"data-group"),
                kind: GroupKind::Data {
                    table_id: table.id,
                    shard_id,
                },
                group_format_version: GROUP_FORMAT_VERSION,
            };
            let spec = DataGroupSpec {
                group: group.clone(),
                authority: pin.authority,
                catalog_group_id: self.catalog.group_id,
                transition_id: id(group.group_id, b"initial-fence"),
                schema_epoch: 1,
                routing_epoch: 1,
                span: KeySpan {
                    start: None,
                    end: None,
                },
            };
            let voters = self.data.initial_voters(&self.control, &self.rpc).await?;
            let started = nabedra_rt::time::now();
            let hosted = async {
                let current = self
                    .control
                    .current(&QueueScope {
                        cluster_id: group.cluster_id,
                        database: group.database.clone(),
                    })
                    .await?;
                if current.ready.view.authority() != pin.authority {
                    return Err(unavailable());
                }
                let ready = ready_members(&self.control, current.ready).await?;
                self.data.host_all(spec.clone(), &self.control, &ready, &voters).await
            }
            .await;
            tracing::debug!(group_id = %group.group_id, stage = "data_host", ms = nabedra_rt::time::elapsed(started).as_millis(), ok = hosted.is_ok(), "initial Data group");
            hosted?;
            let started = nabedra_rt::time::now();
            let activated = self.data.activate(spec, &voters).await;
            tracing::debug!(group_id = %group.group_id, stage = "data_activate", ms = nabedra_rt::time::elapsed(started).as_millis(), ok = activated.is_ok(), "initial Data group");
            let observation = activated?;
            let generation = id(group.group_id, b"control-membership");
            let started = nabedra_rt::time::now();
            let registered = self
                .control
                .register_initial_data_local(&self.operator, generation, pin.authority, observation)
                .await
                .and_then(|applied| accepted(applied.reply));
            tracing::debug!(group_id = %group.group_id, stage = "data_register", ms = nabedra_rt::time::elapsed(started).as_millis(), ok = registered.is_ok(), "initial Data group");
            registered?;
            Ok(ShardRoute {
                group,
                span: KeySpan {
                    start: None,
                    end: None,
                },
                schema_epoch: 1,
                routing_epoch: 1,
            })
        })
    }
}
fn failure(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::Conflict, message, Outcome::NotApplied)
}
pub(super) fn bootstrap_transient(error: &ProtocolError) -> bool {
    matches!(
        error.code,
        ErrorCode::StorageUnavailable | ErrorCode::OutcomeUnknown | ErrorCode::Overloaded
    )
}
