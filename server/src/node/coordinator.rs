//! Captured remote partitions and one v3 publication per logical mutation.
use super::{
    admission::{Binding, StartSubject},
    auth::{self, RequestIds, Route},
    http::{self, Result, cpu, invalid, unavailable},
    job::{self, FileInput, JobEnvelope, JobResult, RowMutation, Task},
    payload_owner::{Command, PayloadLease, Reply},
    runtime::Worker,
};
use crate::{
    api::response::{self, EncodedResponse},
    engine::ExecuteResult,
    protocol::{self, ErrorCode, Operation, Outcome, ProtocolError, Request, TableDescriptor},
    storage::{
        CatalogEntry, ClusterCatalog, Decision, HistoryBudget, HistoryLookup, KeyRange,
        LoadedClusterCatalog, PartitionFile, PreparedPublication, PublicationOutcome, TableData,
    },
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
mod resources;
mod settlement;
pub(super) use resources::RemoteScheduler;
use uuid::Uuid;

fn error(code: ErrorCode) -> ProtocolError {
    http::failure(code)
}
fn terminal(d: Decision) -> EncodedResponse {
    EncodedResponse {
        status: d.response_status,
        body: d.response_body.into_bytes(),
    }
}
struct Peers<'a> {
    worker: &'a Worker,
    key: crate::request_queue::RequestKey,
    data: super::runtime::DataGrant,
    binding: Binding,
    authority: StartSubject,
    digest: String,
    opened: tokio::sync::Mutex<BTreeMap<Uuid, crate::cluster::member::Node>>,
    fence: crate::storage::CatalogFence,
    queue_binding: job::QueueBinding,
    sequence: u64,
    cap: usize,
    result_charge: std::sync::Mutex<Option<crate::scheduling::MemoryCharge>>,
}
impl Peers<'_> {
    async fn frontier(&self) -> Result<usize> {
        let root = self.worker.root().await?;
        super::queue_runtime::validate_coordination_capacity(&root, None, None)
    }
    async fn current(&self) -> Result<()> {
        let root = self.worker.root().await?;
        if self.worker.queue.current_binding(self.key.clone())? != self.queue_binding {
            return Err(unavailable());
        }
        if root.scheduler.scheduler_term != self.fence.scheduler_term
            || root.scheduler.coordinator_node != self.worker.signer.sender().node_id()
        {
            return Err(unavailable());
        }
        let fence = self.fence;
        let catalog = self
            .data
            .storage()
            .load_cluster_catalog()
            .await
            .map_err(|_| unavailable())?;
        if (!root.template.mode.is_fixed()
            && catalog.catalog().owner_cluster != Some(root.cluster_id))
            || catalog.catalog().fence != Some(fence)
        {
            return Err(unavailable());
        }
        let pin = self.worker.pin;
        let authority = self.authority.clone();
        cpu(&self.worker.control_cpu, move |_| {
            authority
                .check(&root, &pin)
                .map(|_| ())
                .map_err(|_| unavailable())
        })
        .await
    }

    async fn control(
        &self,
        target: &crate::cluster::member::Node,
        permit: Uuid,
        route: Route,
    ) -> Result<bool> {
        http::control_backpressure(|| self.control_once(target, permit, route)).await
    }
    async fn control_once(
        &self,
        target: &crate::cluster::member::Node,
        permit: Uuid,
        route: Route,
    ) -> Result<bool> {
        let root = self.worker.root().await?;
        let member = root
            .nodes
            .iter()
            .find(|n| {
                n.node_id == target.node_id
                    && n.incarnation == target.incarnation
                    && n.session == target.session
            })
            .ok_or_else(unavailable)?
            .clone();
        let signer = self.worker.signer.clone();
        let ids = RequestIds {
            operation: Some(self.binding.operation_id()),
            admission: Some(self.binding.admission_id()),
            digest: Some(self.digest.clone()),
            ..Default::default()
        };
        let endpoint = member.endpoint.clone().ok_or_else(unavailable)?;
        let context = cpu(&self.worker.control_cpu, move |_| {
            signer.context(
                &root,
                &member,
                route,
                ids,
                serde_json::to_vec(&serde_json::json!({"version":1,"permit_id":permit}))
                    .map_err(|_| invalid())?,
            )
        })
        .await?;
        let reply = http::small_exchange(
            &self.worker.client,
            &self.worker.signer,
            context,
            &endpoint,
            &self.worker.storage,
            &self.worker.control_cpu,
        )
        .await?;
        match route {
            Route::DependencyOpen => reply
                .dependency()
                .filter(|d| d.permit_id() == permit)
                .map(|_| true)
                .ok_or_else(unavailable),
            Route::DependencyClose => reply
                .close()
                .filter(|d| d.permit_id() == permit)
                .map(|d| d.closed())
                .ok_or_else(unavailable),
            _ => Err(invalid()),
        }
    }
    async fn settlement_evidence(
        &self,
        target: &crate::cluster::member::Node,
        permit: Uuid,
    ) -> settlement::Evidence {
        if let Ok(catalog) = self.data.storage().load_cluster_catalog().await {
            if catalog
                .catalog()
                .fence
                .is_some_and(|fence| fence != self.fence)
            {
                return settlement::Evidence::Fenced;
            }
        }
        if let Ok(root) = self.worker.root().await {
            if root
                .nodes
                .iter()
                .find(|node| node.node_id == target.node_id)
                .is_some_and(|node| {
                    node.incarnation != target.incarnation
                        || matches!(
                            node.state,
                            crate::cluster::member::NodeState::Lost
                                | crate::cluster::member::NodeState::Stopped
                        )
                })
            {
                return settlement::Evidence::Fenced;
            }
        }
        match self.control(target, permit, Route::DependencyClose).await {
            Ok(true) => settlement::Evidence::Closed,
            Ok(false) | Err(_) => settlement::Evidence::Pending,
        }
    }
    async fn settle_dependency<G, C>(
        &self,
        target: &crate::cluster::member::Node,
        permit: Uuid,
        grant: G,
        child: C,
    ) -> C {
        let child = settlement::retain_until_settled(
            grant,
            child,
            || self.settlement_evidence(target, permit),
            nabedra_rt::time::sleep,
        )
        .await;
        self.opened.lock().await.remove(&permit);
        child
    }
    async fn finish(&self) {
        let opened = self.opened.lock().await.clone();
        for (permit, target) in opened {
            self.settle_dependency(&target, permit, (), ()).await;
        }
    }
    async fn job(
        &self,
        member: crate::cluster::member::Node,
        permit: Uuid,
        job: JobEnvelope,
        lease: &mut PayloadLease,
        may_exist: &mut bool,
        verified_settled: &mut bool,
    ) -> Result<job::ResultEnvelope> {
        // Dedicated child dependencies include self-targeted work. Closing the
        // operation's primary permit would wait for this coordinator itself.
        *may_exist = true;
        self.opened.lock().await.insert(permit, member.clone());
        if !self.control(&member, permit, Route::DependencyOpen).await? {
            return Err(unavailable());
        }
        let root = self.worker.root().await?;
        let member = root
            .nodes
            .iter()
            .find(|n| {
                n.node_id == member.node_id
                    && n.incarnation == member.incarnation
                    && n.session == member.session
            })
            .ok_or_else(unavailable)?
            .clone();
        let endpoint = member.endpoint.clone().ok_or_else(unavailable)?;
        let signer = self.worker.signer.clone();
        let expected = job.clone();
        let ids = RequestIds {
            operation: Some(job.operation_id),
            admission: Some(self.binding.admission_id()),
            job: Some(job.job_id),
            attempt: Some(job.attempt_id),
            dependency: Some(permit),
            digest: Some(self.digest.clone()),
        };
        let (context, framed) = cpu(&self.worker.control_cpu, move |_| {
            let bytes = job::encode_job(&job).map_err(|_| invalid())?;
            let context = signer.context(&root, &member, Route::Jobs, ids, bytes)?;
            let mut framed = Vec::with_capacity(context.initial_bytes().len() + 10);
            framed.push(super::frame::FrameKind::Control as u8);
            framed.extend_from_slice(&(context.initial_bytes().len() as u32).to_be_bytes());
            framed.extend_from_slice(context.initial_bytes());
            framed.extend_from_slice(&[5, 0, 0, 0, 0]);
            Ok((context, framed))
        })
        .await?;
        let deadline = nabedra_rt::time::now() + Duration::from_secs(30);
        let request = self.worker.signer.request(
            &endpoint,
            &context,
            axum::body::Body::from(framed),
            true,
        )?;
        let response = self.worker.client.request(request, deadline).await?;
        if response.status() != 200 {
            return Err(unavailable());
        }
        http::framing(response.headers(), 0, true)?;
        if http::one(response.headers(), "content-type")? != "application/octet-stream" {
            return Err(invalid());
        }
        let a = http::one(response.headers(), "x-nabedra-auth")?.to_owned();
        let sig = http::one(response.headers(), "x-nabedra-signature")?.to_owned();
        let mut reader = http::framed_reader(response.into_body());
        let first = nabedra_rt::time::timeout_at(deadline, reader.next_frame())
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| invalid())?
            .ok_or_else(invalid)?;
        if first.kind != super::frame::FrameKind::Control {
            return Err(invalid());
        }
        let root = self.worker.root().await?;
        let handoff = cpu(&self.worker.control_cpu, move |_| {
            let signed = auth::verify_reply(&a, &sig, 200, &context).map_err(|_| invalid())?;
            let content =
                auth::check_reply_content(&signed, first.payload).map_err(|_| invalid())?;
            let eligible =
                auth::check_reply_authority(&signed, &root, &context).map_err(|_| invalid())?;
            let observation = crate::cluster::controller::OwnershipObservation::new(&root)
                .map_err(|_| unavailable())?;
            context
                .into_reply_handoff(eligible, content, &observation)
                .map_err(|_| invalid())
        })
        .await?;
        let Reply::Accepted(result) = lease
            .receive_checked_reply(reader, handoff, Box::new(expected))
            .await
            .map_err(|_| unavailable())?
        else {
            return Err(invalid());
        };
        *verified_settled = true;
        if let JobResult::Error { code, .. } = result.result {
            lease
                .command(Command::ClearReceived)
                .await
                .map_err(|_| unavailable())?;
            return Err(error(code));
        }
        self.current().await?;
        Ok(*result)
    }
}
impl Peers<'_> {
    async fn dispatch_job(
        &self,
        job: JobEnvelope,
        source: &super::payload_owner::ForkSource,
    ) -> Result<(job::ResultEnvelope, PayloadLease)> {
        let deadline = nabedra_rt::time::now() + Duration::from_secs(60);
        self.current().await?;
        let root = self.worker.root().await?;
        let view = self.worker.remote_scheduler.view(&root)?;
        let grant = view
            .scheduler
            .reserve_remote_memory(self.sequence, self.cap, resources::task_bytes(&job)?)
            .await?;
        let member = view
            .nodes
            .get(&grant.node_id())
            .ok_or_else(unavailable)?
            .clone();
        let mut child = self
            .worker
            .owners
            .fork_stream(source, deadline)
            .await
            .map_err(|_| unavailable())?;
        let permit = nabedra_rt::ids::uuid_v4();
        let mut may_exist = false;
        let mut verified_settled = false;
        let result = async {
            child.ready().await.map_err(|_| unavailable())?;
            self.job(
                member.clone(),
                permit,
                job,
                &mut child,
                &mut may_exist,
                &mut verified_settled,
            )
            .await
        }
        .await;
        let child = if may_exist {
            if verified_settled {
                // A checked result through EOF proves the remote task settled.
                // Dependency cleanup still remains owned through its close reply.
                drop(grant);
                self.settle_dependency(&member, permit, (), child).await
            } else {
                // The queue actor remains the single owner while uncertain
                // children settle; stop dependent dispatch without inventing a
                // terminal result or changing this attempt's captured identity.
                let _ = self.worker.mark_reconciling(self.key.clone()).await;
                self.settle_dependency(&member, permit, grant, child).await
            }
        } else {
            drop(grant);
            child
        };
        match result {
            Ok(result) => Ok((result, child)),
            Err(error) => {
                let _ = child.dispose().await;
                Err(error)
            }
        }
    }
}

pub(super) async fn execute(
    worker: &Worker,
    key: crate::request_queue::RequestKey,
    data: super::runtime::DataGrant,
    binding: Binding,
    authority: StartSubject,
    request: Request,
    bytes: Box<[u8]>,
    lease: &mut PayloadLease,
) -> Result<EncodedResponse> {
    let mutation = request.operation.is_mutation();
    let queue_binding = worker.queue.current_binding(key.clone())?;
    let mut peers = Peers {
        worker,
        key: key.clone(),
        data,
        binding,
        authority,
        digest: http::digest(&bytes),
        opened: tokio::sync::Mutex::new(BTreeMap::new()),
        fence: crate::storage::CatalogFence {
            run_id: queue_binding.run_id,
            scheduler_term: queue_binding.scheduler_term,
        },
        queue_binding,
        sequence: worker.queue.request_sequence(key).ok_or_else(unavailable)?,
        cap: worker
            .ready
            .prepared
            .input
            .value
            .template
            .execution
            .max_workers_per_request as usize,
        result_charge: std::sync::Mutex::new(None),
    };
    let result = execute_inner(&mut peers, request, bytes, lease).await;
    peers.finish().await;
    // A failed attempt cannot establish rollback of earlier same-ID work.
    result.map_err(|e| {
        if mutation && e.code != ErrorCode::Conflict {
            ProtocolError::new(
                ErrorCode::OutcomeUnknown,
                "mutation outcome is unknown",
                Outcome::Unknown,
            )
        } else {
            e
        }
    })
}
async fn execute_inner(
    peers: &Peers<'_>,
    request: Request,
    bytes: Box<[u8]>,
    lease: &mut PayloadLease,
) -> Result<EncodedResponse> {
    let is_mutation = request.operation.is_mutation();
    let deadline = nabedra_rt::time::now() + Duration::from_secs(60);
    let budget = HistoryBudget::new(4096, 64 * 1024 * 1024, deadline).map_err(|_| unavailable())?;
    for _ in 0..3 {
        let storage = peers.data.storage();
        let original = bytes.clone();
        let id = request.request_id;
        let pinned = if is_mutation {
            None
        } else {
            peers.worker.queue.pinned(peers.key.clone())
        };
        let expected_fence = peers.fence;
        let loaded = if let Some(pinned) = pinned {
            pinned
        } else {
            storage
                .load_cluster_catalog()
                .await
                .map_err(|_| unavailable())?
        };
        if !peers
            .worker
            .ready
            .prepared
            .input
            .value
            .template
            .mode
            .is_fixed()
            && loaded.catalog().owner_cluster
                != Some(peers.worker.ready.prepared.input.value.cluster_id)
        {
            return Err(unavailable());
        }
        if is_mutation && loaded.catalog().fence != Some(expected_fence) {
            return Err(unavailable());
        }
        let replay = if is_mutation {
            Some(
                storage
                    .lookup_decision(&loaded, id, &original, budget)
                    .await
                    .map_err(|_| unavailable())?,
            )
        } else {
            None
        };
        match replay {
            Some(HistoryLookup::Found(d)) => return Ok(terminal(d)),
            Some(HistoryLookup::IdentityConflict) => return Err(error(ErrorCode::Conflict)),
            _ => {}
        }
        let commit = nabedra_rt::ids::uuid_v4();
        let tables = loaded.catalog().tables.clone();
        let calculated = calculate(peers, &loaded, request.operation.clone(), commit, lease).await;
        let (tables, encoded) = match calculated {
            Ok((tables, value)) => {
                let charge = peers.result_charge.lock().unwrap().take();
                let encoded = if let Some(charge) = charge {
                    peers
                        .worker
                        .cpu
                        .submit_precharged(peers.sequence, peers.cap, charge, move |_, _| {
                            Ok(response::encode_success(id, &value, false))
                        })
                        .await?
                        .receive()
                        .await?
                } else {
                    cpu(&peers.worker.cpu, move |_| {
                        Ok(response::encode_success(id, &value, false))
                    })
                    .await?
                };
                (tables, encoded)
            }
            Err(e)
                if is_mutation
                    && matches!(
                        e.code,
                        ErrorCode::ValidationError
                            | ErrorCode::NotFound
                            | ErrorCode::Conflict
                            | ErrorCode::LimitExceeded
                    ) =>
            {
                lease
                    .command(Command::ClearStaged)
                    .await
                    .map_err(|_| unavailable())?;
                (tables, response::encode_error(Some(id), e))
            }
            Err(e) => return Err(e),
        };
        if !is_mutation {
            return Ok(encoded);
        }
        let original = bytes.clone();
        let status = encoded.status;
        let body = encoded.body.clone();
        let owner = peers
            .key
            .storage
            .as_ref()
            .map(|_| peers.worker.ready.prepared.input.value.cluster_id);
        let candidate = cpu(&peers.worker.cpu, move |_| {
            let base = loaded.catalog();
            let decision = Decision {
                version: 1,
                id: commit,
                parent: base.decision_head,
                base_revision: base.revision,
                revision: base
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| error(ErrorCode::LimitExceeded))?,
                request_id: id,
                request_body: String::from_utf8(original.into_vec()).map_err(|_| invalid())?,
                response_status: status,
                response_body: String::from_utf8(body).map_err(|_| invalid())?,
            };
            // Oversized response fallback must never publish changed tables.
            let tables = if status == 200 {
                tables
            } else {
                base.tables.clone()
            };
            match owner {
                Some(cluster) => PreparedPublication::prepare_owned(
                    loaded,
                    tables,
                    decision,
                    cluster,
                    expected_fence,
                ),
                None => {
                    PreparedPublication::prepare_fenced(loaded, tables, decision, expected_fence)
                }
            }
            .map_err(|_| error(ErrorCode::LimitExceeded))
        })
        .await?;
        if encoded.status == 200 {
            lease
                .command(Command::UploadStaged(peers.data.storage()))
                .await
                .map_err(|_| unavailable())?;
        } else {
            lease
                .command(Command::ClearStaged)
                .await
                .map_err(|_| unavailable())?;
        }
        peers.current().await?;
        let storage = peers.data.storage();
        let outcome = candidate
            .publish(&storage, budget)
            .await
            .map_err(|_| unavailable())?;
        match outcome {
            PublicationOutcome::Terminal(d) => return Ok(terminal(d)),
            PublicationOutcome::IdentityConflict => return Err(error(ErrorCode::Conflict)),
            PublicationOutcome::Unknown => {
                return Err(ProtocolError::new(
                    ErrorCode::OutcomeUnknown,
                    "publication outcome is unknown",
                    Outcome::Unknown,
                ));
            }
            PublicationOutcome::NeedsRecompute => {}
        }
        lease
            .command(Command::ClearStaged)
            .await
            .map_err(|_| unavailable())?;
    }
    Err(ProtocolError::new(
        ErrorCode::OutcomeUnknown,
        "mutation contention unresolved",
        Outcome::Unknown,
    ))
}
fn resolve(root: &ClusterCatalog, id: Uuid) -> Result<(TableDescriptor, CatalogEntry)> {
    root.tables
        .iter()
        .find(|(_, e)| e.id == id)
        .map(|(name, e)| (e.descriptor(name), e.clone()))
        .ok_or_else(|| error(ErrorCode::NotFound))
}
fn inputs(entry: &CatalogEntry) -> Vec<FileInput> {
    match &entry.data {
        TableData::Legacy { snapshot } => vec![FileInput::Legacy {
            object: snapshot.clone(),
            row_count: entry.row_count,
        }],
        TableData::Partitioned { files } => files.iter().cloned().map(FileInput::from).collect(),
    }
}
fn envelope(
    peers: &Peers<'_>,
    loaded: &LoadedClusterCatalog,
    table: &TableDescriptor,
    ordinal: usize,
    task: Task,
) -> Result<JobEnvelope> {
    Ok(JobEnvelope {
        queue_binding: Some(peers.queue_binding),
        version: 1,
        cluster_id: peers.worker.ready.prepared.input.value.cluster_id,
        operation_id: peers.binding.operation_id(),
        job_id: nabedra_rt::ids::uuid_v4(),
        attempt_id: nabedra_rt::ids::uuid_v4(),
        request_digest: peers.digest.clone(),
        snapshot: job::Snapshot {
            catalog_revision: loaded.catalog().revision,
            catalog_commit_id: loaded.catalog().commit_id,
            catalog_etag: loaded.etag().into(),
        },
        ordinal: u32::try_from(ordinal).map_err(|_| invalid())?,
        table: table.clone(),
        task,
    })
}
async fn calculate(
    peers: &Peers<'_>,
    loaded: &LoadedClusterCatalog,
    operation: Operation,
    commit: Uuid,
    lease: &mut PayloadLease,
) -> Result<(BTreeMap<String, CatalogEntry>, ExecuteResult)> {
    let mut tables = loaded.catalog().tables.clone();
    match operation {
        Operation::Create(p) => {
            protocol::validate_schema(&p.name, &p.schema, &p.key)?;
            if tables.contains_key(&p.name) {
                return Err(error(ErrorCode::Conflict));
            }
            let table = TableDescriptor {
                id: nabedra_rt::ids::uuid_v4(),
                name: p.name,
                schema: p.schema,
                key: p.key,
                format: p.format,
            };
            tables.insert(
                table.name.clone(),
                CatalogEntry {
                    id: table.id,
                    schema: table.schema.clone(),
                    key: table.key.clone(),
                    format: table.format,
                    data: TableData::Partitioned { files: Vec::new() },
                    row_count: 0,
                },
            );
            Ok((tables, ExecuteResult::Descriptor(table)))
        }
        Operation::Describe(p) => {
            let e = tables
                .get(&p.name)
                .ok_or_else(|| error(ErrorCode::NotFound))?;
            let value = ExecuteResult::Descriptor(e.descriptor(&p.name));
            Ok((tables, value))
        }
        Operation::List(_) => {
            let value =
                ExecuteResult::Descriptors(tables.iter().map(|(n, e)| e.descriptor(n)).collect());
            Ok((tables, value))
        }
        Operation::Drop(p) => {
            let (table, _) = resolve(loaded.catalog(), p.table_id)?;
            tables.remove(&table.name);
            Ok((tables, ExecuteResult::Null))
        }
        Operation::Explain(p) => {
            let (table, _) = resolve(loaded.catalog(), p.table_id)?;
            let text = crate::engine::explain_query(&table, &p.query)?;
            Ok((tables, ExecuteResult::Explain(text)))
        }
        op @ (Operation::Get(_) | Operation::Collect(_) | Operation::Count(_)) => {
            let id = match &op {
                Operation::Get(p) => p.table_id,
                Operation::Collect(p) | Operation::Count(p) => p.table_id,
                _ => unreachable!(),
            };
            let (table, entry) = resolve(loaded.catalog(), id)?;
            op.validate_for_table(&table)?;
            let mut selected = inputs(&entry);
            if let Operation::Get(p) = &op
                && matches!(entry.data, TableData::Partitioned { .. })
            {
                selected = entry
                    .route(&p.key)
                    .map_err(|_| unavailable())?
                    .into_iter()
                    .cloned()
                    .map(FileInput::from)
                    .collect();
            }
            let mut rows = Vec::new();
            let mut total = 0u64;
            let mut source_rows = 0u64;
            let mut source_bytes = 0u64;
            let mut jobs = Vec::new();
            for (index, file) in selected.into_iter().enumerate() {
                let task = match &op {
                    Operation::Get(p) => Task::Get {
                        file,
                        key: p.key.clone(),
                    },
                    Operation::Collect(p) => Task::Collect {
                        file,
                        query: p.query.clone(),
                    },
                    Operation::Count(p) => Task::Count {
                        file,
                        query: p.query.clone(),
                    },
                    _ => unreachable!(),
                };
                jobs.push((index, envelope(peers, loaded, &table, index, task)?));
            }
            let source = peers
                .worker
                .owners
                .fork_source(lease)
                .map_err(|_| unavailable())?;
            let frontier = peers.frontier().await?;
            let mut pending = jobs.into_iter();
            let mut running = FuturesUnordered::new();
            let mut failure: Option<ProtocolError> = None;
            let mut ordered_rows = BTreeMap::new();
            let mut charges = Vec::new();
            let mut retained = 0u64;
            loop {
                while failure.is_none() && running.len() < frontier {
                    let Some((index, job)) = pending.next() else {
                        break;
                    };
                    let source = &source;
                    running.push(async move { (index, peers.dispatch_job(job, source).await) });
                }
                let Some((index, received)) = running.next().await else {
                    break;
                };
                let (result, mut child) = match received {
                    Ok(value) => value,
                    Err(error) => {
                        if failure.is_none() {
                            failure = Some(error);
                        }
                        continue;
                    }
                };
                let processed = async {
                    if failure.is_some() {
                        return Ok(());
                    }
                    let (r, b) = match result.result {
                        JobResult::Get {
                            source_rows,
                            source_payload_bytes,
                            ..
                        }
                        | JobResult::Collect {
                            source_rows,
                            source_payload_bytes,
                            ..
                        } => {
                            let charge_bytes = crate::snapshot::memory_shape(
                                &table,
                                source_rows as usize,
                                source_payload_bytes as usize,
                                0,
                            )?;
                            retained = retained
                                .checked_add(charge_bytes)
                                .ok_or_else(|| error(ErrorCode::LimitExceeded))?;
                            if retained > peers.worker.cpu.resources().task_memory_limit_bytes {
                                return Err(error(ErrorCode::LimitExceeded));
                            }
                            charges.push(
                                peers
                                    .worker
                                    .cpu
                                    .scheduler()
                                    .charge_memory(0, charge_bytes)?,
                            );
                            let Reply::Rows(part) = child
                                .command(Command::TakeRows)
                                .await
                                .map_err(|_| unavailable())?
                            else {
                                return Err(invalid());
                            };
                            ordered_rows.insert(index, part);
                            (source_rows, source_payload_bytes)
                        }
                        JobResult::Count {
                            source_rows,
                            source_payload_bytes,
                            matched_rows,
                        } => {
                            total = total
                                .checked_add(matched_rows)
                                .ok_or_else(|| error(ErrorCode::LimitExceeded))?;
                            child
                                .command(Command::ClearReceived)
                                .await
                                .map_err(|_| unavailable())?;
                            (source_rows, source_payload_bytes)
                        }
                        _ => return Err(invalid()),
                    };
                    source_rows = source_rows
                        .checked_add(r)
                        .ok_or_else(|| error(ErrorCode::LimitExceeded))?;
                    source_bytes = source_bytes
                        .checked_add(b)
                        .ok_or_else(|| error(ErrorCode::LimitExceeded))?;
                    if source_rows > protocol::MAX_TABLE_ROWS as u64
                        || source_bytes > protocol::MAX_DECODED_PAYLOAD_BYTES as u64
                    {
                        return Err(error(ErrorCode::LimitExceeded));
                    }
                    Ok(())
                }
                .await;
                let disposed = child.dispose().await.map_err(|_| unavailable());
                if let Err(error) = processed.and(disposed) {
                    if failure.is_none() {
                        failure = Some(error);
                    }
                }
            }
            if let Some(error) = failure {
                return Err(error);
            }
            for part in ordered_rows.into_values() {
                rows.try_reserve(part.len())
                    .map_err(|_| error(ErrorCode::LimitExceeded))?;
                rows.extend(part);
            }
            let root = peers.worker.root().await?;
            let remote = peers.worker.remote_scheduler.view(&root)?;
            let local = remote
                .nodes
                .iter()
                .find(|(_, node)| Some(node.node_id) == peers.worker.signer.sender().node_id())
                .map(|(id, _)| *id)
                .ok_or_else(unavailable)?;
            let budget = peers.worker.cpu.resources().task_memory_limit_bytes;
            let remote_reduction = remote
                .scheduler
                .reserve_on(peers.sequence, peers.cap, local, budget)
                .await?;
            let work = move |_, _: &crate::worker_pool::WorkerRuntime| match op {
                Operation::Get(_) => Ok(ExecuteResult::Row(rows.pop())),
                Operation::Collect(p) => {
                    crate::engine::reduce_collect(&table, &p.query, &rows).map(ExecuteResult::Rows)
                }
                Operation::Count(p) => Ok(ExecuteResult::Count(
                    total.min(p.query.limit.unwrap_or(u64::MAX)),
                )),
                _ => unreachable!(),
            };
            let value = if charges.is_empty() {
                peers
                    .worker
                    .cpu
                    .submit_request(peers.sequence, peers.cap, budget, work)
                    .await?
                    .receive()
                    .await?
            } else {
                let (value, charge) = peers
                    .worker
                    .cpu
                    .submit_buffered(peers.sequence, peers.cap, charges, budget, work)
                    .await?
                    .receive()
                    .await?;
                *peers.result_charge.lock().unwrap() = Some(charge);
                value
            };
            drop(remote_reduction);
            Ok((tables, value))
        }
        operation => {
            let id = match &operation {
                Operation::Insert(p) | Operation::Upsert(p) => p.table_id,
                Operation::InsertMany(p) => p.table_id,
                Operation::Update(p) => p.table_id,
                Operation::Delete(p) => p.table_id,
                _ => return Err(invalid()),
            };
            let (table, entry) = resolve(loaded.catalog(), id)?;
            let worker_table = table.clone();
            let worker_entry = entry.clone();
            let (groups, batch_len) = cpu(&peers.worker.cpu, move |_| {
                plan(&worker_table, &worker_entry, operation)
            })
            .await?;
            let target: crate::partition_size::PartitionSize = peers
                .worker
                .ready
                .prepared
                .input
                .value
                .template
                .partition_size
                .parse()
                .map_err(|_| invalid())?;
            let mut replacements = BTreeMap::new();
            let mut upload_ordinal = 0u32;
            let mut result = crate::engine::MutationResult::Null;
            let source = peers
                .worker
                .owners
                .fork_source(lease)
                .map_err(|_| unavailable())?;
            let mut jobs = Vec::new();
            for (index, task) in groups {
                let task = match task {
                    Task::Prepare {
                        file,
                        range,
                        mutation,
                        ..
                    } => Task::Prepare {
                        file,
                        range,
                        mutation,
                        target_bytes: target.bytes() as u64,
                    },
                    _ => unreachable!(),
                };
                jobs.push((index, envelope(peers, loaded, &table, index, task)?));
            }
            let frontier = peers.frontier().await?;
            let mut pending = jobs.into_iter();
            let mut running = FuturesUnordered::new();
            let mut failure = None;
            let mut effects = BTreeMap::new();
            loop {
                while failure.is_none() && running.len() < frontier {
                    let Some((index, job)) = pending.next() else {
                        break;
                    };
                    let source = &source;
                    running.push(async move { (index, peers.dispatch_job(job, source).await) });
                }
                let Some((index, received)) = running.next().await else {
                    break;
                };
                let (received, mut child) = match received {
                    Ok(value) => value,
                    Err(error) => {
                        if failure.is_none() {
                            failure = Some(error);
                        }
                        continue;
                    }
                };
                let processed = async {
                    if failure.is_some() {
                        return Ok(());
                    }
                    let JobResult::Prepared { effect, .. } = received.result else {
                        return Err(invalid());
                    };
                    let Reply::Parts(parts) = child
                        .command(Command::StagePrepared {
                            table: id,
                            commit,
                            ordinal: upload_ordinal,
                        })
                        .await
                        .map_err(|_| unavailable())?
                    else {
                        return Err(invalid());
                    };
                    if let Some(parts) = parts {
                        upload_ordinal = upload_ordinal
                            .checked_add(u32::try_from(parts.len()).map_err(|_| invalid())?)
                            .ok_or_else(|| error(ErrorCode::LimitExceeded))?;
                        replacements.insert(index, parts);
                    }
                    child
                        .transfer_staged_to(lease)
                        .await
                        .map_err(|_| unavailable())?;
                    effects.insert(index, effect);
                    Ok(())
                }
                .await;
                let disposed = child.dispose().await.map_err(|_| unavailable());
                if let Err(error) = processed.and(disposed) {
                    if failure.is_none() {
                        failure = Some(error);
                    }
                }
            }
            if let Some(error) = failure {
                return Err(error);
            }
            if let Some((_, effect)) = effects.pop_last() {
                result = match effect {
                    job::Effect::Null {} => crate::engine::MutationResult::Null,
                    job::Effect::Found { value } => crate::engine::MutationResult::Found(value),
                    job::Effect::Count { value } => {
                        crate::engine::MutationResult::Count(value as usize)
                    }
                };
            }
            if let Some(n) = batch_len {
                result = crate::engine::MutationResult::Count(n);
            }
            if !replacements.is_empty() {
                let old = match entry.data {
                    TableData::Partitioned { files } => files,
                    TableData::Legacy { .. } => Vec::new(),
                };
                let mut files = Vec::<PartitionFile>::new();
                for index in 0..old.len().max(1) {
                    if let Some(parts) = replacements.remove(&index) {
                        files.extend(parts);
                    } else if let Some(file) = old.get(index) {
                        files.push(file.clone());
                    }
                }
                let mut lower = None;
                let mut row_count = 0u64;
                let mut payload = 0u64;
                for file in &mut files {
                    file.range.lower = lower;
                    lower = file.range.upper.clone();
                    row_count = row_count
                        .checked_add(file.row_count)
                        .ok_or_else(|| error(ErrorCode::LimitExceeded))?;
                    payload = payload
                        .checked_add(file.payload_bytes)
                        .ok_or_else(|| error(ErrorCode::LimitExceeded))?;
                }
                if let Some(last) = files.last_mut() {
                    last.range.upper = None;
                }
                if row_count > protocol::MAX_TABLE_ROWS as u64
                    || payload > protocol::MAX_DECODED_PAYLOAD_BYTES as u64
                {
                    return Err(error(ErrorCode::LimitExceeded));
                }
                let entry = tables.get_mut(&table.name).ok_or_else(invalid)?;
                entry.row_count = row_count;
                entry.data = TableData::Partitioned { files };
            }
            Ok((tables, ExecuteResult::Mutation(result)))
        }
    }
}
fn plan(
    table: &TableDescriptor,
    entry: &CatalogEntry,
    operation: Operation,
) -> Result<(BTreeMap<usize, Task>, Option<usize>)> {
    operation.validate_for_table(table)?;
    let batch_len = if let Operation::InsertMany(p) = &operation {
        Some(p.rows.len())
    } else {
        None
    };
    if let Operation::InsertMany(p) = &operation {
        let mut keys = BTreeSet::new();
        for row in &p.rows {
            let key = serde_json::to_string(&crate::engine::row_key(table, row))
                .map_err(|_| invalid())?;
            if !keys.insert(key) {
                return Err(error(ErrorCode::Conflict));
            }
        }
    }
    let mut groups = BTreeMap::new();
    if batch_len == Some(0) {
        return Ok((groups, batch_len));
    }
    let make = |index: usize, op: Operation| -> Result<Task> {
        let (file, range) = match &entry.data {
            TableData::Legacy { snapshot } => (
                Some(FileInput::Legacy {
                    object: snapshot.clone(),
                    row_count: entry.row_count,
                }),
                KeyRange {
                    lower: None,
                    upper: None,
                },
            ),
            TableData::Partitioned { files } if files.is_empty() => (
                None,
                KeyRange {
                    lower: None,
                    upper: None,
                },
            ),
            TableData::Partitioned { files } => (
                Some(files[index].clone().into()),
                files[index].range.clone(),
            ),
        };
        let mutation = match op {
            Operation::Insert(p) => RowMutation::Insert(p),
            Operation::InsertMany(p) => RowMutation::InsertMany(p),
            Operation::Update(p) => RowMutation::Update(p),
            Operation::Upsert(p) => RowMutation::Upsert(p),
            Operation::Delete(p) => RowMutation::Delete(p),
            _ => return Err(invalid()),
        };
        Ok(Task::Prepare {
            file,
            range,
            mutation,
            target_bytes: 1,
        })
    };
    let route = |key: &protocol::Key| -> Result<usize> {
        match &entry.data {
            TableData::Partitioned { files } if !files.is_empty() => {
                let file = entry
                    .route(key)
                    .map_err(|_| unavailable())?
                    .ok_or_else(unavailable)?;
                files
                    .iter()
                    .position(|p| p.object == file.object)
                    .ok_or_else(unavailable)
            }
            _ => Ok(0),
        }
    };
    match operation {
        Operation::InsertMany(p) => {
            let mut batches: BTreeMap<usize, Vec<protocol::Row>> = BTreeMap::new();
            for row in p.rows {
                let index = route(&crate::engine::row_key(table, &row))?;
                batches.entry(index).or_default().push(row);
            }
            for (index, rows) in batches {
                groups.insert(
                    index,
                    make(
                        index,
                        Operation::InsertMany(protocol::BatchPayload {
                            table_id: table.id,
                            rows,
                        }),
                    )?,
                );
            }
        }
        operation => {
            let key = match &operation {
                Operation::Insert(p) | Operation::Upsert(p) => {
                    crate::engine::row_key(table, &p.row)
                }
                Operation::Update(p) => p.key.clone(),
                Operation::Delete(p) => p.key.clone(),
                _ => return Err(invalid()),
            };
            let index = route(&key)?;
            groups.insert(index, make(index, operation)?);
        }
    }
    Ok((groups, batch_len))
}
