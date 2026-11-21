//! The leader executor: steps 1-6 of plan section 6, `table.get`,
//! `DirectForward` and dedup expiry. See the module doc in `mod.rs`.
use super::{
    admission::{Admitted, DirectAdmission},
    latches::{DEFAULT_STRIPES, KeyLatches},
    schema::{DirectSchema, DirectTable, SchemaLookup},
    stats::{DirectStats, DirectStatsSnapshot, ProposalFailure, RetryCause},
};
use crate::oltp_runtime::stage_timing::{perf_stage, timed};
use crate::{
    oltp::{
        mvcc::{
            CanonicalJson, MAX_FIXED_COMMAND_BYTES, MvccKey,
            planner::{index_entry_changes, primary_key_of, updated_row},
            store::{
                Fault, LatestRead, ParticipantCommand, ParticipantEpochs, ParticipantHandle,
                ParticipantReply, compare_keys,
                direct::{
                    ClockRejection, DEFAULT_DIRECT_DEDUP_RETENTION_MS, DedupLookup, DirectCommit,
                    DirectExpire, DirectForward, DirectReply, DirectSchema as GroupSchema,
                    DirectSchemaChange, DirectWrite, IndexChange, LeaderToken,
                    MAX_DIRECT_EXPIRE_ITEMS, MAX_DIRECT_INDEX_CHANGES, MAX_DIRECT_RESULT_BYTES,
                    MAX_DIRECT_WRITES, RangeClock, expiry_cutoff,
                },
            },
        },
        pool::{ByteClass, WorkerSharedResources},
    },
    oltp_replication::{
        authority::timestamp::DirectTimestampClient,
        multiplex::EndpointFuture,
        owned::OwnedCommand,
        service_rpc::direct::{
            DirectData, DirectRequest, DirectResponse, DirectService, FallbackReason,
            ForwardResponse, MAX_DIRECT_DEADLINE,
        },
        types::{CommitTs, Digest, GroupKind},
    },
    protocol::{self, ErrorCode, Key, Operation, Outcome, ProtocolError, Row},
};
use serde_json::value::RawValue;
use sha2::{Digest as _, Sha256};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::Instant;
use uuid::Uuid;

/// The largest encoded `DirectCommit` the leader proposes (plan "Eligible
/// operations": `insert_many` up to 1 MiB). Larger entries use the queued path.
pub(crate) const MAX_DIRECT_ENTRY_BYTES: usize = 1 << 20;
/// Pause between attempts while this node waits for its first entry of a
/// new term to apply (range clock rule 3) or storage to come back.
const RETRY_PAUSE: Duration = Duration::from_millis(2);
/// Bound on one `DirectExpire` proposal.
const EXPIRE_PROPOSAL_DEADLINE: Duration = Duration::from_secs(5);

/// The capability check. No leader proposes a direct command while it is
/// false; requests fall back with `Inactive`, and nothing is written.
pub(crate) trait DirectGate: Send + Sync {
    fn direct_active(&self) -> bool;
    /// The queued-history window (T7c): a mutation whose anchor holds no
    /// dedup record falls back with `History` instead of executing.
    fn history_settling(&self) -> bool {
        false
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct DirectConfig {
    /// In-flight requests per worker. Further requests get a typed 429.
    pub max_in_flight: usize,
    pub latch_stripes: usize,
    /// `direct_dedup_retention_ms`.
    pub dedup_retention_ms: u64,
    /// How often the leader proposes `DirectExpire` on the ranges it served.
    pub expire_interval: Duration,
}
impl Default for DirectConfig {
    fn default() -> Self {
        Self {
            max_in_flight: 256,
            latch_stripes: DEFAULT_STRIPES,
            dedup_retention_ms: DEFAULT_DIRECT_DEDUP_RETENTION_MS,
            expire_interval: Duration::from_secs(60),
        }
    }
}

pub(crate) struct DirectExecutor {
    resources: Arc<WorkerSharedResources>,
    config: DirectConfig,
    gate: Arc<dyn DirectGate>,
    schema: Arc<dyn DirectSchema>,
    timestamps: DirectTimestampClient,
    admission: DirectAdmission,
    latches: KeyLatches,
    stats: DirectStats,
    /// Ranges this worker served as leader, for the expiry task.
    served: Mutex<HashMap<Uuid, Weak<ParticipantHandle>>>,
}

/// A request that passed validation.
struct Parsed {
    operation: Operation,
}

/// (row key, head version, old row, new row).
type RowChange = (MvccKey, Option<CommitTs>, Option<Row>, Option<Row>);
/// The planned writes of one attempt.
struct Computed {
    writes: Vec<DirectWrite>,
    result: serde_json::Value,
}

/// What `activated` does while the table has no lifecycle to bind (a DDL
/// holds it, or it is missing).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Activation {
    /// Answer `Fallback{Schema}` at once: the gateway settles the request
    /// at its anchor and the queued path waits for the DDL.
    FallBack,
    /// Wait within the deadline: a forward marker needs an active group.
    Wait,
}

enum Attempt<T> {
    Done(T),
    Retry(RetryCause),
}

/// What steps 3 and 4 produced: a commit to propose, or the attempt's end.
enum Prepared {
    Commit {
        epochs: ParticipantEpochs,
        computed: Computed,
        result: CanonicalJson,
    },
    Exit(Attempt<DirectResponse>),
}

impl DirectExecutor {
    pub(crate) fn new(
        resources: Arc<WorkerSharedResources>,
        config: DirectConfig,
        gate: Arc<dyn DirectGate>,
        schema: Arc<dyn DirectSchema>,
        timestamps: DirectTimestampClient,
    ) -> Arc<Self> {
        Arc::new(Self {
            admission: DirectAdmission::new(resources.clone(), config.max_in_flight),
            latches: KeyLatches::new(config.latch_stripes),
            resources,
            config,
            gate,
            schema,
            timestamps,
            stats: DirectStats::default(),
            served: Mutex::default(),
        })
    }

    /// Installs this executor as the worker's `Method::Direct` service.
    pub(crate) fn install(self: &Arc<Self>) {
        let service: Arc<dyn DirectService> = self.clone();
        crate::oltp_replication::service_rpc::direct::install(&self.resources, &service);
    }

    /// The `direct` section of `/v1/runtime/stats`.
    pub(crate) fn stats(&self) -> DirectStatsSnapshot {
        self.stats
            .snapshot(self.admission.in_flight(), self.admission.max_in_flight())
    }

    /// Executes one direct request on the replica behind `handle`.
    pub(crate) async fn execute(
        &self,
        handle: &Arc<ParticipantHandle>,
        request: DirectRequest,
    ) -> DirectResponse {
        let started = std::time::Instant::now();
        self.stats.request();
        let mut get = false;
        let admit = std::time::Instant::now();
        let admission = self.admit(handle, &request);
        perf_stage!("leader.admit").since(admit);
        let response = match admission {
            Err(response) => response,
            Ok((admitted, parsed)) => {
                let deadline = Instant::now() + request.deadline.min(MAX_DIRECT_DEADLINE);
                let response = if matches!(parsed.operation, Operation::Get(_)) {
                    get = true;
                    self.stats.get();
                    self.get(handle, &request, parsed, deadline).await
                } else {
                    self.stats.mutation();
                    self.mutate(handle, &request, parsed, deadline).await
                };
                drop(admitted);
                response
            }
        };
        if get {
            self.stats.get_time(started.elapsed());
            perf_stage!("leader.get.total").since(started);
        } else {
            self.stats.mutation_time(started.elapsed());
            perf_stage!("leader.mutation.total").since(started);
        }
        self.count(&response);
        response
    }

    /// `DirectForward` at the anchor's leader (plan addendum 2026-11-19):
    /// returns the committed result when the request already ran directly,
    /// otherwise commits a forward marker and answers `Fallback{Forwarded}`.
    pub(crate) async fn forward(
        &self,
        handle: &Arc<ParticipantHandle>,
        request: DirectRequest,
    ) -> ForwardResponse {
        self.stats.request();
        self.stats.forward();
        let response = match self.admit(handle, &request) {
            Err(response) => response,
            // User decision 2026-11-21 (RV-FINAL M3): a get has no
            // exactly-once identity, so it never leaves a marker. A gateway
            // of an older build that still forwards one takes the queued
            // path; nothing is proposed or read.
            Ok((_, parsed)) if matches!(parsed.operation, Operation::Get(_)) => {
                DirectResponse::Fallback {
                    reason: FallbackReason::Ineligible,
                }
            }
            Ok((admitted, parsed)) => {
                let deadline = Instant::now() + request.deadline.min(MAX_DIRECT_DEADLINE);
                let response = self
                    .forward_admitted(handle, &request, parsed, deadline)
                    .await;
                drop(admitted);
                response
            }
        };
        if !matches!(
            response,
            DirectResponse::Fallback {
                reason: FallbackReason::Forwarded
            }
        ) {
            self.count(&response);
        }
        response
    }

    fn count(&self, response: &DirectResponse) {
        match response {
            DirectResponse::Done(_) => {}
            DirectResponse::Fallback { reason } => self.stats.fallback(*reason),
            DirectResponse::StaleRoute { .. } => self.stats.stale_route(),
            DirectResponse::NotLeader { .. } => self.stats.not_leader(),
            DirectResponse::Failed(error)
                if error.code == ErrorCode::Overloaded && error.outcome == Outcome::NotApplied =>
            {
                self.stats.overloaded()
            }
            DirectResponse::Failed(error) => self.stats.failed(error.outcome == Outcome::Unknown),
        }
    }

    /// Steps 1 and the request checks: admission, binding to this replica,
    /// the body digest and eligibility.
    fn admit(
        &self,
        handle: &Arc<ParticipantHandle>,
        request: &DirectRequest,
    ) -> Result<(Admitted, Parsed), DirectResponse> {
        if !self.gate.direct_active() {
            return Err(DirectResponse::Fallback {
                reason: FallbackReason::Inactive,
            });
        }
        let admitted = self.admission.try_admit(request.body.len())?;
        let identity = handle.identity();
        if identity.group_id != request.group
            || identity.database != request.database
            || !matches!(identity.kind, GroupKind::Data { table_id, .. } if table_id == request.table_id)
            || handle.direct_clock().is_err()
        {
            return Err(failed(invalid("direct request targets another group")));
        }
        let digest = Digest::from_bytes(Sha256::digest(request.body.as_slice()).into());
        if digest != request.digest {
            return Err(failed(invalid(
                "direct request digest differs from its body",
            )));
        }
        let parsed = protocol::parse_request(request.body.as_slice()).map_err(failed)?;
        if parsed.request_id != request.request_id {
            return Err(failed(invalid("direct request id differs from its body")));
        }
        let table = match &parsed.operation {
            Operation::Get(p) | Operation::Delete(p) => Some(p.table_id),
            Operation::Insert(p) | Operation::Upsert(p) => Some(p.table_id),
            Operation::Update(p) => Some(p.table_id),
            Operation::InsertMany(p) if !p.rows.is_empty() => Some(p.table_id),
            _ => None,
        };
        match table {
            None => Err(DirectResponse::Fallback {
                reason: FallbackReason::Ineligible,
            }),
            Some(table) if table != request.table_id => Err(failed(invalid(
                "direct request table differs from its body",
            ))),
            Some(_) => Ok((
                admitted,
                Parsed {
                    operation: parsed.operation,
                },
            )),
        }
    }

    fn served(&self, handle: &Arc<ParticipantHandle>) {
        let mut served = self.served.lock().unwrap_or_else(|p| p.into_inner());
        served.retain(|_, handle| handle.strong_count() > 0);
        served
            .entry(handle.identity().group_id)
            .or_insert_with(|| Arc::downgrade(handle));
    }

    /// The leader token, waiting while this node leads but has not applied
    /// an entry of its term yet (rule 3).
    async fn token(
        &self,
        handle: &ParticipantHandle,
        clock: &RangeClock,
        deadline: Instant,
    ) -> Result<LeaderToken, DirectResponse> {
        loop {
            if let Some(token) = clock.leader_token() {
                return Ok(token);
            }
            let (leader, local) = handle.direct_leadership();
            let leading = leader.is_some() && leader == local;
            if !leading || Instant::now() + RETRY_PAUSE >= deadline {
                return Err(DirectResponse::NotLeader {
                    hint: leader.filter(|leader| Some(*leader) != local),
                });
            }
            tokio::time::sleep(RETRY_PAUSE).await;
        }
    }

    /// Step 2: both epochs against the group's local config.
    async fn epochs(
        &self,
        handle: &ParticipantHandle,
        clock: &RangeClock,
        request: &DirectRequest,
        deadline: Instant,
    ) -> Result<ParticipantEpochs, DirectResponse> {
        let epochs = handle.direct_epochs().await.map_err(failed)?;
        self.checked_epochs(handle, clock, request, epochs, deadline)
            .await
    }

    /// Step 2's checks on epochs already read: the request's epochs, the
    /// group's activation and the table's drop fence.
    async fn checked_epochs(
        &self,
        handle: &ParticipantHandle,
        clock: &RangeClock,
        request: &DirectRequest,
        epochs: ParticipantEpochs,
        deadline: Instant,
    ) -> Result<ParticipantEpochs, DirectResponse> {
        if epochs.schema_epoch != request.schema_epoch
            || epochs.routing_epoch != request.routing_epoch
        {
            return Err(stale_route(Some(&epochs)));
        }
        let epochs = self
            .activated(
                handle,
                clock,
                request.table_id,
                epochs,
                Activation::FallBack,
                deadline,
            )
            .await?;
        match epochs.direct_schema {
            // The table's drop fenced this group: no direct commit applies.
            Some(GroupSchema {
                lifecycle: None, ..
            }) => Err(failed(ProtocolError::new(
                ErrorCode::NotFound,
                "table or shard not found",
                Outcome::NotApplied,
            ))),
            _ => Ok(epochs),
        }
    }

    /// `data.direct.v1` reached Control (the gate is open) but not yet this
    /// group: activate it, bound to the table's current Catalog lifecycle,
    /// then re-read the group's epochs. A DDL fence that applied first wins;
    /// activation never undoes it.
    ///
    /// The lifecycle is read only while no DDL holds or awaits the table's
    /// exclusive lifecycle guard (T7b): a DDL that read the capability as
    /// inactive did not fence, so binding its pre-DDL lifecycle would let
    /// this leader commit with a descriptor the DDL is about to replace.
    /// While a DDL holds it, a mutation falls back (the queued path waits on
    /// the guard) and a forward waits within its deadline, because its
    /// marker needs an active group. Transient Catalog and proposal failures
    /// are retried within the deadline (T6 concern 5).
    async fn activated(
        &self,
        handle: &ParticipantHandle,
        clock: &RangeClock,
        table_id: Uuid,
        mut epochs: ParticipantEpochs,
        on_ddl: Activation,
        deadline: Instant,
    ) -> Result<ParticipantEpochs, DirectResponse> {
        loop {
            if epochs.direct_schema.is_some() {
                return Ok(epochs);
            }
            match self
                .activate_once(handle, table_id, epochs.authority, deadline)
                .await
            {
                Ok(true) => {}
                Ok(false) if on_ddl == Activation::FallBack => {
                    return Err(DirectResponse::Fallback {
                        reason: FallbackReason::Schema,
                    });
                }
                Ok(false) => {
                    if !self.pause(deadline, RetryCause::Unavailable).await {
                        return Err(DirectResponse::Fallback {
                            reason: FallbackReason::Schema,
                        });
                    }
                }
                Err(error) if error.code == ErrorCode::StorageUnavailable => {
                    if !self.pause(deadline, RetryCause::Unavailable).await {
                        return Err(failed(unavailable_not_applied()));
                    }
                    // A lost leadership redirects instead of retrying here.
                    self.token(handle, clock, deadline).await?;
                }
                Err(error) => return Err(failed(error)),
            }
            epochs = by(deadline, handle.direct_epochs()).await.map_err(failed)?;
        }
    }

    /// One activation attempt. `false` when the table is missing or a DDL
    /// holds it (no lifecycle to bind now).
    async fn activate_once(
        &self,
        handle: &ParticipantHandle,
        table_id: Uuid,
        authority: crate::oltp_replication::types::Authority,
        deadline: Instant,
    ) -> Result<bool, ProtocolError> {
        let Some(lifecycle) = by(deadline, self.schema.lifecycle(table_id, authority)).await?
        else {
            return Ok(false);
        };
        if let Err(error) = by(
            deadline,
            handle.change_direct_schema(DirectSchemaChange::Activate { lifecycle }),
        )
        .await
        {
            self.stats.proposal_failed(ProposalFailure::Activation);
            tracing::debug!(group = %handle.identity().group_id, code = ?error.code,
                message = %error.message, "direct group activation failed");
            // Activation writes no request: a retry is always safe.
            return Err(ProtocolError::new(
                if error.code == ErrorCode::ValidationError {
                    error.code
                } else {
                    ErrorCode::StorageUnavailable
                },
                error.message,
                Outcome::NotApplied,
            ));
        }
        Ok(true)
    }

    async fn table(
        &self,
        request: &DirectRequest,
        epochs: &ParticipantEpochs,
        operation: &Operation,
        deadline: Instant,
    ) -> Result<Arc<DirectTable>, DirectResponse> {
        let lookup = catalog_read(
            deadline,
            self.schema.table(
                request.table_id,
                request.schema_epoch,
                epochs.direct_schema,
                epochs.authority,
            ),
        )
        .await?;
        let table = match lookup {
            SchemaLookup::Table(table) => table,
            SchemaLookup::Blocked => {
                return Err(DirectResponse::Fallback {
                    reason: FallbackReason::Schema,
                });
            }
            SchemaLookup::StaleEpoch => return Err(stale_route(Some(epochs))),
            SchemaLookup::Missing => {
                return Err(failed(ProtocolError::new(
                    ErrorCode::NotFound,
                    "table or shard not found",
                    Outcome::NotApplied,
                )));
            }
        };
        operation
            .validate_for_table(&table.descriptor)
            .map_err(failed)?;
        Ok(table)
    }

    /// Waits out a transient failure, or reports the deadline.
    async fn pause(&self, deadline: Instant, cause: RetryCause) -> bool {
        self.stats.retry(cause);
        if Instant::now() + RETRY_PAUSE >= deadline {
            return false;
        }
        tokio::time::sleep(RETRY_PAUSE).await;
        true
    }

    /// A pre-append refusal can retry locally. If Raft may have appended, the
    /// gateway must know that before following another leader or route.
    fn proposal_failed(
        &self,
        handle: &ParticipantHandle,
        clock: &RangeClock,
        token: LeaderToken,
        deadline: Instant,
        error: ProtocolError,
    ) -> Result<(), DirectResponse> {
        // RV-FINAL M1: a full device refuses new work before proposing it.
        if error.outcome == Outcome::NotApplied && crate::oltp::pool::is_no_space(&error) {
            return Err(failed(error));
        }
        let expired = Instant::now() >= deadline;
        let reason = if expired {
            ProposalFailure::Deadline
        } else if clock.leader_token() != Some(token) {
            ProposalFailure::Leadership
        } else {
            ProposalFailure::Stalled
        };
        self.stats.proposal_failed(reason);
        tracing::debug!(
            group = %handle.identity().group_id,
            ?reason,
            leadership = ?handle.direct_leadership(),
            code = ?error.code,
            message = %error.message,
            "direct proposal produced no reply"
        );
        if error.outcome == Outcome::NotApplied {
            Ok(())
        } else if expired {
            Err(failed(ProtocolError::new(
                ErrorCode::OutcomeUnknown,
                "direct request deadline expired after a proposal of unknown outcome",
                Outcome::Unknown,
            )))
        } else {
            Err(failed(error))
        }
    }

    async fn get(
        &self,
        handle: &Arc<ParticipantHandle>,
        request: &DirectRequest,
        parsed: Parsed,
        deadline: Instant,
    ) -> DirectResponse {
        let Operation::Get(payload) = &parsed.operation else {
            unreachable!("only gets reach get");
        };
        let result = async {
            let clock = handle.direct_clock().map_err(failed)?;
            timed(
                perf_stage!("leader.get.token"),
                self.token(handle, clock, deadline),
            )
            .await?;
            loop {
                // PERF2b: the barrier's applied check, the epochs, the
                // newest version and the key's dedup record come from one
                // snapshot (one Query-class native job, not four).
                let Ok(barrier) = timed(
                    perf_stage!("leader.get.barrier"),
                    by(deadline, handle.direct_lease_index()),
                )
                .await
                else {
                    if !self.pause(deadline, RetryCause::Leadership).await {
                        return Err(not_leader_or(handle));
                    }
                    continue;
                };
                let snapshot = match timed(
                    perf_stage!("leader.get.read"),
                    by(
                        deadline,
                        handle.direct_snapshot(
                            barrier,
                            request.table_id,
                            payload.key.clone(),
                            request.request_id,
                        ),
                    ),
                )
                .await
                {
                    Ok(Some(snapshot)) => snapshot,
                    Ok(None) => {
                        if !self.pause(deadline, RetryCause::Leadership).await {
                            return Err(not_leader_or(handle));
                        }
                        continue;
                    }
                    Err(error) if error.code == ErrorCode::StorageUnavailable => {
                        if !self.pause(deadline, RetryCause::Unavailable).await {
                            return Err(failed(error));
                        }
                        continue;
                    }
                    Err(error) => return Err(failed(error)),
                };
                let epochs = timed(
                    perf_stage!("leader.get.epochs"),
                    self.checked_epochs(handle, clock, request, snapshot.epochs, deadline),
                )
                .await?;
                timed(
                    perf_stage!("leader.get.table"),
                    self.table(request, &epochs, &parsed.operation, deadline),
                )
                .await?;
                match snapshot.latest {
                    Err(error) if error.code == ErrorCode::StorageUnavailable => {
                        if !self.pause(deadline, RetryCause::Unavailable).await {
                            return Err(failed(error));
                        }
                    }
                    Err(error) => return Err(failed(error)),
                    Ok(read @ LatestRead::Value { .. }) => {
                        // The key's record of this id: a get that reuses
                        // the id of a mutation on its key conflicts (409),
                        // and one whose id a queued mutation settled here (a
                        // marker) goes queued, where Status answers it.
                        // Gets themselves leave no record and have no
                        // exactly-once identity (user decision 2026-11-21,
                        // RV-FINAL M3), so the queued-history window does
                        // not hold them back: a read cannot apply or hide a
                        // pre-boundary mutation.
                        match snapshot.dedup {
                            Err(error) if error.code == ErrorCode::StorageUnavailable => {
                                if !self.pause(deadline, RetryCause::Unavailable).await {
                                    return Err(failed(error));
                                }
                            }
                            Err(error) => return Err(failed(error)),
                            Ok(Some(found)) => return self.settled(found, request.digest),
                            Ok(None) => return self.get_result(read, &epochs),
                        }
                    }
                    Ok(read) => return self.get_result(read, &epochs),
                }
            }
        }
        .await;
        result.unwrap_or_else(|response| response)
    }

    fn get_result(
        &self,
        read: LatestRead,
        epochs: &ParticipantEpochs,
    ) -> Result<DirectResponse, DirectResponse> {
        match read {
            LatestRead::Blocked => Err(DirectResponse::Fallback {
                reason: FallbackReason::Intent,
            }),
            LatestRead::SplitInProgress => Err(DirectResponse::Fallback {
                reason: FallbackReason::SplitInProgress,
            }),
            LatestRead::NotOwned => Err(stale_route(Some(epochs))),
            LatestRead::Value { value: None, .. } => Ok(DirectResponse::Done(
                DirectData::from_json("null").map_err(failed)?,
            )),
            LatestRead::Value {
                value: Some(raw), ..
            } => {
                // Re-encode the decoded row exactly as the queued path does.
                let row: Row = serde_json::from_str(raw.get()).map_err(|_| failed(corrupt()))?;
                let encoded = serde_json::to_string(&row).map_err(|_| failed(corrupt()))?;
                Ok(DirectResponse::Done(
                    DirectData::from_json(&encoded).map_err(failed)?,
                ))
            }
        }
    }

    /// Sorted row keys of a mutation and their rows (inserts and upserts).
    fn row_keys(
        &self,
        table: &DirectTable,
        operation: &Operation,
    ) -> Result<Vec<MvccKey>, DirectResponse> {
        let table_id = table.descriptor.id;
        let key = |primary_key: Key| MvccKey::Row {
            table_id,
            primary_key,
        };
        let mut keys = match operation {
            Operation::Insert(p) | Operation::Upsert(p) => {
                vec![key(primary_key_of(&table.descriptor.key, &p.row))]
            }
            Operation::InsertMany(p) => p
                .rows
                .iter()
                .map(|row| key(primary_key_of(&table.descriptor.key, row)))
                .collect(),
            Operation::Update(p) => vec![key(p.key.clone())],
            Operation::Delete(p) => vec![key(p.key.clone())],
            _ => {
                return Err(DirectResponse::Fallback {
                    reason: FallbackReason::Ineligible,
                });
            }
        };
        if keys.len() > MAX_DIRECT_WRITES {
            return Err(DirectResponse::Fallback {
                reason: FallbackReason::ResourceLimit,
            });
        }
        keys.sort_by(compare_keys);
        if keys
            .windows(2)
            .any(|pair| compare_keys(&pair[0], &pair[1]).is_eq())
        {
            return Err(failed(ProtocolError::new(
                ErrorCode::Conflict,
                "duplicate primary key in batch",
                Outcome::NotApplied,
            )));
        }
        Ok(keys)
    }

    async fn mutate(
        &self,
        handle: &Arc<ParticipantHandle>,
        request: &DirectRequest,
        parsed: Parsed,
        deadline: Instant,
    ) -> DirectResponse {
        let result = async {
            let clock = handle.direct_clock().map_err(failed)?;
            timed(
                perf_stage!("leader.mutation.token"),
                self.token(handle, clock, deadline),
            )
            .await?;
            let epochs = timed(
                perf_stage!("leader.mutation.epochs"),
                self.epochs(handle, clock, request, deadline),
            )
            .await?;
            let table = timed(
                perf_stage!("leader.mutation.table"),
                self.table(request, &epochs, &parsed.operation, deadline),
            )
            .await?;
            let keys = self.row_keys(&table, &parsed.operation)?;
            let encoded: Vec<Vec<u8>> = keys
                .iter()
                .map(|key| serde_json::to_vec(key).map_err(|_| failed(corrupt())))
                .collect::<Result<_, _>>()?;
            let latched = timed(
                perf_stage!("leader.mutation.latch"),
                self.latches
                    .lock(encoded.iter().map(Vec::as_slice), deadline),
            )
            .await
            .map_err(|_| {
                failed(ProtocolError::new(
                    ErrorCode::StorageUnavailable,
                    "deadline expired waiting for a key latch",
                    Outcome::NotApplied,
                ))
            })?;
            if let Some(waited) = latched.waited {
                self.stats.latch_wait(waited);
            }
            self.served(handle);
            loop {
                if Instant::now() >= deadline {
                    return Err(failed(unavailable_not_applied()));
                }
                match self
                    .attempt(
                        handle,
                        clock,
                        request,
                        &table,
                        &parsed.operation,
                        &keys,
                        deadline,
                    )
                    .await?
                {
                    Attempt::Done(response) => return Ok(response),
                    Attempt::Retry(cause) => {
                        if !self.pause(deadline, cause).await {
                            return Err(failed(unavailable_not_applied()));
                        }
                    }
                }
            }
        }
        .await;
        result.unwrap_or_else(|response| response)
    }

    /// Steps 3 to 6 once. `Retry` goes back to step 3.
    #[allow(clippy::too_many_arguments)]
    async fn attempt(
        &self,
        handle: &Arc<ParticipantHandle>,
        clock: &RangeClock,
        request: &DirectRequest,
        table: &DirectTable,
        operation: &Operation,
        keys: &[MvccKey],
        deadline: Instant,
    ) -> Result<Attempt<DirectResponse>, DirectResponse> {
        // T4fix F4: the token is taken before the timestamp is fetched.
        let token = timed(
            perf_stage!("leader.attempt.token"),
            self.token(handle, clock, deadline),
        )
        .await?;
        // PERF2b: the commit timestamp is fetched while steps 3 and 4 run,
        // in this task. `mutate` already holds the latches (plan section 5,
        // rule 1), and registration below still refuses a timestamp at or
        // below `max_read_ts`. An attempt that ends before the commit drops
        // the fetch; an unused timestamp is only a gap.
        let fetch = timed(
            perf_stage!("leader.attempt.timestamp_fetch"),
            by(deadline, self.timestamps.commit_ts()),
        );
        let prepare = self.prepare(handle, clock, request, table, operation, keys, deadline);
        tokio::pin!(fetch, prepare);
        let mut fetched = None;
        let prepared = loop {
            tokio::select! {
                biased;
                ts = &mut fetch, if fetched.is_none() => fetched = Some(ts),
                prepared = &mut prepare => break prepared,
            }
        };
        let (epochs, computed, result) = match prepared? {
            Prepared::Commit {
                epochs,
                computed,
                result,
            } => (epochs, computed, result),
            Prepared::Exit(exit) => return Ok(exit),
        };
        let waited = std::time::Instant::now();
        let fetched = match fetched {
            Some(fetched) => fetched,
            None => fetch.await,
        };
        perf_stage!("leader.attempt.timestamp").since(waited);
        let commit_ts = match fetched {
            Ok(ts) if above_read_versions(ts, &computed.writes) => ts,
            // A version newer than the overlapped fetch applied meanwhile:
            // apply would reject the commit, so fetch again first.
            Ok(_) => match by(deadline, self.timestamps.commit_ts()).await {
                Ok(ts) => ts,
                Err(_) => return Ok(Attempt::Retry(RetryCause::Unavailable)),
            },
            Err(_) => return Ok(Attempt::Retry(RetryCause::Unavailable)),
        };
        let build = std::time::Instant::now();
        let commit = DirectCommit {
            authority: epochs.authority,
            table_id: request.table_id,
            // The group's direct epoch (T7 DDL fence), not the route's.
            schema_epoch: epochs
                .direct_schema
                .map_or(epochs.schema_epoch, |schema| schema.epoch),
            routing_epoch: epochs.routing_epoch,
            request_id: request.request_id,
            digest: request.digest,
            commit_ts,
            writes: computed.writes,
            result: result.clone(),
            applied_unix_ms: unix_ms(),
        };
        if encoded_len(&commit) > MAX_DIRECT_ENTRY_BYTES {
            return Err(DirectResponse::Fallback {
                reason: FallbackReason::ResourceLimit,
            });
        }
        let rows = commit.writes.len();
        let index_changes = commit.writes.iter().map(|w| w.index.len()).sum();
        match clock.register_pending(token, commit_ts, commit.keys()) {
            Ok(()) => {}
            Err(ClockRejection::StaleTimestamp { .. } | ClockRejection::Duplicate) => {
                return Ok(Attempt::Retry(RetryCause::StaleTimestamp));
            }
            Err(ClockRejection::NotServing) => return Ok(Attempt::Retry(RetryCause::Leadership)),
            Err(ClockRejection::InvalidKey) => {
                return Err(failed(invalid("direct commit key cannot be encoded")));
            }
        }
        let command = match OwnedCommand::new_owned(
            ParticipantCommand::Direct(commit),
            &self.resources,
            ByteClass::Data,
        ) {
            Ok(command) => command,
            Err(error) => {
                clock.settle(commit_ts);
                return Err(failed(error));
            }
        };
        perf_stage!("leader.attempt.build_command").since(build);
        self.stats.proposed(rows, index_changes);
        // T9 process barrier (D04 before quorum): timestamp registered, entry not
        // yet proposed.
        #[cfg(test)]
        crate::oltp_runtime::recovery_tests::pause("direct-proposing", request.request_id).await;
        let applied = match timed(
            perf_stage!("leader.attempt.propose"),
            by(deadline, handle.propose(command)),
        )
        .await
        {
            Ok(applied) => applied,
            Err(error) => {
                // T4: the proposal no longer holds its local pending slot.
                clock.settle(commit_ts);
                self.proposal_failed(handle, clock, token, deadline, error)?;
                return Ok(Attempt::Retry(RetryCause::Unavailable));
            }
        };
        let fault = match applied.reply {
            ParticipantReply::Direct(DirectReply::Applied { .. }) => {
                self.stats.applied();
                // T9 process barrier (D02, D04): the entry has reached quorum and
                // applied here; the reply has not left the leader yet.
                #[cfg(test)]
                crate::oltp_runtime::recovery_tests::pause("direct-applied", request.request_id)
                    .await;
                return Ok(Attempt::Done(DirectResponse::Done(
                    DirectData::from_json(result.as_str()).map_err(failed)?,
                )));
            }
            ParticipantReply::Direct(DirectReply::Replayed { result }) => {
                self.stats.replayed();
                return Ok(Attempt::Done(DirectResponse::Done(
                    DirectData::from_json(result.get()).map_err(failed)?,
                )));
            }
            ParticipantReply::Direct(DirectReply::Forwarded) => {
                return Err(DirectResponse::Fallback {
                    reason: FallbackReason::Forwarded,
                });
            }
            ParticipantReply::Direct(DirectReply::Rejected(fault))
            | ParticipantReply::Rejected(fault) => fault,
            _ => return Err(failed(corrupt())),
        };
        // A definite rejection: this entry wrote nothing.
        match fault {
            Fault::Conflict => Ok(Attempt::Retry(RetryCause::Conflict)),
            Fault::StaleAuthority => Ok(Attempt::Retry(RetryCause::Authority)),
            other => Err(fault_response(other, &epochs)),
        }
    }

    /// Steps 3 and 4 of one attempt: the barrier, the dedup record, the
    /// current rows and the planned writes.
    #[allow(clippy::too_many_arguments)]
    async fn prepare(
        &self,
        handle: &Arc<ParticipantHandle>,
        clock: &RangeClock,
        request: &DirectRequest,
        table: &DirectTable,
        operation: &Operation,
        keys: &[MvccKey],
        deadline: Instant,
    ) -> Result<Prepared, DirectResponse> {
        let MvccKey::Row {
            primary_key: anchor,
            ..
        } = &keys[0]
        else {
            unreachable!("row keys only");
        };
        let leadership = || {
            if Instant::now() + RETRY_PAUSE >= deadline {
                return Err(not_leader_or(handle));
            }
            Ok(Prepared::Exit(Attempt::Retry(RetryCause::Leadership)))
        };
        let Ok(barrier) = timed(
            perf_stage!("leader.attempt.barrier"),
            by(deadline, handle.direct_lease_index()),
        )
        .await
        else {
            return leadership();
        };
        // A cancelled earlier attempt of this request may still be pending
        // on these keys (its latch is gone but its entry is not applied):
        // wait for it, so the dedup lookup below sees its record.
        let waited = std::time::Instant::now();
        for key in keys {
            if by(deadline, clock.wait_latest(key)).await.is_err() {
                return Ok(Prepared::Exit(Attempt::Retry(RetryCause::Unavailable)));
            }
        }
        perf_stage!("leader.attempt.wait_latest").since(waited);
        // PERF2b: the barrier's applied check, the epochs, the dedup record
        // and the anchor's newest version come from one snapshot (one
        // Query-class native job, not four).
        let snapshot = loop {
            let snapshot = match timed(
                perf_stage!("leader.attempt.read"),
                by(
                    deadline,
                    handle.direct_snapshot(
                        barrier,
                        request.table_id,
                        anchor.clone(),
                        request.request_id,
                    ),
                ),
            )
            .await
            {
                Ok(Some(snapshot)) => snapshot,
                Ok(None) => return leadership(),
                Err(error) if error.code == ErrorCode::StorageUnavailable => {
                    return Ok(Prepared::Exit(Attempt::Retry(RetryCause::Unavailable)));
                }
                Err(error) => return Err(failed(error)),
            };
            if snapshot.epochs.direct_schema.is_some() {
                break snapshot;
            }
            // Not activated yet: activate (or fall back), then read again so
            // every read follows the epochs it is checked against.
            self.checked_epochs(handle, clock, request, snapshot.epochs, deadline)
                .await?;
        };
        let epochs = timed(
            perf_stage!("leader.attempt.epochs"),
            self.checked_epochs(handle, clock, request, snapshot.epochs, deadline),
        )
        .await?;
        // The dedup record decides before any fallback (T4fix concern 1).
        match snapshot.dedup {
            Err(error) if error.code == ErrorCode::StorageUnavailable => {
                return Err(stale_route(Some(&epochs)));
            }
            Err(error) if error.code == ErrorCode::ValidationError => {
                return Err(stale_route(Some(&epochs)));
            }
            Err(error) => return Err(failed(error)),
            Ok(Some(found)) => {
                return self
                    .settled(found, request.digest)
                    .map(|done| Prepared::Exit(Attempt::Done(done)));
            }
            // T7c: this id may belong to a queued request accepted before
            // activation without a marker. Only the queued path knows it.
            Ok(None) if self.gate.history_settling() => {
                return Err(DirectResponse::Fallback {
                    reason: FallbackReason::History,
                });
            }
            Ok(None) => {}
        }
        let read = std::time::Instant::now();
        let mut current = Vec::with_capacity(keys.len());
        let mut first = Some(snapshot.latest);
        for key in keys {
            let latest = match first.take() {
                Some(latest) => latest_value(latest, &epochs)?,
                None => self.latest(handle, key.clone(), &epochs, deadline).await?,
            };
            match latest {
                Some((version, value)) => {
                    // Row keys hold rows; index entries are read separately.
                    let row = value
                        .map(|raw| serde_json::from_str::<Row>(raw.get()))
                        .transpose()
                        .map_err(|_| failed(corrupt()))?;
                    current.push((version, row));
                }
                None => return Ok(Prepared::Exit(Attempt::Retry(RetryCause::Unavailable))),
            }
        }
        perf_stage!("leader.attempt.latest").since(read);
        let Some(computed) = timed(
            perf_stage!("leader.attempt.compute"),
            self.compute(handle, table, operation, keys, current, &epochs, deadline),
        )
        .await?
        else {
            return Ok(Prepared::Exit(Attempt::Retry(RetryCause::Unavailable)));
        };
        let result = CanonicalJson::from_serializable(
            &self.resources,
            &computed.result,
            MAX_DIRECT_RESULT_BYTES,
        )
        .map_err(|_| DirectResponse::Fallback {
            reason: FallbackReason::ResourceLimit,
        })?;
        if computed.writes.is_empty() {
            // A read-only outcome commits nothing and is recomputed on retry.
            self.stats.read_only();
            return Ok(Prepared::Exit(Attempt::Done(DirectResponse::Done(
                DirectData::from_json(result.as_str()).map_err(failed)?,
            ))));
        }
        Ok(Prepared::Commit {
            epochs,
            computed,
            result,
        })
    }

    fn settled(
        &self,
        found: DedupLookup,
        digest: Digest,
    ) -> Result<DirectResponse, DirectResponse> {
        match found {
            DedupLookup::Committed {
                digest: stored,
                result,
                ..
            } if stored == digest => {
                self.stats.replayed();
                Ok(DirectResponse::Done(
                    DirectData::from_json(result.get()).map_err(failed)?,
                ))
            }
            DedupLookup::Forwarded { digest: stored } if stored == digest => {
                Err(DirectResponse::Fallback {
                    reason: FallbackReason::Forwarded,
                })
            }
            _ => Err(failed(reused())),
        }
    }

    /// One key's newest version. `None` asks for a retry: a waiter was
    /// failed by a leadership change, or storage was briefly unavailable.
    async fn latest(
        &self,
        handle: &ParticipantHandle,
        key: MvccKey,
        epochs: &ParticipantEpochs,
        deadline: Instant,
    ) -> Result<Option<(Option<CommitTs>, Option<Box<RawValue>>)>, DirectResponse> {
        latest_value(by(deadline, handle.direct_latest(key)).await, epochs)
    }

    /// Step 4 with the queued planner's row and index functions.
    #[allow(clippy::too_many_arguments)]
    async fn compute(
        &self,
        handle: &ParticipantHandle,
        table: &DirectTable,
        operation: &Operation,
        keys: &[MvccKey],
        current: Vec<(Option<CommitTs>, Option<Row>)>,
        epochs: &ParticipantEpochs,
        deadline: Instant,
    ) -> Result<Option<Computed>, DirectResponse> {
        let mut changes: Vec<RowChange> = Vec::new();
        let result = match operation {
            Operation::Insert(p) | Operation::Upsert(p) => {
                let insert = matches!(operation, Operation::Insert(_));
                let (version, old) = current.into_iter().next().expect("one key");
                if insert && old.is_some() {
                    return Err(failed(already_exists()));
                }
                changes.push((keys[0].clone(), version, old, Some(p.row.clone())));
                serde_json::Value::Null
            }
            Operation::InsertMany(p) => {
                let mut rows: HashMap<Vec<u8>, &Row> = HashMap::with_capacity(p.rows.len());
                for row in &p.rows {
                    let key = MvccKey::Row {
                        table_id: table.descriptor.id,
                        primary_key: primary_key_of(&table.descriptor.key, row),
                    };
                    rows.insert(
                        serde_json::to_vec(&key).map_err(|_| failed(corrupt()))?,
                        row,
                    );
                }
                for (key, (version, old)) in keys.iter().zip(current) {
                    if old.is_some() {
                        return Err(failed(already_exists()));
                    }
                    let encoded = serde_json::to_vec(key).map_err(|_| failed(corrupt()))?;
                    let row = rows.get(&encoded).ok_or_else(|| failed(corrupt()))?;
                    changes.push((key.clone(), version, None, Some((*row).clone())));
                }
                serde_json::json!(p.rows.len())
            }
            Operation::Update(p) => {
                let (version, old) = current.into_iter().next().expect("one key");
                match old {
                    None => serde_json::Value::Bool(false),
                    Some(old) => {
                        let new = updated_row(&old, p.values.clone());
                        changes.push((keys[0].clone(), version, Some(old), Some(new)));
                        serde_json::Value::Bool(true)
                    }
                }
            }
            Operation::Delete(_) => {
                let (version, old) = current.into_iter().next().expect("one key");
                match old {
                    None => serde_json::Value::Bool(false),
                    Some(old) => {
                        changes.push((keys[0].clone(), version, Some(old), None));
                        serde_json::Value::Bool(true)
                    }
                }
            }
            _ => {
                return Err(DirectResponse::Fallback {
                    reason: FallbackReason::Ineligible,
                });
            }
        };
        let mut writes = Vec::with_capacity(changes.len());
        for (key, version, old, new) in changes {
            let MvccKey::Row { primary_key, .. } = &key else {
                unreachable!("row keys only");
            };
            let value = new
                .as_ref()
                .map(|row| {
                    CanonicalJson::from_serializable(&self.resources, row, MAX_FIXED_COMMAND_BYTES)
                })
                .transpose()
                .map_err(|_| DirectResponse::Fallback {
                    reason: FallbackReason::ResourceLimit,
                })?;
            let mut index = Vec::new();
            for definition in &table.indexes {
                let entries = index_entry_changes(
                    definition,
                    &table.descriptor,
                    primary_key,
                    old.as_ref(),
                    value.as_ref(),
                    &self.resources,
                )
                .map_err(failed)?;
                for (key, value) in entries
                    .delete
                    .map(|key| (key, None))
                    .into_iter()
                    .chain(entries.put.map(|(key, cover)| (key, Some(cover))))
                {
                    let Some((expected, _)) =
                        self.latest(handle, key.clone(), epochs, deadline).await?
                    else {
                        return Ok(None);
                    };
                    index.push(IndexChange {
                        key,
                        expected_version: expected,
                        value,
                    });
                }
            }
            if index.len() > MAX_DIRECT_INDEX_CHANGES {
                return Err(DirectResponse::Fallback {
                    reason: FallbackReason::ResourceLimit,
                });
            }
            writes.push(DirectWrite {
                key,
                expected_version: version,
                value,
                index,
            });
        }
        Ok(Some(Computed { writes, result }))
    }

    async fn forward_admitted(
        &self,
        handle: &Arc<ParticipantHandle>,
        request: &DirectRequest,
        parsed: Parsed,
        deadline: Instant,
    ) -> ForwardResponse {
        let result = async {
            let clock = handle.direct_clock().map_err(failed)?;
            let token = self.token(handle, clock, deadline).await?;
            let epochs = handle.direct_epochs().await.map_err(failed)?;
            // A forward marker is gated like Direct: activate the group.
            let epochs = self
                .activated(
                    handle,
                    clock,
                    request.table_id,
                    epochs,
                    Activation::Wait,
                    deadline,
                )
                .await?;
            let lookup = catalog_read(
                deadline,
                self.schema.table(
                    request.table_id,
                    request.schema_epoch,
                    None,
                    epochs.authority,
                ),
            )
            .await?;
            let table = match lookup {
                SchemaLookup::Table(table) => table,
                // The anchor key still follows from the stored key column
                // only through the descriptor, so a forward needs one.
                SchemaLookup::StaleEpoch => return Err(stale_route(Some(&epochs))),
                SchemaLookup::Blocked | SchemaLookup::Missing => {
                    return Err(failed(ProtocolError::new(
                        ErrorCode::StorageUnavailable,
                        "table descriptor unavailable for the forward marker",
                        Outcome::NotApplied,
                    )));
                }
            };
            let keys = self.row_keys(&table, &parsed.operation)?;
            let MvccKey::Row {
                primary_key: anchor,
                ..
            } = keys[0].clone()
            else {
                unreachable!("row keys only");
            };
            let encoded = serde_json::to_vec(&keys[0]).map_err(|_| failed(corrupt()))?;
            let _latched = self
                .latches
                .lock([encoded.as_slice()], deadline)
                .await
                .map_err(|_| failed(unavailable_not_applied()))?;
            self.served(handle);
            loop {
                if Instant::now() >= deadline {
                    return Err(failed(unavailable_not_applied()));
                }
                let epochs = handle.direct_epochs().await.map_err(failed)?;
                if by(deadline, handle.direct_barrier()).await.is_err() {
                    if !self.pause(deadline, RetryCause::Leadership).await {
                        return Err(failed(unavailable_not_applied()));
                    }
                    continue;
                }
                match by(
                    deadline,
                    handle.direct_dedup_after_barrier(
                        request.table_id,
                        anchor.clone(),
                        request.request_id,
                    ),
                )
                .await
                {
                    Err(error)
                        if matches!(
                            error.code,
                            ErrorCode::StorageUnavailable | ErrorCode::ValidationError
                        ) =>
                    {
                        return Err(stale_route(Some(&epochs)));
                    }
                    Err(error) => return Err(failed(error)),
                    Ok(Some(found)) => return self.settled(found, request.digest),
                    Ok(None) => {}
                }
                let command = OwnedCommand::new_owned(
                    ParticipantCommand::DirectForward(DirectForward {
                        authority: epochs.authority,
                        table_id: request.table_id,
                        anchor: anchor.clone(),
                        request_id: request.request_id,
                        digest: request.digest,
                        applied_unix_ms: unix_ms(),
                    }),
                    &self.resources,
                    ByteClass::Progress,
                )
                .map_err(failed)?;
                let applied = match by(deadline, handle.propose(command)).await {
                    Ok(applied) => applied,
                    Err(error) => {
                        self.proposal_failed(handle, clock, token, deadline, error)?;
                        if !self.pause(deadline, RetryCause::Unavailable).await {
                            return Err(failed(unavailable_not_applied()));
                        }
                        continue;
                    }
                };
                let fault = match applied.reply {
                    ParticipantReply::Direct(DirectReply::Forwarded) => {
                        self.stats.marker_written();
                        return Err(DirectResponse::Fallback {
                            reason: FallbackReason::Forwarded,
                        });
                    }
                    ParticipantReply::Direct(DirectReply::Replayed { result }) => {
                        self.stats.replayed();
                        return Ok(DirectResponse::Done(
                            DirectData::from_json(result.get()).map_err(failed)?,
                        ));
                    }
                    ParticipantReply::Direct(DirectReply::Rejected(fault))
                    | ParticipantReply::Rejected(fault) => fault,
                    _ => return Err(failed(corrupt())),
                };
                match fault {
                    Fault::Conflict => return Err(failed(reused())),
                    // A frozen source will never accept a new marker. The gateway
                    // refreshes its route and still requires settlement before queueing.
                    Fault::SplitInProgress => return Err(fault_response(fault, &epochs)),
                    Fault::StaleAuthority => {
                        if !self.pause(deadline, RetryCause::Authority).await {
                            return Err(failed(unavailable_not_applied()));
                        }
                    }
                    other => return Err(fault_response(other, &epochs)),
                }
            }
        }
        .await;
        result.unwrap_or_else(|response| response)
    }

    /// Step 7: deletes dedup records and markers older than the retention
    /// on one range this node leads, page by page until complete. Returns
    /// the number deleted. Proposes nothing unless the capability is active
    /// and this node serves the range.
    pub(crate) async fn expire(&self, handle: &ParticipantHandle) -> Result<u64, ProtocolError> {
        let clock = handle.direct_clock()?;
        if !self.gate.direct_active() || clock.leader_token().is_none() {
            return Ok(0);
        }
        let retention_ms = self.config.dedup_retention_ms.max(1);
        let Some(cutoff_unix_ms) = expiry_cutoff(unix_ms(), retention_ms) else {
            return Ok(0);
        };
        let mut deleted = 0;
        // Bounded: one interval deletes at most this many pages.
        for _ in 0..64 {
            let epochs = handle.direct_epochs().await?;
            // Nothing to expire on a group direct commits never reached.
            if epochs.direct_schema.is_none() {
                return Ok(deleted);
            }
            let command = OwnedCommand::new_owned(
                ParticipantCommand::DirectExpire(DirectExpire {
                    authority: epochs.authority,
                    cutoff_unix_ms,
                    retention_ms,
                    max_items: MAX_DIRECT_EXPIRE_ITEMS,
                }),
                &self.resources,
                ByteClass::Progress,
            )?;
            let bound = Instant::now() + EXPIRE_PROPOSAL_DEADLINE;
            match by(bound, handle.propose(command)).await?.reply {
                ParticipantReply::DirectExpired {
                    deleted: page,
                    complete,
                } => {
                    self.stats.expired(page);
                    deleted += page;
                    if complete {
                        break;
                    }
                }
                // A split is copying the range, or the fence moved: the
                // next interval tries again.
                ParticipantReply::Rejected(_) => break,
                _ => return Err(corrupt()),
            }
        }
        Ok(deleted)
    }

    /// One expiry pass over every range this worker leads: the ranges it
    /// served direct requests on, and every local Data replica registered on
    /// its service lane (T5 concern 3), so a new leader also expires records
    /// on ranges that receive no direct request. `expire` proposes nothing
    /// where this node does not serve as leader.
    pub(crate) async fn expire_served(&self) -> u64 {
        let handles: Vec<Arc<ParticipantHandle>> = {
            let mut served = self.served.lock().unwrap_or_else(|p| p.into_inner());
            served.retain(|_, handle| handle.strong_count() > 0);
            let mut handles: HashMap<Uuid, Arc<ParticipantHandle>> = served
                .iter()
                .filter_map(|(group, handle)| Some((*group, handle.upgrade()?)))
                .collect();
            drop(served);
            for handle in
                crate::oltp_replication::service_rpc::direct::registered_replicas(&self.resources)
            {
                handles.entry(handle.identity().group_id).or_insert(handle);
            }
            handles.into_values().collect()
        };
        let mut deleted = 0;
        for handle in handles {
            match self.expire(&handle).await {
                Ok(count) => deleted += count,
                Err(error) => {
                    tracing::debug!(group=%handle.identity().group_id, code=?error.code, "direct dedup expiry failed");
                }
            }
        }
        deleted
    }

    /// Starts the periodic expiry task. It holds the executor weakly and
    /// ends once the executor is dropped.
    pub(crate) fn spawn_expiry(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let executor = Arc::downgrade(self);
        let interval = self.config.expire_interval;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(executor) = executor.upgrade() else {
                    return;
                };
                executor.expire_served().await;
            }
        })
    }
}

impl DirectService for DirectExecutor {
    fn execute<'a>(
        &'a self,
        handle: &'a Arc<ParticipantHandle>,
        request: DirectRequest,
    ) -> EndpointFuture<'a, DirectResponse> {
        Box::pin(async move { Ok(DirectExecutor::execute(self, handle, request).await) })
    }
    fn forward<'a>(
        &'a self,
        handle: &'a Arc<ParticipantHandle>,
        request: DirectRequest,
    ) -> EndpointFuture<'a, ForwardResponse> {
        Box::pin(async move { Ok(DirectExecutor::forward(self, handle, request).await) })
    }
    fn stats(&self) -> serde_json::Value {
        serde_json::to_value(DirectExecutor::stats(self)).unwrap_or(serde_json::Value::Null)
    }
}

fn fault_response(fault: Fault, epochs: &ParticipantEpochs) -> DirectResponse {
    match fault {
        Fault::StaleEpoch => stale_route(Some(epochs)),
        Fault::SplitInProgress => DirectResponse::Fallback {
            reason: FallbackReason::SplitInProgress,
        },
        Fault::Incomplete => DirectResponse::Fallback {
            reason: FallbackReason::Intent,
        },
        Fault::ResourceLimit => DirectResponse::Fallback {
            reason: FallbackReason::ResourceLimit,
        },
        // The store-side `data.direct.v1` gate (T7): nothing was written.
        Fault::DirectInactive => DirectResponse::Fallback {
            reason: FallbackReason::Inactive,
        },
        _ => failed(ProtocolError::new(
            ErrorCode::InternalError,
            "direct commit rejected",
            Outcome::NotApplied,
        )),
    }
}

fn stale_route(epochs: Option<&ParticipantEpochs>) -> DirectResponse {
    DirectResponse::StaleRoute {
        schema_epoch: epochs.map(|e| e.schema_epoch),
        routing_epoch: epochs.map(|e| e.routing_epoch),
    }
}
/// After a failed barrier: redirect when another node leads, else report
/// the deadline.
fn not_leader_or(handle: &ParticipantHandle) -> DirectResponse {
    let (leader, local) = handle.direct_leadership();
    if leader.is_some() && leader != local {
        DirectResponse::NotLeader { hint: leader }
    } else {
        failed(unavailable_not_applied())
    }
}
/// A key's newest version as `DirectExecutor::latest` reads it. `None` asks
/// for a retry: a waiter was failed by a leadership change, or storage was
/// briefly unavailable.
#[allow(clippy::type_complexity)]
fn latest_value(
    read: Result<LatestRead, ProtocolError>,
    epochs: &ParticipantEpochs,
) -> Result<Option<(Option<CommitTs>, Option<Box<RawValue>>)>, DirectResponse> {
    match read {
        Ok(LatestRead::Value { version, value }) => Ok(Some((version, value))),
        Ok(LatestRead::Blocked) => Err(DirectResponse::Fallback {
            reason: FallbackReason::Intent,
        }),
        Ok(LatestRead::SplitInProgress) => Err(DirectResponse::Fallback {
            reason: FallbackReason::SplitInProgress,
        }),
        Ok(LatestRead::NotOwned) => Err(stale_route(Some(epochs))),
        Err(error) if error.code == ErrorCode::StorageUnavailable => Ok(None),
        Err(error) => Err(failed(error)),
    }
}

/// True when `commit_ts` is above every version the attempt read. A
/// timestamp fetched while the reads ran (PERF2b) can be older than a
/// version that applied meanwhile; apply would reject it with `Conflict`.
fn above_read_versions(commit_ts: CommitTs, writes: &[DirectWrite]) -> bool {
    writes.iter().all(|write| {
        write
            .expected_version
            .is_none_or(|version| commit_ts > version)
            && write.index.iter().all(|change| {
                change
                    .expected_version
                    .is_none_or(|version| commit_ts > version)
            })
    })
}

/// A deadline can cut a submitted proposal before its response is known.
async fn by<T>(
    deadline: Instant,
    future: impl std::future::Future<Output = Result<T, ProtocolError>>,
) -> Result<T, ProtocolError> {
    tokio::time::timeout_at(deadline, future)
        .await
        .unwrap_or_else(|_| {
            Err(ProtocolError::new(
                ErrorCode::StorageUnavailable,
                "direct request deadline expired",
                Outcome::Unknown,
            ))
        })
}
/// RV-FINAL L3: a Catalog read bounded by the request deadline. Nothing was
/// proposed yet, so running out of time is `not_applied`, never a call the
/// gateway's own deadline cuts as uncertain.
async fn catalog_read<T>(
    deadline: Instant,
    read: impl std::future::Future<Output = Result<T, ProtocolError>>,
) -> Result<T, DirectResponse> {
    match tokio::time::timeout_at(deadline, read).await {
        Ok(read) => read.map_err(failed),
        Err(_) => Err(failed(unavailable_not_applied())),
    }
}
fn failed(error: ProtocolError) -> DirectResponse {
    DirectResponse::Failed(error)
}
fn unavailable_not_applied() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "direct request deadline expired before it applied",
        Outcome::NotApplied,
    )
}
fn invalid(message: &str) -> ProtocolError {
    ProtocolError::new(ErrorCode::ValidationError, message, Outcome::NotApplied)
}
fn corrupt() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::InternalError,
        "direct read or reply is malformed",
        Outcome::Unknown,
    )
}
fn reused() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Conflict,
        "request_id was already used with a different request body",
        Outcome::NotApplied,
    )
}
fn already_exists() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Conflict,
        "primary key already exists",
        Outcome::NotApplied,
    )
}
fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(1)
        .max(1)
}
/// The JSON length of `commit` without allocating it.
fn encoded_len(commit: &DirectCommit) -> usize {
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 += buf.len();
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    match serde_json::to_writer(&mut count, commit) {
        Ok(()) => count.0,
        Err(_) => usize::MAX,
    }
}
