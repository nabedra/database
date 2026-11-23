//! Index administration observes an exact Catalog identity and runs on the
//! worker that owns the database. The create request UUID is its build ID.
//! Status is an observation. Create and resume may return Building while the
//! detached actor runs or before the operator retries a saturated build slot.
use super::*;
use crate::oltp::mvcc::{
    MvccKey,
    catalog::{CatalogRead, OwnedRecord, PinnedCatalog},
    index::{IndexClosedKind, IndexDefinition, IndexNameRecord, IndexPhase, IndexRecord},
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
struct Selector {
    table_id: Uuid,
    index_id: Uuid,
    build_id: Uuid,
    attempt: u64,
}

enum Operation {
    Create(IndexDefinition),
    Status(Selector),
    Resume(Selector),
    Retire(Selector),
}

/// How an index admin read treats a Catalog intent that covers the record.
/// A metadata transaction holds one between its Prepare and its Resolve, so a
/// read racing the build actor's Ready publication can find it (IDX1).
#[derive(Clone, Copy)]
enum OnBlocked<'a> {
    /// Status, or a repeat that found the job's gate busy. Help a decided
    /// intent resolve, wait a bounded time for an undecided one, then answer
    /// `exhausted`. Status applies nothing, so it answers a typed retryable
    /// error. A repeated mutation keeps its unknown outcome.
    Help { exhausted: fn() -> ProtocolError },
    /// The caller holds the job's gate and reconciles its own operation once.
    Reconcile(&'a Operation),
    /// The caller has just applied a mutation: an intent is an unknown outcome.
    Unknown,
}

/// Help attempts before a blocked read gives up. The waits between them sum
/// to 355 ms, longer than an undecided metadata transaction's decision takes.
const BLOCKED_READ_ATTEMPTS: u32 = 8;

impl Operation {
    fn parse(operation: &str, payload: &str) -> Result<Option<Self>, ProtocolError> {
        if operation == "storage.index.create" {
            let definition: IndexDefinition =
                serde_json::from_str(payload).map_err(|_| invalid())?;
            if definition.table_id.is_nil()
                || definition.index_id.is_nil()
                || definition.columns.is_empty()
            {
                return Err(invalid());
            }
            protocol::validate_identifier(definition.name.as_str())?;
            return Ok(Some(Self::Create(definition)));
        }
        let action: fn(Selector) -> Self = match operation {
            "storage.index.status" => Self::Status,
            "storage.index.resume" => Self::Resume,
            "storage.index.retire" => Self::Retire,
            _ => return Ok(None),
        };
        let selector: Selector = serde_json::from_str(payload).map_err(|_| invalid())?;
        if selector.table_id.is_nil()
            || selector.index_id.is_nil()
            || selector.build_id.is_nil()
            || selector.attempt == 0
        {
            return Err(invalid());
        }
        Ok(Some(action(selector)))
    }

    fn selector(&self, request_id: Uuid) -> Selector {
        match self {
            Self::Create(definition) => Selector {
                table_id: definition.table_id,
                index_id: definition.index_id,
                build_id: request_id,
                attempt: 1,
            },
            Self::Status(selector) | Self::Resume(selector) | Self::Retire(selector) => *selector,
        }
    }
}

impl Selector {
    fn owner(self, catalog_group_id: Uuid) -> MaintenanceOwnerRef {
        MaintenanceOwnerRef {
            catalog_group_id,
            table_id: self.table_id,
            index_id: self.index_id,
            build_id: self.build_id,
            attempt: self.attempt,
        }
    }
}

#[derive(Serialize)]
struct Report<'a> {
    definition: &'a IndexDefinition,
    build_id: Uuid,
    attempt: u64,
    state: &'static str,
}

impl WorkerRuntime {
    pub(in crate::oltp_runtime) fn validate_index_admin(
        operation: &str,
        payload: &str,
    ) -> Result<bool, ProtocolError> {
        Ok(Operation::parse(operation, payload)?.is_some())
    }

    pub(in crate::oltp_runtime) async fn index_admin(
        self: &Arc<Self>,
        request_id: Uuid,
        operation: &str,
        payload: &str,
    ) -> Result<EncodedResponse, ProtocolError> {
        let operation = Operation::parse(operation, payload)?.ok_or_else(invalid)?;
        let creating = matches!(operation, Operation::Create(_));
        let selector = operation.selector(request_id);
        let authority = self.index_authority(None).await?;
        if matches!(operation, Operation::Status(_)) {
            let record = self
                .read_index_admin(
                    authority,
                    selector,
                    OnBlocked::Help {
                        exhausted: publication_pending,
                    },
                )
                .await?;
            self.index_authority(Some(authority)).await?;
            return Ok(report(
                request_id,
                record.as_deref().ok_or_else(not_found)?,
                false,
            ));
        }

        let digest = crate::oltp_replication::owned::serialized_digest(&(
            b"nabedra/index-operator/v1",
            self.services.catalog.participant().identity(),
            selector.table_id,
            selector.index_id,
        ))?;
        let gate = self.gate(Uuid::from_bytes(
            digest.as_bytes()[..16].try_into().expect("fixed digest"),
        ))?;
        let gate = match gate.try_lock_owned() {
            Ok(gate) => gate,
            Err(_) => {
                // A repeated create/resume can observe the exact durable job
                // while its actor owns the gate. Retire must wait for that actor.
                let record = self
                    .read_index_admin(authority, selector, OnBlocked::Help { exhausted: unknown })
                    .await?;
                if let Some(record) = record.as_deref() {
                    match &operation {
                        Operation::Create(definition) if *definition == record.definition => {
                            self.index_authority(Some(authority)).await?;
                            return Ok(report(request_id, record, true));
                        }
                        Operation::Resume(_)
                            if matches!(record.phase, IndexPhase::Building { .. }) =>
                        {
                            self.index_authority(Some(authority)).await?;
                            return Ok(report(request_id, record, false));
                        }
                        _ => {}
                    }
                }
                return Err(index_busy());
            }
        };
        self.index_authority(Some(authority)).await?;
        let mut record = self
            .read_index_admin(authority, selector, OnBlocked::Reconcile(&operation))
            .await?;
        if let (Operation::Create(definition), Some(existing)) = (&operation, record.as_deref()) {
            if existing.definition != *definition {
                return Err(conflict(
                    "index build UUID already binds a different definition",
                ));
            }
        }
        if let Some(IndexRecord {
            definition,
            phase: IndexPhase::Building { source, .. },
            ..
        }) = record.as_deref()
        {
            if source.authority.run_id == authority.run_id {
                let owner = selector.owner(self.services.catalog.participant().identity().group_id);
                let txn = TxnId {
                    request_id: self.index_declaration_identity(selector.build_id)?,
                    attempt: 1,
                };
                if let Some(declaration) = self.services.status.inspect_transaction(txn).await? {
                    let manifest = declaration.decision.ok_or_else(unknown)?.manifest;
                    self.reconcile_index_declaration(
                        authority,
                        owner,
                        IntentRef { txn, manifest },
                        definition,
                    )
                    .await?;
                }
            }
        }
        if record
            .as_deref()
            .is_some_and(|record| matches!(record.phase, IndexPhase::Ready { .. }))
        {
            let owner = selector.owner(self.services.catalog.participant().identity().group_id);
            let proof = self
                .services
                .catalog
                .participant()
                .strong_index_attempt_closed(owner)
                .await?;
            let closure = proof.archive_evidence();
            if closure.statement.kind != IndexClosedKind::Ready {
                return Err(unknown());
            }
            if proof.restored_observation().is_none() {
                self.reconcile_ready_index(authority, owner, closure.txn, closure.manifest)
                    .await?;
            }
        }
        let service = self.index_service(Some(authority));
        match operation {
            Operation::Status(_) => unreachable!("status returned before mutation admission"),
            Operation::Create(definition) => {
                if record.is_none() {
                    self.validate_index_definition(authority, &definition)
                        .await?;
                    let identity = self.index_declaration_identity(request_id)?;
                    service
                        .declare(
                            definition,
                            selector.build_id,
                            selector.attempt,
                            Some(identity),
                        )
                        .await?;
                    record = Some(
                        self.read_index_admin(authority, selector, OnBlocked::Unknown)
                            .await?
                            .ok_or_else(unknown)?,
                    );
                }
            }
            Operation::Resume(_) => {
                let record = record.as_deref().ok_or_else(not_found)?;
                match &record.phase {
                    IndexPhase::Building { source, .. }
                        if source.authority.run_id == authority.run_id => {}
                    IndexPhase::Ready { .. } => return Ok(report(request_id, record, false)),
                    _ => {
                        return Err(conflict(
                            "index build cannot resume from its current phase or run",
                        ));
                    }
                }
            }
            Operation::Retire(_) => {
                let record = record.as_deref().ok_or_else(not_found)?;
                if matches!(record.phase, IndexPhase::Retired { .. }) {
                    return Ok(report(request_id, record, false));
                }
                service.retire(record).await?;
                let retired = self
                    .read_index_admin(authority, selector, OnBlocked::Unknown)
                    .await?
                    .ok_or_else(unknown)?;
                if !matches!(retired.phase, IndexPhase::Retired { .. }) {
                    return Err(unknown());
                }
                return Ok(report(request_id, &retired, true));
            }
        }
        let record = record.ok_or_else(unknown)?;
        let reply = report(request_id, &record, true);
        if let IndexPhase::Building { source, .. } = &record.phase {
            if source.authority.run_id == authority.run_id {
                let actor = match self.index_builds.try_admit() {
                    Ok(actor) => actor,
                    Err(_) if creating => return Ok(reply),
                    Err(error) => return Err(error),
                };
                let handle = actor.spawn(move || async move {
                    let _gate = gate;
                    if let Err(error) = service.resume(&record).await {
                        tracing::warn!(index=%selector.index_id, build=%selector.build_id, attempt=selector.attempt,
                            code=?error.code, message=%error.message, "index build stopped before completion");
                    }
                    Ok(())
                })?;
                drop(handle);
            }
        }
        Ok(reply)
    }

    fn index_observation(&self, authority: Authority) -> Result<PinnedCatalog, ProtocolError> {
        self.services
            .catalog
            .snapshot(ReadTs::new(u64::MAX).map_err(|_| unknown())?, authority)
    }

    async fn read_index_admin(
        &self,
        authority: Authority,
        selector: Selector,
        blocked: OnBlocked<'_>,
    ) -> Result<Option<OwnedRecord<IndexRecord>>, ProtocolError> {
        let pin = self.index_observation(authority)?;
        let key = MvccKey::CatalogIndex {
            table_id: selector.table_id,
            index_id: selector.index_id,
        };
        let mut attempts = 0;
        let record = loop {
            let intent = match self
                .services
                .catalog
                .read::<IndexRecord>(&pin, key.clone())
                .await?
            {
                CatalogRead::Value { record, .. } => break record,
                CatalogRead::Blocked(intent) => intent,
            };
            match blocked {
                OnBlocked::Unknown => return Err(unknown()),
                OnBlocked::Reconcile(_) if attempts > 0 => return Err(unknown()),
                OnBlocked::Reconcile(operation) => {
                    let owner = selector.owner(pin.catalog.group_id);
                    if let Operation::Create(definition) = operation {
                        if intent.txn.request_id
                            != self.index_declaration_identity(selector.build_id)?
                        {
                            return Err(unknown());
                        }
                        self.reconcile_index_declaration(authority, owner, intent, definition)
                            .await?;
                    } else {
                        self.reconcile_ready_index(authority, owner, intent.txn, intent.manifest)
                            .await?;
                    }
                }
                OnBlocked::Help { exhausted } => {
                    if attempts == BLOCKED_READ_ATTEMPTS {
                        return Err(exhausted());
                    }
                    if attempts > 0 {
                        let backoff = Duration::from_millis(5 << (attempts - 1));
                        nabedra_rt::time::sleep(backoff.min(Duration::from_millis(100))).await;
                    }
                    // Help resolves a decided intent and leaves an undecided
                    // one to its actor. A failed observation is not an outcome.
                    self.help_intent(intent).await.map_err(|error| {
                        if error.outcome == Outcome::Unknown {
                            exhausted()
                        } else {
                            error
                        }
                    })?;
                }
            }
            attempts += 1;
        };
        if let Some(index) = &record {
            if index.definition.table_id != selector.table_id
                || index.definition.index_id != selector.index_id
            {
                return Err(unknown());
            }
            let (build_id, attempt, _) = index.phase.attempt_binding();
            if build_id != selector.build_id || attempt != selector.attempt {
                return Err(conflict("index build UUID or attempt differs from Catalog"));
            }
        }
        Ok(record)
    }

    async fn validate_index_definition(
        &self,
        authority: Authority,
        definition: &IndexDefinition,
    ) -> Result<(), ProtocolError> {
        let pin = self.index_observation(authority)?;
        let CatalogRead::Value { record, .. } = self
            .services
            .catalog
            .describe(&pin, definition.table_id)
            .await?
        else {
            return Err(unknown());
        };
        definition.validate(&record.ok_or_else(not_found)?.descriptor)?;
        match self
            .services
            .catalog
            .read::<IndexNameRecord>(
                &pin,
                MvccKey::CatalogIndexName {
                    table_id: definition.table_id,
                    name: Text::try_from(definition.name.as_str()).map_err(|_| invalid())?,
                },
            )
            .await?
        {
            CatalogRead::Value { record: None, .. } => Ok(()),
            CatalogRead::Value { .. } => Err(conflict("index name already exists")),
            CatalogRead::Blocked(_) => Err(unknown()),
        }
    }

    fn index_declaration_identity(&self, request_id: Uuid) -> Result<Uuid, ProtocolError> {
        let identity = crate::oltp_replication::owned::serialized_digest(&(
            b"nabedra/index-operator-declare/v1".as_slice(),
            self.services.catalog.participant().identity(),
            request_id,
        ))?;
        Ok(Uuid::from_bytes(
            identity.as_bytes()[..16].try_into().expect("fixed digest"),
        ))
    }
}

fn report(request_id: Uuid, record: &IndexRecord, mutation: bool) -> EncodedResponse {
    let (build_id, attempt, _) = record.phase.attempt_binding();
    let state = match record.phase {
        IndexPhase::Building { .. } => "building",
        IndexPhase::Ready { .. } => "ready",
        IndexPhase::Failed { .. } => "failed",
        IndexPhase::Retired { .. } => "retired",
    };
    response::encode_success(
        request_id,
        &Report {
            definition: &record.definition,
            build_id,
            attempt,
            state,
        },
        mutation,
    )
}

fn invalid() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid index administration request",
        Outcome::NotApplied,
    )
}

fn conflict(message: &'static str) -> ProtocolError {
    ProtocolError::new(ErrorCode::Conflict, message, Outcome::NotApplied)
}

fn not_found() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::NotFound,
        "index or table does not exist",
        Outcome::NotApplied,
    )
}

fn publication_pending() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "index publication is in progress; retry the status read",
        Outcome::NotApplied,
    )
}

fn index_busy() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Overloaded,
        "index maintenance is already running",
        Outcome::NotApplied,
    )
}
