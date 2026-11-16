//! Replicated request records live only in worker RAM. Transport owns authenticated
//! installation and must replicate each returned record before exposing its state.
#[cfg(feature = "olap")]
pub(crate) mod admission;
use crate::protocol::{self, ErrorCode, Outcome, ProtocolError};
use crate::storage::context::{RequestContext, StorageKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
pub struct QueueLimits {
    pub max_pending_requests: usize,
    pub max_queue_bytes_per_replica: usize,
    pub max_retained_result_bytes_per_worker: usize,
    pub result_retention_ms: u64,
}
impl QueueLimits {
    fn validate(self) -> Result<(), QueueError> {
        if self.max_pending_requests == 0
            || self.max_queue_bytes_per_replica == 0
            || self.max_retained_result_bytes_per_worker == 0
            || self.result_retention_ms == 0
        {
            return Err(QueueError::Invalid);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestState {
    Queued,
    Running,
    Committing,
    Reconciling,
    Succeeded,
    Failed,
    Abandoned,
}
impl RequestState {
    pub fn terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Abandoned)
    }
}
/// Internal identity leaves the public operation UUID and exact bytes intact.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestKey {
    pub storage: Option<StorageKey>,
    pub request_id: Uuid,
}
impl RequestKey {
    pub fn new(
        request_id: Uuid,
        context: Option<&RequestContext>,
    ) -> Result<Self, crate::storage::StorageError> {
        Ok(Self {
            request_id,
            storage: context.map(RequestContext::storage_key).transpose()?,
        })
    }
}
impl From<Uuid> for RequestKey {
    fn from(request_id: Uuid) -> Self {
        Self {
            storage: None,
            request_id,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestRecord {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<RequestContext>,
    pub run_id: Uuid,
    pub scheduler_term: u64,
    pub membership_revision: u64,
    pub request_id: Uuid,
    pub digest: String,
    pub sequence: u64,
    pub version: u64,
    pub body: Vec<u8>,
    pub accepted: bool,
    pub state: RequestState,
    pub pinned_catalog: Option<Vec<u8>>,
    pub response_status: Option<u16>,
    pub response_body: Option<Vec<u8>>,
    pub completed_at_ms: Option<u64>,
    pub result_expired: bool,
    pub replicas: BTreeSet<Uuid>,
}
#[cfg(any(feature = "olap", test))]
type BindingDigests = (Option<[u8; 32]>, Option<[u8; 32]>);

impl RequestRecord {
    pub fn identity(&self) -> RequestKey {
        RequestKey {
            storage: self.context.as_ref().and_then(|c| c.storage_key().ok()),
            request_id: self.request_id,
        }
    }
    pub(crate) fn context_bytes(&self) -> Result<usize, QueueError> {
        self.context
            .as_ref()
            .map(|c| {
                serde_json::to_vec(c)
                    .map(|v| v.len())
                    .map_err(|_| QueueError::Invalid)
            })
            .transpose()
            .map(|n| n.unwrap_or(0))
    }

    fn queue_bytes(&self) -> Result<usize, QueueError> {
        // Include allocator/tree/record metadata, all request and pinned snapshot
        // bytes, the serialized context plus its separate composite-key copy,
        // and bounded UUID replication evidence. Results have their own cap.
        1024usize
            .checked_add(self.body.len())
            .and_then(|n| n.checked_add(self.context_bytes().ok()?.checked_mul(2)?))
            .and_then(|n| n.checked_add(self.pinned_catalog.as_ref().map_or(0, Vec::len)))
            .and_then(|n| {
                self.replicas
                    .len()
                    .checked_mul(32)
                    .and_then(|m| n.checked_add(m))
            })
            .ok_or(QueueError::Overloaded)
    }
    #[cfg(any(feature = "olap", test))]
    pub(crate) fn binding_digests(&self) -> Result<BindingDigests, QueueError> {
        match &self.context {
            None => Ok((None, None)),
            Some(c) => {
                let key = c.storage_key().map_err(|_| QueueError::Invalid)?;
                let bytes = serde_json::to_vec(c).map_err(|_| QueueError::Invalid)?;
                Ok((
                    Some(Sha256::digest(key.digest().as_bytes()).into()),
                    Some(Sha256::digest(&bytes).into()),
                ))
            }
        }
    }
    fn result_bytes(&self) -> usize {
        self.response_body.as_ref().map_or(0, Vec::len)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueSnapshot {
    pub run_id: Uuid,
    pub scheduler_term: u64,
    pub membership_revision: u64,
    pub members: BTreeSet<Uuid>,
    pub records: Vec<RequestRecord>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub request_id: Uuid,
    pub run_id: Uuid,
    pub state: RequestState,
    pub result: Option<serde_json::Value>,
    pub error: Option<ProtocolError>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueError {
    Invalid,
    Conflict,
    Overloaded,
    Unavailable,
    NotFound,
    Abandoned,
    Stale,
}
impl QueueError {
    pub fn protocol(self) -> ProtocolError {
        let (code, message, outcome) = match self {
            Self::Invalid => (
                ErrorCode::ValidationError,
                "invalid request queue record",
                Outcome::NotApplied,
            ),
            Self::Conflict => (
                ErrorCode::Conflict,
                "request identity has a different body",
                Outcome::NotApplied,
            ),
            Self::Overloaded => (
                ErrorCode::Overloaded,
                "request replica capacity exhausted",
                Outcome::NotApplied,
            ),
            Self::NotFound => (
                ErrorCode::NotFound,
                "receipt unavailable or expired",
                Outcome::NotApplied,
            ),
            Self::Abandoned => (
                ErrorCode::NotFound,
                "old-run request abandoned; check durable mutation decision",
                Outcome::Unknown,
            ),
            Self::Unavailable | Self::Stale => (
                ErrorCode::StorageUnavailable,
                "request replication or scheduler authority unavailable",
                Outcome::Unknown,
            ),
        };
        ProtocolError::new(code, message, outcome)
    }
}
/// Lock this only for short state transitions; never hold a queue lock during I/O.
pub struct RequestQueue {
    run_id: Uuid,
    scheduler_term: u64,
    membership_revision: u64,
    members: BTreeSet<Uuid>,
    limits: QueueLimits,
    next_sequence: u64,
    records: BTreeMap<RequestKey, RequestRecord>,
}
impl RequestQueue {
    pub fn new(
        run_id: Uuid,
        scheduler_term: u64,
        membership_revision: u64,
        limits: QueueLimits,
        members: BTreeSet<Uuid>,
    ) -> Result<Self, QueueError> {
        limits.validate()?;
        if run_id.is_nil()
            || scheduler_term == 0
            || members.is_empty()
            || members.iter().any(Uuid::is_nil)
        {
            return Err(QueueError::Invalid);
        }
        Ok(Self {
            run_id,
            scheduler_term,
            membership_revision,
            members,
            limits,
            next_sequence: 1,
            records: BTreeMap::new(),
        })
    }
    pub fn run_id(&self) -> Uuid {
        self.run_id
    }
    pub fn membership_revision(&self) -> u64 {
        self.membership_revision
    }
    pub fn scheduler_term(&self) -> u64 {
        self.scheduler_term
    }
    pub fn members(&self) -> &BTreeSet<Uuid> {
        &self.members
    }
    pub fn record(&self, id: impl Into<RequestKey>) -> Option<&RequestRecord> {
        let id = id.into();
        self.records.get(&id)
    }
    /// Roll back only an exact proposal that has never been accepted. Accepted
    /// records retain their logical identity through every execution outcome.
    pub(crate) fn discard_unaccepted(
        &mut self,
        id: impl Into<RequestKey>,
        version: u64,
    ) -> Result<(), QueueError> {
        let id = id.into();
        let record = self.records.get(&id).ok_or(QueueError::NotFound)?;
        if record.accepted || record.version != version || record.state != RequestState::Queued {
            return Err(QueueError::Conflict);
        }
        self.records.remove(&id);
        Ok(())
    }
    pub fn pending(&self) -> usize {
        self.records
            .values()
            .filter(|r| !r.state.terminal())
            .count()
    }
    pub fn charged_bytes(&self) -> (usize, usize) {
        self.records.values().fold((0, 0), |(q, r), record| {
            (
                q.saturating_add(record.queue_bytes().unwrap_or(usize::MAX)),
                r.saturating_add(record.result_bytes()),
            )
        })
    }
    pub fn propose(&mut self, body: Vec<u8>, now_ms: u64) -> Result<RequestRecord, QueueError> {
        self.propose_with_context(body, None, now_ms)
    }
    pub fn propose_with_context(
        &mut self,
        body: Vec<u8>,
        context: Option<RequestContext>,
        now_ms: u64,
    ) -> Result<RequestRecord, QueueError> {
        if let Some(c) = &context {
            c.validate(now_ms / 1000).map_err(|_| QueueError::Invalid)?;
        }
        let request = protocol::parse_request(&body).map_err(|_| QueueError::Invalid)?;
        if let Some(record) = self.records.get(&RequestKey {
            storage: context
                .as_ref()
                .map(|c| c.storage_key())
                .transpose()
                .map_err(|_| QueueError::Invalid)?,
            request_id: request.request_id,
        }) {
            return if record.digest == digest(&body)
                && (record.body.is_empty() || record.body == body)
            {
                Ok(record.clone())
            } else {
                Err(QueueError::Conflict)
            };
        }
        self.expire(now_ms);
        let sequence = self.next_sequence;
        let next = sequence.checked_add(1).ok_or(QueueError::Overloaded)?;
        let record = RequestRecord {
            context,
            run_id: self.run_id,
            scheduler_term: self.scheduler_term,
            membership_revision: self.membership_revision,
            request_id: request.request_id,
            digest: digest(&body),
            sequence,
            version: 1,
            body,
            accepted: false,
            state: RequestState::Queued,
            pinned_catalog: None,
            response_status: None,
            response_body: None,
            completed_at_ms: None,
            result_expired: false,
            replicas: BTreeSet::new(),
        };
        self.install(record.clone())?;
        self.next_sequence = next;
        Ok(record)
    }
    fn validate_record(&self, record: &RequestRecord) -> Result<(), QueueError> {
        // Expiry is checked at admission/execution, not on replica recovery.
        // A valid accepted snapshot remains recoverable after credentials expire.
        if let Some(c) = &record.context {
            c.validate(0).map_err(|_| QueueError::Invalid)?;
            if record.context_bytes()? > crate::storage::context::MAX_CONTEXT_BYTES {
                return Err(QueueError::Invalid);
            }
        }
        if record.run_id != self.run_id
            || record.scheduler_term != self.scheduler_term
            || record.membership_revision != self.membership_revision
        {
            return Err(QueueError::Stale);
        }
        let expired_identity =
            record.result_expired && record.state.terminal() && record.body.is_empty();
        if record.sequence == 0
            || record.version == 0
            || record.version == u64::MAX
            || record.digest.len() != 64
            || !record.digest.bytes().all(|b| b.is_ascii_hexdigit())
            || (!expired_identity
                && (record.digest != digest(&record.body)
                    || protocol::parse_request(&record.body)
                        .map_err(|_| QueueError::Invalid)?
                        .request_id
                        != record.request_id))
            || !record.replicas.is_subset(&self.members)
            || record
                .pinned_catalog
                .as_ref()
                .is_some_and(|v| v.len() > protocol::MAX_CATALOG_BYTES + 4100)
            || record
                .response_body
                .as_ref()
                .is_some_and(|v| v.len() > protocol::MAX_BODY_BYTES)
            || (record.state.terminal() != record.completed_at_ms.is_some())
            || (!record.state.terminal()
                && (record.response_status.is_some()
                    || record.response_body.is_some()
                    || record.result_expired))
            || (!record.accepted && record.state != RequestState::Queued)
            || (record.result_expired && record.response_body.is_some())
            || (record.state.terminal() && record.response_status.is_none())
        {
            return Err(QueueError::Invalid);
        }
        if let Some(body) = &record.response_body {
            validate_response(
                record.request_id,
                record.response_status.ok_or(QueueError::Invalid)?,
                body,
            )?;
        }
        if record.state.terminal() && !record.result_expired && record.response_body.is_none() {
            return Err(QueueError::Invalid);
        }
        Ok(())
    }
    pub fn install(&mut self, record: RequestRecord) -> Result<(), QueueError> {
        self.validate_record(&record)?;
        if let Some(old) = self.records.get(&record.identity()) {
            if old.context != record.context
                || old.digest != record.digest
                || (!old.body.is_empty() && !record.body.is_empty() && old.body != record.body)
                || old.sequence != record.sequence
            {
                return Err(QueueError::Conflict);
            }
            if record.version < old.version {
                return Err(QueueError::Stale);
            }
            if record.version == old.version {
                let mut merged = old.clone();
                merged.replicas.extend(record.replicas.iter().copied());
                let mut incoming = record.clone();
                incoming.replicas = merged.replicas.clone();
                if merged != incoming {
                    return Err(QueueError::Conflict);
                }
                // Replication evidence is bounded by membership.
                self.check_capacity(&merged)?;
                self.records.insert(record.identity(), merged);
                return Ok(());
            }
            if old.accepted && !record.accepted
                || old.state.terminal()
                    && (old.state != record.state
                        || old.response_status != record.response_status
                        || old.result_expired && !record.result_expired
                        || !record.result_expired && old.response_body != record.response_body)
                || old.pinned_catalog.is_some()
                    && old.pinned_catalog != record.pinned_catalog
                    && !record.result_expired
            {
                return Err(QueueError::Conflict);
            }
        } else if self.records.values().any(|r| r.sequence == record.sequence) {
            return Err(QueueError::Conflict);
        }
        self.check_capacity(&record)?;
        self.next_sequence = self.next_sequence.max(
            record
                .sequence
                .checked_add(1)
                .ok_or(QueueError::Overloaded)?,
        );
        self.records.insert(record.identity(), record);
        Ok(())
    }
    fn check_capacity(&self, incoming: &RequestRecord) -> Result<(), QueueError> {
        let mut pending = usize::from(!incoming.state.terminal());
        let mut bytes = incoming.queue_bytes()?;
        // Reserve all-ready acknowledgment metadata at initial installation, so
        // acceptance cannot fail solely because proof UUIDs grow afterward.
        bytes = bytes
            .checked_add(
                self.members
                    .len()
                    .saturating_sub(incoming.replicas.len())
                    .checked_mul(32)
                    .ok_or(QueueError::Overloaded)?,
            )
            .ok_or(QueueError::Overloaded)?;
        let scope = incoming.identity().storage;
        let mut scope_pending = usize::from(!incoming.state.terminal());
        let mut results = incoming.result_bytes();
        for old in self
            .records
            .values()
            .filter(|r| r.identity() != incoming.identity())
        {
            if scope.is_some() && scope == old.identity().storage && !old.state.terminal() {
                scope_pending = scope_pending.checked_add(1).ok_or(QueueError::Overloaded)?;
            }
            pending = pending
                .checked_add(usize::from(!old.state.terminal()))
                .ok_or(QueueError::Overloaded)?;
            bytes = bytes
                .checked_add(old.queue_bytes()?)
                .and_then(|n| {
                    n.checked_add(
                        self.members
                            .len()
                            .saturating_sub(old.replicas.len())
                            .checked_mul(32)?,
                    )
                })
                .ok_or(QueueError::Overloaded)?;
            results = results
                .checked_add(old.result_bytes())
                .ok_or(QueueError::Overloaded)?;
        }
        // The configured authenticated data bearer has one stable quota scope
        // per storage target. Credential/expiry rotation cannot create capacity.
        // Application middleware owns finer user quotas. All scopes still share
        // this physical worker's global byte/count limits.
        let per_scope = self.limits.max_pending_requests.div_ceil(4).max(1);
        if scope.is_some() && scope_pending > per_scope
            || pending > self.limits.max_pending_requests
            || bytes > self.limits.max_queue_bytes_per_replica
            || results > self.limits.max_retained_result_bytes_per_worker
        {
            return Err(QueueError::Overloaded);
        }
        Ok(())
    }
    /// Call only after this member authenticated installation of this exact version.
    pub fn acknowledge(
        &mut self,
        id: impl Into<RequestKey>,
        member: Uuid,
    ) -> Result<RequestRecord, QueueError> {
        let id = id.into();
        let version = self.records.get(&id).ok_or(QueueError::NotFound)?.version;
        self.acknowledge_version(id, version, member)
    }
    pub fn acknowledge_version(
        &mut self,
        id: impl Into<RequestKey>,
        version: u64,
        member: Uuid,
    ) -> Result<RequestRecord, QueueError> {
        let id = id.into();
        if !self.members.contains(&member)
            || self.records.get(&id).is_none_or(|r| r.version != version)
        {
            return Err(QueueError::Stale);
        }
        let mut record = self.records.get(&id).ok_or(QueueError::NotFound)?.clone();
        record.replicas.insert(member);
        if record.replicas == self.members && !record.accepted {
            record.accepted = true;
            record.version = record.version.checked_add(1).ok_or(QueueError::Invalid)?;
        }
        self.check_capacity(&record)?;
        self.records.insert(id, record.clone());
        Ok(record)
    }
    pub fn ready(&self, id: impl Into<RequestKey>) -> bool {
        let id = id.into();
        self.records
            .get(&id)
            .is_some_and(|r| r.accepted && r.replicas == self.members)
    }
    pub fn start(
        &mut self,
        id: impl Into<RequestKey>,
        pinned_catalog: Option<Vec<u8>>,
    ) -> Result<RequestRecord, QueueError> {
        let id = id.into();
        if !self.ready(id.clone()) {
            return Err(QueueError::Unavailable);
        }
        let mut record = self.records.get(&id).ok_or(QueueError::NotFound)?.clone();
        if !matches!(
            record.state,
            RequestState::Queued | RequestState::Reconciling
        ) {
            return Err(QueueError::Conflict);
        }
        let mutation = protocol::parse_request(&record.body)
            .map_err(|_| QueueError::Invalid)?
            .operation
            .is_mutation();
        if !mutation && record.pinned_catalog.is_none() && pinned_catalog.is_none() {
            return Err(QueueError::Invalid);
        }
        if let Some(pin) = pinned_catalog {
            if record
                .pinned_catalog
                .as_ref()
                .is_some_and(|old| old != &pin)
            {
                return Err(QueueError::Conflict);
            }
            record.pinned_catalog = Some(pin);
        }
        record.state = RequestState::Running;
        self.changed(record)
    }
    fn changed(&mut self, mut record: RequestRecord) -> Result<RequestRecord, QueueError> {
        record.version = record.version.checked_add(1).ok_or(QueueError::Invalid)?;
        record.replicas.clear();
        self.install(record.clone())?;
        Ok(record)
    }
    pub fn transition(
        &mut self,
        id: impl Into<RequestKey>,
        state: RequestState,
    ) -> Result<RequestRecord, QueueError> {
        let id = id.into();
        let mut record = self.records.get(&id).ok_or(QueueError::NotFound)?.clone();
        if !record.accepted
            || record.state.terminal()
            || !matches!(state, RequestState::Committing | RequestState::Reconciling)
        {
            return Err(QueueError::Conflict);
        }
        record.state = state;
        self.changed(record)
    }
    pub fn finish(
        &mut self,
        id: impl Into<RequestKey>,
        status: u16,
        body: Vec<u8>,
        now_ms: u64,
    ) -> Result<RequestRecord, QueueError> {
        let id = id.into();
        self.expire(now_ms);
        let mut record = self.records.get(&id).ok_or(QueueError::NotFound)?.clone();
        if !record.accepted || record.state.terminal() || !(200..600).contains(&status) {
            return Err(QueueError::Conflict);
        }
        validate_response(id.request_id, status, &body)?;
        record.state = if status == 200 {
            RequestState::Succeeded
        } else {
            RequestState::Failed
        };
        record.response_status = Some(status);
        record.response_body = Some(body);
        record.completed_at_ms = Some(now_ms);
        // Terminal conservation wins over result retention at a finite result
        // cap. Never strand a committed mutation in Running; lookup explicitly
        // reports unavailable and durable history can recover its decision.
        if self.check_capacity(&record) == Err(QueueError::Overloaded) {
            record.response_body = None;
            record.result_expired = true;
            record.body.clear();
            record.pinned_catalog = None;
        }
        self.changed(record)
    }
    pub fn lookup(
        &self,
        run_id: Uuid,
        id: impl Into<RequestKey>,
        now_ms: u64,
    ) -> Result<Receipt, QueueError> {
        let id = id.into();
        if run_id != self.run_id {
            return Err(QueueError::Abandoned);
        }
        let record = self.records.get(&id).ok_or(QueueError::NotFound)?;
        if !record.accepted {
            return Err(QueueError::Unavailable);
        }
        if record.result_expired
            || record
                .completed_at_ms
                .is_some_and(|t| now_ms.saturating_sub(t) >= self.limits.result_retention_ms)
        {
            return Err(QueueError::NotFound);
        }
        let value = record
            .response_body
            .as_ref()
            .map(|b| {
                serde_json::from_slice::<serde_json::Value>(b).map_err(|_| QueueError::Invalid)
            })
            .transpose()?;
        let result = value
            .as_ref()
            .and_then(|v| v.get("data"))
            .filter(|v| !v.is_null())
            .cloned();
        let error = value
            .as_ref()
            .and_then(|v| v.get("error"))
            .map(|v| serde_json::from_value(v.clone()).map_err(|_| QueueError::Invalid))
            .transpose()?;
        Ok(Receipt {
            request_id: id.request_id,
            run_id,
            state: record.state,
            result,
            error,
        })
    }
    pub fn expire(&mut self, now_ms: u64) {
        for record in self.records.values_mut() {
            if !record.result_expired
                && record
                    .completed_at_ms
                    .is_some_and(|t| now_ms.saturating_sub(t) >= self.limits.result_retention_ms)
            {
                record.response_body = None;
                record.body.clear();
                record.pinned_catalog = None;
                record.result_expired = true;
                record.version = record.version.saturating_add(1);
                record.replicas.clear();
            }
        }
    }
    pub fn records(&self) -> impl Iterator<Item = &RequestRecord> {
        self.records.values()
    }
    pub fn snapshot_page(&self, after_sequence: Option<u64>) -> (QueueSnapshot, bool) {
        let next = self
            .records
            .values()
            .filter(|r| after_sequence.is_none_or(|after| r.sequence > after))
            .min_by_key(|r| r.sequence);
        let more =
            next.is_some_and(|next| self.records.values().any(|r| r.sequence > next.sequence));
        (
            QueueSnapshot {
                run_id: self.run_id,
                scheduler_term: self.scheduler_term,
                membership_revision: self.membership_revision,
                members: self.members.clone(),
                records: next.into_iter().cloned().collect(),
            },
            !more,
        )
    }
    pub fn snapshot(&self) -> QueueSnapshot {
        QueueSnapshot {
            run_id: self.run_id,
            scheduler_term: self.scheduler_term,
            membership_revision: self.membership_revision,
            members: self.members.clone(),
            records: self.records.values().cloned().collect(),
        }
    }
    /// Atomic preflight: a too-small joining replica installs no partial queue.
    pub fn install_snapshot(&mut self, snapshot: QueueSnapshot) -> Result<(), QueueError> {
        if snapshot.run_id != self.run_id
            || snapshot.scheduler_term != self.scheduler_term
            || snapshot.membership_revision != self.membership_revision
            || snapshot.members != self.members
        {
            return Err(QueueError::Stale);
        }
        let mut replacement = Self::new(
            self.run_id,
            self.scheduler_term,
            self.membership_revision,
            self.limits,
            self.members.clone(),
        )?;
        for record in snapshot.records {
            replacement.install(record)?;
        }
        for old in self.records.values() {
            let incoming = replacement
                .records
                .get(&old.identity())
                .ok_or(QueueError::Conflict)?;
            if incoming.version < old.version
                || incoming.digest != old.digest
                || incoming.sequence != old.sequence
                || old.accepted && !incoming.accepted
                || old.state.terminal() && old.state != incoming.state
            {
                return Err(QueueError::Conflict);
            }
        }
        *self = replacement;
        Ok(())
    }
    /// The caller MUST commit the catalog fence before takeover, then replicate
    /// this snapshot to every new ready member before new work or admissions.
    pub fn takeover(
        &mut self,
        term: u64,
        membership_revision: u64,
        members: BTreeSet<Uuid>,
    ) -> Result<QueueSnapshot, QueueError> {
        if term < self.scheduler_term
            || membership_revision < self.membership_revision
            || term == self.scheduler_term && membership_revision == self.membership_revision
            || members.is_empty()
        {
            return Err(QueueError::Stale);
        }
        let mut replacement =
            Self::new(self.run_id, term, membership_revision, self.limits, members)?;
        for mut record in self.records.values().cloned() {
            record.scheduler_term = term;
            record.membership_revision = membership_revision;
            record.replicas.clear();
            if matches!(
                record.state,
                RequestState::Running | RequestState::Committing
            ) {
                record.state = RequestState::Reconciling;
            }
            record.version = record.version.checked_add(1).ok_or(QueueError::Invalid)?;
            replacement.install(record)?;
        }
        *self = replacement;
        Ok(self.snapshot())
    }
}
fn validate_response(id: Uuid, status: u16, body: &[u8]) -> Result<(), QueueError> {
    if body.len() > protocol::MAX_BODY_BYTES {
        return Err(QueueError::Invalid);
    }
    if status == 200 {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Success {
            request_id: Uuid,
            data: Box<serde_json::value::RawValue>,
        }
        let response: Success = serde_json::from_slice(body).map_err(|_| QueueError::Invalid)?;
        let _ = response.data;
        if response.request_id != id {
            return Err(QueueError::Invalid);
        }
    } else {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Error {
            request_id: Uuid,
            error: ProtocolError,
        }
        let response: Error = serde_json::from_slice(body).map_err(|_| QueueError::Invalid)?;
        if response.request_id != id || response.error.status_code() != status {
            return Err(QueueError::Invalid);
        }
    }
    Ok(())
}
fn digest(body: &[u8]) -> String {
    Sha256::digest(body)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod tests;
