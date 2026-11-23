use super::*;
use crate::oltp_replication::service_rpc::ReadScope;

// A query can have at most this many scan pages in flight. Keep the copy
// header, snapshot page, and per-record memory charge bounded at that width.
pub(crate) use nabedra_oltp_types::authority::MAX_READ_GRANTS;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadGrantSet {
    pub(crate) revision: u64,
    pub(crate) grants: Vec<ReadGrant>,
}

pub(super) struct GrantEntry {
    pub(super) set: ReadGrantSet,
    pub(super) copying: bool,
}

const _: () = assert!(
    MAX_READ_GRANTS * std::mem::size_of::<ReadGrant>()
        + std::mem::size_of::<GrantEntry>()
        + std::mem::size_of::<TerminalMetadata>()
        + 256
        <= PROOF_METADATA_BYTES
);

impl ReadGrantSet {
    pub(in crate::oltp_runtime) fn initial() -> Self {
        Self {
            revision: 1,
            grants: Vec::new(),
        }
    }

    pub(super) fn is_initial(&self) -> bool {
        self.revision == 1 && self.grants.is_empty()
    }

    pub(super) fn validate(
        &self,
        view: AuthorityView,
        id: Uuid,
        digest: Digest,
        accepted: bool,
    ) -> Result<(), QueueError> {
        if self.revision == 0
            || self.grants.len() > MAX_READ_GRANTS
            || !accepted && !self.grants.is_empty()
            || self
                .grants
                .windows(2)
                .any(|pair| pair[0].ticket >= pair[1].ticket)
        {
            return Err(QueueError::Invalid);
        }
        for grant in &self.grants {
            if grant.ticket.is_nil()
                || grant.group.is_nil()
                || grant.node == 0
                || grant.owner.validate().is_err()
                || grant.worker.validate().is_err()
                || grant.scope.request.run_id != view.run_id
                || grant.scope.request.request_id != id
                || grant.scope.request.digest != digest
                || grant.scope.authority.run_id != view.run_id
                || grant.scope.authority.owner_term == 0
                || grant.scope.authority.owner_term > view.owner_term
            {
                return Err(QueueError::Invalid);
            }
        }
        Ok(())
    }

    pub(super) fn validate_context(
        &self,
        context: &AcceptedContext,
        bytes: &[u8],
    ) -> Result<(), QueueError> {
        if !self.grants.is_empty() {
            let pin = context.read.as_ref().ok_or(QueueError::Invalid)?;
            let digest = hash(bytes);
            if self.grants.iter().any(|grant| {
                grant.scope.request.accepted_context != digest
                    || grant.scope.read_ts != pin.storage_pin.read_ts
            }) {
                return Err(QueueError::Conflict);
            }
        }
        Ok(())
    }
}

impl WorkerReplica {
    pub(crate) fn read_continuation_copied(&self, scope: ReadScope) -> bool {
        self.local == self.authority.owner
            && self.authority.ready.view.authority() == scope.authority
            && scope.request.run_id == scope.authority.run_id
            && self.record(scope.request.request_id).is_some_and(|record| {
                record.state == RequestState::Running
                    && self.copies_complete(scope.request.request_id)
            })
    }

    pub(crate) fn check_read_continuation(
        &self,
        current: QueueAuthority,
        scope: ReadScope,
    ) -> Result<RequestState, QueueError> {
        self.check_owner(current, self.local)?;
        if scope.authority != current.ready.view.authority()
            || scope.request.run_id != scope.authority.run_id
        {
            return Err(QueueError::Stale);
        }
        let record = self
            .record(scope.request.request_id)
            .ok_or(QueueError::NotFound)?;
        let bytes = record.pinned_catalog.as_deref().ok_or(QueueError::Invalid)?;
        let context: AcceptedContext = serde_json::from_slice(bytes).map_err(invalid)?;
        if !record.accepted
            || !matches!(
                record.state,
                RequestState::Running | RequestState::Reconciling
            )
            || record.run_id != scope.request.run_id
            || Digest::parse(&record.digest).map_err(invalid)? != scope.request.digest
            || hash(bytes) != scope.request.accepted_context
            || context
                .read
                .is_none_or(|read| read.storage_pin.read_ts != scope.read_ts)
        {
            return Err(QueueError::Invalid);
        }
        Ok(record.state)
    }

    pub(super) fn read_grant_set(&self, id: Uuid) -> ReadGrantSet {
        self.read_grants
            .get(&id)
            .map_or_else(ReadGrantSet::initial, |entry| entry.set.clone())
    }

    pub(crate) fn has_read_grants(&self, id: Uuid) -> bool {
        self.read_grants
            .get(&id)
            .is_some_and(|entry| entry.copying || !entry.set.grants.is_empty())
    }

    pub(crate) fn knows_read_grant(&self, grant: ReadGrant) -> bool {
        self.read_grants
            .get(&grant.scope.request.request_id)
            .is_some_and(|entry| entry.set.grants.contains(&grant))
    }

    pub(super) fn check_read_grant_revision(
        &self,
        id: Uuid,
        set: &ReadGrantSet,
    ) -> Result<(), QueueError> {
        if let Some(old) = self.read_grants.get(&id) {
            if set.revision < old.set.revision {
                return Err(QueueError::Stale);
            }
            if set.revision == old.set.revision && *set != old.set {
                return Err(QueueError::Conflict);
            }
        }
        Ok(())
    }

    pub(super) fn record_read_grants(&mut self, id: Uuid, set: ReadGrantSet) -> bool {
        if self.read_grant_set(id) == set {
            return false;
        }
        self.invalidate_terminal_barrier(id);
        self.read_grants.insert(
            id,
            GrantEntry {
                set,
                copying: false,
            },
        );
        true
    }

    fn validate_grant_set(&self, id: Uuid, set: &ReadGrantSet) -> Result<(), QueueError> {
        let record = self.record(id).ok_or(QueueError::NotFound)?;
        set.validate(
            self.authority.ready.view,
            id,
            Digest::parse(&record.digest).map_err(invalid)?,
            record.accepted,
        )?;
        if !set.grants.is_empty() {
            let bytes = record
                .pinned_catalog
                .as_deref()
                .ok_or(QueueError::Invalid)?;
            let context = serde_json::from_slice(bytes).map_err(invalid)?;
            set.validate_context(&context, bytes)?;
        }
        Ok(())
    }

    pub(crate) fn install_read_grants(
        &mut self,
        current: QueueAuthority,
        sender: WorkerIdentity,
        id: Uuid,
        set: ReadGrantSet,
    ) -> Result<(), QueueError> {
        self.install_environment(current, sender)?;
        if !self.is_ready_member() {
            return Err(QueueError::Stale);
        }
        self.validate_grant_set(id, &set)?;
        self.check_read_grant_revision(id, &set)?;
        if self.record_read_grants(id, set) {
            self.changed(id)?;
        }
        Ok(())
    }

    pub(crate) fn update_read_grant(
        &mut self,
        current: QueueAuthority,
        grant: ReadGrant,
        add: bool,
    ) -> Result<Option<ReadGrantSet>, QueueError> {
        self.mutating_owner(current)?;
        let id = grant.scope.request.request_id;
        let record = self.record(id).ok_or(QueueError::NotFound)?;
        if record.run_id != grant.scope.request.run_id {
            return Err(QueueError::Stale);
        }
        if add
            && (record.state != RequestState::Running
                || current.owner != grant.owner
                || current.ready.view.authority() != grant.scope.authority
                || self.read_member(grant.scope.authority, grant.worker.worker_id)
                    != Some(grant.worker))
        {
            return Err(QueueError::Stale);
        }
        let mut set = self.read_grant_set(id);
        match set.grants.binary_search_by_key(&grant.ticket, |g| g.ticket) {
            Ok(index) if set.grants[index] != grant => return Err(QueueError::Conflict),
            Ok(_) if add => {}
            Ok(index) => {
                set.grants.remove(index);
            }
            Err(index) if add && set.grants.len() < MAX_READ_GRANTS => {
                set.grants.insert(index, grant)
            }
            Err(_) if add => return Err(QueueError::Overloaded),
            Err(_) => {}
        }
        if set != self.read_grant_set(id) {
            set.revision = set.revision.checked_add(1).ok_or(QueueError::Overloaded)?;
            self.validate_grant_set(id, &set)?;
            self.changed(id)?;
            self.invalidate_terminal_barrier(id);
            self.read_grants.insert(
                id,
                GrantEntry {
                    set: set.clone(),
                    copying: true,
                },
            );
        }
        Ok(self
            .read_grants
            .get(&id)
            .filter(|entry| entry.copying)
            .map(|_| set))
    }

    fn invalidate_terminal_barrier(&mut self, id: Uuid) {
        // A grant revision is part of the frame digest, even if the queue
        // record's numeric version did not change.
        if self.record(id).is_some_and(|record| record.state.terminal()) {
            self.barriers.remove(&id);
        }
    }

    pub(crate) fn pending_read_grants(&self, id: Uuid) -> Vec<ReadGrant> {
        self.read_grant_set(id).grants
    }

    pub(crate) fn pending_read_grant_copy(&self, id: Uuid) -> Option<ReadGrantSet> {
        self.read_grants
            .get(&id)
            .filter(|entry| entry.copying)
            .map(|entry| entry.set.clone())
    }

    pub(crate) fn complete_read_grant_copy(
        &mut self,
        current: QueueAuthority,
        id: Uuid,
        set: &ReadGrantSet,
    ) -> Result<(), QueueError> {
        self.check_owner(current, self.local)?;
        let entry = self.read_grants.get_mut(&id).ok_or(QueueError::NotFound)?;
        if entry.set != *set {
            return Err(QueueError::Stale);
        }
        entry.copying = false;
        Ok(())
    }
}
