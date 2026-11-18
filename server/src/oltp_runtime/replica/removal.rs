//! Logical queue eligibility changes retain complete surviving RAM copies.
//! These statements grant no authority to detach a pool or stop a process.
use super::*;
use std::ops::Bound::{Excluded, Unbounded};

impl WorkerReplica {
    pub(crate) fn member_after(&self, after: Option<Uuid>) -> Option<WorkerIdentity> {
        self.members
            .range((after.map_or(Unbounded, Excluded), Unbounded))
            .next()
            .map(|(_, worker)| *worker)
    }
    pub(crate) fn begin_removal(
        &mut self,
        current: QueueAuthority,
        ticket: JoinTicket,
    ) -> Result<QueueCopyStatement, QueueError> {
        self.begin_removal_merge(current, ticket)?;
        if self.removal_root.is_none()
            && self
                .removal_member(ticket, self.removal_worker_after)?
                .is_some()
        {
            return Err(QueueError::Unavailable);
        }
        let statement = self.removal_statement(ticket)?;
        if let Some(root) = self.removal_root {
            return (root == statement).then_some(root).ok_or(QueueError::Stale);
        }
        self.removal_root = Some(statement);
        self.removal_worker_after = None;
        Ok(statement)
    }

    pub(crate) fn begin_removal_merge(
        &mut self,
        current: QueueAuthority,
        ticket: JoinTicket,
    ) -> Result<(), QueueError> {
        self.check_owner(current, self.local)?;
        self.check_removal_ticket(ticket)?;
        if let Some(existing) = self.removal {
            return (existing == ticket).then_some(()).ok_or(QueueError::Stale);
        }
        self.mutating_owner(current)?;
        self.removal = Some(ticket);
        Ok(())
    }

    pub(crate) fn removal_merge_step(
        &self,
        ticket: JoinTicket,
    ) -> Result<Option<(WorkerIdentity, Option<Uuid>)>, QueueError> {
        if self.removal != Some(ticket) {
            return Err(QueueError::Stale);
        }
        if self.removal_frozen(ticket) {
            return Ok(None);
        }
        Ok(self
            .removal_member(ticket, self.removal_worker_after)?
            .map(|worker| (worker, self.removal_record_after)))
    }

    pub(crate) fn removal_record(
        &self,
        current: QueueAuthority,
        ticket: JoinTicket,
        after: Option<Uuid>,
    ) -> Result<Option<RecordFrame>, QueueError> {
        self.check_owner(current, current.owner)?;
        self.check_removal_ticket(ticket)?;
        if !self.is_ready_member() || self.local == ticket.target || self.local == current.owner {
            return Err(QueueError::Stale);
        }
        self.record_after(after)
            .map(|id| self.frame(id))
            .transpose()
    }

    pub(crate) fn merge_removal_record(
        &mut self,
        ticket: JoinTicket,
        worker: WorkerIdentity,
        after: Option<Uuid>,
        prepared: PreparedInstall,
        retirement_generation: Option<u64>,
    ) -> Result<(), QueueError> {
        self.check_owner(prepared.authority, self.local)?;
        let id = prepared.record_id();
        let sequence = prepared.record.record().sequence;
        if self.removal_frozen(ticket)
            || self.removal_merge_step(ticket)? != Some((worker, after))
            || worker == self.local
            || after.is_some_and(|after| after >= id)
            || self
                .missing_record_retirement_generation(id, sequence)
                .is_some_and(|generation| retirement_generation != Some(generation))
        {
            return Err(QueueError::Stale);
        }
        let local_grants = self.read_grant_set(id);
        let incoming_grants = prepared.read_grants;
        if local_grants.revision == incoming_grants.revision && local_grants != incoming_grants {
            return Err(QueueError::Conflict);
        }
        let record_changed = self.queue.reconcile_validated(prepared.record)?;
        if record_changed {
            if let Some(metadata) = prepared.terminal {
                self.terminal_metadata.insert(id, metadata);
            }
        }
        let grants_changed = incoming_grants.revision > local_grants.revision
            && self.record_read_grants(id, incoming_grants);
        if record_changed || grants_changed {
            self.changed(id)?;
        }
        self.removal_record_after = Some(id);
        Ok(())
    }

    pub(crate) fn acknowledge_removal_inventory(
        &mut self,
        ticket: JoinTicket,
        worker: WorkerIdentity,
        after: Option<Uuid>,
    ) -> Result<(), QueueError> {
        if self.removal_merge_step(ticket)? != Some((worker, after)) {
            return Err(QueueError::Stale);
        }
        self.removal_worker_after = Some(worker.worker_id);
        self.removal_record_after = None;
        Ok(())
    }

    pub(crate) fn removal_statement(
        &self,
        ticket: JoinTicket,
    ) -> Result<QueueCopyStatement, QueueError> {
        self.check_removal_ticket(ticket)?;
        let mut hash = Sha256::new();
        hash.update(b"nabedra/ram-queue-removal/v1\0");
        let mut count = 0u64;
        for record in self.queue.records() {
            count = count.checked_add(1).ok_or(QueueError::Overloaded)?;
            hash.update(record.run_id.as_bytes());
            hash.update(record.request_id.as_bytes());
            hash.update(record.scheduler_term.to_be_bytes());
            hash.update(record.membership_revision.to_be_bytes());
            hash.update(record.sequence.to_be_bytes());
            hash.update(record.version.to_be_bytes());
            hash.update([
                u8::from(record.accepted),
                record.state as u8,
                u8::from(record.result_expired),
            ]);
            hash.update(record.response_status.unwrap_or(0).to_be_bytes());
            hash.update(record.completed_at_ms.unwrap_or(0).to_be_bytes());
            let grants = self.read_grant_set(record.request_id);
            hash.update(
                crate::oltp_replication::owned::serialized_digest(&grants)
                    .map_err(invalid)?
                    .as_bytes(),
            );
            for bytes in [
                record.digest.as_bytes(),
                record.body.as_slice(),
                record
                    .pinned_catalog
                    .as_deref()
                    .ok_or(QueueError::Invalid)?,
                record.response_body.as_deref().unwrap_or_default(),
            ] {
                hash.update((bytes.len() as u64).to_be_bytes());
                hash.update(bytes);
            }
        }
        Ok(QueueCopyStatement {
            ticket,
            source: self.authority.owner,
            snapshot_id: ticket.ticket_id,
            snapshot_cut: count,
            final_cut: count,
            record_version_hash: Digest::from_bytes(hash.finalize().into()),
        })
    }

    pub(crate) fn removal_frozen(&self, ticket: JoinTicket) -> bool {
        self.removal == Some(ticket)
            && self.removal_root.is_some_and(|root| {
                root.ticket == ticket
                    && root.source == self.local
                    && ticket.source_ready == self.authority.ready
            })
    }
    pub(crate) fn removal_progress(
        &self,
        ticket: JoinTicket,
    ) -> Result<(Option<Uuid>, Option<Uuid>), QueueError> {
        if !self.removal_frozen(ticket) {
            return Err(QueueError::Stale);
        }
        Ok((self.removal_worker_after, self.removal_record_after))
    }
    pub(crate) fn acknowledge_removal_record(
        &mut self,
        ticket: JoinTicket,
        worker: WorkerIdentity,
        after: Option<Uuid>,
        record: RecordVersion,
    ) -> Result<(), QueueError> {
        if !self.removal_frozen(ticket)
            || self.removal_member(ticket, self.removal_worker_after)? != Some(worker)
            || self.removal_record_after != after
            || self.record_after(after) != Some(record.request_id)
            || self.removal_record_version(record.request_id)? != record
        {
            return Err(QueueError::Stale);
        }
        self.removal_record_after = Some(record.request_id);
        Ok(())
    }
    pub(crate) fn acknowledge_removal_worker(
        &mut self,
        ticket: JoinTicket,
        worker: WorkerIdentity,
    ) -> Result<(), QueueError> {
        if !self.removal_frozen(ticket)
            || self.removal_member(ticket, self.removal_worker_after)? != Some(worker)
            || worker != self.local && self.record_after(self.removal_record_after).is_some()
        {
            return Err(QueueError::Stale);
        }
        self.removal_worker_after = Some(worker.worker_id);
        self.removal_record_after = None;
        Ok(())
    }
    pub(crate) fn removal_member(
        &self,
        ticket: JoinTicket,
        after: Option<Uuid>,
    ) -> Result<Option<WorkerIdentity>, QueueError> {
        self.check_removal_ticket(ticket)?;
        Ok(self
            .members
            .range((after.map_or(Unbounded, Excluded), Unbounded))
            .map(|(_, worker)| *worker)
            .find(|worker| *worker != ticket.target))
    }
    fn check_removal_ticket(&self, ticket: JoinTicket) -> Result<(), QueueError> {
        if ticket.ticket_id.is_nil()
            || ticket.source_ready != self.authority.ready
            || self.members.get(&ticket.target.worker_id) != Some(&ticket.target)
            || ticket.target == self.authority.owner
        {
            return Err(QueueError::Stale);
        }
        Ok(())
    }
    pub(crate) fn removal_ready_root(
        &self,
        ticket: JoinTicket,
    ) -> Result<(Digest, u64), QueueError> {
        let mut root = ReadySetHasher::default();
        for worker in self
            .members
            .values()
            .filter(|worker| **worker != ticket.target)
        {
            root.push(*worker).map_err(invalid)?;
        }
        Ok((root.finish(), root.count()))
    }
    pub(crate) fn record_after(&self, after: Option<Uuid>) -> Option<Uuid> {
        self.queue.record_after(after)
    }
    fn removal_record_version(&self, id: Uuid) -> Result<RecordVersion, QueueError> {
        let record = self.queue.record(id).ok_or(QueueError::NotFound)?;
        RecordFrame::canonical_version(
            &self.record_header(record)?,
            [
                record.body.as_slice(),
                record
                    .pinned_catalog
                    .as_deref()
                    .ok_or(QueueError::Invalid)?,
                record.response_body.as_deref().unwrap_or_default(),
            ],
        )
    }
}
