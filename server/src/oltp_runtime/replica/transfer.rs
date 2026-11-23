use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinPhase {
    Snapshot,
    Delta,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinSnapshot {
    pub ticket: JoinTicket,
    pub source: WorkerIdentity,
    pub snapshot_id: Uuid,
    pub snapshot_cut: u64,
}

/// One raw record per bounded message. `done` is true on empty, final and
/// exhausted pages, false only while another eligible record remains.
pub struct JoinPage {
    pub snapshot: JoinSnapshot,
    pub phase: JoinPhase,
    pub final_cut: Option<u64>,
    pub after_sequence: Option<u64>,
    pub next_sequence: Option<u64>,
    pub done: bool,
    pub record: Option<RecordFrame>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinPageHeader {
    pub snapshot: JoinSnapshot,
    pub phase: JoinPhase,
    pub final_cut: Option<u64>,
    pub after_sequence: Option<u64>,
    pub next_sequence: Option<u64>,
    pub done: bool,
}
impl JoinPage {
    pub fn header(&self) -> JoinPageHeader {
        JoinPageHeader {
            snapshot: self.snapshot,
            phase: self.phase,
            final_cut: self.final_cut,
            after_sequence: self.after_sequence,
            next_sequence: self.next_sequence,
            done: self.done,
        }
    }
}

pub(super) struct JoinExport {
    snapshot: JoinSnapshot,
    pub(super) final_cut: Option<u64>,
    snapshot_after: Option<u64>,
    snapshot_done: bool,
    delta_after: Option<u64>,
    delta_done: bool,
}
pub(super) struct JoinImport {
    ticket: JoinTicket,
    snapshot: Option<JoinSnapshot>,
    snapshot_after: Option<u64>,
    snapshot_done: bool,
    delta_after: Option<u64>,
    delta_done: bool,
    final_cut: Option<u64>,
    last_snapshot: Option<(JoinPageHeader, Option<RecordVersion>)>,
    last_delta: Option<(JoinPageHeader, Option<RecordVersion>)>,
}
impl JoinImport {
    pub(super) fn new(ticket: JoinTicket) -> Self {
        Self {
            ticket,
            snapshot: None,
            snapshot_after: None,
            snapshot_done: false,
            delta_after: None,
            delta_done: false,
            final_cut: None,
            last_snapshot: None,
            last_delta: None,
        }
    }
}

impl WorkerReplica {
    pub fn begin_join(
        &mut self,
        current: QueueAuthority,
        ticket: JoinTicket,
    ) -> Result<JoinSnapshot, QueueError> {
        self.check_owner(current, self.local)?;
        if let Some(export) = &self.export {
            return if export.snapshot.ticket == ticket {
                Ok(export.snapshot)
            } else {
                Err(QueueError::Stale)
            };
        }
        self.mutating_owner(current)?;
        if ticket.ticket_id.is_nil()
            || ticket.source_ready != current.ready
            || self.members.contains_key(&ticket.target.worker_id)
        {
            return Err(QueueError::Stale);
        }
        ticket.target.validate().map_err(invalid)?;
        let snapshot = JoinSnapshot {
            ticket,
            source: self.local,
            snapshot_id: nabedra_rt::ids::uuid_v4(),
            snapshot_cut: self.change_cut,
        };
        self.export = Some(JoinExport {
            snapshot,
            final_cut: None,
            snapshot_after: None,
            snapshot_done: false,
            delta_after: None,
            delta_done: false,
        });
        Ok(snapshot)
    }

    pub(crate) fn join_export_progress(
        &self,
        current: QueueAuthority,
        snapshot: JoinSnapshot,
        phase: JoinPhase,
    ) -> Result<(Option<u64>, bool), QueueError> {
        self.check_owner(current, self.local)?;
        let export = self.export.as_ref().ok_or(QueueError::NotFound)?;
        if export.snapshot != snapshot {
            return Err(QueueError::Stale);
        }
        Ok(match phase {
            JoinPhase::Snapshot => (export.snapshot_after, export.snapshot_done),
            JoinPhase::Delta => (export.delta_after, export.delta_done),
        })
    }

    /// Advance only after the target authenticated the exact page. Cancellation
    /// before this transition resends that page, which the importer recognizes.
    pub(crate) fn acknowledge_join_page(
        &mut self,
        current: QueueAuthority,
        page: JoinPageHeader,
    ) -> Result<(), QueueError> {
        self.check_owner(current, self.local)?;
        let export = self.export.as_mut().ok_or(QueueError::NotFound)?;
        if export.snapshot != page.snapshot {
            return Err(QueueError::Stale);
        }
        let (after, done) = match page.phase {
            JoinPhase::Snapshot => (&mut export.snapshot_after, &mut export.snapshot_done),
            JoinPhase::Delta => (&mut export.delta_after, &mut export.delta_done),
        };
        if *done || page.after_sequence != *after {
            return Err(QueueError::Stale);
        }
        *after = page.next_sequence;
        *done = page.done;
        Ok(())
    }

    /// Snapshot pages may overlap admissions. The final delta includes every
    /// record changed after the initial cut, including a previously paged row.
    /// Sealing pauses state changes, never receipts or authority traffic.
    pub fn seal_join(
        &mut self,
        current: QueueAuthority,
        snapshot: JoinSnapshot,
    ) -> Result<u64, QueueError> {
        self.check_owner(current, self.local)?;
        let export = self.export.as_mut().ok_or(QueueError::NotFound)?;
        if export.snapshot != snapshot {
            return Err(QueueError::Stale);
        }
        Ok(*export.final_cut.get_or_insert(self.change_cut))
    }

    pub fn join_page(
        &self,
        current: QueueAuthority,
        snapshot: JoinSnapshot,
        phase: JoinPhase,
        after_sequence: Option<u64>,
    ) -> Result<JoinPage, QueueError> {
        self.check_owner(current, self.local)?;
        let export = self.export.as_ref().ok_or(QueueError::NotFound)?;
        if export.snapshot != snapshot {
            return Err(QueueError::Stale);
        }
        if phase == JoinPhase::Delta && export.final_cut.is_none() {
            return Err(QueueError::Unavailable);
        }
        let eligible = |record: &&RequestRecord| {
            after_sequence.is_none_or(|after| record.sequence > after)
                && (phase == JoinPhase::Snapshot
                    || self
                        .changes
                        .get(&record.request_id)
                        .is_some_and(|cut| *cut > snapshot.snapshot_cut))
        };
        let next = self
            .queue
            .records()
            .filter(eligible)
            .min_by_key(|record| record.sequence);
        let more = next.is_some_and(|next| {
            self.queue
                .records()
                .filter(eligible)
                .any(|r| r.sequence > next.sequence)
        });
        Ok(JoinPage {
            snapshot,
            phase,
            final_cut: if phase == JoinPhase::Delta {
                export.final_cut
            } else {
                None
            },
            after_sequence,
            next_sequence: next.map(|r| r.sequence).or(after_sequence),
            done: !more,
            record: next.map(|r| self.frame(r.request_id)).transpose()?,
        })
    }

    /// The source owner and ticket are authenticated independently of the page
    /// contents. A complete snapshot alone never marks the process ready.
    pub fn install_join_page(
        &mut self,
        current: QueueAuthority,
        sender: WorkerIdentity,
        page: &JoinPage,
    ) -> Result<(), QueueError> {
        let prepared = page
            .record
            .as_ref()
            .map(|frame| {
                PreparedInstall::prepare(self.install_environment(current, sender)?, frame)
            })
            .transpose()?;
        self.install_prepared_join_page(current, sender, page.header(), prepared)
    }
    pub fn install_prepared_join_page(
        &mut self,
        current: QueueAuthority,
        sender: WorkerIdentity,
        page: JoinPageHeader,
        prepared: Option<PreparedInstall>,
    ) -> Result<(), QueueError> {
        self.check_owner(current, sender)?;
        let import = self.import.as_ref().ok_or(QueueError::Stale)?;
        if page.snapshot.ticket != import.ticket
            || page.snapshot.source != sender
            || page.snapshot.snapshot_id.is_nil()
            || import.snapshot.is_some_and(|s| s != page.snapshot)
            || page.phase == JoinPhase::Snapshot && page.final_cut.is_some()
            || page.phase == JoinPhase::Delta
                && (!import.snapshot_done
                    || page
                        .final_cut
                        .is_none_or(|cut| cut < page.snapshot.snapshot_cut)
                    || import
                        .final_cut
                        .is_some_and(|cut| Some(cut) != page.final_cut))
        {
            return Err(QueueError::Stale);
        }
        let version = prepared.as_ref().map(|record| record.version);
        let (last, complete) = match page.phase {
            JoinPhase::Snapshot => (import.last_snapshot, import.snapshot_done),
            JoinPhase::Delta => (import.last_delta, import.delta_done),
        };
        // A lost reply retries the exact immutable page, without rewinding a
        // cursor or overwriting a later delta version.
        if last == Some((page, version)) {
            return Ok(());
        }
        let expected = match page.phase {
            JoinPhase::Snapshot => import.snapshot_after,
            JoinPhase::Delta => import.delta_after,
        };
        if complete
            && version.is_none()
            && page.done
            && page.after_sequence == expected
            && page.next_sequence == expected
        {
            return Ok(());
        }
        if complete {
            return Err(QueueError::Stale);
        }
        if page.after_sequence != expected {
            return Err(QueueError::Stale);
        }
        match prepared {
            Some(prepared) => {
                let sequence = prepared.record.record().sequence;
                if Some(sequence) != page.next_sequence
                    || expected.is_some_and(|after| sequence <= after)
                {
                    return Err(QueueError::Invalid);
                }
                self.install_prepared(prepared)?;
            }
            None if page.done && page.next_sequence == page.after_sequence => {}
            None => return Err(QueueError::Invalid),
        }
        let import = self.import.as_mut().ok_or(QueueError::Stale)?;
        import.snapshot = Some(page.snapshot);
        match page.phase {
            JoinPhase::Snapshot => {
                import.last_snapshot = Some((page, version));
                import.snapshot_after = page.next_sequence;
                import.snapshot_done = page.done;
            }
            JoinPhase::Delta => {
                import.last_delta = Some((page, version));
                import.delta_after = page.next_sequence;
                import.delta_done = page.done;
                import.final_cut = page.final_cut;
            }
        }
        Ok(())
    }

    /// Hashes one record at a time; no queue-size JSON snapshot or full payload
    /// clone. The leader still needs authenticated matching source/target copy
    /// evidence and current ready-view checks before PublishReady.
    fn version_root(&self) -> Result<Digest, QueueError> {
        let mut digest = Sha256::new();
        digest.update(b"nabedra/ram-queue-cut/v1\0");
        let mut after = None;
        while let Some(record) = self
            .queue
            .records()
            .filter(|r| after.is_none_or(|a| r.sequence > a))
            .min_by_key(|r| r.sequence)
        {
            let frame = self.frame(record.request_id)?;
            let version = frame.version(&frame.decode_header()?)?;
            digest.update(record.sequence.to_be_bytes());
            digest.update(version.request_id.as_bytes());
            digest.update(version.version.to_be_bytes());
            digest.update(version.digest.as_bytes());
            after = Some(record.sequence);
        }
        Ok(Digest::from_bytes(digest.finalize().into()))
    }

    pub fn join_statement(
        &self,
        current: QueueAuthority,
    ) -> Result<QueueCopyStatement, QueueError> {
        if current != self.authority {
            return Err(QueueError::Stale);
        }
        let (snapshot, final_cut) = if let Some(export) = &self.export {
            (
                export.snapshot,
                export.final_cut.ok_or(QueueError::Unavailable)?,
            )
        } else if let Some(import) = &self.import {
            if !import.snapshot_done || !import.delta_done {
                return Err(QueueError::Unavailable);
            }
            (
                import.snapshot.ok_or(QueueError::Unavailable)?,
                import.final_cut.ok_or(QueueError::Unavailable)?,
            )
        } else {
            return Err(QueueError::NotFound);
        };
        Ok(QueueCopyStatement {
            ticket: snapshot.ticket,
            source: snapshot.source,
            snapshot_id: snapshot.snapshot_id,
            snapshot_cut: snapshot.snapshot_cut,
            final_cut,
            record_version_hash: self.version_root()?,
        })
    }
}
