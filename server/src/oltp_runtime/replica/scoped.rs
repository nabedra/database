//! Test-only protocol for a Data-colocated RF3 RAM queue. Existing queue
//! transport and Control APIs have no scoped owner or queue-generation fence.
use super::*;
use crate::{
    oltp::mvcc::catalog::ShardRoute,
    oltp_replication::{multiplex::EndpointFuture, owned::serialized_digest},
    protocol::Operation,
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use nabedra_rt::time::{Instant as Deadline, timeout_at};

pub(in crate::oltp_runtime) const SCOPED_QUEUE_VERSION: u32 = 2;
const MAJORITY: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::oltp_runtime) struct ScopedPlacement {
    route: ShardRoute,
    generation: Uuid,
    authority: QueueAuthority,
    members: [WorkerIdentity; 3],
    digest: Digest,
}

impl ScopedPlacement {
    /// Production callers would need a current authoritative queue placement.
    /// Verified Data membership alone cannot authorize a queue owner.
    pub(in crate::oltp_runtime) fn new(
        route: ShardRoute,
        generation: Uuid,
        view: AuthorityView,
        owner: WorkerIdentity,
        mut members: [WorkerIdentity; 3],
    ) -> Result<Self, QueueError> {
        route.group.validate().map_err(invalid)?;
        view.validate().map_err(invalid)?;
        if !matches!(route.group.kind, GroupKind::Data { .. })
            || route.schema_epoch == 0
            || route.routing_epoch == 0
            || generation.is_nil()
            || !members.contains(&owner)
        {
            return Err(QueueError::Invalid);
        }
        members.sort_unstable_by_key(|worker| worker.worker_id);
        let mut root = ReadySetHasher::default();
        for worker in members {
            root.push(worker).map_err(invalid)?;
        }
        let authority = QueueAuthority {
            ready: ReadyMembership {
                view,
                ready_root: root.finish(),
                ready_count: root.count(),
            },
            owner,
        };
        let digest = serialized_digest(&(
            "nabedra/scoped-queue",
            SCOPED_QUEUE_VERSION,
            &route,
            generation,
            authority,
        ))
        .map_err(invalid)?;
        Ok(Self {
            route,
            generation,
            authority,
            members,
            digest,
        })
    }

    pub(super) fn authority(&self) -> QueueAuthority {
        self.authority
    }

    fn member_bit(&self, worker: WorkerIdentity) -> Result<u8, QueueError> {
        let index = self
            .members
            .iter()
            .position(|member| *member == worker)
            .ok_or(QueueError::Stale)?;
        Ok(1 << index)
    }

    fn ensure_owner(&self, current: &Self, sender: WorkerIdentity) -> Result<(), QueueError> {
        if self != current || self.authority.owner != sender {
            return Err(QueueError::Stale);
        }
        Ok(())
    }

    pub(super) fn ensure_sender(
        &self,
        current: &Self,
        sender: WorkerIdentity,
        receiver: WorkerIdentity,
    ) -> Result<(), QueueError> {
        self.ensure_owner(current, sender)?;
        self.member_bit(receiver)?;
        Ok(())
    }

    pub(super) fn frame_record(&self, frame: &RecordFrame) -> Result<ScopedRecord, QueueError> {
        let header = frame.decode_header()?;
        let scope = QueueScope {
            cluster_id: self.route.group.cluster_id,
            database: self.route.group.database.clone(),
        };
        header.preflight(&scope, self.authority)?;
        // These frames use the existing bounded body/context/result encoding
        // inside the fixture. The scoped protocol never asserts all-ready proof.
        if header.all_copy_evidence {
            return Err(QueueError::Invalid);
        }
        let version = frame.version(&header)?;
        let body = frame.sections[0].as_ref().ok_or(QueueError::Invalid)?;
        if hash(body.as_ref()) != header.digest {
            return Err(QueueError::Conflict);
        }
        let operation = protocol::parse_request(body.as_ref()).map_err(invalid)?;
        let (table, key) = match &operation.operation {
            Operation::Get(payload) => (payload.table_id, &payload.key),
            Operation::Update(payload) => (payload.table_id, &payload.key),
            _ => return Err(QueueError::Invalid),
        };
        if operation.request_id != header.request_id
            || !matches!(self.route.group.kind, GroupKind::Data { table_id, .. } if table_id == table)
            || !self.route.contains_key(key)
        {
            return Err(QueueError::Conflict);
        }
        let context_bytes = frame.sections[1].as_ref().ok_or(QueueError::Invalid)?;
        let context: AcceptedContext =
            serde_json::from_slice(context_bytes.as_ref()).map_err(invalid)?;
        context.validate_binding(
            &scope,
            header.ready.view.run_id,
            header.request_id,
            header.digest,
            operation.operation.is_mutation(),
        )?;
        let read = context.read.ok_or(QueueError::Invalid)?;
        if read.schema_epoch != self.route.schema_epoch
            || read.routing_epoch < self.route.routing_epoch
        {
            return Err(QueueError::Stale);
        }
        if !header.state.terminal() && (header.accepted || header.state != RequestState::Queued) {
            return Err(QueueError::Invalid);
        }
        Ok(ScopedRecord {
            binding: RequestBinding {
                run_id: header.ready.view.run_id,
                request_id: header.request_id,
                digest: header.digest,
                accepted_context: hash(context_bytes.as_ref()),
            },
            sequence: header.sequence,
            version,
            terminal: header.state.terminal(),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub(super) struct ScopedRecord {
    binding: RequestBinding,
    sequence: u64,
    version: RecordVersion,
    terminal: bool,
}

impl ScopedRecord {
    fn ensure_terminal_successor(self, terminal: Self) -> Result<(), QueueError> {
        if self.terminal
            || !terminal.terminal
            || self.binding != terminal.binding
            || self.sequence != terminal.sequence
            || self.version.version >= terminal.version.version
        {
            return Err(QueueError::Conflict);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub(super) struct InstalledQuorum {
    placement: Digest,
    record: ScopedRecord,
    holders: u8,
}

impl InstalledQuorum {
    pub(super) fn request_id(self) -> Uuid {
        self.record.binding.request_id
    }

    pub(super) fn verify(
        self,
        placement: &ScopedPlacement,
        local_record: ScopedRecord,
    ) -> Result<(), QueueError> {
        if self.placement != placement.digest || self.holders.count_ones() < MAJORITY {
            return Err(QueueError::Stale);
        }
        if self.record != local_record || local_record.terminal {
            return Err(QueueError::Conflict);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub(super) struct AcceptedQuorum {
    installation: InstalledQuorum,
    holders: u8,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct TerminalQuorum {
    placement: Digest,
    terminal: ScopedRecord,
    holders: u8,
}

impl TerminalQuorum {
    pub(super) fn verify(
        self,
        current: &ScopedPlacement,
        terminal: &RecordFrame,
    ) -> Result<(), QueueError> {
        if self.placement != current.digest || self.holders.count_ones() < MAJORITY {
            return Err(QueueError::Stale);
        }
        if self.terminal != current.frame_record(terminal)? {
            return Err(QueueError::Conflict);
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(super) enum ScopedCopy<'a> {
    Install(&'a RecordFrame),
    Mark(InstalledQuorum),
    Terminal {
        accepted: AcceptedQuorum,
        frame: &'a RecordFrame,
    },
}

impl ScopedCopy<'_> {
    fn statement(self, placement: &ScopedPlacement) -> Result<Digest, QueueError> {
        let placement_id = placement.digest;
        match self {
            Self::Install(frame) => {
                let record = placement.frame_record(frame)?;
                if record.terminal {
                    return Err(QueueError::Invalid);
                }
                serialized_digest(&(placement_id, "install", record))
            }
            Self::Mark(installed) => {
                installed.verify(placement, installed.record)?;
                serialized_digest(&(placement_id, "mark", installed))
            }
            Self::Terminal { accepted, frame } => {
                let admission = accepted.installation;
                admission.verify(placement, admission.record)?;
                if accepted.holders.count_ones() < MAJORITY {
                    return Err(QueueError::Stale);
                }
                let terminal = placement.frame_record(frame)?;
                admission.record.ensure_terminal_successor(terminal)?;
                serialized_digest(&(placement_id, "terminal", accepted, terminal))
            }
        }
        .map_err(invalid)
    }

    /// Preflight before altering RAM. Send this ACK only after the complete
    /// record or its exact marker is successfully installed.
    pub(super) fn prepare_acknowledgement(
        self,
        placement: &ScopedPlacement,
        current: &ScopedPlacement,
        sender: WorkerIdentity,
        receiver: WorkerIdentity,
    ) -> Result<ScopedAck, QueueError> {
        placement.ensure_sender(current, sender, receiver)?;
        Ok(ScopedAck {
            worker: receiver,
            statement: self.statement(placement)?,
        })
    }

    pub(super) fn check_terminal_marker(self, marker: InstalledQuorum) -> Result<(), QueueError> {
        if !matches!(self, Self::Terminal { accepted, .. } if marker == accepted.installation) {
            return Err(QueueError::Conflict);
        }
        Ok(())
    }
}

/// Authenticated transport must bind the returned statement to the target
/// worker's current enrollment, the request nonce and the response.
pub(super) struct ScopedAck {
    worker: WorkerIdentity,
    statement: Digest,
}

pub(super) trait ScopedTransport {
    fn current(&self) -> EndpointFuture<'_, ScopedPlacement>;

    fn copy<'a>(
        &'a self,
        placement: &'a ScopedPlacement,
        sender: WorkerIdentity,
        target: WorkerIdentity,
        copy: ScopedCopy<'a>,
    ) -> EndpointFuture<'a, ScopedAck>;
}

/// A single ingress deadline covers both admission rounds. Dropping an
/// incomplete send cannot undo an installation the recipient already made.
pub(super) async fn accept(
    placement: &ScopedPlacement,
    sender: WorkerIdentity,
    wire: &impl ScopedTransport,
    frame: &RecordFrame,
    deadline: Deadline,
) -> Result<AcceptedQuorum, QueueError> {
    let record = placement.frame_record(frame)?;
    let holders = copy_quorum(
        placement,
        sender,
        wire,
        ScopedCopy::Install(frame),
        deadline,
    )
    .await?;
    let installation = InstalledQuorum {
        placement: placement.digest,
        record,
        holders,
    };
    let holders = copy_quorum(
        placement,
        sender,
        wire,
        ScopedCopy::Mark(installation),
        deadline,
    )
    .await?;
    Ok(AcceptedQuorum {
        installation,
        holders,
    })
}

/// The caller must first resolve the durable decision and required visibility.
/// This boundary confirms copies of the exact terminal response only.
pub(super) async fn confirm_terminal(
    placement: &ScopedPlacement,
    sender: WorkerIdentity,
    wire: &impl ScopedTransport,
    accepted: AcceptedQuorum,
    frame: &RecordFrame,
    deadline: Deadline,
) -> Result<TerminalQuorum, QueueError> {
    let terminal = placement.frame_record(frame)?;
    let copy = ScopedCopy::Terminal { accepted, frame };
    let holders = copy_quorum(placement, sender, wire, copy, deadline).await?;
    Ok(TerminalQuorum {
        placement: placement.digest,
        terminal,
        holders,
    })
}

async fn copy_quorum(
    placement: &ScopedPlacement,
    sender: WorkerIdentity,
    wire: &impl ScopedTransport,
    copy: ScopedCopy<'_>,
    deadline: Deadline,
) -> Result<u8, QueueError> {
    if nabedra_rt::time::now() >= deadline {
        return Err(QueueError::Unavailable);
    }
    timeout_at(deadline, async {
        let current = wire.current().await.map_err(|_| QueueError::Unavailable)?;
        placement.ensure_owner(&current, sender)?;
        let statement = copy.statement(placement)?;
        if nabedra_rt::time::now() >= deadline {
            return Err(QueueError::Unavailable);
        }
        let mut replies = FuturesUnordered::new();
        for target in placement.members {
            let reply = wire.copy(placement, sender, target, copy);
            replies.push(async move { (target, reply.await) });
        }
        let mut holders = 0u8;
        while let Some((target, reply)) = replies.next().await {
            if let Ok(ack) = reply
                && ack.worker == target
                && ack.statement == statement
            {
                holders |= placement.member_bit(ack.worker)?;
                if holders.count_ones() >= MAJORITY {
                    let current = wire.current().await.map_err(|_| QueueError::Unavailable)?;
                    placement.ensure_owner(&current, sender)?;
                    if nabedra_rt::time::now() >= deadline {
                        return Err(QueueError::Unavailable);
                    }
                    return Ok(holders);
                }
            }
        }
        Err(QueueError::Unavailable)
    })
    .await
    .map_err(|_| QueueError::Unavailable)?
}
