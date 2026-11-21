//! Bounded chunk transfer of a file-backed group snapshot. Each native job owns
//! the transfer lock and charged bytes until actual disk settlement, including
//! when its network waiter is cancelled.
use super::*;
use crate::oltp::pool::{GroupSnapshot, GroupSnapshotHeader, IncomingSnapshot};
use crate::oltp_replication::{
    entry::BoundedStoredMembership, multiplex::ReplicaMetadata, types::Digest,
};
use sha2::{Digest as _, Sha256};
use std::{os::unix::fs::FileExt, time::Instant};
use uuid::Uuid;

const CHUNK: usize = 64 << 10;
const DESCRIPTION: usize = 64 << 10;
const METADATA: usize = 1 << 20;

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Method {
    Begin,
    Chunk,
    Finish,
    Abort,
}
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    from_node: u64,
    to_node: u64,
    transfer: Uuid,
    vote: Vote,
    method: Method,
    offset: u64,
    payload_bytes: u64,
    checksum: Option<Digest>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Description {
    header: GroupSnapshotHeader,
    bytes: u64,
    last_log_id: Option<LogId>,
    membership: BoundedStoredMembership,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reply {
    offset: u64,
    installed_vote: Option<Vote>,
}

pub(super) struct Incoming {
    transfer: Uuid,
    from_node: u64,
    vote: Vote,
    description: Description,
    file: IncomingSnapshot,
    received: u64,
    digest: Sha256,
    touched: Instant,
    _owner: Arc<ReplicaMetadata>,
}
pub(super) fn target_node(bytes: &[u8], purpose: Purpose) -> Result<u64, ProtocolError> {
    let max_bytes = match purpose {
        Purpose::Authority
        | Purpose::Storage
        | Purpose::Settlement
        | Purpose::ServiceMutation
        | Purpose::ApiIngress => super::super::service_rpc::SERVICE_HEADER_BYTES,
        _ => 1024,
    };
    crate::protocol::json_input(bytes, max_bytes)
        .map_err(|_| invalid("invalid replica route bounds"))?;
    #[derive(Deserialize)]
    struct Route {
        to_node: u64,
    }
    serde_json::from_slice::<Route>(bytes)
        .map(|route| route.to_node)
        .map_err(|_| invalid("invalid replica route"))
}

#[cfg(test)]
#[test]
fn o16_scoped_service_route_keeps_the_service_header_limit() {
    let expanded = format!(r#"{{"to_node":3,"scope":"{}"}}"#, "a".repeat(1400));
    assert_eq!(
        target_node(expanded.as_bytes(), Purpose::Authority).unwrap(),
        3
    );
    assert!(target_node(expanded.as_bytes(), Purpose::Snapshot).is_err());
    let oversized = format!(r#"{{"to_node":3,"scope":"{}"}}"#, "a".repeat(4096));
    assert!(target_node(oversized.as_bytes(), Purpose::Authority).is_err());
}
fn parse(bytes: &[u8]) -> Result<Header, ProtocolError> {
    crate::protocol::json_input(bytes, 1024)
        .map_err(|_| invalid("invalid snapshot header bounds"))?;
    serde_json::from_slice(bytes).map_err(|_| invalid("invalid snapshot header"))
}
fn traffic(kind: &GroupKind) -> TrafficClass {
    match kind {
        GroupKind::Control | GroupKind::Timestamps | GroupKind::Status { .. } => {
            TrafficClass::ControlSnapshot
        }
        _ => TrafficClass::Snapshot,
    }
}
fn native(class: TrafficClass) -> ExecutionClass {
    if class == TrafficClass::ControlSnapshot {
        ExecutionClass::Control
    } else {
        ExecutionClass::Query
    }
}

impl<C: TypedConfig> TypedNetwork<C> {
    async fn snapshot_rpc(
        &self,
        head: Header,
        payload: Option<ChargedBytes>,
        ttl: Duration,
    ) -> Result<Reply, ProtocolError> {
        let shared = self.factory.shared.clone();
        let class = traffic(&self.factory.resident.identity().kind);
        let header = shared.encode_in(class.byte_class(), &head, 1024)?;
        let reply = tokio::time::timeout(
            ttl,
            self.factory.client.request(
                self.factory.resident.identity(),
                self.node.worker_id,
                Purpose::Snapshot,
                header,
                [payload, None, None],
                16 << 10,
            ),
        )
        .await
        .map_err(|_| unavailable())??;
        shared
            .execute(native(class), self.target, 0, move || {
                crate::protocol::json_input(reply.header.as_slice(), 1024)
                    .map_err(|_| invalid("invalid snapshot reply bounds"))?;
                serde_json::from_slice(reply.header.as_slice())
                    .map_err(|_| invalid("invalid snapshot reply"))
            })
            .await
    }
    pub(super) async fn stream_snapshot(
        &self,
        vote: Vote,
        snapshot: SnapshotOf<C, TypedSnapshot>,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
        let transfer = Uuid::new_v4();
        let base = Header {
            from_node: self.factory.local.node_id,
            to_node: self.target,
            transfer,
            vote,
            method: Method::Begin,
            offset: 0,
            payload_bytes: 0,
            checksum: None,
        };
        let ttl = option.soft_ttl();
        let send = async {
            let shared = self.factory.shared.clone();
            let class = traffic(&self.factory.resident.identity().kind);
            // SNAP1: a cut writes its file here, on this replication task.
            let file = snapshot.snapshot.file().await?;
            let description_owner = self.factory.resident.metadata(METADATA)?;
            let description = Description {
                header: file.header.clone(),
                bytes: file.bytes,
                last_log_id: snapshot.meta.last_log_id,
                membership: BoundedStoredMembership(snapshot.meta.last_membership.clone()),
            };
            let payload = shared
                .execute(native(class), self.target, 0, {
                    let shared = shared.clone();
                    move || {
                        let _owner = description_owner;
                        shared.encode_in(class.byte_class(), &description, DESCRIPTION)
                    }
                })
                .await?;
            let begin = self
                .snapshot_rpc(
                    Header {
                        payload_bytes: payload.len() as u64,
                        ..base
                    },
                    Some(payload),
                    ttl,
                )
                .await?;
            if begin.offset != 0 || begin.installed_vote.is_some() {
                return Err(invalid("snapshot begin reply differs"));
            }
            let mut offset = 0;
            let mut digest = Sha256::new();
            while offset < file.bytes {
                let length = (file.bytes - offset).min(CHUNK as u64) as usize;
                let input = file.file.clone();
                let current = offset;
                let (bytes, next_digest) = shared
                    .execute(native(class), self.target, 0, {
                        let shared = shared.clone();
                        move || {
                            let mut bytes = shared.allocate_bytes(class.byte_class(), length)?;
                            input
                                .read_exact_at(bytes.as_mut_slice(), current)
                                .map_err(|_| unavailable())?;
                            digest.update(bytes.as_mut_slice());
                            Ok((bytes.freeze(), digest))
                        }
                    })
                    .await?;
                digest = next_digest;
                let reply = self
                    .snapshot_rpc(
                        Header {
                            method: Method::Chunk,
                            offset,
                            payload_bytes: length as u64,
                            ..base
                        },
                        Some(bytes),
                        ttl,
                    )
                    .await?;
                offset = offset.checked_add(length as u64).ok_or_else(unavailable)?;
                if reply.offset != offset || reply.installed_vote.is_some() {
                    return Err(invalid("snapshot chunk acknowledgement differs"));
                }
                #[cfg(test)]
                if current == 0 {
                    crate::oltp_runtime::recovery_tests::maintenance::hold_snapshot(
                        self.factory.resident.identity().group_id,
                        offset,
                        file.bytes,
                    )
                    .await;
                }
            }
            let checksum = Digest::from_bytes(digest.finalize().into());
            let reply = self
                .snapshot_rpc(
                    Header {
                        method: Method::Finish,
                        offset,
                        checksum: Some(checksum),
                        ..base
                    },
                    None,
                    ttl,
                )
                .await?;
            if reply.offset != file.bytes {
                return Err(invalid("snapshot final acknowledgement differs"));
            }
            // Keep the file and resident metadata owner through the final install ACK.
            drop(snapshot);
            Ok(SnapshotResponse::<C>::new(
                reply.installed_vote.ok_or_else(unavailable)?,
            ))
        };
        tokio::pin!(cancel);
        let result = tokio::select! { closed = &mut cancel => Err(StreamingError::Closed(closed)), result = send => result.map_err(|_| StreamingError::Unreachable(Unreachable::from_string("typed snapshot transfer unavailable"))) };
        if result.is_err() {
            // Cleanup has a finite deadline. Receiver ownership also expires on
            // the next transfer, and pool reopen removes orphan receive files.
            let _ = self
                .snapshot_rpc(
                    Header {
                        method: Method::Abort,
                        ..base
                    },
                    None,
                    ttl.min(Duration::from_secs(1)),
                )
                .await;
        }
        result
    }
}

impl<C, M> ReplicaEndpoint<C, M>
where
    C: TypedConfig<R = Applied<Option<<C as TypedConfig>::Reply>>>,
    M: TypedMachine<C>,
{
    pub(super) fn snapshot_preflight(
        &self,
        session: &AuthenticatedSession,
        bytes: &[u8],
    ) -> Result<ReceivePlan, ProtocolError> {
        let head = parse(bytes)?;
        self.verify_sender(session, head.from_node, head.to_node)?;
        if head.transfer.is_nil()
            || head.vote.leader_id().node_id != head.from_node
            || head.checksum.is_some() != matches!(head.method, Method::Finish)
        {
            return Err(invalid("invalid snapshot sender or checksum"));
        }
        let limit = match head.method {
            Method::Begin => DESCRIPTION,
            Method::Chunk => CHUNK,
            Method::Finish | Method::Abort => 0,
        };
        if head.payload_bytes > limit as u64
            || matches!(head.method, Method::Chunk) && head.payload_bytes == 0
        {
            return Err(invalid("invalid snapshot chunk bounds"));
        }
        Ok(ReceivePlan {
            class: traffic(&self.resident.identity().kind),
            section_lengths: [head.payload_bytes, 0, 0],
            decode_budget_bytes: 0,
        })
    }
    pub(super) async fn snapshot_handle(
        &self,
        session: AuthenticatedSession,
        bytes: ChargedBytes,
        sections: ChargedSections,
    ) -> Result<ChargedReply, ProtocolError> {
        self.snapshot_preflight(&session, bytes.as_slice())?;
        let head = parse(bytes.as_slice())?;
        let class = traffic(&self.resident.identity().kind);
        let shared = self.shared.clone();
        let mut guard = self.incoming.clone().lock_owned().await;
        if guard
            .as_ref()
            .is_some_and(|incoming| incoming.touched.elapsed() > Duration::from_secs(60))
        {
            *guard = None;
        }
        let reply = match head.method {
            Method::Begin => {
                let owner = Arc::new(self.resident.metadata(METADATA)?);
                let expected = self.resident.identity().clone();
                let (description, owner) = shared
                    .execute(native(class), head.from_node, 0, move || {
                        let input = sections
                            .section(0)
                            .ok_or_else(|| invalid("missing snapshot description"))?;
                        if crate::oltp::mvcc::json_decode_budget(input.as_slice())? > METADATA {
                            return Err(invalid("snapshot metadata ownership exceeds reservation"));
                        }
                        let description: Description = serde_json::from_slice(input.as_slice())
                            .map_err(|_| invalid("invalid snapshot description"))?;
                        if description.header.identity != expected
                            || description.bytes == 0
                            || description.header.applied.map(super::super::hosted::log_id)
                                != description.last_log_id
                        {
                            return Err(invalid("snapshot group or applied cut differs"));
                        }
                        Ok((description, owner))
                    })
                    .await?;
                let file = self
                    .state
                    .store()
                    .new_incoming_snapshot(self.resident.node_id(), head.transfer)
                    .await?;
                *guard = Some(Incoming {
                    transfer: head.transfer,
                    from_node: head.from_node,
                    vote: head.vote,
                    description,
                    file,
                    received: 0,
                    digest: Sha256::new(),
                    touched: Instant::now(),
                    _owner: owner,
                });
                Reply {
                    offset: 0,
                    installed_vote: None,
                }
            }
            Method::Abort => {
                if guard.as_ref().is_some_and(|incoming| {
                    incoming.transfer == head.transfer
                        && incoming.from_node == head.from_node
                        && incoming.vote == head.vote
                }) {
                    *guard = None;
                }
                Reply {
                    offset: 0,
                    installed_vote: None,
                }
            }
            Method::Chunk => {
                let (returned, received) = shared
                    .execute(native(class), head.from_node, 0, move || {
                        let incoming = guard
                            .as_mut()
                            .ok_or_else(|| invalid("unknown snapshot transfer"))?;
                        check(incoming, &head)?;
                        let input = sections
                            .section(0)
                            .ok_or_else(|| invalid("missing snapshot chunk"))?;
                        let end = head
                            .offset
                            .checked_add(input.len() as u64)
                            .ok_or_else(unavailable)?;
                        if end > incoming.description.bytes || head.offset != incoming.received {
                            return Err(invalid("snapshot chunk offset differs"));
                        }
                        incoming
                            .file
                            .file
                            .write_all_at(input.as_slice(), head.offset)
                            .map_err(|_| unavailable())?;
                        incoming.digest.update(input.as_slice());
                        incoming.received = end;
                        incoming.touched = Instant::now();
                        Ok((guard, end))
                    })
                    .await?;
                guard = returned;
                Reply {
                    offset: received,
                    installed_vote: None,
                }
            }
            Method::Finish => {
                let (returned, file) = shared
                    .execute(native(class), head.from_node, 0, move || {
                        let incoming = guard
                            .as_ref()
                            .ok_or_else(|| invalid("unknown snapshot transfer"))?;
                        check(incoming, &head)?;
                        if incoming.received != incoming.description.bytes
                            || head.offset != incoming.received
                            || head.checksum
                                != Some(Digest::from_bytes(
                                    incoming.digest.clone().finalize().into(),
                                ))
                        {
                            return Err(invalid("snapshot transfer checksum or size differs"));
                        }
                        incoming.file.file.sync_all().map_err(|_| unavailable())?;
                        if incoming
                            .file
                            .file
                            .metadata()
                            .map_err(|_| unavailable())?
                            .len()
                            != incoming.received
                        {
                            return Err(invalid("snapshot received file length differs"));
                        }
                        let file = GroupSnapshot {
                            file: incoming.file.file.clone(),
                            path: incoming.file.path.clone(),
                            bytes: incoming.received,
                            header: incoming.description.header.clone(),
                        };
                        Ok((guard, file))
                    })
                    .await?;
                guard = returned;
                let snapshot = self.state.snapshot(file).await.map_err(|_| unavailable())?;
                let incoming = guard.as_ref().ok_or_else(unavailable)?;
                if snapshot.meta.last_log_id != incoming.description.last_log_id
                    || snapshot.meta.last_membership != incoming.description.membership.0
                {
                    return Err(invalid(
                        "snapshot file membership differs from sender metadata",
                    ));
                }
                let result = self
                    .raft
                    .install_full_snapshot(head.vote, snapshot)
                    .await
                    .map_err(|_| unavailable())?;
                let offset = incoming.received;
                *guard = None;
                Reply {
                    offset,
                    installed_vote: Some(result.vote),
                }
            }
        };
        drop(guard);
        Ok(ChargedReply {
            header: shared.encode_in(class.byte_class(), &reply, 1024)?,
            sections: [None, None, None],
        })
    }
}
fn check(incoming: &Incoming, head: &Header) -> Result<(), ProtocolError> {
    if incoming.transfer != head.transfer
        || incoming.from_node != head.from_node
        || incoming.vote != head.vote
    {
        return Err(invalid("snapshot transfer identity differs"));
    }
    Ok(())
}
