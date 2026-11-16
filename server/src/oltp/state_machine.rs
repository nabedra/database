//! One-group persistent OpenRaft adapter. Applying an entry commits rows,
//! identity result, membership and applied position in one synchronous batch.
use super::{
    DurableDecision, OltpEngine, StoreSnapshot, active_namespace, codec, encode, failure, query,
    snapshot,
};
use crate::{
    oltp_replication::{Peer, ReplicatedCommand, ReplicatedResult, SnapshotFile, TypeConfig},
    protocol::{self, ErrorCode, Operation, Outcome, ProtocolError},
};
use futures_util::{Stream, StreamExt};
use openraft::{
    EntryPayload, OptionalSend, Raft, RaftSnapshotBuilder, ReadPolicy, StoredMembership,
    storage::{EntryResponder, RaftStateMachine},
    type_config::alias::{EntryOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf},
};
use rocksdb::{Snapshot, WriteBatch};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::Semaphore;
use uuid::Uuid;

#[derive(Clone)]
pub struct StateMachine {
    engine: OltpEngine,
    snapshots: PathBuf,
    io: Arc<Semaphore>,
}

#[derive(Serialize, Deserialize)]
struct SnapshotReference {
    path: PathBuf,
    bytes: u64,
    meta: SnapshotMetaOf<TypeConfig>,
}

pub struct StateSnapshotBuilder {
    owner: StateMachine,
    prepared: Option<io::Result<StoreSnapshot>>,
}

impl StateMachine {
    pub fn new(engine: OltpEngine, snapshots: PathBuf, io: Arc<Semaphore>) -> io::Result<Self> {
        if engine.manifest().group_id.is_none() || io.available_permits() == 0 {
            return Err(invalid(
                "state machine requires replicated topology and control I/O capacity",
            ));
        }
        fs::create_dir_all(&snapshots)?;
        let metadata = fs::symlink_metadata(&snapshots)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(invalid("snapshot path is not an owned directory"));
        }
        Ok(Self {
            engine,
            snapshots,
            io,
        })
    }

    pub fn engine(&self) -> &OltpEngine {
        &self.engine
    }

    pub fn log_store(&self, peer: &Peer) -> io::Result<crate::oltp_replication::LogStore> {
        if peer.cluster_id != self.engine.manifest().cluster_id
            || Some(peer.group_id) != self.engine.manifest().group_id
            || peer.engine != self.engine.manifest().engine
            || peer.format_version != self.engine.manifest().format_version
            || Some(peer.database.as_str()) != self.engine.manifest().database.as_deref()
        {
            return Err(invalid("Raft identity differs from state pool"));
        }
        crate::oltp_replication::LogStore::new(
            self.engine.shared_db(),
            peer,
            self.io.clone(),
            self.engine.recovery_marker(),
        )
    }

    /// A fresh quorum ReadIndex barrier includes application of the required
    /// prefix. No follower or lease read silently substitutes for this path.
    pub async fn read(
        &self,
        raft: &Raft<TypeConfig, Self>,
        operation: Operation,
    ) -> Result<Value, ProtocolError> {
        if operation.is_mutation() {
            return Err(failure(
                ErrorCode::UnsupportedOperation,
                "mutations require a consensus command",
            ));
        }
        raft.ensure_linearizable(ReadPolicy::ReadIndex)
            .await
            .map_err(|_| {
                ProtocolError::new(
                    ErrorCode::StorageUnavailable,
                    "read quorum or leader authority unavailable",
                    Outcome::NotApplied,
                )
            })?;
        operation.validate()?;
        query::check_body(&operation, protocol::MAX_BODY_BYTES)?;
        let engine = self.engine.clone();
        self.engine
            .blocking(move || engine.read(&engine.inner.db.snapshot(), &operation))
            .await
    }

    async fn control<T: Send + 'static>(
        &self,
        work: impl FnOnce(OltpEngine) -> io::Result<T> + Send + 'static,
    ) -> io::Result<T> {
        let permit = self
            .io
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| invalid("state I/O closed"))?;
        let engine = self.engine.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            if engine.requires_recovery() {
                return Err(protocol_io(super::recovery_unknown()));
            }
            work(engine)
        })
        .await
        .map_err(|_| invalid("state I/O task failed"))?
    }

    async fn builder(&self) -> StateSnapshotBuilder {
        let path = self
            .snapshots
            .join(format!("snapshot-{}.bin", Uuid::new_v4()));
        let prepared = self
            .control(move |engine| {
                engine
                    .export_snapshot_sync(&path, Value::Null)
                    .map_err(protocol_io)
            })
            .await;
        StateSnapshotBuilder {
            owner: self.clone(),
            prepared: Some(prepared),
        }
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotData = SnapshotFile;
    type SnapshotBuilder = StateSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> io::Result<(Option<LogIdOf<TypeConfig>>, StoredMembershipOf<TypeConfig>)> {
        self.control(|engine| {
            let snapshot = engine.inner.db.snapshot();
            applied(&snapshot)
        })
        .await
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> io::Result<()>
    where
        Strm: Stream<Item = io::Result<EntryResponder<TypeConfig>>> + Unpin + OptionalSend,
    {
        while let Some(entry) = entries.next().await {
            let (entry, responder) = entry?;
            let owner = self.engine.writer_ownership().await;
            let result = self
                .control(move |engine| {
                    let _owner = owner;
                    engine.apply_committed(&entry)
                })
                .await?;
            if let Some(responder) = responder {
                responder.send(result);
            }
        }
        Ok(())
    }

    async fn try_create_snapshot_builder(&mut self, _force: bool) -> Option<Self::SnapshotBuilder> {
        Some(self.builder().await)
    }
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.builder().await
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<TypeConfig>,
        incoming: SnapshotFile,
    ) -> io::Result<()> {
        let meta = meta.clone();
        let owner = self.engine.writer_ownership().await;
        let destination = self
            .snapshots
            .join(format!("installed-{}.bin", Uuid::new_v4()));
        self.control(move |engine| {
            let _owner = owner;
            // Copy a bounded stream into the state machine's retained directory.
            // Network temporary-file cleanup cannot delete the recovery source.
            let mut source = incoming.file.try_clone()?;
            source.seek(SeekFrom::Start(0))?;
            let mut target = OpenOptions::new()
                .create_new(true)
                .write(true)
                .read(true)
                .open(&destination)?;
            io::copy(&mut source, &mut target)?;
            target.sync_all()?;
            File::open(
                destination
                    .parent()
                    .ok_or_else(|| invalid("snapshot parent missing"))?,
            )?
            .sync_all()?;
            let bytes = target.metadata()?.len();
            target.seek(SeekFrom::Start(0))?;
            let (_, header) = snapshot::read_header(&mut target).map_err(protocol_io)?;
            let recorded = meta_from_header(&header)?;
            if recorded != meta {
                return Err(invalid("snapshot bytes do not match consensus metadata"));
            }
            let current = engine.inner.db.snapshot();
            let (last, _) = applied(&current)?;
            if last.as_ref().is_some_and(|last| {
                meta.last_log_id
                    .as_ref()
                    .is_none_or(|incoming| incoming.index < last.index)
            }) {
                return Err(invalid("snapshot would move applied state backwards"));
            }
            drop(current);
            let reference = SnapshotReference {
                path: destination,
                bytes,
                meta,
            };
            engine
                .install_snapshot_sync(
                    Arc::new(target),
                    Some(encode(&reference).map_err(protocol_io)?),
                )
                .map_err(protocol_io)?;
            Ok(())
        })
        .await
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> io::Result<Option<SnapshotOf<TypeConfig, SnapshotFile>>> {
        let directory = self.snapshots.clone();
        self.control(move |engine| {
            let Some(bytes) = engine
                .inner
                .db
                .get(snapshot::CURRENT_SNAPSHOT)
                .map_err(disk)?
            else {
                return Ok(None);
            };
            let reference: SnapshotReference = decode(&bytes)?;
            if reference.path.parent() != Some(directory.as_path()) {
                return Err(invalid("snapshot reference escaped owned directory"));
            }
            let file = SnapshotFile::open(reference.path)?;
            if file.bytes != reference.bytes {
                return Err(invalid("snapshot file length changed"));
            }
            Ok(Some(SnapshotOf::<TypeConfig, SnapshotFile> {
                meta: reference.meta,
                snapshot: file,
            }))
        })
        .await
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateSnapshotBuilder {
    type SnapshotData = SnapshotFile;
    async fn build_snapshot(&mut self) -> io::Result<SnapshotOf<TypeConfig, SnapshotFile>> {
        let snapshot = self
            .prepared
            .take()
            .ok_or_else(|| invalid("snapshot builder already consumed"))??;
        let meta = meta_from_header(&snapshot.header)?;
        let file = SnapshotFile {
            file: snapshot.file,
            path: snapshot.path.clone(),
            bytes: snapshot.bytes,
        };
        let reference = SnapshotReference {
            path: snapshot.path,
            bytes: snapshot.bytes,
            meta: meta.clone(),
        };
        self.owner
            .control(move |engine| {
                let mut batch = WriteBatch::default();
                batch.put(
                    snapshot::CURRENT_SNAPSHOT,
                    encode(&reference).map_err(protocol_io)?,
                );
                engine.durable_write(batch).map_err(protocol_io)
            })
            .await?;
        Ok(SnapshotOf::<TypeConfig, SnapshotFile> {
            meta,
            snapshot: file,
        })
    }
}

impl OltpEngine {
    fn apply_committed(&self, entry: &EntryOf<TypeConfig>) -> io::Result<ReplicatedResult> {
        let _guard = self.writer();
        if self.requires_recovery() {
            return Err(protocol_io(super::recovery_unknown()));
        }
        let snapshot = self.inner.db.snapshot();
        let namespace = active_namespace(&snapshot).map_err(protocol_io)?;
        let (last, _) = applied(&snapshot)?;
        if let Some(last) = &last {
            if entry.log_id.index < last.index
                || (entry.log_id.index == last.index && entry.log_id != *last)
            {
                return Err(invalid("conflicting or backwards state application"));
            }
            if entry.log_id == *last {
                return match &entry.payload {
                    EntryPayload::Normal(command) => Ok(ReplicatedResult {
                        result: replayed(&snapshot, namespace, command)?
                            .ok_or_else(|| invalid("applied command has no durable result"))?,
                    }),
                    _ => Ok(ReplicatedResult {
                        result: Ok(Value::Null),
                    }),
                };
            }
        }
        let mut batch = WriteBatch::default();
        let result = match &entry.payload {
            EntryPayload::Blank => Ok(Value::Null),
            EntryPayload::Membership(membership) => {
                // Proposal and transport boundaries validate enrolled peers.
                // The committed membership is persisted without rewriting it.
                let stored = StoredMembership::new(Some(entry.log_id), membership.clone());
                batch.put(
                    namespace.category(b'v'),
                    encode(&stored).map_err(protocol_io)?,
                );
                Ok(Value::Null)
            }
            EntryPayload::Normal(command) => {
                self.prepare_replicated(&snapshot, namespace, command, &mut batch)?
            }
        };
        batch.put(
            namespace.category(b'p'),
            encode(&entry.log_id).map_err(protocol_io)?,
        );
        self.durable_write(batch).map_err(protocol_io)?;
        Ok(ReplicatedResult { result })
    }
}

fn applied(
    snapshot: &Snapshot<'_>,
) -> io::Result<(Option<LogIdOf<TypeConfig>>, StoredMembershipOf<TypeConfig>)> {
    let namespace = active_namespace(snapshot).map_err(protocol_io)?;
    let last = snapshot
        .get(namespace.category(b'p'))
        .map_err(disk)?
        .map(|bytes| decode(&bytes))
        .transpose()?;
    let membership = snapshot
        .get(namespace.category(b'v'))
        .map_err(disk)?
        .map(|bytes| decode(&bytes))
        .transpose()?
        .unwrap_or_default();
    Ok((last, membership))
}
#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
struct Authority {
    run_id: Uuid,
    term: u64,
}
fn authority(
    snapshot: &Snapshot<'_>,
    namespace: codec::Namespace,
) -> io::Result<Option<Authority>> {
    snapshot
        .get(namespace.category(b'a'))
        .map_err(disk)?
        .map(|bytes| decode(&bytes))
        .transpose()
}
fn replayed(
    snapshot: &Snapshot<'_>,
    namespace: codec::Namespace,
    command: &ReplicatedCommand,
) -> io::Result<Option<Result<Value, ProtocolError>>> {
    let (request, digest) = match command {
        ReplicatedCommand::Fence { run_id, term, .. } => {
            return Ok(authority(snapshot, namespace)?
                .filter(|active| active.run_id == *run_id && active.term == *term)
                .map(|_| Ok(Value::Null)));
        }
        ReplicatedCommand::Operation {
            request, digest, ..
        } => (request, digest),
    };
    let Some(bytes) = snapshot
        .get(namespace.decision(request.request_id))
        .map_err(disk)?
    else {
        return Ok(None);
    };
    let decision: DurableDecision = decode(&bytes)?;
    Ok(Some(if decision.digest == *digest {
        decision.result
    } else {
        Err(failure(
            ErrorCode::Conflict,
            "request identity has a different body",
        ))
    }))
}
impl OltpEngine {
    fn prepare_replicated(
        &self,
        snapshot: &Snapshot<'_>,
        namespace: codec::Namespace,
        command: &ReplicatedCommand,
        batch: &mut WriteBatch,
    ) -> io::Result<Result<Value, ProtocolError>> {
        match command {
            ReplicatedCommand::Fence {
                expected_run,
                expected_term,
                run_id,
                term,
            } => {
                if run_id.is_nil()
                    || expected_run.is_some_and(|run| run.is_nil())
                    || expected_term.checked_add(1) != Some(*term)
                {
                    return Err(invalid("invalid execution fence transition"));
                }
                let active = authority(snapshot, namespace)?;
                if active
                    == Some(Authority {
                        run_id: *run_id,
                        term: *term,
                    })
                {
                    return Ok(Ok(Value::Null));
                }
                if active.map(|value| value.run_id) != *expected_run
                    || active.map_or(0, |value| value.term) != *expected_term
                {
                    return Ok(Err(failure(
                        ErrorCode::Conflict,
                        "execution fence expectation does not match active authority",
                    )));
                }
                batch.put(
                    namespace.category(b'a'),
                    encode(&Authority {
                        run_id: *run_id,
                        term: *term,
                    })
                    .map_err(protocol_io)?,
                );
                Ok(Ok(Value::Null))
            }
            ReplicatedCommand::Operation {
                request,
                digest,
                run_id,
                term,
                create_table_id,
            } => {
                if !request.operation.is_mutation()
                    || digest.len() != 64
                    || !digest.bytes().all(|b| b.is_ascii_hexdigit())
                    || run_id.is_nil()
                    || *term == 0
                {
                    return Err(invalid("invalid replicated mutation identity"));
                }
                match (&request.operation, create_table_id) {
                    (Operation::Create(_), Some(id)) if !id.is_nil() => {}
                    (Operation::Create(_), _) => {
                        return Err(invalid("replicated create lacks its resolved table UUID"));
                    }
                    (_, None) => {}
                    _ => return Err(invalid("unexpected generated UUID in replicated command")),
                }
                if let Some(result) = replayed(snapshot, namespace, command)? {
                    return Ok(result);
                }
                let result = if authority(snapshot, namespace)?
                    != Some(Authority {
                        run_id: *run_id,
                        term: *term,
                    }) {
                    Err(failure(
                        ErrorCode::Conflict,
                        "execution run or owner term is fenced",
                    ))
                } else {
                    match request.operation.validate() {
                        Err(error) => Err(error),
                        Ok(()) => {
                            let input =
                                query::check_body(&request.operation, protocol::MAX_BODY_BYTES)
                                    .map_err(protocol_io)?
                                    .checked_mul(8)
                                    .ok_or_else(|| invalid("replicated command memory overflow"))?;
                            let available = self
                                .resources()
                                .query_memory_bytes
                                .checked_sub(input)
                                .ok_or_else(|| invalid("replica lacks command working memory"))?;
                            match self.prepare(
                                snapshot,
                                &request.operation,
                                batch,
                                available,
                                *create_table_id,
                            ) {
                                Err(error)
                                    if matches!(
                                        error.code,
                                        ErrorCode::LimitExceeded
                                            | ErrorCode::StorageUnavailable
                                            | ErrorCode::InternalError
                                            | ErrorCode::OutcomeUnknown
                                            | ErrorCode::Overloaded
                                    ) =>
                                {
                                    return Err(protocol_io(error));
                                }
                                result => result,
                            }
                        }
                    }
                };
                if result.is_err() {
                    *batch = WriteBatch::default();
                }
                if let Ok(value) = &result {
                    query::check_body(value, protocol::MAX_BODY_BYTES - 128)
                        .map_err(protocol_io)?;
                }
                let decision = DurableDecision {
                    digest: digest.clone(),
                    result: result.clone(),
                    run_id: *run_id,
                };
                batch.put(
                    namespace.decision(request.request_id),
                    encode(&decision).map_err(protocol_io)?,
                );
                Ok(result)
            }
        }
    }
}
fn meta_from_header(header: &super::SnapshotHeader) -> io::Result<SnapshotMetaOf<TypeConfig>> {
    let last_log_id = serde_json::from_value(header.metadata["applied"].clone())
        .map_err(|_| invalid("invalid snapshot applied position"))?;
    let last_membership = if header.metadata["membership"].is_null() {
        StoredMembershipOf::<TypeConfig>::default()
    } else {
        serde_json::from_value(header.metadata["membership"].clone())
            .map_err(|_| invalid("invalid snapshot membership"))?
    };
    Ok(SnapshotMetaOf::<TypeConfig> {
        last_log_id,
        last_membership,
    })
}
fn decode<T: DeserializeOwned>(bytes: &[u8]) -> io::Result<T> {
    serde_json::from_slice(bytes).map_err(|_| invalid("corrupt state metadata"))
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn protocol_io(error: ProtocolError) -> io::Error {
    invalid(&format!(
        "state storage/application failure: {}",
        error.message
    ))
}
fn disk(_: rocksdb::Error) -> io::Error {
    invalid("state RocksDB unavailable")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oltp::{OltpResources, sync_options};
    use serde_json::json;

    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!("oltp-raft-state-{}", Uuid::new_v4())))
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn engine(dir: &Directory) -> OltpEngine {
        let engine = OltpEngine::open_replica(
            &dir.0,
            OltpResources::default(),
            Uuid::from_u128(1),
            Uuid::from_u128(2),
        )
        .unwrap();
        engine.bind_database("default").unwrap();
        let fence = EntryOf::<TypeConfig> {
            log_id: serde_json::from_value(json!({"leader_id":{"term":1,"node_id":1},"index":0}))
                .unwrap(),
            payload: EntryPayload::Normal(ReplicatedCommand::Fence {
                expected_run: None,
                expected_term: 0,
                run_id: Uuid::from_u128(7),
                term: 1,
            }),
        };
        if applied(&engine.inner.db.snapshot()).unwrap().0.is_none() {
            engine.apply_committed(&fence).unwrap().result.unwrap();
        }
        engine
    }
    fn entry(index: u64, operation: Operation, create: Option<Uuid>) -> EntryOf<TypeConfig> {
        let request = protocol::Request {
            request_id: Uuid::new_v4(),
            operation,
        };
        EntryOf::<TypeConfig> {
            log_id: serde_json::from_value(
                json!({"leader_id":{"term":1,"node_id":1},"index":index}),
            )
            .unwrap(),
            payload: EntryPayload::Normal(ReplicatedCommand::Operation {
                request,
                digest: "a".repeat(64),
                run_id: Uuid::from_u128(7),
                term: 1,
                create_table_id: create,
            }),
        }
    }
    fn operation(name: &str, payload: Value) -> Operation {
        protocol::parse_request(
            &serde_json::to_vec(
                &json!({"request_id":Uuid::new_v4(),"operation":name,"payload":payload}),
            )
            .unwrap(),
        )
        .unwrap()
        .operation
    }
    fn create(index: u64, id: Uuid) -> EntryOf<TypeConfig> {
        entry(
            index,
            operation(
                "table.create",
                json!({"name":"items","schema":{"id":"int64"},"key":"id","format":"parquet"}),
            ),
            Some(id),
        )
    }

    #[tokio::test]
    async fn o08_o10_o11_replicated_commands_are_deterministic_atomic_and_deduplicated() {
        let left_dir = Directory::new();
        let right_dir = Directory::new();
        let left = engine(&left_dir);
        let right = engine(&right_dir);
        let table = Uuid::from_u128(100);
        let create = create(1, table);
        let a = left.apply_committed(&create).unwrap().result.unwrap();
        let b = right.apply_committed(&create).unwrap().result.unwrap();
        assert_eq!(a, b);
        assert_eq!(a["id"], json!(table));
        assert_eq!(left.apply_committed(&create).unwrap().result.unwrap(), a);
        let insert = entry(
            2,
            operation(
                "table.insert_many",
                json!({"table_id":table,"rows":[{"id":-1},{"id":2}]}),
            ),
            None,
        );
        assert_eq!(
            left.apply_committed(&insert).unwrap().result.unwrap(),
            json!(2)
        );
        right.apply_committed(&insert).unwrap().result.unwrap();
        let mut duplicate = insert.clone();
        duplicate.log_id.index = 3;
        assert_eq!(
            left.apply_committed(&duplicate).unwrap().result.unwrap(),
            json!(2)
        );
        let snapshot = left.inner.db.snapshot();
        let (last, _) = applied(&snapshot).unwrap();
        assert_eq!(last.unwrap().index, 3);
        let get = operation("table.get", json!({"table_id":table,"key":-1}));
        assert_eq!(left.read(&snapshot, &get).unwrap(), json!({"id":-1}));
        drop(snapshot);
        drop(left);
        let left = engine(&left_dir);
        assert_eq!(
            left.lookup_decision(match &insert.payload {
                EntryPayload::Normal(ReplicatedCommand::Operation { request, .. }) =>
                    request.request_id,
                _ => unreachable!(),
            })
            .await
            .unwrap()
            .unwrap()
            .result,
            Ok(json!(2))
        );
        assert_eq!(
            left.execute(get).await.unwrap_err().code,
            ErrorCode::UnsupportedOperation
        );
    }

    #[tokio::test]
    async fn o11_o16_snapshot_staging_preserves_consensus_keys_and_old_read_pins() {
        let source_dir = Directory::new();
        let target_dir = Directory::new();
        let source = engine(&source_dir);
        let target = engine(&target_dir);
        let table = Uuid::from_u128(100);
        source
            .apply_committed(&create(1, table))
            .unwrap()
            .result
            .unwrap();
        source
            .apply_committed(&entry(
                2,
                operation("table.insert", json!({"table_id":table,"row":{"id":1}})),
                None,
            ))
            .unwrap()
            .result
            .unwrap();
        let mut state = StateMachine::new(
            source.clone(),
            source_dir.0.join("snapshots"),
            Arc::new(Semaphore::new(2)),
        )
        .unwrap();
        let mut builder = state.try_create_snapshot_builder(true).await.unwrap();
        source
            .apply_committed(&entry(
                3,
                operation("table.insert", json!({"table_id":table,"row":{"id":2}})),
                None,
            ))
            .unwrap()
            .result
            .unwrap();
        let snapshot = builder.build_snapshot().await.unwrap();
        assert_eq!(snapshot.meta.last_log_id.as_ref().unwrap().index, 2);
        target
            .inner
            .db
            .put_opt(b"\x30unrelated-log", b"keep", &sync_options())
            .unwrap();
        let before = target.inner.db.snapshot();
        let mut target_state = StateMachine::new(
            target.clone(),
            target_dir.0.join("snapshots"),
            Arc::new(Semaphore::new(2)),
        )
        .unwrap();
        target_state
            .install_snapshot(&snapshot.meta, snapshot.snapshot.clone())
            .await
            .unwrap();
        assert_eq!(
            target.inner.db.get(b"\x30unrelated-log").unwrap().unwrap(),
            b"keep"
        );
        let query = operation(
            "query.count",
            json!({"table_id":table,"query":{"select":null,"filter":null,"order_by":null,"limit":null}}),
        );
        assert_eq!(
            target.read(&target.inner.db.snapshot(), &query).unwrap(),
            json!(1)
        );
        assert_eq!(
            target.read(&before, &query).unwrap_err().code,
            ErrorCode::NotFound
        );
        let current = target_state.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(current.meta, snapshot.meta);
        drop(before);
        drop(target_state);
        drop(target);
        let target = engine(&target_dir);
        let mut target_state = StateMachine::new(
            target.clone(),
            target_dir.0.join("snapshots"),
            Arc::new(Semaphore::new(2)),
        )
        .unwrap();
        assert_eq!(
            target_state.applied_state().await.unwrap().0.unwrap().index,
            2
        );
        assert_eq!(
            target_state
                .get_current_snapshot()
                .await
                .unwrap()
                .unwrap()
                .meta,
            snapshot.meta
        );
        let corrupt = target_dir.0.join("corrupt.bin");
        fs::copy(&snapshot.snapshot.path, &corrupt).unwrap();
        {
            use std::io::Write;
            let mut file = OpenOptions::new().write(true).open(&corrupt).unwrap();
            file.seek(SeekFrom::End(-1)).unwrap();
            file.write_all(&[255]).unwrap();
            file.sync_all().unwrap();
        }
        let generation = active_namespace(&target.inner.db.snapshot()).unwrap().0;
        assert!(
            target
                .install_snapshot_sync(Arc::new(File::open(corrupt).unwrap()), None)
                .is_err()
        );
        assert_eq!(
            active_namespace(&target.inner.db.snapshot()).unwrap().0,
            generation
        );
        assert_eq!(
            target.read(&target.inner.db.snapshot(), &query).unwrap(),
            json!(1)
        );
        // Rewrite a complete, correctly checksummed header to claim a newer
        // position while preserving the older state records in its body.
        let inconsistent = target_dir.0.join("inconsistent.bin");
        let mut bytes = fs::read(&snapshot.snapshot.path).unwrap();
        let old_header = u32::from_be_bytes(bytes[16..20].try_into().unwrap()) as usize;
        let mut header: super::super::SnapshotHeader =
            serde_json::from_slice(&bytes[20..20 + old_header]).unwrap();
        header.metadata["applied"]["index"] = json!(5);
        let encoded = serde_json::to_vec(&header).unwrap();
        let mut rewritten = bytes[..16].to_vec();
        rewritten.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
        rewritten.extend_from_slice(&encoded);
        rewritten.extend_from_slice(&bytes[20 + old_header..bytes.len() - 32]);
        {
            use sha2::Digest;
            let checksum = sha2::Sha256::digest(&rewritten);
            rewritten.extend_from_slice(&checksum);
        }
        fs::write(&inconsistent, rewritten).unwrap();
        bytes.clear();
        assert!(
            target
                .install_snapshot_sync(Arc::new(File::open(inconsistent).unwrap()), None)
                .is_err()
        );
        assert_eq!(
            active_namespace(&target.inner.db.snapshot()).unwrap().0,
            generation
        );
        assert_eq!(
            target.read(&target.inner.db.snapshot(), &query).unwrap(),
            json!(1)
        );
        target
            .apply_committed(&entry(
                3,
                operation("table.insert", json!({"table_id":table,"row":{"id":3}})),
                None,
            ))
            .unwrap()
            .result
            .unwrap();
        assert!(
            target
                .install_snapshot_sync(snapshot.snapshot.file.clone(), None)
                .is_err()
        );
        assert_eq!(
            target.read(&target.inner.db.snapshot(), &query).unwrap(),
            json!(2)
        );
        assert_eq!(
            active_namespace(&target.inner.db.snapshot()).unwrap().0,
            generation
        );
        assert_eq!(
            target.inner.db.get(b"\x30unrelated-log").unwrap().unwrap(),
            b"keep"
        );
    }

    #[tokio::test]
    async fn o15_r04_r05_consensus_fence_orders_prior_commits_and_rejects_late_old_work() {
        let dir = Directory::new();
        let engine = self::engine(&dir);
        let table = Uuid::from_u128(100);
        engine
            .apply_committed(&create(1, table))
            .unwrap()
            .result
            .unwrap();
        let committed = entry(
            2,
            operation("table.insert", json!({"table_id":table,"row":{"id":1}})),
            None,
        );
        engine.apply_committed(&committed).unwrap().result.unwrap();
        let fence = EntryOf::<TypeConfig> {
            log_id: serde_json::from_value(json!({"leader_id":{"term":2,"node_id":2},"index":3}))
                .unwrap(),
            payload: EntryPayload::Normal(ReplicatedCommand::Fence {
                expected_run: Some(Uuid::from_u128(7)),
                expected_term: 1,
                run_id: Uuid::from_u128(8),
                term: 2,
            }),
        };
        assert_eq!(
            engine.apply_committed(&fence).unwrap().result.unwrap(),
            Value::Null
        );
        assert_eq!(
            engine.apply_committed(&fence).unwrap().result.unwrap(),
            Value::Null
        );
        let late = entry(
            4,
            operation("table.insert", json!({"table_id":table,"row":{"id":9}})),
            None,
        );
        assert_eq!(
            engine
                .apply_committed(&late)
                .unwrap()
                .result
                .unwrap_err()
                .code,
            ErrorCode::Conflict
        );
        let get = operation("table.get", json!({"table_id":table,"key":9}));
        assert_eq!(
            engine.read(&engine.inner.db.snapshot(), &get).unwrap(),
            Value::Null
        );
        let mut replay = committed.clone();
        replay.log_id.index = 5;
        assert_eq!(
            engine.apply_committed(&replay).unwrap().result.unwrap(),
            Value::Null
        );
        let mut wrong = fence.clone();
        wrong.log_id.index = 6;
        if let EntryPayload::Normal(ReplicatedCommand::Fence { run_id, .. }) = &mut wrong.payload {
            *run_id = Uuid::from_u128(9);
        }
        assert_eq!(
            engine
                .apply_committed(&wrong)
                .unwrap()
                .result
                .unwrap_err()
                .code,
            ErrorCode::Conflict
        );
        let mut current = entry(
            7,
            operation("table.insert", json!({"table_id":table,"row":{"id":10}})),
            None,
        );
        if let EntryPayload::Normal(ReplicatedCommand::Operation { run_id, term, .. }) =
            &mut current.payload
        {
            *run_id = Uuid::from_u128(8);
            *term = 2;
        }
        engine.apply_committed(&current).unwrap().result.unwrap();
        drop(engine);
        let engine = self::engine(&dir);
        let snapshot = engine.inner.db.snapshot();
        let namespace = active_namespace(&snapshot).unwrap();
        let active = authority(&snapshot, namespace).unwrap().unwrap();
        assert_eq!(active.run_id, Uuid::from_u128(8));
        assert_eq!(active.term, 2);
        assert_eq!(
            engine
                .read(
                    &snapshot,
                    &operation("table.get", json!({"table_id":table,"key":1}))
                )
                .unwrap(),
            json!({"id":1})
        );
        assert_eq!(
            engine
                .read(
                    &snapshot,
                    &operation("table.get", json!({"table_id":table,"key":10}))
                )
                .unwrap(),
            json!({"id":10})
        );
    }

    #[tokio::test]
    async fn o02_o11_replica_identity_and_public_mutation_helpers_cannot_bypass_consensus() {
        let dir = Directory::new();
        let engine = self::engine(&dir);
        let sequence = engine.inner.db.latest_sequence_number();
        engine.bind_database("default").unwrap();
        assert_eq!(
            engine.bind_database("other").unwrap_err().code,
            ErrorCode::Conflict
        );
        let file = dir.0.join("empty.bin");
        fs::write(&file, b"").unwrap();
        assert_eq!(
            engine
                .install_store_snapshot(Arc::new(File::open(file).unwrap()))
                .await
                .unwrap_err()
                .code,
            ErrorCode::UnsupportedOperation
        );
        assert_eq!(
            engine
                .execute(operation(
                    "table.create",
                    json!({"name":"bypass","schema":{"id":"int64"},"key":"id","format":"parquet"})
                ))
                .await
                .unwrap_err()
                .code,
            ErrorCode::UnsupportedOperation
        );
        assert_eq!(engine.inner.db.latest_sequence_number(), sequence);
        drop(engine);
        assert!(
            OltpEngine::open_replica_with_database(
                &dir.0,
                OltpResources::default(),
                Uuid::from_u128(1),
                Uuid::from_u128(2),
                "other"
            )
            .is_err()
        );
        assert!(OltpEngine::open(&dir.0, OltpResources::default()).is_err());
    }

    struct Builder;
    impl
        openraft::testing::log::StoreBuilder<
            TypeConfig,
            crate::oltp_replication::LogStore,
            StateMachine,
            Directory,
        > for Builder
    {
        async fn build(
            &self,
        ) -> Result<
            (Directory, crate::oltp_replication::LogStore, StateMachine),
            openraft::StorageError<TypeConfig>,
        > {
            let dir = Directory::new();
            let engine = OltpEngine::open_replica(
                &dir.0,
                OltpResources::default(),
                Uuid::from_u128(1),
                Uuid::from_u128(2),
            )
            .unwrap();
            engine.bind_database("default").unwrap();
            let state =
                StateMachine::new(engine, dir.0.join("snapshots"), Arc::new(Semaphore::new(2)))
                    .unwrap();
            let peer = Peer {
                node_id: 1,
                cluster_id: Uuid::from_u128(1),
                group_id: Uuid::from_u128(2),
                incarnation: Uuid::from_u128(3),
                engine: "oltp".into(),
                format_version: 2,
                database: "default".into(),
                ..Default::default()
            };
            let log = state.log_store(&peer).unwrap();
            Ok((dir, log, state))
        }
    }

    #[tokio::test]
    async fn o06_o11_openraft_official_storage_contract() {
        openraft::testing::log::Suite::<
            TypeConfig,
            crate::oltp_replication::LogStore,
            StateMachine,
            Builder,
            Directory,
        >::test_all(Builder)
        .await
        .unwrap();
    }
}
