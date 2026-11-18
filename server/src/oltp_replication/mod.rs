//! Durable per-group consensus components. The standalone RF1 runtime does not
//! silently adopt these stores or claim replicated durability.
pub(crate) mod authority;
pub mod entry;
pub(crate) mod hosted;
mod log;
pub(crate) mod manager;
pub mod multiplex;
mod network;
pub mod owned;
mod proof;
pub(crate) mod service_rpc;
pub mod typed_log;
pub(crate) mod typed_network;
pub mod types;
pub use log::LogStore;
pub use network::{NetworkFactory, Server, TlsMaterial, validate_membership};
pub use proof::ReadySetHasher;

use crate::protocol::{ProtocolError, Request};
use serde::{Deserialize, Serialize};
use std::{fs::File, io, path::PathBuf, sync::Arc};
use uuid::Uuid;

/// Permanent membership identity. Replacing a disk requires a new node ID.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Peer {
    pub node_id: u64,
    pub engine: String,
    pub format_version: u32,
    pub database: String,
    pub cluster_id: Uuid,
    pub group_id: Uuid,
    pub incarnation: Uuid,
    pub address: String,
    pub server_name: String,
    pub certificate_sha256: String,
}

/// The leader fixes generated values before proposing the command. Followers
/// apply mutations in log order, without consulting local clocks or generating IDs.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum ReplicatedCommand {
    Fence {
        expected_run: Option<Uuid>,
        expected_term: u64,
        run_id: Uuid,
        term: u64,
    },
    Operation {
        request: Request,
        digest: String,
        run_id: Uuid,
        term: u64,
        create_table_id: Option<Uuid>,
    },
}
impl std::fmt::Display for ReplicatedCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fence { run_id, term, .. } => write!(f, "fence {run_id} {term}"),
            Self::Operation { request, .. } => {
                write!(f, "{} {}", request.request_id, request.operation.name())
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicatedResult {
    pub result: Result<serde_json::Value, ProtocolError>,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = ReplicatedCommand,
        R = ReplicatedResult,
        NodeId = u64,
        Node = Peer,
);

/// An immutable snapshot file. Keeping this handle pins its bytes during transfer.
/// Export/install use bounded record streams, independently of the table size.
#[derive(Clone, Debug)]
pub struct SnapshotFile {
    pub file: Arc<File>,
    pub path: PathBuf,
    pub bytes: u64,
}
impl SnapshotFile {
    pub fn open(path: PathBuf) -> io::Result<Self> {
        let file = File::open(&path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot must be a regular file",
            ));
        }
        Ok(Self {
            file: Arc::new(file),
            path,
            bytes: metadata.len(),
        })
    }
}

#[cfg(test)]
mod wire_tests {
    use super::*;
    use crate::protocol::{Operation, Row, RowPayload, Scalar};

    /// O01/O03: raw scalars must survive both the command and log-entry codecs.
    /// Internal/untagged enum buffering would break RawValue decoding here.
    #[test]
    fn externally_tagged_commands_preserve_all_scalar_wire_types() {
        for value in [0.0, -0.0, 1.5, f64::MAX] {
            let row = Row::from([
                ("id".into(), Scalar::Int64(i64::MIN)),
                ("max".into(), Scalar::Int64(i64::MAX)),
                ("value".into(), Scalar::Float64(value)),
                ("active".into(), Scalar::Bool(true)),
                ("text".into(), Scalar::Utf8("ñ\u{0}\\\"".into())),
            ]);
            let command = ReplicatedCommand::Operation {
                request: Request {
                    request_id: Uuid::new_v4(),
                    operation: Operation::Insert(RowPayload {
                        table_id: Uuid::new_v4(),
                        row: row.clone(),
                    }),
                },
                digest: "a".repeat(64),
                run_id: Uuid::new_v4(),
                term: 1,
                create_table_id: None,
            };
            use openraft::{
                entry::RaftEntry,
                type_config::alias::{EntryOf, LogIdOf, VoteOf},
            };
            let vote = VoteOf::<TypeConfig>::new_committed(1, 1);
            let entry = EntryOf::<TypeConfig>::new_normal(
                LogIdOf::<TypeConfig>::new(*vote.leader_id(), 1),
                command,
            );
            let bytes = serde_json::to_vec(&entry).unwrap();
            let decoded: EntryOf<TypeConfig> = serde_json::from_slice(&bytes).unwrap();
            let openraft::EntryPayload::Normal(ReplicatedCommand::Operation { request, .. }) =
                decoded.payload
            else {
                panic!("operation variant")
            };
            let Operation::Insert(payload) = request.operation else {
                panic!("insert operation")
            };
            assert_eq!(payload.row, row);
            let Scalar::Float64(decoded) = payload.row["value"] else {
                panic!("float")
            };
            assert_eq!(decoded.to_bits(), value.to_bits());
        }
    }
}
