use super::status::{StatusCommand, StatusReply};
use super::{
    consensus::Applied,
    control::{ControlCommand, ControlReply},
    timestamp::{TimestampCommand, TimestampReply},
};
use crate::oltp::mvcc::store::{ParticipantCommand, ParticipantReply};
use crate::oltp_replication::{
    entry::OwnedRaftEntry, owned::OwnedCommand, typed_log::TypedConfig, types::ReplicaMember,
};

/// The openraft runtime of the five OLTP groups below.
///
/// Product builds name the type `declare_raft_types!` picks when `AsyncRuntime` is left
/// out (`openraft::impls::TokioRuntime`, a re-export of `openraft_rt_tokio::TokioRuntime`),
/// so product types and generated code do not change (check `rt-zero-cost`). openraft
/// 0.10 deprecates the path in favour of the `openraft-rt-tokio` crate, which the server
/// does not depend on directly.
#[cfg(not(feature = "sim"))]
#[allow(deprecated)]
pub(crate) type OltpRaftRuntime = openraft::impls::TokioRuntime;
/// The openraft runtime of the five OLTP groups below, on the simulated node context:
/// the node clock for `C::now()` and timers, the node task registry, and election
/// timeouts seeded per node.
#[cfg(feature = "sim")]
pub(crate) type OltpRaftRuntime = nabedra_rt::sim::openraft10::SimRuntime;

openraft::declare_raft_types!(
    pub(crate) ControlConfig:
        D=OwnedCommand<ControlCommand>,R=Applied<Option<ControlReply>>,
        NodeId=u64,Node=ReplicaMember,Entry=OwnedRaftEntry<ControlCommand>,
        AsyncRuntime=OltpRaftRuntime,
);
impl TypedConfig for ControlConfig {
    fn accepts(kind: &crate::oltp_replication::types::GroupKind) -> bool {
        matches!(kind, crate::oltp_replication::types::GroupKind::Control)
    }
    type Command = ControlCommand;
    type Reply = ControlReply;
}

openraft::declare_raft_types!(
    pub(crate) TimestampConfig:
        D=OwnedCommand<TimestampCommand>,R=Applied<Option<TimestampReply>>,
        NodeId=u64,Node=ReplicaMember,Entry=OwnedRaftEntry<TimestampCommand>,
        AsyncRuntime=OltpRaftRuntime,
);
impl TypedConfig for TimestampConfig {
    fn accepts(kind: &crate::oltp_replication::types::GroupKind) -> bool {
        matches!(kind, crate::oltp_replication::types::GroupKind::Timestamps)
    }
    type Command = TimestampCommand;
    type Reply = TimestampReply;
}
openraft::declare_raft_types!(
    pub(crate) StatusConfig:
        D=OwnedCommand<StatusCommand>,R=Applied<Option<StatusReply>>,
        NodeId=u64,Node=ReplicaMember,Entry=OwnedRaftEntry<StatusCommand>,
        AsyncRuntime=OltpRaftRuntime,
);
impl TypedConfig for StatusConfig {
    const MAX_REPLY_DECODE_BYTES: usize = 512 << 10;
    type Command = StatusCommand;
    type Reply = StatusReply;
    fn accepts(kind: &crate::oltp_replication::types::GroupKind) -> bool {
        matches!(
            kind,
            crate::oltp_replication::types::GroupKind::Status { .. }
        )
    }
}
openraft::declare_raft_types!(
    pub(crate) DataConfig:
        D=OwnedCommand<ParticipantCommand>,R=Applied<Option<ParticipantReply>>,
        NodeId=u64,Node=ReplicaMember,Entry=OwnedRaftEntry<ParticipantCommand>,
        AsyncRuntime=OltpRaftRuntime,
);
impl TypedConfig for DataConfig {
    type Command = ParticipantCommand;
    type Reply = ParticipantReply;
    fn accepts(kind: &crate::oltp_replication::types::GroupKind) -> bool {
        matches!(kind, crate::oltp_replication::types::GroupKind::Data { .. })
    }
    fn requires_index_reclamation(command: &ParticipantCommand) -> bool {
        matches!(command, ParticipantCommand::ReclaimIndex(_))
    }
}
openraft::declare_raft_types!(
    pub(crate) CatalogConfig:
        D=OwnedCommand<ParticipantCommand>,R=Applied<Option<ParticipantReply>>,
        NodeId=u64,Node=ReplicaMember,Entry=OwnedRaftEntry<ParticipantCommand>,
        AsyncRuntime=OltpRaftRuntime,
);
impl TypedConfig for CatalogConfig {
    type Command = ParticipantCommand;
    type Reply = ParticipantReply;
    fn accepts(kind: &crate::oltp_replication::types::GroupKind) -> bool {
        matches!(kind, crate::oltp_replication::types::GroupKind::Catalog)
    }
}
