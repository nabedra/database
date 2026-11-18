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

openraft::declare_raft_types!(
    pub(crate) ControlConfig:
        D=OwnedCommand<ControlCommand>,R=Applied<Option<ControlReply>>,
        NodeId=u64,Node=ReplicaMember,Entry=OwnedRaftEntry<ControlCommand>,
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
);
impl TypedConfig for DataConfig {
    type Command = ParticipantCommand;
    type Reply = ParticipantReply;
    fn accepts(kind: &crate::oltp_replication::types::GroupKind) -> bool {
        matches!(kind, crate::oltp_replication::types::GroupKind::Data { .. })
    }
}
openraft::declare_raft_types!(
    pub(crate) CatalogConfig:
        D=OwnedCommand<ParticipantCommand>,R=Applied<Option<ParticipantReply>>,
        NodeId=u64,Node=ReplicaMember,Entry=OwnedRaftEntry<ParticipantCommand>,
);
impl TypedConfig for CatalogConfig {
    type Command = ParticipantCommand;
    type Reply = ParticipantReply;
    fn accepts(kind: &crate::oltp_replication::types::GroupKind) -> bool {
        matches!(kind, crate::oltp_replication::types::GroupKind::Catalog)
    }
}
