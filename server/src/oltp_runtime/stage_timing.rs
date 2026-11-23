pub(crate) use nabedra_resources::{perf_stage, stage_timing::*};

/// One of three stages by Raft group kind: Data, Timestamps, and the
/// metadata groups (Control, Status, Catalog).
pub(crate) fn by_kind(
    kind: &crate::oltp_replication::types::GroupKind,
    names: [&'static str; 3],
) -> &'static Stage {
    use crate::oltp_replication::types::GroupKind;
    stage(match kind {
        GroupKind::Data { .. } => names[0],
        GroupKind::Timestamps => names[1],
        _ => names[2],
    })
}
