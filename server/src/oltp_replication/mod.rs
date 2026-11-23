//! Durable per-group consensus components: typed Raft groups hosted in shared
//! pools, their multiplexed authenticated transport and the authority services.
pub(crate) mod authority;
pub mod entry;
pub(crate) mod hosted;
pub(crate) mod manager;
pub mod multiplex;
pub mod owned;
mod proof;
pub(crate) mod queue_access;
pub(crate) mod service_rpc;
mod tls;
pub mod typed_log;
pub(crate) mod typed_network;
pub mod types;
pub use proof::ReadySetHasher;
pub use tls::TlsMaterial;

pub(crate) fn raft_config() -> openraft::Config {
    openraft::Config {
        heartbeat_interval: 100,
        election_timeout_min: 200,
        election_timeout_max: 400,
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(hosted::SNAPSHOT_LOGS_SINCE_LAST),
        max_in_snapshot_log_to_keep: hosted::SNAPSHOT_LOGS_TO_KEEP,
        ..Default::default()
    }
}

/// One of three stages by Raft group kind: Data, Timestamps, and the
/// metadata groups (Control, Status, Catalog).
pub(super) fn by_kind(
    kind: &types::GroupKind,
    names: [&'static str; 3],
) -> &'static nabedra_resources::stage_timing::Stage {
    use types::GroupKind;
    nabedra_resources::stage_timing::stage(match kind {
        GroupKind::Data { .. } => names[0],
        GroupKind::Timestamps => names[1],
        _ => names[2],
    })
}

#[cfg(test)]
pub(crate) fn acceptance_clause(scenario: &str, assertion: &str, status: &str, detail: &str) {
    let marker = serde_json::json!({
        "scenario": scenario, "assertion": assertion, "status": status, "detail": detail,
    });
    println!("NABEDRA_OLTP_ASSERT {marker}");
}
