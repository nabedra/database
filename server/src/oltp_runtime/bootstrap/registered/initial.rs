use super::{Applied, ControlReply, Mutex, ProtocolError, Uuid, accepted, startup_read};
use std::future::Future;
use tokio::time::Instant;

/// The first Control read can succeed while a later proposal crosses an
/// election. Reuse the command's operation ID so a lost reply also replays
/// the original Control decision.
pub(super) async fn propose<F, Fut>(
    deadline: Instant,
    last_error: &Mutex<Option<ProtocolError>>,
    stage: &'static str,
    operation: Uuid,
    mut proposal: F,
) -> Result<(), ProtocolError>
where
    F: FnMut(Uuid) -> Fut,
    Fut: Future<Output = Result<Applied<ControlReply>, ProtocolError>>,
{
    let applied = startup_read(deadline, last_error, stage, || proposal(operation)).await?;
    accepted(applied.reply)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::WorkerResources,
        oltp::pool::{ByteClass, SharedResourceConfig, WorkerSharedResources},
        oltp_replication::{
            authority::consensus::owned_applied,
            types::{AppliedPosition, Digest, WorkerIdentity},
        },
        protocol::{ErrorCode, Outcome},
    };
    use std::{future::ready, sync::Arc, time::Duration};

    #[tokio::test(start_paused = true)]
    async fn o16_initial_control_proposal_retries_preappend_refusal_with_same_id() {
        let worker = WorkerIdentity {
            worker_id: Uuid::from_u128(1),
            incarnation: Uuid::from_u128(2),
            certificate_sha256: Digest::from_bytes([3; 32]),
            enrollment_revision: 1,
        };
        let resource = WorkerSharedResources::new(
            1,
            WorkerResources::default(),
            SharedResourceConfig::default(),
        )
        .unwrap();
        let applied = owned_applied(
            AppliedPosition {
                term: 2,
                node_id: 1,
                index: 1,
            },
            ControlReply::Enrolled(worker),
            Arc::new(resource.reserve_scratch(ByteClass::Progress, 4096).unwrap()),
        );
        let refusal = ProtocolError::new(
            ErrorCode::StorageUnavailable,
            "typed Raft leader refused the proposal before append",
            Outcome::NotApplied,
        );
        let operation = Uuid::from_u128(4);
        let last_error = Mutex::new(None);
        let mut attempted = Vec::new();

        propose(
            nabedra_rt::time::now() + Duration::from_secs(1),
            &last_error,
            "enroll initial worker",
            operation,
            |id| {
                attempted.push(id);
                ready(if attempted.len() == 1 {
                    Err(refusal.clone())
                } else {
                    Ok(applied.clone())
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(attempted, [operation, operation]);
        let error = last_error.into_inner().unwrap().unwrap();
        assert_eq!((error.code, error.outcome), (refusal.code, refusal.outcome));
    }
}
