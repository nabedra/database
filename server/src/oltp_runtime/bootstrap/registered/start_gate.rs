//! RANGES-0 and RANGES-0b: a registered worker asks the other Control voters
//! about its identity before any of its system groups opens.
//!
//! A worker in its first bootstrap opens its system groups with `pool.group`,
//! which creates them, because it has no retained identity file. The same
//! start on an emptied or replaced volume therefore rejoined every system
//! group under its old replica identities with no vote and no log, and could
//! vote again in a term and help elect a leader that lacks a committed entry
//! (record `2026-11-25-oltp-sim-emptied-volume-votes`). Nothing local tells
//! the two apart: the volume marker can survive. Control can: an enrollment
//! of this worker id means it took part before, so its pool was lost.
use super::*;
use crate::oltp_replication::manager::system_recovery::StartAnswer;

/// How a start proceeds after the other Control voters answered.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Gate {
    Open,
    Refuse(&'static str),
    Wait,
}

/// A first bootstrap on a pool this process created. Opens only when the
/// Control leader's committed state does not know the worker id, or when every
/// other Control voter is in its own first bootstrap with an untouched Control
/// replica, so that no Control entry can have committed.
pub(super) fn first_bootstrap(local: Uuid, answers: &[Option<StartAnswer>]) -> Gate {
    let mut fresh = !answers.is_empty();
    for answer in answers {
        match answer {
            Some(StartAnswer::Revoked) => return Gate::Refuse(REVOKED),
            Some(StartAnswer::Enrolled(Some(identity))) if identity.worker_id == local => {
                return Gate::Refuse(LOST);
            }
            Some(StartAnswer::Enrolled(None)) => return Gate::Open,
            Some(StartAnswer::Fresh) => {}
            Some(StartAnswer::Enrolled(Some(_)) | StartAnswer::Starting) | None => fresh = false,
        }
    }
    if fresh { Gate::Open } else { Gate::Wait }
}

/// A retained restart. Its pool is the retained one; only a revocation
/// refuses it here (Control checks the incarnation when it enrolls).
pub(super) fn retained(answers: &[Option<StartAnswer>]) -> Gate {
    if answers.iter().any(|answer| answer == &Some(StartAnswer::Revoked)) {
        return Gate::Refuse(REVOKED);
    }
    if answers
        .iter()
        .any(|answer| matches!(answer, Some(StartAnswer::Enrolled(_))))
    {
        Gate::Open
    } else {
        Gate::Wait
    }
}

const LOST: &str = "Control already enrolled this worker id, so its pool was lost or replaced: \
     replace the worker instead of starting it on a new pool under its old identity";
const REVOKED: &str =
    "a replacement revoked this worker identity and its pool: start the replacement instead";

/// How long a retained restart waits for Control before it opens anyway:
/// its own Control replica may be needed for the Control quorum.
const RETAINED_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

/// Asks every other Control voter until the gate opens or refuses. A
/// first bootstrap waits until `deadline`; a retained restart for at most
/// [`RETAINED_WAIT`].
pub(super) async fn wait(
    manager: &HostedGroupManager,
    control: &GroupIdentity,
    local: WorkerIdentity,
    peers: &[WorkerEnrollment],
    first: bool,
    deadline: nabedra_rt::time::Instant,
) -> Result<(), ProtocolError> {
    let peers: Vec<WorkerIdentity> = peers
        .iter()
        .map(WorkerIdentity::from)
        .filter(|peer| peer.worker_id != local.worker_id)
        .collect();
    let retained_until = nabedra_rt::time::now() + RETAINED_WAIT;
    loop {
        let mut answers = Vec::with_capacity(peers.len());
        for peer in &peers {
            let answer = nabedra_rt::time::timeout(
                std::time::Duration::from_secs(2),
                manager.start_check(control, *peer),
            )
            .await
            .ok()
            .and_then(Result::ok);
            answers.push(answer);
        }
        let gate = if first {
            first_bootstrap(local.worker_id, &answers)
        } else {
            retained(&answers)
        };
        tracing::debug!(?gate, ?answers, first, "registered start gate");
        match gate {
            Gate::Open => return Ok(()),
            Gate::Refuse(message) => {
                eprintln!("nabedra-db: registered start refused: {message}");
                return Err(ProtocolError::new(
                    ErrorCode::Conflict,
                    message,
                    Outcome::NotApplied,
                ));
            }
            Gate::Wait if !first && nabedra_rt::time::now() >= retained_until => return Ok(()),
            Gate::Wait if nabedra_rt::time::now() >= deadline => return Err(unavailable()),
            Gate::Wait => nabedra_rt::time::sleep(std::time::Duration::from_millis(200)).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(worker: Uuid) -> WorkerIdentity {
        WorkerIdentity {
            worker_id: worker,
            incarnation: Uuid::from_u128(7),
            enrollment_revision: 1,
            certificate_sha256: crate::oltp_replication::types::Digest::parse(
                "0000000000000000000000000000000000000000000000000000000000000001",
            )
            .unwrap(),
        }
    }

    /// O17 (RANGES-0b): an enrolled worker id on a new pool is refused, a
    /// revoked one too; Control's "not enrolled" or an untouched cluster
    /// opens; anything else waits.
    #[test]
    fn o17_ranges0b_first_bootstrap_gate() {
        let local = Uuid::from_u128(1);
        let other = Uuid::from_u128(2);
        use StartAnswer::*;
        assert_eq!(
            first_bootstrap(local, &[Some(Starting), Some(Enrolled(Some(identity(local))))]),
            Gate::Refuse(LOST)
        );
        assert_eq!(first_bootstrap(local, &[None, Some(Revoked)]), Gate::Refuse(REVOKED));
        assert_eq!(first_bootstrap(local, &[None, Some(Enrolled(None))]), Gate::Open);
        // A record of another worker id is not an answer about this one.
        assert_eq!(
            first_bootstrap(local, &[Some(Enrolled(Some(identity(other)))), None]),
            Gate::Wait
        );
        assert_eq!(first_bootstrap(local, &[Some(Fresh), Some(Fresh)]), Gate::Open);
        // One voter that cannot answer, or holds state it cannot read yet.
        assert_eq!(first_bootstrap(local, &[Some(Fresh), None]), Gate::Wait);
        assert_eq!(first_bootstrap(local, &[Some(Fresh), Some(Starting)]), Gate::Wait);
        assert_eq!(first_bootstrap(local, &[]), Gate::Wait);
        assert_eq!(retained(&[Some(Starting), Some(Revoked)]), Gate::Refuse(REVOKED));
        assert_eq!(retained(&[None, Some(Enrolled(Some(identity(local))))]), Gate::Open);
        assert_eq!(retained(&[Some(Fresh), None]), Gate::Wait);
    }
}
