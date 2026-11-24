//! Private terminal-copy verification. Only the current leader's own
//! authenticated metadata reads can supply acknowledgements.
use crate::oltp_replication::proof::{ReadySetHasher, hash, member_hash};
use crate::oltp_replication::types::{
    Digest, Invalid, ReadyMembership, TerminalCopyStatement, WorkerIdentity,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

/// No Deserialize implementation: application callers cannot manufacture an
/// authenticated ACK by parsing a matching JSON shape.
pub(crate) struct AuthenticatedAck {
    member: WorkerIdentity,
    statement: Digest,
    nonce: Uuid,
}
impl AuthenticatedAck {
    /// Called only after the transport verifies the TLS peer, its current
    /// enrollment, RPC purpose, statement digest and response nonce.
    pub(in crate::oltp_replication) fn from_transport(
        member: WorkerIdentity,
        statement: Digest,
        nonce: Uuid,
    ) -> Self {
        Self {
            member,
            statement,
            nonce,
        }
    }
}

/// This serialized record goes into the timestamp command only through the
/// private leader service. It cannot authorize a raw worker client_write call.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct SealedTerminalProof {
    pub(super) statement: TerminalCopyStatement,
    pub(super) statement_digest: Digest,
    pub(super) acknowledgement_root: Digest,
    pub(super) acknowledgement_count: u64,
}

/// A local capability, deliberately neither Deserialize nor publicly
/// constructible. The durable wire representation is private to this component.
pub struct VerifiedTerminalCopies {
    proof: SealedTerminalProof,
}
impl VerifiedTerminalCopies {
    pub fn statement(&self) -> &TerminalCopyStatement {
        &self.proof.statement
    }
    pub(super) fn into_sealed(self) -> SealedTerminalProof {
        self.proof
    }
}

/// Consume one authoritative ready worker and its authenticated ACK at a time.
/// The service stream-merges bounded sorted pages and takes a fresh authority
/// read before finish. Apply checks the persisted release guard once more.
pub(crate) struct TerminalVerifier {
    statement: TerminalCopyStatement,
    statement_digest: Digest,
    ready: ReadySetHasher,
    acknowledgement_root: Digest,
}
impl TerminalVerifier {
    pub(crate) fn begin(
        statement: TerminalCopyStatement,
        current: ReadyMembership,
    ) -> Result<Self, Invalid> {
        statement.validate()?;
        if statement.ready != current {
            return Err(Invalid("terminal proof has a stale ready view"));
        }
        let statement_digest = statement_digest(&statement)?;
        Ok(Self {
            statement,
            statement_digest,
            ready: ReadySetHasher::default(),
            acknowledgement_root: hash(&[b"nabedra/terminal-acks/v1"]),
        })
    }
    pub(crate) fn accept(
        &mut self,
        required: WorkerIdentity,
        ack: &AuthenticatedAck,
    ) -> Result<(), Invalid> {
        if self.ready.count() >= self.statement.ready.ready_count
            || ack.member != required
            || ack.statement != self.statement_digest
            || ack.nonce.is_nil()
        {
            return Err(Invalid("terminal ACK does not match its required copy"));
        }
        self.ready.push(required)?;
        self.acknowledgement_root = hash(&[
            b"nabedra/terminal-ack/v1",
            member_hash(
                b"nabedra/ack-worker/v1",
                self.acknowledgement_root,
                required,
            )
            .as_bytes(),
            ack.statement.as_bytes(),
            ack.nonce.as_bytes(),
        ]);
        Ok(())
    }
    pub(crate) fn finish(
        self,
        current: ReadyMembership,
    ) -> Result<VerifiedTerminalCopies, Invalid> {
        if current != self.statement.ready
            || self.ready.count() != current.ready_count
            || self.ready.finish() != current.ready_root
        {
            return Err(Invalid(
                "terminal copies are incomplete or authority changed",
            ));
        }
        Ok(VerifiedTerminalCopies {
            proof: SealedTerminalProof {
                statement: self.statement,
                statement_digest: self.statement_digest,
                acknowledgement_root: self.acknowledgement_root,
                acknowledgement_count: self.ready.count(),
            },
        })
    }
}

/// Bounded metadata is hashed directly into SHA-256, without growing a JSON
/// Vec. The domain separates this statement from every other proof purpose.
pub(crate) fn statement_digest(statement: &TerminalCopyStatement) -> Result<Digest, Invalid> {
    struct Writer(Sha256);
    impl std::io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer(Sha256::new());
    writer.0.update(b"nabedra/terminal-copy-statement/v1\0");
    serde_json::to_writer(&mut writer, statement)
        .map_err(|_| Invalid("invalid terminal statement"))?;
    Ok(Digest::from_bytes(writer.0.finalize().into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oltp_replication::types::{
        AuthorityView, PinIdentity, RequestBinding, StoragePin, Timestamp,
    };

    fn fixture(count: u128) -> (TerminalCopyStatement, Vec<WorkerIdentity>) {
        let run_id = nabedra_rt::ids::uuid_v4();
        let request_id = nabedra_rt::ids::uuid_v4();
        let digest = Digest::from_bytes([1; 32]);
        let members: Vec<_> = (1..=count)
            .map(|id| WorkerIdentity {
                worker_id: Uuid::from_u128(id),
                incarnation: nabedra_rt::ids::uuid_v4(),
                certificate_sha256: Digest::from_bytes([id as u8; 32]),
                enrollment_revision: 1,
            })
            .collect();
        let mut ready = ReadySetHasher::default();
        for member in &members {
            ready.push(*member).unwrap();
        }
        let statement = TerminalCopyStatement {
            ready: ReadyMembership {
                view: AuthorityView {
                    run_id,
                    owner_term: 1,
                    membership_revision: 1,
                },
                ready_root: ready.finish(),
                ready_count: count as u64,
            },
            request: RequestBinding {
                run_id,
                request_id,
                digest,
                accepted_context: Digest::from_bytes([2; 32]),
            },
            terminal_version: 7,
            response_hash: Digest::from_bytes([3; 32]),
            terminal_digest: Some(Digest::from_bytes([4; 32])),
            pin_identity: PinIdentity {
                run_id,
                request_id,
                digest,
                nonce: nabedra_rt::ids::uuid_v4(),
            },
            storage_pin: StoragePin {
                pin_id: nabedra_rt::ids::uuid_v4(),
                read_ts: Timestamp::new(10).unwrap(),
                allocator_epoch: 1,
            },
        };
        (statement, members)
    }
    fn ack(member: WorkerIdentity, statement: &TerminalCopyStatement) -> AuthenticatedAck {
        // Model only: real ACK provenance is supplied by the mTLS callback.
        AuthenticatedAck::from_transport(
            member,
            statement_digest(statement).unwrap(),
            nabedra_rt::ids::uuid_v4(),
        )
    }

    #[test]
    fn q03_q06_streamed_proof_requires_every_sorted_current_holder() {
        let (statement, members) = fixture(300);
        let mut verifier = TerminalVerifier::begin(statement.clone(), statement.ready).unwrap();
        for page in members.chunks(128) {
            for member in page {
                verifier.accept(*member, &ack(*member, &statement)).unwrap();
            }
        }
        let verified = verifier.finish(statement.ready).unwrap();
        assert_eq!(verified.statement(), &statement);
        let proof = verified.into_sealed();
        assert_eq!(proof.acknowledgement_count, 300);
        assert_eq!(proof.statement, statement);
        let mut missing = TerminalVerifier::begin(statement.clone(), statement.ready).unwrap();
        for member in members.iter().skip(1) {
            missing.accept(*member, &ack(*member, &statement)).unwrap();
        }
        assert!(missing.finish(statement.ready).is_err());
        let mut duplicate = TerminalVerifier::begin(statement.clone(), statement.ready).unwrap();
        duplicate
            .accept(members[0], &ack(members[0], &statement))
            .unwrap();
        assert!(
            duplicate
                .accept(members[0], &ack(members[0], &statement))
                .is_err()
        );
    }

    #[test]
    fn r05_u02_stale_enrollment_context_terminal_and_view_cannot_seal() {
        let (statement, members) = fixture(1);
        for field in 0..4 {
            let mut supplied = ack(members[0], &statement);
            match field {
                0 => supplied.member.incarnation = nabedra_rt::ids::uuid_v4(),
                1 => supplied.member.enrollment_revision += 1,
                2 => supplied.member.certificate_sha256 = Digest::from_bytes([0; 32]),
                _ => supplied.nonce = Uuid::nil(),
            }
            let mut verifier = TerminalVerifier::begin(statement.clone(), statement.ready).unwrap();
            assert!(verifier.accept(members[0], &supplied).is_err());
        }
        for field in 0..4 {
            let mut altered = statement.clone();
            match field {
                0 => altered.request.accepted_context = Digest::from_bytes([9; 32]),
                1 => altered.terminal_version += 1,
                2 => altered.response_hash = Digest::from_bytes([9; 32]),
                _ => altered.storage_pin.pin_id = nabedra_rt::ids::uuid_v4(),
            }
            let mut verifier = TerminalVerifier::begin(altered.clone(), altered.ready).unwrap();
            assert!(
                verifier
                    .accept(members[0], &ack(members[0], &statement))
                    .is_err()
            );
        }
        let mut verifier = TerminalVerifier::begin(statement.clone(), statement.ready).unwrap();
        verifier
            .accept(members[0], &ack(members[0], &statement))
            .unwrap();
        let mut changed = statement.ready;
        changed.view.membership_revision += 1;
        assert!(verifier.finish(changed).is_err());
    }
}
