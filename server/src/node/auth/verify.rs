//! Signature possession only; content, authority and replay remain caller obligations.
use super::{ActualRoute, Auth, Enrollment, Error, decode_signature, worker_verifier};
use ring::signature::{ED25519, UnparsedPublicKey};
use std::sync::Arc;
#[allow(dead_code)] // Enrollment/content/authority callers follow in later minis.
pub(crate) struct EnrollmentProof {
    enrollment: Enrollment,
    public_key: [u8; 32],
    verifier: [u8; 32],
}
#[allow(dead_code)] // Enrollment/content/authority callers follow in later minis.
impl EnrollmentProof {
    pub(crate) fn claims(&self) -> &Enrollment {
        &self.enrollment
    }
    pub(crate) fn public_key(&self) -> [u8; 32] {
        self.public_key
    }
    pub(crate) fn verifier(&self) -> [u8; 32] {
        self.verifier
    }
}
#[allow(dead_code)] // Enrollment/content/authority callers follow in later minis.
struct RequestAssociation {
    auth: Auth,
    canonical: Box<[u8]>,
}
#[allow(dead_code)] // Enrollment/content/authority callers follow in later minis.
pub(crate) struct SignedRequest {
    request: Arc<RequestAssociation>,
}
#[allow(dead_code)] // Enrollment/content/authority callers follow in later minis.
impl SignedRequest {
    pub(crate) fn auth(&self) -> &Auth {
        &self.request.auth
    }
}
impl std::fmt::Debug for EnrollmentProof {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EnrollmentProof")
    }
}
impl std::fmt::Debug for SignedRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SignedRequest")
    }
}
#[allow(dead_code)] // Enrollment/content/authority callers follow in later minis.
pub(crate) fn verify_enrollment(bytes: &[u8]) -> Result<EnrollmentProof, Error> {
    let enrollment = Enrollment::decode(bytes)?;
    let unsigned = enrollment.unsigned_bytes()?;
    let public_key = enrollment.public_key()?;
    verify(
        public_key,
        enrollment.proof()?,
        Domain::Enrollment,
        &unsigned,
    )?;
    let verifier = worker_verifier(
        enrollment.cluster_id(),
        enrollment.node_id(),
        enrollment.incarnation(),
        enrollment.session_id(),
        &public_key,
    );
    Ok(EnrollmentProof {
        enrollment,
        public_key,
        verifier,
    })
}
#[allow(dead_code)] // Enrollment/content/authority callers follow in later minis.
pub(crate) fn verify_request(
    auth_hex: &str,
    signature_hex: &str,
    actual: &ActualRoute,
) -> Result<SignedRequest, Error> {
    let auth = Auth::decode(auth_hex, actual)?;
    let signature = decode_signature(signature_hex)?;
    let canonical = auth.canonical_bytes()?;
    verify(
        auth.sender().public_key()?,
        signature,
        Domain::Request,
        &canonical,
    )?;
    Ok(SignedRequest {
        request: Arc::new(RequestAssociation {
            auth,
            canonical: canonical.into_boxed_slice(),
        }),
    })
}
// Callers cannot supply domain text. Check bounds before prefix+message allocation.
#[derive(Clone, Copy)]
enum Domain {
    Enrollment,
    Request,
}
fn verify(public: [u8; 32], signature: [u8; 64], domain: Domain, body: &[u8]) -> Result<(), Error> {
    let (prefix, cap): (&[u8], usize) = match domain {
        Domain::Enrollment => (b"meshdb.enroll.v1\0", 16 * 1024),
        Domain::Request => (b"meshdb.http-request.v1\0", 4096),
    };
    if body.len() > cap {
        return Err(Error::Limit);
    }
    let mut input = Vec::with_capacity(prefix.len() + body.len());
    input.extend_from_slice(prefix);
    input.extend_from_slice(body);
    UnparsedPublicKey::new(&ED25519, public)
        .verify(&input, &signature)
        .map_err(|_| Error::Invalid)
}
#[cfg(test)]
mod tests;

mod content;

use super::{
    Dependency, DependencyCloseReply, Drain, DrainReply, Readiness, ReadinessReply, Receiver,
    ReplyAuth,
};
use crate::{
    cluster::controller::OwnershipObservation,
    node::job::{JobEnvelope, ResultEnvelope},
    protocol::{ProtocolError, Request},
};
use serde_json::value::RawValue;

#[allow(dead_code)] // Mini 3 consumes retained association/observation.
enum ExpectedContent {
    Operation(Box<Request>),
    Job(Box<JobEnvelope>),
    Readiness(Readiness),
    Open(Dependency),
    Close(Dependency),
    Drain(Drain),
}
#[allow(dead_code)] // Mini 3 rechecks this exact binding against current authority.
struct TargetBinding {
    receiver: Receiver,
    verifier: [u8; 32],
    observation: OwnershipObservation,
}
struct ReplyStatuses;
pub(crate) struct RequestContext {
    request: Arc<RequestAssociation>,
    initial: Box<[u8]>,
    expected: Arc<ExpectedContent>,
    target: Arc<TargetBinding>,
    statuses: ReplyStatuses,
}
struct ReplyAssociation {
    request: Arc<RequestAssociation>,
    reply: ReplyAuth,
    #[allow(dead_code)] // Retained signed bytes; pending transport consumer.
    canonical: Box<[u8]>,
    expected: Arc<ExpectedContent>,
    target: Arc<TargetBinding>,
}
#[allow(dead_code)] // Mini 3 and runtime consumers follow.
pub(crate) struct SignedReply {
    reply: Arc<ReplyAssociation>,
}
#[allow(dead_code)] // Mini 3 combines content with eligibility by pointer identity.
pub(crate) struct CheckedContent {
    request: Arc<RequestAssociation>,
    bytes: Box<[u8]>,
    parsed: ExpectedContent,
}
enum ReplyContent {
    Operation(Box<RawValue>),
    Job(Box<ResultEnvelope>),
    Readiness(ReadinessReply),
    Open(Dependency),
    Close(DependencyCloseReply),
    Drain(DrainReply),
    Rejection(ProtocolError),
    UnattributedRejection(ProtocolError),
}
pub(crate) struct CheckedReplyContent {
    reply: Arc<ReplyAssociation>,
    bytes: Box<[u8]>,
    parsed: ReplyContent,
}
macro_rules! opaque_debug { ($($t:ty),*) => {$(impl std::fmt::Debug for $t {
 fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(stringify!($t)) }
})*}; }
opaque_debug!(
    RequestContext,
    SignedReply,
    CheckedContent,
    CheckedReplyContent
);

fn verify_response(public: [u8; 32], signature: [u8; 64], body: &[u8]) -> Result<(), Error> {
    if body.len() > 4096 {
        return Err(Error::Limit);
    }
    let mut input = Vec::with_capacity(24 + body.len());
    input.extend_from_slice(b"meshdb.http-response.v1\0");
    input.extend_from_slice(body);
    UnparsedPublicKey::new(&ED25519, public)
        .verify(&input, &signature)
        .map_err(|_| Error::Invalid)
}

pub(crate) use content::{check_reply_content, check_request_content, verify_reply};

mod eligibility;
#[allow(unused_imports)]
pub(crate) use eligibility::{check_reply_authority, check_request_authority};
pub(crate) use eligibility::{work as verified_work, worker as checked_worker};

// Provisional authority only. Final admission consumes matching content and
// rechecks complete-root ordering, local boot, replay and current local state.
#[allow(dead_code)]
pub(crate) struct ReceiverIdentity {
    cluster: uuid::Uuid,
    node: uuid::Uuid,
    incarnation: uuid::Uuid,
    session: uuid::Uuid,
    public_key: [u8; 32],
}
#[allow(dead_code)]
pub(crate) struct EligibleRequest {
    request: Arc<RequestAssociation>,
    observation: OwnershipObservation,
    local: ReceiverIdentity,
    pending: PendingLocalChecks,
}
pub(crate) struct EligibleReply {
    reply: Arc<ReplyAssociation>,
    observation: OwnershipObservation,
    pending: PendingReplyChecks,
}
/// Exact coordinator and operation binding; permit IDs in small requests are
/// read from associated CheckedContent during final combination.
#[allow(dead_code)]
pub(crate) struct DependencyBinding {
    coordinator: super::Sender,
    operation_id: uuid::Uuid,
    admission_id: uuid::Uuid,
    request_digest: [u8; 32],
}
#[allow(dead_code)]
pub(crate) enum PendingLocalChecks {
    /// Open admission barrier and operation capacity remain required.
    NewOperation {
        operation_id: uuid::Uuid,
        request_digest: [u8; 32],
    },
    /// Same initialized live boot and exact checked challenge remain required.
    Probe,
    /// Open barrier, live retained operation, exact open retry or room. Never reopen closing.
    Open { binding: DependencyBinding },
    /// Exact existing pre-barrier open permit plus partition capacity. No operation slot.
    Job {
        binding: DependencyBinding,
        permit_id: uuid::Uuid,
        job_id: uuid::Uuid,
        attempt_id: uuid::Uuid,
    },
    /// Match retained binding if present; absence can only observe closed:true.
    Close { binding: DependencyBinding },
    /// Compare checked content drain ID AND current local barrier/drain identity.
    Drain { drain_id: uuid::Uuid },
}
#[allow(dead_code)]
pub(crate) enum PendingReplyChecks {
    /// Requires retained accepted work, even when target is now draining.
    AcceptedOperation {
        operation_id: uuid::Uuid,
        request_digest: [u8; 32],
    },
    AcceptedJob {
        binding: DependencyBinding,
        permit_id: uuid::Uuid,
        job_id: uuid::Uuid,
        attempt_id: uuid::Uuid,
    },
    Probe {
        challenge: uuid::Uuid,
    },
    Open {
        binding: DependencyBinding,
        permit_id: uuid::Uuid,
    },
    Close {
        binding: DependencyBinding,
        permit_id: uuid::Uuid,
    },
    Drain {
        drain_id: uuid::Uuid,
    },
}
opaque_debug!(
    ReceiverIdentity,
    EligibleRequest,
    EligibleReply,
    DependencyBinding,
    PendingLocalChecks,
    PendingReplyChecks
);

#[cfg(test)]
pub(super) mod consume_tests;

/// Association and pure cross-proof claims only, never final admission.
pub(super) struct CombinedRequest {
    eligible: EligibleRequest,
    content: CheckedContent,
}
pub(super) fn combine_request(
    eligible: EligibleRequest,
    content: CheckedContent,
) -> Result<CombinedRequest, Error> {
    use super::Route;
    if !Arc::ptr_eq(&eligible.request, &content.request) {
        return Err(Error::Invalid);
    }
    let matches = match (eligible.auth().route(), &eligible.pending, &content.parsed) {
        (
            Route::Operations,
            PendingLocalChecks::NewOperation { .. },
            ExpectedContent::Operation(_),
        )
        | (Route::Jobs, PendingLocalChecks::Job { .. }, ExpectedContent::Job(_))
        | (Route::Readiness, PendingLocalChecks::Probe, ExpectedContent::Readiness(_))
        | (Route::DependencyOpen, PendingLocalChecks::Open { .. }, ExpectedContent::Open(_))
        | (Route::DependencyClose, PendingLocalChecks::Close { .. }, ExpectedContent::Close(_)) => {
            true
        }
        (Route::Drain, PendingLocalChecks::Drain { drain_id }, ExpectedContent::Drain(body)) => {
            *drain_id == body.drain_id()
        }
        _ => false,
    };
    if !matches {
        return Err(Error::Invalid);
    }
    Ok(CombinedRequest { eligible, content })
}
impl CombinedRequest {
    pub(super) fn auth(&self) -> &Auth {
        self.eligible.auth()
    }
    pub(super) fn observation(&self) -> &OwnershipObservation {
        &self.eligible.observation
    }
    pub(super) fn local(&self) -> &ReceiverIdentity {
        &self.eligible.local
    }
    pub(super) fn into_handoff(self) -> RequestHandoff {
        let EligibleRequest {
            request,
            observation,
            local,
            pending,
        } = self.eligible;
        let CheckedContent { bytes, parsed, .. } = self.content;
        let body = match parsed {
            ExpectedContent::Operation(v) => RequestBody::Operation(v),
            ExpectedContent::Job(v) => RequestBody::Job(v),
            ExpectedContent::Readiness(v) => RequestBody::Readiness(v),
            ExpectedContent::Open(v) => RequestBody::Open(v),
            ExpectedContent::Close(v) => RequestBody::Close(v),
            ExpectedContent::Drain(v) => RequestBody::Drain(v),
        };
        RequestHandoff {
            binding: RequestBinding { request },
            bytes,
            body,
            observation,
            local,
            pending,
        }
    }
}
impl ReceiverIdentity {
    pub(super) fn matches_auth(&self, auth: &Auth) -> bool {
        let r = auth.receiver();
        auth.cluster_id() == self.cluster
            && r.node_id() == self.node
            && r.incarnation() == self.incarnation
            && r.session_id() == self.session
    }
}
/// Memory-only checked request. Execution, permits and capacity remain pending.
#[allow(dead_code)] // Pending semantic/runtime consumer.
pub(crate) struct RequestHandoff {
    binding: RequestBinding,
    bytes: Box<[u8]>,
    body: RequestBody,
    observation: OwnershipObservation,
    local: ReceiverIdentity,
    pending: PendingLocalChecks,
}
#[allow(dead_code)] // Pending semantic/runtime consumer.
pub(crate) struct RequestBinding {
    request: Arc<RequestAssociation>,
}
#[allow(dead_code)] // Pending semantic/runtime consumer.
pub(crate) enum RequestBody {
    Operation(Box<Request>),
    Job(Box<JobEnvelope>),
    Readiness(Readiness),
    Open(Dependency),
    Close(Dependency),
    Drain(Drain),
}
impl RequestBinding {
    #[allow(dead_code)] // Pending semantic/runtime consumer.
    pub(crate) fn auth(&self) -> &Auth {
        &self.request.auth
    }
    #[allow(dead_code)] // Pending transport consumer.
    pub(crate) fn canonical_bytes(&self) -> &[u8] {
        &self.request.canonical
    }
}
impl RequestHandoff {
    #[allow(dead_code)] // Pending semantic/runtime consumer.
    pub(crate) fn binding(&self) -> &RequestBinding {
        &self.binding
    }
    #[allow(dead_code)] // Pending checked CONTROL transfer.
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    #[allow(dead_code)] // Pending live admission owner.
    pub(crate) fn pending(&self) -> &PendingLocalChecks {
        &self.pending
    }
    #[allow(dead_code)] // Pending semantic/runtime consumer.
    pub(crate) fn into_parts(
        self,
    ) -> (
        RequestBinding,
        Box<[u8]>,
        RequestBody,
        OwnershipObservation,
        ReceiverIdentity,
        PendingLocalChecks,
    ) {
        (
            self.binding,
            self.bytes,
            self.body,
            self.observation,
            self.local,
            self.pending,
        )
    }
}
opaque_debug!(CombinedRequest, RequestHandoff, RequestBinding, RequestBody);

#[cfg(test)]
mod reply_handoff_tests;

/// Memory-only reply transfer. Live admission and continuation ownership remain pending.
#[allow(dead_code)] // Pending semantic/runtime consumer.
pub(crate) struct ReplyHandoff {
    binding: ReplyContextBinding,
    observation: OwnershipObservation,
    pending: PendingReplyChecks,
    content: CheckedReplyContent,
}
/// Keeps invocation associations without yielding a reusable RequestContext.
#[allow(dead_code)] // Pending semantic/runtime consumer.
pub(crate) struct ReplyContextBinding {
    context: RequestContext,
    reply: Arc<ReplyAssociation>,
}
#[allow(dead_code)] // Pending semantic/runtime consumer.
pub(crate) enum ReplyBody {
    Operation(Box<RawValue>),
    Job(Box<ResultEnvelope>),
    Readiness(ReadinessReply),
    Open(Dependency),
    Close(DependencyCloseReply),
    Drain(DrainReply),
    Rejection(ProtocolError),
    UnattributedRejection(ProtocolError),
}
impl RequestContext {
    /// Consumes this attempt even on failure. The caller must first reserve its
    /// continuation under the actual admission mutex and check fresh root/boot/work.
    /// One shared boot sequence owner must prohibit duplicate outgoing invocations;
    /// Arc identity cannot repair deliberate sender/target/sequence reuse on wire.
    #[allow(dead_code)] // Pending reply admission owner.
    pub(crate) fn into_reply_handoff(
        self,
        eligible: EligibleReply,
        content: CheckedReplyContent,
        current: &OwnershipObservation,
    ) -> Result<ReplyHandoff, Error> {
        if !Arc::ptr_eq(&eligible.reply, &content.reply)
            || !Arc::ptr_eq(&eligible.reply.request, &self.request)
            || !Arc::ptr_eq(&eligible.reply.expected, &self.expected)
            || !Arc::ptr_eq(&eligible.reply.target, &self.target)
            || eligible.observation != *current
        {
            return Err(Error::Invalid);
        }
        let EligibleReply {
            reply,
            observation,
            pending,
        } = eligible;
        Ok(ReplyHandoff {
            binding: ReplyContextBinding {
                context: self,
                reply,
            },
            observation,
            pending,
            content,
        })
    }
}
impl ReplyContextBinding {
    #[allow(dead_code)] // Pending semantic/runtime consumer.
    pub(crate) fn auth(&self) -> &Auth {
        self.context.auth()
    }
    #[allow(dead_code)] // Pending semantic/runtime consumer.
    pub(crate) fn initial_bytes(&self) -> &[u8] {
        self.context.initial_bytes()
    }
    #[allow(dead_code)] // Pending result consumer needs the complete original job.
    pub(crate) fn expected_job(&self) -> Option<&JobEnvelope> {
        match self.context.expected.as_ref() {
            ExpectedContent::Job(v) => Some(v),
            _ => None,
        }
    }
    #[allow(dead_code)] // Pending semantic/runtime consumer.
    pub(crate) fn reply_auth(&self) -> &ReplyAuth {
        &self.reply.reply
    }
}
impl ReplyHandoff {
    #[allow(dead_code)] // Pending semantic/runtime consumer.
    pub(crate) fn context(&self) -> &RequestContext {
        &self.binding.context
    }
    #[allow(dead_code)] // Pending semantic/runtime consumer.
    pub(crate) fn binding(&self) -> &ReplyContextBinding {
        &self.binding
    }
    #[allow(dead_code)] // Pending semantic/runtime consumer.
    pub(crate) fn auth(&self) -> &ReplyAuth {
        self.binding.reply_auth()
    }
    #[allow(dead_code)] // Pending semantic/runtime consumer.
    pub(crate) fn status(&self) -> u16 {
        self.auth().status()
    }
    #[allow(dead_code)] // Pending checked CONTROL transfer.
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.content.bytes
    }
    #[allow(dead_code)] // Pending live admission owner.
    pub(crate) fn pending(&self) -> &PendingReplyChecks {
        &self.pending
    }
    #[allow(dead_code)] // Pending live admission owner.
    pub(crate) fn observation(&self) -> &OwnershipObservation {
        &self.observation
    }
    #[allow(dead_code)] // Pending semantic/runtime consumer.
    pub(crate) fn operation(&self) -> Option<&RawValue> {
        self.content.operation()
    }
    #[allow(dead_code)] // Pending semantic/runtime consumer.
    pub(crate) fn job(&self) -> Option<&ResultEnvelope> {
        self.content.job()
    }
    #[allow(dead_code)] // Pending semantic/runtime consumer.
    pub(crate) fn readiness(&self) -> Option<&ReadinessReply> {
        self.content.readiness()
    }
    #[allow(dead_code)] // Pending semantic/runtime consumer.
    pub(crate) fn dependency(&self) -> Option<&Dependency> {
        self.content.dependency()
    }
    #[allow(dead_code)] // Pending semantic/runtime consumer.
    pub(crate) fn close(&self) -> Option<&DependencyCloseReply> {
        self.content.close()
    }
    #[allow(dead_code)] // Pending semantic/runtime consumer.
    pub(crate) fn drain(&self) -> Option<&DrainReply> {
        self.content.drain()
    }
    #[allow(dead_code)] // Pending semantic/runtime consumer.
    pub(crate) fn error(&self) -> Option<&ProtocolError> {
        self.content.error()
    }
    #[allow(dead_code)] // Pending semantic/runtime consumer.
    pub(crate) fn is_unattributed_rejection(&self) -> bool {
        self.content.is_unattributed_rejection()
    }
    #[allow(dead_code)] // Pending semantic/runtime consumer.
    pub(crate) fn into_parts(
        self,
    ) -> (
        ReplyContextBinding,
        Box<[u8]>,
        ReplyBody,
        OwnershipObservation,
        PendingReplyChecks,
    ) {
        let body = match self.content.parsed {
            ReplyContent::Operation(v) => ReplyBody::Operation(v),
            ReplyContent::Job(v) => ReplyBody::Job(v),
            ReplyContent::Readiness(v) => ReplyBody::Readiness(v),
            ReplyContent::Open(v) => ReplyBody::Open(v),
            ReplyContent::Close(v) => ReplyBody::Close(v),
            ReplyContent::Drain(v) => ReplyBody::Drain(v),
            ReplyContent::Rejection(v) => ReplyBody::Rejection(v),
            ReplyContent::UnattributedRejection(v) => ReplyBody::UnattributedRejection(v),
        };
        (
            self.binding,
            self.content.bytes,
            body,
            self.observation,
            self.pending,
        )
    }
}
opaque_debug!(ReplyHandoff, ReplyContextBinding, ReplyBody);

/// Fresh accepted-work authority, tied to the retained invocation association.
#[allow(dead_code)] // R04c2 matches this against its retained start subject.
pub(crate) struct AcceptedRequestAuthority {
    request: Arc<RequestAssociation>,
    observation: OwnershipObservation,
    local: ReceiverIdentity,
}
opaque_debug!(AcceptedRequestAuthority);
