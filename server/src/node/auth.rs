//! Checked wire records and fixed-domain signing keys; neither grants membership.
mod credentials;
mod sequence;
mod verify;
mod wire;

#[allow(unused_imports)] // Content/authority and enrollment callers follow in later minis.
pub(crate) use verify::{EnrollmentProof, SignedRequest, verify_enrollment, verify_request};

pub(crate) use credentials::worker_verifier;
#[allow(unused_imports)] // R03a2b and boot callers pending.
pub(crate) use credentials::{ApiSigningKey, WorkerSigningKey};

pub(crate) use wire::{
    ActualRoute, Auth, Dependency, DependencyCloseReply, Drain, DrainReply, Enrollment,
    EnrollmentUnsigned, Readiness, ReadinessReply, Receiver, ReplyAuth, RequestIds, Role, Route,
    decode_signature,
};

// Pending proof/HTTP callers; remove these allowances as they are consumed.
#[allow(unused_imports)]
pub(crate) use wire::{EnrollmentReply, Registration, Scope, Sender};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Error {
    Invalid,
    Limit,
    Replay,
    Exhausted,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Invalid => "Invalid",
            Self::Limit => "Limit",
            Self::Replay => "Replay",
            Self::Exhausted => "Exhausted",
        })
    }
}
impl std::error::Error for Error {}

#[allow(unused_imports)] // Mini 3/runtime consumers follow.
pub(crate) use verify::{
    CheckedContent, CheckedReplyContent, RequestContext, SignedReply, check_reply_content,
    check_request_content, verify_reply,
};

#[allow(unused_imports)] // Provisional eligibility, final admission is R03a3/R04.
pub(crate) use verify::{
    DependencyBinding, EligibleReply, EligibleRequest, PendingLocalChecks, PendingReplyChecks,
    ReceiverIdentity, check_reply_authority, check_request_authority,
};

#[allow(unused_imports)] // Boot sender owner follows in R03/R04.
pub(crate) use sequence::SenderSequence;

mod replay;

#[allow(unused_imports)] // Pending receiver admission/runtime consumer.
pub(crate) use replay::{ReplayState, ReplayedRequest};
#[allow(unused_imports)] // Pending checked semantic transfer.
pub(crate) use verify::{RequestBinding, RequestBody, RequestHandoff};

#[allow(unused_imports)] // Pending reply continuation/result consumer.
pub(crate) use verify::{ReplyBody, ReplyContextBinding, ReplyHandoff};

#[allow(unused_imports)] // R04c2 retained starts.
pub(crate) use verify::AcceptedRequestAuthority;

pub(crate) use verify::{checked_worker, verified_work};
