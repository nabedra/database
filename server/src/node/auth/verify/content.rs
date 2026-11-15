//! Bounded first-body/CONTROL checks. No authority, replay or stream completion.
use super::*;
use crate::{
    cluster::{member::NodeState, state::Control},
    node::auth::{Role, Route},
    node::job::{MAX_CONTROL_BYTES, parse_job, parse_result},
    protocol::{ErrorCode, MAX_BODY_BYTES, Outcome, deserialize_object, parse_request},
};
use ring::digest::{SHA256, digest};
use serde::{Deserialize, Deserializer, de::DeserializeOwned};
use uuid::Uuid;
const SMALL_CAP: usize = 16 * 1024;
fn require(ok: bool) -> Result<(), Error> {
    if ok { Ok(()) } else { Err(Error::Invalid) }
}
fn bound(bytes: &[u8], cap: usize) -> Result<(), Error> {
    if bytes.len() > cap {
        Err(Error::Limit)
    } else {
        Ok(())
    }
}
fn hash(bytes: &[u8]) -> String {
    digest(&SHA256, bytes)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn request_content(auth: &Auth, bytes: &[u8]) -> Result<ExpectedContent, Error> {
    bound(
        bytes,
        match auth.route() {
            Route::Operations => MAX_BODY_BYTES,
            Route::Jobs => MAX_CONTROL_BYTES,
            _ => SMALL_CAP,
        },
    )?;
    let digest = hash(bytes);
    require(auth.control_sha256() == digest)?;
    Ok(match auth.route() {
        Route::Operations => {
            require(auth.request_digest() == Some(digest.as_str()))?;
            let request = parse_request(bytes).map_err(|_| Error::Invalid)?;
            require(Some(request.request_id) == auth.operation_id())?;
            ExpectedContent::Operation(Box::new(request))
        }
        Route::Jobs => {
            let job = parse_job(bytes).map_err(|_| Error::Invalid)?;
            require(
                job.cluster_id == auth.cluster_id()
                    && Some(job.operation_id) == auth.operation_id()
                    && Some(job.job_id) == auth.job_id()
                    && Some(job.attempt_id) == auth.attempt_id()
                    && Some(job.request_digest.as_str()) == auth.request_digest(),
            )?;
            ExpectedContent::Job(Box::new(job))
        }
        Route::Readiness => ExpectedContent::Readiness(Readiness::decode(bytes)?),
        Route::DependencyOpen => ExpectedContent::Open(Dependency::decode(bytes)?),
        Route::DependencyClose => ExpectedContent::Close(Dependency::decode(bytes)?),
        Route::Drain => ExpectedContent::Drain(Drain::decode(bytes)?),
    })
}
#[allow(dead_code)] // Mini 3 combines this with authority.
pub(crate) fn check_request_content(
    signed: &SignedRequest,
    bytes: Vec<u8>,
) -> Result<CheckedContent, Error> {
    let parsed = request_content(signed.auth(), &bytes)?;
    Ok(CheckedContent {
        request: Arc::clone(&signed.request),
        bytes: bytes.into_boxed_slice(),
        parsed,
    })
}
#[allow(dead_code)] // Runtime send and mini 3 follow.
impl RequestContext {
    pub(crate) fn for_send(
        auth: Auth,
        initial: Vec<u8>,
        target_root: &Control,
    ) -> Result<Self, Error> {
        let expected = request_content(&auth, &initial)?;
        let observation = OwnershipObservation::new(target_root).map_err(|_| Error::Invalid)?;
        require(target_root.cluster_id == auth.cluster_id())?;
        let receiver = auth.receiver();
        let node = target_root
            .nodes
            .iter()
            .find(|n| n.node_id == receiver.node_id())
            .ok_or(Error::Invalid)?;
        let session = node.session.as_ref().ok_or(Error::Invalid)?;
        require(
            node.incarnation == Some(receiver.incarnation())
                && session.id == receiver.session_id()
                && !node.retired_incarnations.contains(&receiver.incarnation())
                && !matches!(node.state, NodeState::Lost | NodeState::Stopped),
        )?;
        let verifier = hex32(&session.verifier)?;
        let target = TargetBinding {
            receiver: receiver.clone(),
            verifier,
            observation,
        };
        let canonical = auth.canonical_bytes()?.into_boxed_slice();
        Ok(Self {
            request: Arc::new(RequestAssociation { auth, canonical }),
            initial: initial.into_boxed_slice(),
            expected: Arc::new(expected),
            target: Arc::new(target),
            statuses: ReplyStatuses,
        })
    }
    pub(crate) fn auth(&self) -> &Auth {
        &self.request.auth
    }
    pub(crate) fn initial_bytes(&self) -> &[u8] {
        &self.initial
    }
}
fn hex32(s: &str) -> Result<[u8; 32], Error> {
    require(s.len() == 64)?;
    let mut out = [0; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).map_err(|_| Error::Invalid)?;
    }
    Ok(out)
}
impl ReplyStatuses {
    fn contains(&self, status: u16) -> bool {
        status == 200
            || [
                ErrorCode::ValidationError,
                ErrorCode::Unauthorized,
                ErrorCode::NotFound,
                ErrorCode::Conflict,
                ErrorCode::LimitExceeded,
                ErrorCode::Overloaded,
                ErrorCode::StorageUnavailable,
                ErrorCode::InternalError,
            ]
            .iter()
            .any(|c| c.status_code() == status)
    }
}
#[allow(dead_code)] // Mini 3/current authority and runtime follow.
pub(crate) fn verify_reply(
    auth_hex: &str,
    signature_hex: &str,
    actual_status: u16,
    expected: &RequestContext,
) -> Result<SignedReply, Error> {
    let reply = ReplyAuth::decode(auth_hex)?;
    let canonical = reply.canonical_bytes()?;
    let sender = reply.sender();
    let key = sender.public_key()?;
    verify_response(key, decode_signature(signature_hex)?, &canonical)?;
    let original = expected.auth();
    let target = &expected.target;
    require(
        reply.cluster_id() == original.cluster_id()
            && reply.request_sequence() == original.sequence()
            && reply.scope() == original.scope()
            && reply.receiver() == original.sender()
            && reply.status() == actual_status
            && expected.statuses.contains(actual_status)
            && sender.role() == Role::Worker
            && sender.node_id() == Some(target.receiver.node_id())
            && sender.incarnation() == Some(target.receiver.incarnation())
            && sender.process_id() == target.receiver.incarnation()
            && sender.session_id() == Some(target.receiver.session_id())
            && worker_verifier(
                original.cluster_id(),
                target.receiver.node_id(),
                target.receiver.incarnation(),
                target.receiver.session_id(),
                &key,
            ) == target.verifier,
    )?;
    Ok(SignedReply {
        reply: Arc::new(ReplyAssociation {
            request: Arc::clone(&expected.request),
            reply,
            canonical: canonical.into_boxed_slice(),
            expected: Arc::clone(&expected.expected),
            target: Arc::clone(&expected.target),
        }),
    })
}
#[allow(dead_code)] // Runtime consumers follow.
impl SignedReply {
    pub(crate) fn auth(&self) -> &ReplyAuth {
        &self.reply.reply
    }
}
#[allow(dead_code)] // Mini 3 combines this with authority.
pub(crate) fn check_reply_content(
    signed: &SignedReply,
    bytes: Vec<u8>,
) -> Result<CheckedReplyContent, Error> {
    let association = &signed.reply;
    let expected = association.expected.as_ref();
    let status = signed.auth().status();
    let cap = match expected {
        ExpectedContent::Operation(_) => MAX_BODY_BYTES,
        ExpectedContent::Job(_) if status == 200 => MAX_CONTROL_BYTES,
        _ => SMALL_CAP,
    };
    bound(&bytes, cap)?;
    require(hash(&bytes) == signed.auth().control_sha256())?;
    let parsed = if status != 200 {
        let (id, error) = error_envelope(&bytes, status)?;
        if let (Some(received), Some(operation)) = (id, association.request.auth.operation_id()) {
            require(received == operation)?;
        }
        if let ExpectedContent::Operation(request) = expected {
            match id {
                Some(id) => {
                    require(id == request.request_id)?;
                    ReplyContent::Rejection(error)
                }
                None => ReplyContent::UnattributedRejection(error),
            }
        } else {
            ReplyContent::Rejection(error)
        }
    } else {
        match expected {
            ExpectedContent::Operation(request) => {
                ReplyContent::Operation(operation_success(&bytes, request.request_id)?)
            }
            ExpectedContent::Job(job) => ReplyContent::Job(Box::new(
                parse_result(&bytes, job).map_err(|_| Error::Invalid)?,
            )),
            ExpectedContent::Readiness(request) => {
                let reply = ReadinessReply::decode(&bytes)?;
                require(reply.challenge() == request.challenge())?;
                ReplyContent::Readiness(reply)
            }
            ExpectedContent::Open(request) => {
                let reply = Dependency::decode(&bytes)?;
                require(reply.permit_id() == request.permit_id())?;
                ReplyContent::Open(reply)
            }
            ExpectedContent::Close(request) => {
                let reply = DependencyCloseReply::decode(&bytes)?;
                require(reply.permit_id() == request.permit_id())?;
                ReplyContent::Close(reply)
            }
            ExpectedContent::Drain(request) => {
                let reply = DrainReply::decode(&bytes)?;
                require(reply.drain_id() == request.drain_id())?;
                ReplyContent::Drain(reply)
            }
        }
    };
    Ok(CheckedReplyContent {
        reply: Arc::clone(association),
        bytes: bytes.into_boxed_slice(),
        parsed,
    })
}
// RawValue skips serde_json's recursive visitor. Preserve the default nesting
// bound with a lexical preflight, without interpreting opaque success data.
fn envelope<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    bound(bytes, MAX_BODY_BYTES)?;
    let (mut depth, mut string, mut escape) = (0u32, false, false);
    for &b in bytes {
        if string {
            if escape {
                escape = false;
            } else if b == b'\\' {
                escape = true;
            } else if b == b'"' {
                string = false;
            }
        } else {
            match b {
                b'"' => string = true,
                b'{' | b'[' => {
                    depth += 1;
                    if depth >= 128 {
                        return Err(Error::Limit);
                    }
                }
                b'}' | b']' => depth = depth.checked_sub(1).ok_or(Error::Invalid)?,
                _ => {}
            }
        }
    }
    let mut d = serde_json::Deserializer::from_slice(bytes);
    let result = deserialize_object(&mut d).map_err(|_| Error::Invalid)?;
    d.end().map_err(|_| Error::Invalid)?;
    Ok(result)
}
fn operation_success(bytes: &[u8], id: Uuid) -> Result<Box<RawValue>, Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Success {
        request_id: Uuid,
        data: Box<RawValue>,
    }
    let success: Success = envelope(bytes)?;
    require(success.request_id == id)?;
    Ok(success.data)
}
fn nullable_id<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Uuid>, D::Error> {
    Option::deserialize(d)
}
fn string_enum<'de, D: Deserializer<'de>, T: DeserializeOwned>(d: D) -> Result<T, D::Error> {
    let text = String::deserialize(d)?;
    T::deserialize(serde::de::value::StringDeserializer::<D::Error>::new(text))
}
fn remote_error<'de, D: Deserializer<'de>>(d: D) -> Result<ProtocolError, D::Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Remote {
        #[serde(deserialize_with = "string_enum")]
        code: ErrorCode,
        message: String,
        #[serde(deserialize_with = "string_enum")]
        outcome: Outcome,
    }
    let error: Remote = deserialize_object(d)?;
    Ok(ProtocolError::new(error.code, error.message, error.outcome))
}
fn error_envelope(bytes: &[u8], status: u16) -> Result<(Option<Uuid>, ProtocolError), Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Rejection {
        #[serde(deserialize_with = "nullable_id")]
        request_id: Option<Uuid>,
        #[serde(deserialize_with = "remote_error")]
        error: ProtocolError,
    }
    let rejection: Rejection = envelope(bytes)?;
    require(rejection.error.status_code() == status)?;
    Ok((rejection.request_id, rejection.error))
}
#[allow(dead_code)] // Borrowed claims for mini 3 and semantic owners, no extraction.
impl CheckedContent {
    pub(crate) fn operation(&self) -> Option<&Request> {
        if let ExpectedContent::Operation(v) = &self.parsed {
            Some(v)
        } else {
            None
        }
    }
    pub(crate) fn job(&self) -> Option<&JobEnvelope> {
        if let ExpectedContent::Job(v) = &self.parsed {
            Some(v)
        } else {
            None
        }
    }
    pub(crate) fn readiness(&self) -> Option<&Readiness> {
        if let ExpectedContent::Readiness(v) = &self.parsed {
            Some(v)
        } else {
            None
        }
    }
    pub(crate) fn dependency(&self) -> Option<&Dependency> {
        match &self.parsed {
            ExpectedContent::Open(v) | ExpectedContent::Close(v) => Some(v),
            _ => None,
        }
    }
    pub(crate) fn drain(&self) -> Option<&Drain> {
        if let ExpectedContent::Drain(v) = &self.parsed {
            Some(v)
        } else {
            None
        }
    }
}
#[allow(dead_code)] // Borrowed data remain attached to signed status/context.
impl CheckedReplyContent {
    pub(crate) fn operation(&self) -> Option<&RawValue> {
        if let ReplyContent::Operation(v) = &self.parsed {
            Some(v)
        } else {
            None
        }
    }
    pub(crate) fn job(&self) -> Option<&ResultEnvelope> {
        if let ReplyContent::Job(v) = &self.parsed {
            Some(v)
        } else {
            None
        }
    }
    pub(crate) fn readiness(&self) -> Option<&ReadinessReply> {
        if let ReplyContent::Readiness(v) = &self.parsed {
            Some(v)
        } else {
            None
        }
    }
    pub(crate) fn dependency(&self) -> Option<&Dependency> {
        if let ReplyContent::Open(v) = &self.parsed {
            Some(v)
        } else {
            None
        }
    }
    pub(crate) fn close(&self) -> Option<&DependencyCloseReply> {
        if let ReplyContent::Close(v) = &self.parsed {
            Some(v)
        } else {
            None
        }
    }
    pub(crate) fn drain(&self) -> Option<&DrainReply> {
        if let ReplyContent::Drain(v) = &self.parsed {
            Some(v)
        } else {
            None
        }
    }
    pub(crate) fn error(&self) -> Option<&ProtocolError> {
        match &self.parsed {
            ReplyContent::Rejection(v) | ReplyContent::UnattributedRejection(v) => Some(v),
            _ => None,
        }
    }
    pub(crate) fn is_unattributed_rejection(&self) -> bool {
        matches!(self.parsed, ReplyContent::UnattributedRejection(_))
    }
}
#[cfg(test)]
mod tests;
