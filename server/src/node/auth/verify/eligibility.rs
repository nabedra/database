//! Bounded synchronous provisional checks over a supplied root, outside locks.
//! No fresh GET, live initialization, permit, replay or capacity is proved here.
use super::*;
use crate::{
    cluster::{
        member::{Node, NodeState, Registration},
        state::Control,
    },
    node::auth::{Role, Route, Sender, WorkerSigningKey},
};
use uuid::Uuid;
fn require(ok: bool) -> Result<(), Error> {
    if ok { Ok(()) } else { Err(Error::Invalid) }
}
fn bytes32(s: &str) -> Result<[u8; 32], Error> {
    require(s.len() == 64)?;
    let mut out = [0; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).map_err(|_| Error::Invalid)?;
    }
    Ok(out)
}
fn current<'a>(
    root: &'a Control,
    node: Uuid,
    incarnation: Uuid,
    session: Uuid,
    key: &[u8; 32],
) -> Result<&'a Node, Error> {
    let n = root
        .nodes
        .iter()
        .find(|n| n.node_id == node)
        .ok_or(Error::Invalid)?;
    let s = n.session.as_ref().ok_or(Error::Invalid)?;
    require(
        n.incarnation == Some(incarnation)
            && s.id == session
            && !n.retired_incarnations.contains(&incarnation)
            && !matches!(n.state, NodeState::Lost | NodeState::Stopped)
            && bytes32(&s.verifier)?
                == worker_verifier(root.cluster_id, node, incarnation, session, key),
    )?;
    Ok(n)
}
pub(crate) fn worker<'a>(root: &'a Control, s: &Sender) -> Result<&'a Node, Error> {
    require(s.role() == Role::Worker && s.incarnation() == Some(s.process_id()))?;
    current(
        root,
        s.node_id().ok_or(Error::Invalid)?,
        s.incarnation().ok_or(Error::Invalid)?,
        s.session_id().ok_or(Error::Invalid)?,
        &s.public_key()?,
    )
}
fn ready(n: &Node) -> bool {
    n.state == NodeState::Ready && n.registration == Registration::Verified
}
pub(crate) fn work(n: &Node) -> bool {
    matches!(n.state, NodeState::Ready | NodeState::Draining)
        && n.registration == Registration::Verified
}
fn close(n: &Node) -> bool {
    ready(n)
        || (n.state == NodeState::Draining
            && matches!(
                n.registration,
                Registration::Registered | Registration::Verified
            ))
}
fn probe(n: &Node) -> bool {
    ready(n) || (n.state == NodeState::Starting && n.registration == Registration::Registered)
}
fn dependency(a: &Auth) -> Result<DependencyBinding, Error> {
    Ok(DependencyBinding {
        coordinator: a.sender().clone(),
        operation_id: a.operation_id().ok_or(Error::Invalid)?,
        admission_id: a.admission_id().ok_or(Error::Invalid)?,
        request_digest: bytes32(a.request_digest().ok_or(Error::Invalid)?)?,
    })
}
fn drain_id(n: &Node) -> Result<Uuid, Error> {
    require(
        n.state == NodeState::Draining
            && matches!(
                n.registration,
                Registration::Registered | Registration::Verified
            ),
    )?;
    Ok(n.drain.as_ref().ok_or(Error::Invalid)?.id)
}
#[allow(dead_code)] // R03a3/final-admission caller follows.
impl ReceiverIdentity {
    /// Captures the local boot key, without claiming initialization/enrollment.
    pub(crate) fn new(
        cluster: Uuid,
        node: Uuid,
        incarnation: Uuid,
        session: Uuid,
        key: &WorkerSigningKey,
    ) -> Self {
        Self {
            cluster,
            node,
            incarnation,
            session,
            public_key: key.public_key(),
        }
    }
    pub(crate) fn same_boot(&self, other: &Self) -> bool {
        self.cluster == other.cluster
            && self.node == other.node
            && self.incarnation == other.incarnation
            && self.session == other.session
            && self.public_key == other.public_key
    }
}
#[allow(dead_code)] // R03a3/final-admission caller follows.
pub(crate) fn check_request_authority(
    signed: &SignedRequest,
    root: &Control,
    local: &ReceiverIdentity,
    pinned_api: &[u8; 32],
) -> Result<EligibleRequest, Error> {
    let observation = OwnershipObservation::new(root).map_err(|_| Error::Invalid)?;
    let a = signed.auth();
    let target = request_target(a, root, local, pinned_api)?;
    let pending = match a.route() {
        Route::Operations => {
            require(ready(target))?;
            PendingLocalChecks::NewOperation {
                operation_id: a.operation_id().ok_or(Error::Invalid)?,
                request_digest: bytes32(a.request_digest().ok_or(Error::Invalid)?)?,
            }
        }
        Route::Readiness => {
            require(probe(target))?;
            PendingLocalChecks::Probe
        }
        Route::DependencyOpen => {
            require(ready(target))?;
            PendingLocalChecks::Open {
                binding: dependency(a)?,
            }
        }
        Route::Jobs => {
            require(work(target))?;
            PendingLocalChecks::Job {
                binding: dependency(a)?,
                permit_id: a.dependency_id().ok_or(Error::Invalid)?,
                job_id: a.job_id().ok_or(Error::Invalid)?,
                attempt_id: a.attempt_id().ok_or(Error::Invalid)?,
            }
        }
        Route::DependencyClose => {
            require(close(target))?;
            PendingLocalChecks::Close {
                binding: dependency(a)?,
            }
        }
        Route::Drain => PendingLocalChecks::Drain {
            drain_id: drain_id(target)?,
        },
    };
    Ok(EligibleRequest {
        request: Arc::clone(&signed.request),
        observation,
        local: ReceiverIdentity {
            cluster: local.cluster,
            node: local.node,
            incarnation: local.incarnation,
            session: local.session,
            public_key: local.public_key,
        },
        pending,
    })
}
#[allow(dead_code)] // Reply acceptance owner follows.
pub(crate) fn check_reply_authority(
    signed: &SignedReply,
    root: &Control,
    expected: &RequestContext,
) -> Result<EligibleReply, Error> {
    let association = &signed.reply;
    require(
        Arc::ptr_eq(&association.request, &expected.request)
            && Arc::ptr_eq(&association.target, &expected.target)
            && Arc::ptr_eq(&association.expected, &expected.expected),
    )?;
    let observation = OwnershipObservation::new(root).map_err(|_| Error::Invalid)?;
    let a = expected.auth();
    let reply = signed.auth();
    let r = &expected.target.receiver;
    require(
        root.cluster_id == a.cluster_id()
            && reply.cluster_id() == root.cluster_id
            && reply.receiver() == a.sender()
            && reply.request_sequence() == a.sequence()
            && reply.scope() == a.scope()
            && reply.sender().node_id() == Some(r.node_id())
            && reply.sender().incarnation() == Some(r.incarnation())
            && reply.sender().session_id() == Some(r.session_id()),
    )?;
    let target = worker(root, reply.sender())?;
    // The original API key is already retained by the exact context. Workers
    // additionally require their current durable commitment at reply acceptance.
    if a.sender().role() == Role::Worker {
        let sender = worker(root, a.sender())?;
        require(if a.route() == Route::DependencyClose {
            close(sender)
        } else {
            work(sender)
        })?;
    }
    let pending = match expected.expected.as_ref() {
        ExpectedContent::Operation(request) => {
            require(work(target))?;
            PendingReplyChecks::AcceptedOperation {
                operation_id: request.request_id,
                request_digest: bytes32(a.request_digest().ok_or(Error::Invalid)?)?,
            }
        }
        ExpectedContent::Job(job) => {
            require(work(target))?;
            PendingReplyChecks::AcceptedJob {
                binding: dependency(a)?,
                permit_id: a.dependency_id().ok_or(Error::Invalid)?,
                job_id: job.job_id,
                attempt_id: job.attempt_id,
            }
        }
        ExpectedContent::Readiness(request) => {
            require(probe(target))?;
            PendingReplyChecks::Probe {
                challenge: request.challenge(),
            }
        }
        ExpectedContent::Open(request) => {
            require(ready(target))?;
            PendingReplyChecks::Open {
                binding: dependency(a)?,
                permit_id: request.permit_id(),
            }
        }
        ExpectedContent::Close(request) => {
            require(close(target))?;
            PendingReplyChecks::Close {
                binding: dependency(a)?,
                permit_id: request.permit_id(),
            }
        }
        ExpectedContent::Drain(request) => {
            let durable = drain_id(target)?;
            require(request.drain_id() == durable)?;
            PendingReplyChecks::Drain { drain_id: durable }
        }
    };
    Ok(EligibleReply {
        reply: Arc::clone(association),
        observation,
        pending,
    })
}
#[allow(dead_code)] // Borrowed requirements only, no accepted-admission constructor.
impl EligibleRequest {
    pub(crate) fn auth(&self) -> &Auth {
        &self.request.auth
    }
    pub(crate) fn matches_content(&self, c: &CheckedContent) -> bool {
        Arc::ptr_eq(&self.request, &c.request)
    }
    pub(crate) fn observation(&self) -> &OwnershipObservation {
        &self.observation
    }
    pub(crate) fn local(&self) -> &ReceiverIdentity {
        &self.local
    }
    pub(crate) fn pending(&self) -> &PendingLocalChecks {
        &self.pending
    }
}
#[allow(dead_code)]
impl EligibleReply {
    pub(crate) fn auth(&self) -> &ReplyAuth {
        &self.reply.reply
    }
    pub(crate) fn matches_content(&self, c: &CheckedReplyContent) -> bool {
        Arc::ptr_eq(&self.reply, &c.reply)
    }
    pub(crate) fn observation(&self) -> &OwnershipObservation {
        &self.observation
    }
    pub(crate) fn pending(&self) -> &PendingReplyChecks {
        &self.pending
    }
}
#[allow(dead_code)]
impl DependencyBinding {
    pub(crate) fn coordinator(&self) -> &Sender {
        &self.coordinator
    }
    pub(crate) fn operation_id(&self) -> Uuid {
        self.operation_id
    }
    pub(crate) fn admission_id(&self) -> Uuid {
        self.admission_id
    }
    pub(crate) fn request_digest(&self) -> &[u8; 32] {
        &self.request_digest
    }
}
#[cfg(test)]
mod tests;

impl RequestBinding {
    #[allow(dead_code)] // R04c2 queued-start consumer.
    pub(crate) fn check_accepted_authority(
        &self,
        root: &Control,
        local: &ReceiverIdentity,
        pinned_api: &[u8; 32],
    ) -> Result<AcceptedRequestAuthority, Error> {
        let observation = OwnershipObservation::new(root).map_err(|_| Error::Invalid)?;
        let a = self.auth();
        let target = request_target(a, root, local, pinned_api)?;
        require(match a.route() {
            Route::Operations | Route::Jobs => work(target),
            Route::Readiness => probe(target),
            Route::DependencyOpen => ready(target),
            Route::DependencyClose => close(target),
            Route::Drain => drain_id(target).is_ok(),
        })?;
        Ok(AcceptedRequestAuthority {
            request: Arc::clone(&self.request),
            observation,
            local: ReceiverIdentity {
                cluster: local.cluster,
                node: local.node,
                incarnation: local.incarnation,
                session: local.session,
                public_key: local.public_key,
            },
        })
    }
}
#[allow(dead_code)] // R04c2 retained start association checks.
impl AcceptedRequestAuthority {
    pub(crate) fn observation(&self) -> &OwnershipObservation {
        &self.observation
    }
    pub(crate) fn local(&self) -> &ReceiverIdentity {
        &self.local
    }
    pub(crate) fn matches_binding(&self, binding: &RequestBinding) -> bool {
        Arc::ptr_eq(&self.request, &binding.request)
    }
}

impl ReceiverIdentity {
    pub(crate) fn check_current<'a>(&self, root: &'a Control) -> Result<&'a Node, Error> {
        require(self.cluster == root.cluster_id)?;
        current(
            root,
            self.node,
            self.incarnation,
            self.session,
            &self.public_key,
        )
    }
}

fn request_target<'a>(
    a: &Auth,
    root: &'a Control,
    local: &ReceiverIdentity,
    pinned_api: &[u8; 32],
) -> Result<&'a Node, Error> {
    let r = a.receiver();
    require(
        root.cluster_id == a.cluster_id()
            && local.cluster == root.cluster_id
            && local.node == r.node_id()
            && local.incarnation == r.incarnation()
            && local.session == r.session_id(),
    )?;
    let target = local.check_current(root)?;
    if a.sender().role() == Role::Api {
        require(a.sender().public_key()? == *pinned_api)?;
    } else {
        let sender = worker(root, a.sender())?;
        require(if a.route() == Route::DependencyClose {
            close(sender)
        } else {
            work(sender)
        })?;
    }
    Ok(target)
}
