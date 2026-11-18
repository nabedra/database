use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use uuid::Uuid;

use super::{CACHE_LEADERS, CONTROL_WORK_TIMEOUT, Control, Error, METADATA, catalog};
use crate::{
    http, partition,
    raft::{GroupError, GroupStatus},
};

const CONTROL_PATH: &str = "/internal/events/v1/control";
const DATA_PATH: &str = "/internal/events/v1/data";
const INBOUND_BYTES: usize = 2 * 1024 * 1024 + 4096;
const PROBE_TIMEOUT: Duration = Duration::from_millis(450);
const CALL_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_PROBE_ATTEMPTS: usize = 6;

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub(super) struct Fence {
    pub id: Uuid,
    pub step: u64,
    pub coordinator: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) enum Rpc {
    Inspect {
        group: String,
    },
    Open {
        group: String,
        #[serde(default)]
        election_after: Option<u64>,
        fence: Fence,
    },
    Initialize {
        group: String,
        members: BTreeMap<u64, String>,
        fence: Fence,
    },
    Retire {
        group: String,
        removed_at: u64,
        fence: Fence,
    },
    Group {
        group: String,
        call: GroupCall,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) enum GroupCall {
    MetaRead {
        query: catalog::Query,
    },
    MetaWrite {
        command: catalog::Command,
    },
    PartitionRead {
        query: partition::Query,
    },
    PartitionWrite {
        command: partition::Command,
    },
    PartitionStep {
        command: partition::Command,
        fence: Fence,
    },
    Membership,
    AddLearner {
        id: u64,
        address: String,
        fence: Fence,
    },
    Place {
        previous: BTreeSet<u64>,
        target: BTreeSet<u64>,
        fence: Fence,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) enum Fault {
    NotLeader(Option<u64>),
    Domain(Error),
}

impl From<Error> for Fault {
    fn from(error: Error) -> Self {
        Self::Domain(error)
    }
}

impl From<GroupError> for Fault {
    fn from(error: GroupError) -> Self {
        match error {
            GroupError::NotLeader(leader) => Self::NotLeader(leader),
            GroupError::Closed | GroupError::Unavailable => Self::Domain(Error::Unavailable),
            GroupError::Overloaded => Self::Domain(Error::Capacity),
            GroupError::OutcomeUnknown => Self::Domain(Error::Unknown),
            GroupError::Other(_) => Self::Domain(Error::Internal),
        }
    }
}

type RpcReply = Result<Value, Fault>;

impl GroupCall {
    pub(super) fn subscription(&self) -> Option<&str> {
        match self {
            Self::PartitionWrite {
                command:
                    partition::Command::Claim { name, .. }
                    | partition::Command::Renew { name, .. }
                    | partition::Command::Release { name, .. }
                    | partition::Command::Ack { name, .. },
            }
            | Self::PartitionRead {
                query: partition::Query::Poll { name, .. },
            } => Some(name),
            _ => None,
        }
    }
}

impl Rpc {
    fn data(&self) -> bool {
        matches!(
            self,
            Self::Group {
                call: GroupCall::PartitionWrite {
                    command: partition::Command::Publish { .. }
                        | partition::Command::PublishMany { .. },
                },
                ..
            }
        )
    }

    fn mutates(&self) -> bool {
        matches!(
            self,
            Self::Open { .. }
                | Self::Initialize { .. }
                | Self::Retire { .. }
                | Self::Group {
                    call: GroupCall::MetaWrite { .. }
                        | GroupCall::PartitionWrite { .. }
                        | GroupCall::PartitionStep { .. }
                        | GroupCall::AddLearner { .. }
                        | GroupCall::Place { .. },
                    ..
                }
        )
    }

    fn budget(&self) -> Duration {
        match self {
            Self::Inspect { .. } => PROBE_TIMEOUT,
            Self::Open { .. }
            | Self::Initialize { .. }
            | Self::Retire { .. }
            | Self::Group {
                call:
                    GroupCall::AddLearner { .. }
                    | GroupCall::Place { .. }
                    | GroupCall::PartitionStep { .. },
                ..
            } => CONTROL_WORK_TIMEOUT,
            Self::Group { .. } => CALL_TIMEOUT,
        }
    }
}

pub(super) fn router(control: Arc<Control>) -> Router {
    Router::new()
        .route(CONTROL_PATH, post(control_request))
        .route(DATA_PATH, post(data_request))
        .with_state(control)
}

async fn control_request(State(control): State<Arc<Control>>, request: Request) -> Response {
    receive(control, request, false).await
}

async fn data_request(State(control): State<Arc<Control>>, request: Request) -> Response {
    receive(control, request, true).await
}

async fn receive(control: Arc<Control>, request: Request, data: bool) -> Response {
    if !control.broker.peers().authenticated(request.headers()) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let lane = if data {
        &control.peer_data
    } else {
        &control.peer_control
    };
    let Ok(_permit) = lane.try_acquire() else {
        return Json(RpcReply::Err(Fault::Domain(Error::Capacity))).into_response();
    };
    let body = match tokio::time::timeout(
        CALL_TIMEOUT,
        to_bytes(request.into_body(), INBOUND_BYTES),
    )
    .await
    {
        Ok(Ok(body)) => body,
        _ => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
    };
    let rpc = match serde_json::from_slice::<Rpc>(&body) {
        Ok(rpc) if rpc.data() == data => rpc,
        _ => return StatusCode::BAD_REQUEST.into_response(),
    };
    Json(control.execute(rpc).await).into_response()
}

impl Control {
    async fn execute(&self, request: Rpc) -> RpcReply {
        match request {
            Rpc::Inspect { group } => {
                let status = self.broker.group(&group).ok_or(Error::NotFound)?.status();
                encode(status)
            }
            Rpc::Open {
                group,
                election_after,
                fence,
            } => {
                self.require_current(fence).await?;
                self.open_group_after(&group, election_after).await?;
                encode(())
            }
            Rpc::Initialize {
                group,
                members,
                fence,
            } => {
                self.require_current(fence).await?;
                let group = self.broker.group(&group).ok_or(Error::Unavailable)?;
                group.initialize(members).await?;
                encode(())
            }
            Rpc::Retire {
                group,
                removed_at,
                fence,
            } => {
                self.require_current(fence).await?;
                self.broker.retire_group(&group, removed_at).await?;
                encode(())
            }
            Rpc::Group { group, call } => self.execute_group(&group, call).await,
        }
    }

    async fn execute_group(&self, group_id: &str, call: GroupCall) -> RpcReply {
        let group = self.broker.group(group_id).ok_or(Error::Unavailable)?;
        if group.leader() != Some(self.config.broker_id) {
            return Err(Fault::NotLeader(group.leader()));
        }
        match call {
            GroupCall::MetaRead { query } if group_id == METADATA => {
                encode(group.read(move |tx| catalog::query(tx, query)).await?)
            }
            GroupCall::MetaWrite { command } if group_id == METADATA => {
                group.write(encode(command)?).await.map_err(Into::into)
            }
            GroupCall::PartitionRead { mut query } if group_id != METADATA => {
                if let partition::Query::Poll { now_ms, .. } = &mut query {
                    *now_ms = http::now_ms();
                }
                encode(group.read(move |tx| partition::query(tx, query)).await?)
            }
            GroupCall::PartitionWrite { mut command } if group_id != METADATA => {
                match &mut command {
                    partition::Command::Publish { now_ms, .. }
                    | partition::Command::PublishMany { now_ms, .. } => {
                        if self
                            .disk_available
                            .load(std::sync::atomic::Ordering::Relaxed)
                            < self.config.min_free_disk_bytes
                        {
                            return Err(Error::Disk.into());
                        }
                        *now_ms = http::now_ms();
                        return group
                            .write_coalesced(encode(command)?)
                            .await
                            .map_err(Into::into);
                    }
                    partition::Command::Claim { now_ms, .. }
                    | partition::Command::Renew { now_ms, .. }
                    | partition::Command::Release { now_ms, .. }
                    | partition::Command::Ack { now_ms, .. } => *now_ms = http::now_ms(),
                    // Retention may run on a local preferred leader even while
                    // metadata quorum is temporarily unavailable.
                    partition::Command::Retain { .. } => {}
                    _ => return Err(Error::Invalid.into()),
                }
                group.write(encode(command)?).await.map_err(Into::into)
            }
            GroupCall::PartitionStep { command, fence } if group_id != METADATA => {
                self.require_current(fence).await?;
                group.write(encode(command)?).await.map_err(Into::into)
            }
            GroupCall::Membership => encode(group.committed_membership().await?),
            GroupCall::AddLearner { id, address, fence } => {
                self.require_current(fence).await?;
                group.add_learner(id, address).await?;
                encode(())
            }
            GroupCall::Place {
                previous,
                target,
                fence,
            } => {
                self.require_current(fence).await?;
                group.change_membership_from(&previous, target).await?;
                encode(())
            }
            _ => Err(Error::Invalid.into()),
        }
    }

    // Fence every topology effect against the current metadata quorum. If a
    // controller died after dispatch, only the same persisted step may resume.
    async fn require_current(&self, fence: Fence) -> Result<(), Fault> {
        let view = if let Some(group) = self.broker.group(METADATA)
            && group.leader() == Some(self.config.broker_id)
        {
            group
                .read(|tx| catalog::query(tx, catalog::Query::Work))
                .await?
        } else {
            let request = Rpc::Group {
                group: METADATA.into(),
                call: GroupCall::MetaRead {
                    query: catalog::Query::Work,
                },
            };
            let mut targets = self.metadata_candidates();
            let hint = self.known_leader(METADATA);
            if let Some(hint) = hint {
                targets.remove(&hint);
            }
            targets.remove(&fence.coordinator);
            let mut reply = Err(Error::Unavailable);
            for target in Some(fence.coordinator)
                .into_iter()
                .chain(hint.filter(|hint| *hint != fence.coordinator))
                .chain(targets)
                .filter(|id| *id != self.config.broker_id)
            {
                if let Ok(value) = self.remote_call(target, &request).await {
                    reply = decode(value)?;
                    self.remember_leader(METADATA, Some(target));
                    break;
                }
            }
            reply
        };
        if matches!(view, Ok(catalog::Reply::Work(Some(work))) if work.id == fence.id && work.step == fence.step)
        {
            Ok(())
        } else {
            Err(Error::Unavailable.into())
        }
    }

    pub(super) async fn peer_operation<T: DeserializeOwned>(
        &self,
        target: u64,
        request: &Rpc,
    ) -> Result<T, Error> {
        match self.call_peer(target, request).await {
            Ok(value) => decode(value),
            Err(Fault::Domain(error)) => Err(error),
            Err(Fault::NotLeader(_)) => Err(Error::Unavailable),
        }
    }

    async fn call_peer(&self, target: u64, request: &Rpc) -> RpcReply {
        if target == self.config.broker_id {
            let request = serde_json::from_value(encode(request)?).map_err(|_| Error::Internal)?;
            return self.execute(request).await;
        }
        self.remote_call(target, request).await
    }

    async fn remote_call(&self, target: u64, request: &Rpc) -> RpcReply {
        let roster = self.known_roster();
        let broker = roster
            .get(&target)
            .or_else(|| self.bootstrap.get(&target))
            .ok_or(Error::Unavailable)?;
        let path = if request.data() {
            DATA_PATH
        } else {
            CONTROL_PATH
        };
        self.broker
            .peers()
            .post_with_timeout(&broker.peer_url, path, request, request.budget())
            .await
            .unwrap_or_else(|_| {
                Err(Fault::Domain(if request.mutates() {
                    Error::Unknown
                } else {
                    Error::Unavailable
                }))
            })
    }

    pub(super) fn remember_leader(&self, group: &str, id: Option<u64>) {
        let mut leaders = self.leaders.lock().expect("leader cache lock");
        if let Some(id) = id {
            if leaders.len() >= CACHE_LEADERS {
                leaders.pop_first();
            }
            leaders.insert(group.to_owned(), id);
        } else {
            leaders.remove(group);
        }
    }

    pub(super) fn known_leader(&self, group: &str) -> Option<u64> {
        self.broker
            .group(group)
            .and_then(|group| group.leader())
            .or_else(|| {
                self.leaders
                    .lock()
                    .expect("leader cache lock")
                    .get(group)
                    .copied()
            })
    }

    async fn locate(
        &self,
        group: &str,
        candidates: &BTreeSet<u64>,
        omit: Option<u64>,
    ) -> Option<u64> {
        let mut probes = Vec::new();
        if let Some(local) = self.broker.group(group)
            && let Some(leader) = local.leader()
            && omit != Some(leader)
            && (candidates.contains(&leader) || group == METADATA)
        {
            return Some(leader);
        }
        // A metadata voter move can precede cache refresh. If the old voters
        // give no usable answer, also interrogate known ingress brokers.
        probes.extend(candidates.iter().copied());
        if group == METADATA {
            probes.extend(
                self.known_roster()
                    .into_keys()
                    .filter(|id| !candidates.contains(id)),
            );
        }
        let inspect = Rpc::Inspect {
            group: group.into(),
        };
        for target in probes
            .into_iter()
            .filter(|id| Some(*id) != omit)
            .take(MAX_PROBE_ATTEMPTS)
        {
            let Ok(status) = self.peer_operation::<GroupStatus>(target, &inspect).await else {
                continue;
            };
            if let Some(leader) = status.leader
                && Some(leader) != omit
                && (candidates.contains(&leader) || group == METADATA)
            {
                return Some(leader);
            }
        }
        None
    }

    pub(super) async fn call_group<T: DeserializeOwned>(
        &self,
        group: &str,
        candidates: &BTreeSet<u64>,
        request: &Rpc,
    ) -> Result<T, Error> {
        let target = match self
            .known_leader(group)
            .filter(|id| candidates.contains(id) || group == METADATA)
        {
            Some(id) => id,
            None => self
                .locate(group, candidates, None)
                .await
                .ok_or(Error::Unavailable)?,
        };
        match self.call_peer(target, request).await {
            Ok(value) => {
                self.remember_leader(group, Some(target));
                decode(value)
            }
            Err(Fault::Domain(error @ (Error::Unavailable | Error::Unknown))) => {
                self.remember_leader(group, None);
                // Do not resubmit a write whose outcome might be unknown.
                if request.mutates() {
                    return Err(error);
                }
                let next = self
                    .locate(group, candidates, Some(target))
                    .await
                    .ok_or(error)?;
                self.finish_forward(group, next, request).await
            }
            Err(Fault::NotLeader(hint)) => {
                self.remember_leader(group, None);
                let known = hint
                    .filter(|id| *id != target && (candidates.contains(id) || group == METADATA));
                let next = match known {
                    Some(id) => id,
                    None => self
                        .locate(group, candidates, Some(target))
                        .await
                        .ok_or(Error::Unavailable)?,
                };
                self.finish_forward(group, next, request).await
            }
            Err(Fault::Domain(error)) => Err(error),
        }
    }

    async fn finish_forward<T: DeserializeOwned>(
        &self,
        group: &str,
        target: u64,
        request: &Rpc,
    ) -> Result<T, Error> {
        let result = self.peer_operation(target, request).await?;
        self.remember_leader(group, Some(target));
        Ok(result)
    }
}

fn encode<T: Serialize>(value: T) -> Result<Value, Fault> {
    serde_json::to_value(value).map_err(|_| Error::Internal.into())
}

fn decode<T: DeserializeOwned>(value: Value) -> Result<T, Error> {
    serde_json::from_value(value).map_err(|_| Error::Internal)
}
