use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Path, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::forward::{Fence, GroupCall, Rpc};
use super::{Control, Error, METADATA, RESTORE_REBIND, catalog, parse_partition_id, partition_id};
use crate::{
    http::{self, ApiError},
    partition,
    raft::{GroupError, GroupStatus, MAX_HOSTED_GROUPS, Membership},
};

const ADMIN_MAX_BYTES: usize = 16 * 1024;
const HEALTH_INTERVAL: Duration = Duration::from_secs(1);
const CONTROLLER_INTERVAL: Duration = Duration::from_millis(200);
const RETENTION_PAGE: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct BackupLayout {
    pub cluster_id: Uuid,
    pub revision: u64,
    pub groups: BTreeMap<String, BTreeMap<u64, String>>,
    pub roster: BTreeMap<u64, catalog::BrokerSpec>,
}

pub(super) fn router(control: Arc<Control>) -> Router {
    Router::new()
        .route("/readyz", get(readiness))
        .route("/v1/admin/broker", get(broker_status))
        .route("/v1/admin/groups/{group}", get(group_status))
        .route("/v1/admin/topics/{topic}", get(topic))
        .route(
            "/v1/admin/topics/{topic}/partitions/{partition}",
            get(partition_status),
        )
        .route(
            "/v1/admin/topics/{topic}/partitions/{partition}/subscriptions/{subscription}",
            get(subscription_status),
        )
        .route("/v1/admin/operations", post(submit))
        .route("/v1/admin/operations/{id}", get(operation))
        .with_state(control)
}

async fn readiness(State(control): State<Arc<Control>>) -> Response {
    if control.ready() {
        StatusCode::OK.into_response()
    } else {
        StatusCode::SERVICE_UNAVAILABLE.into_response()
    }
}

async fn submit(
    State(control): State<Arc<Control>>,
    request: Request,
) -> Result<Response, ApiError> {
    let grant = http::authorize_operator(request.headers(), &control.keyring, http::now_ms())?;
    let _permit = control
        .admin_slots
        .try_acquire()
        .map_err(|_| api_error(Error::Capacity))?;
    let bytes = tokio::time::timeout(
        Duration::from_secs(5),
        to_bytes(request.into_body(), ADMIN_MAX_BYTES),
    )
    .await
    .map_err(|_| api_error(Error::Invalid))?
    .map_err(|_| {
        ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "operator request exceeds its byte limit",
        )
    })?;
    let operation = serde_json::from_slice::<catalog::Operation>(&bytes)
        .map_err(|_| api_error(Error::Invalid))?;
    if !control.topology_loaded() {
        return Err(api_error(Error::Unavailable));
    }
    let operation_id = operation.operation_id;
    let action = operation.action.label();
    let result = control
        .meta_write(catalog::Command::Submit {
            operation,
            new_topic_id: Uuid::new_v4(),
            allow_plaintext: control.config.allow_plaintext,
        })
        .await;
    tracing::info!(%operation_id, action, tenant = %grant.tenant, actor = %grant.subject,
        result = if result.is_ok() { "recorded" } else { "error" }, "Events operator request");
    match result.map_err(api_error)? {
        catalog::Reply::Receipt(receipt) => Ok(receipt_response(receipt)),
        _ => Err(api_error(Error::Internal)),
    }
}

async fn operation(
    State(control): State<Arc<Control>>,
    headers: axum::http::HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Response, ApiError> {
    http::authorize_operator(&headers, &control.keyring, http::now_ms())?;
    let _permit = control
        .admin_slots
        .try_acquire()
        .map_err(|_| api_error(Error::Capacity))?;
    match control
        .meta_read(catalog::Query::Operation { id })
        .await
        .map_err(api_error)?
    {
        catalog::Reply::Receipt(receipt) => Ok(receipt_response(receipt)),
        _ => Err(api_error(Error::Internal)),
    }
}

async fn topic(
    State(control): State<Arc<Control>>,
    headers: axum::http::HeaderMap,
    Path(topic): Path<String>,
) -> Result<Json<catalog::Topic>, ApiError> {
    http::authorize_operator(&headers, &control.keyring, http::now_ms())?;
    let _permit = control
        .admin_slots
        .try_acquire()
        .map_err(|_| api_error(Error::Capacity))?;
    if !partition::valid_name(&topic) {
        return Err(api_error(Error::Invalid));
    }
    match control
        .meta_read(catalog::Query::Topic { name: topic })
        .await
        .map_err(api_error)?
    {
        catalog::Reply::Topic(topic) => Ok(Json(topic)),
        _ => Err(api_error(Error::Internal)),
    }
}

async fn broker_status(
    State(control): State<Arc<Control>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    http::authorize_operator(&headers, &control.keyring, http::now_ms())?;
    let _permit = control
        .admin_slots
        .try_acquire()
        .map_err(|_| api_error(Error::Capacity))?;
    let groups = control.broker.group_ids();
    let leaders = groups
        .iter()
        .filter(|id| {
            control
                .broker
                .group(id)
                .is_some_and(|group| group.leader() == Some(control.config.broker_id))
        })
        .count();
    Ok(Json(serde_json::json!({
        "broker_id": control.config.broker_id,
        "ready": control.ready(),
        "groups": groups.len(),
        "leaders": leaders,
        "disk_available_bytes": control.disk_available.load(Ordering::Relaxed),
        "publish": control.publish_metrics.report(),
        "poll": control.poll_metrics.report(),
    })))
}

async fn group_status(
    State(control): State<Arc<Control>>,
    headers: axum::http::HeaderMap,
    Path(group): Path<String>,
) -> Result<Json<GroupStatus>, ApiError> {
    http::authorize_operator(&headers, &control.keyring, http::now_ms())?;
    let _permit = control
        .admin_slots
        .try_acquire()
        .map_err(|_| api_error(Error::Capacity))?;
    // This endpoint describes the local view on the addressed broker. It
    // remains useful for a failed group without a quorum-confirmed leader.
    let group = control
        .broker
        .group(&group)
        .ok_or_else(|| api_error(Error::NotFound))?;
    Ok(Json(group.status()))
}

async fn partition_status(
    State(control): State<Arc<Control>>,
    headers: axum::http::HeaderMap,
    Path((topic, part)): Path<(String, u32)>,
) -> Result<Json<partition::Status>, ApiError> {
    http::authorize_operator(&headers, &control.keyring, http::now_ms())?;
    let _permit = control
        .admin_slots
        .try_acquire()
        .map_err(|_| api_error(Error::Capacity))?;
    match control
        .status(&topic, part, partition::Query::Status)
        .await?
    {
        partition::QueryReply::Status(status) => Ok(Json(status)),
        _ => Err(api_error(Error::Internal)),
    }
}

async fn subscription_status(
    State(control): State<Arc<Control>>,
    headers: axum::http::HeaderMap,
    Path((topic, part, name)): Path<(String, u32, String)>,
) -> Result<Json<partition::SubscriptionStatus>, ApiError> {
    http::authorize_operator(&headers, &control.keyring, http::now_ms())?;
    let _permit = control
        .admin_slots
        .try_acquire()
        .map_err(|_| api_error(Error::Capacity))?;
    match control
        .status(&topic, part, partition::Query::SubscriptionStatus { name })
        .await?
    {
        partition::QueryReply::SubscriptionStatus(status) => Ok(Json(status)),
        _ => Err(api_error(Error::Internal)),
    }
}

fn receipt_response(receipt: catalog::Receipt) -> Response {
    let status = match receipt.stage {
        catalog::Stage::Pending => StatusCode::ACCEPTED,
        catalog::Stage::Completed { .. } => StatusCode::OK,
    };
    (status, Json(receipt)).into_response()
}

fn api_error(error: Error) -> ApiError {
    match error {
        Error::Invalid => ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "invalid operator request",
        ),
        Error::Conflict => ApiError::new(
            StatusCode::CONFLICT,
            "conflict",
            "operator request conflicts with committed topology",
        ),
        Error::Busy => ApiError::new(
            StatusCode::CONFLICT,
            "operation_pending",
            "a topology operation is already in progress",
        ),
        Error::PartitionLimit => ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "partition_limit",
            "cluster supports at most 4095 data partitions",
        ),
        other => http::BackendError::from(other).into(),
    }
}

impl Control {
    async fn status(
        &self,
        topic: &str,
        part: u32,
        query: partition::Query,
    ) -> Result<partition::QueryReply, ApiError> {
        let definition = self.cache_topic(topic, false).await.map_err(api_error)?;
        let placement = definition
            .partitions
            .get(part as usize)
            .ok_or_else(|| api_error(Error::NotFound))?;
        if let partition::Query::SubscriptionStatus { name } = &query
            && !definition.subscriptions.contains_key(name)
        {
            return Err(api_error(Error::NotFound));
        }
        let result: Result<partition::QueryReply, partition::Error> = self
            .partition_admin(
                definition.id,
                part,
                &placement.voters,
                GroupCall::PartitionRead { query },
            )
            .await
            .map_err(api_error)?;
        result.map_err(Into::into)
    }

    pub(super) async fn backup_layout(&self) -> Result<BackupLayout, Error> {
        let before = self.read_header().await?;
        if !catalog::metadata_is_quiet(&before) {
            return Err(Error::Busy);
        }
        let mut groups = BTreeMap::from([(
            METADATA.to_owned(),
            voter_addresses(&before, &before.metadata)?,
        )]);
        let mut after = None;
        while let catalog::Reply::Topics(page) = self
            .meta_read(catalog::Query::Topics {
                after: after.clone(),
                limit: 4,
            })
            .await?
        {
            if page.is_empty() {
                break;
            }
            for topic in page {
                if groups.len().saturating_add(topic.partitions.len()) > MAX_HOSTED_GROUPS {
                    return Err(Error::Capacity);
                }
                for (index, placement) in topic.partitions.iter().enumerate() {
                    groups.insert(
                        partition_id(topic.id, index as u32),
                        voter_addresses(&before, &placement.voters)?,
                    );
                }
                after = Some(topic.name);
            }
        }
        let after = self.read_header().await?;
        if after.revision != before.revision || !catalog::metadata_is_quiet(&after) {
            return Err(Error::Conflict);
        }
        let layout = BackupLayout {
            cluster_id: self.config.cluster_id,
            revision: before.revision,
            groups,
            roster: before.roster,
        };
        if serde_json::to_vec(&layout)
            .map_err(|_| Error::Internal)?
            .len()
            > 4 * 1024 * 1024
        {
            return Err(Error::Capacity);
        }
        Ok(layout)
    }

    pub(super) async fn controllers(self: Arc<Self>) {
        let mut timer = tokio::time::interval(CONTROLLER_INTERVAL);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            timer.tick().await;
            if self
                .broker
                .group(METADATA)
                .is_none_or(|group| group.leader() != Some(self.config.broker_id))
            {
                continue;
            }
            let _ = self.advance_topology().await;
        }
    }

    pub(super) async fn monitor(self: Arc<Self>) {
        let mut timer = tokio::time::interval(HEALTH_INTERVAL);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut cursor = 0usize;
        loop {
            timer.tick().await;
            let available = self.broker.disk_available_bytes().unwrap_or_default();
            self.disk_available.store(available, Ordering::Relaxed);
            if let Ok(header) = self.read_header().await {
                self.remember_header(header);
                self.mark_ready();
                let refresh = self.topics.lock().expect("topics lock").stale_name();
                if let Some(name) = refresh {
                    let _ = self.cache_topic(&name, true).await;
                }
            } else {
                self.try_readiness(cursor).await;
            }
            cursor = cursor.wrapping_add(1);
        }
    }

    async fn try_readiness(&self, cursor: usize) {
        let route = self.topics.lock().expect("topics lock").probe_route(cursor);
        if let Some((topic, part, voters)) = route {
            let status = self
                .partition_admin::<Result<partition::QueryReply, partition::Error>>(
                    topic,
                    part,
                    &voters,
                    GroupCall::PartitionRead {
                        query: partition::Query::Status,
                    },
                )
                .await;
            if matches!(status, Ok(Ok(_))) {
                self.mark_ready();
                return;
            }
        }
        let group_ids = self.broker.group_ids();
        // Check only one local group per tick so probe work stays bounded.
        let Some(id) = cursor
            .checked_rem(group_ids.len())
            .and_then(|idx| group_ids.get(idx))
        else {
            return;
        };
        if id != METADATA
            && let Some(group) = self.broker.group(id)
            && group.leader() == Some(self.config.broker_id)
            && group.barrier().await.is_ok()
        {
            self.mark_ready();
        }
    }

    pub(super) async fn retain(self: Arc<Self>) {
        let mut timer = tokio::time::interval(HEALTH_INTERVAL);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut cursor = 0usize;
        let mut anchor = None;
        loop {
            timer.tick().await;
            let ids = self.broker.group_ids();
            if ids.is_empty() {
                continue;
            }
            for group_id in ids
                .iter()
                .cycle()
                .skip(cursor % ids.len())
                .take(RETENTION_PAGE.min(ids.len()))
            {
                let Some((topic, _)) = parse_partition_id(group_id) else {
                    continue;
                };
                let Some(group) = self.broker.group(group_id) else {
                    continue;
                };
                if group.leader() != Some(self.config.broker_id) {
                    continue;
                }
                let Some(definition) = self.read_cached_by_id(topic, &mut anchor).await else {
                    continue;
                };
                let retain_ms = i64::try_from(definition.retention_seconds.saturating_mul(1000))
                    .unwrap_or(i64::MAX);
                let cutoff_ms = http::now_ms().saturating_sub(retain_ms);
                let status = group
                    .read(|tx| partition::query(tx, partition::Query::Status))
                    .await;
                if let Ok(Ok(partition::QueryReply::Status(status))) = status
                    && status
                        .oldest_published_ms
                        .is_some_and(|time| time < cutoff_ms)
                    && let Ok(command) = serde_json::to_value(partition::Command::Retain {
                        cutoff_ms,
                        limit: partition::MAX_RETAIN_EVENTS,
                    })
                {
                    let _ = group.write(command).await;
                }
            }
            cursor = cursor.wrapping_add(RETENTION_PAGE);
        }
    }

    async fn read_cached_by_id(
        &self,
        id: Uuid,
        anchor: &mut Option<catalog::Topic>,
    ) -> Option<catalog::Topic> {
        if let Some(topic) = anchor.as_ref().filter(|topic| topic.id == id) {
            return Some(topic.clone());
        }
        let cached = self.topics.lock().expect("topics lock").by_id(id);
        if cached.is_some() {
            return cached;
        }
        if let Ok(catalog::Reply::Topic(topic)) =
            self.meta_read(catalog::Query::TopicById { id }).await
        {
            *anchor = Some(topic.clone());
            self.topics.lock().expect("topics lock").put(topic.clone());
            return Some(topic);
        }
        None
    }

    async fn read_header(&self) -> Result<catalog::Header, Error> {
        match self.meta_read(catalog::Query::Header).await? {
            catalog::Reply::Header(header) if header.cluster_id == self.config.cluster_id => {
                Ok(header)
            }
            _ => Err(Error::Unavailable),
        }
    }

    async fn advance_topology(&self) -> Result<(), Error> {
        let header = match self.read_header().await {
            Ok(header) => header,
            Err(_)
                if self
                    .broker
                    .group(METADATA)
                    .is_some_and(|group| group.leader() == Some(self.config.broker_id)) =>
            {
                let command = catalog::Command::Bootstrap {
                    cluster_id: self.config.cluster_id,
                    config: self.config.bootstrap.clone(),
                    allow_plaintext: self.config.allow_plaintext,
                    restore: self.config.data_dir.join(RESTORE_REBIND).exists(),
                };
                match self.meta_write(command).await? {
                    catalog::Reply::Header(header) => header,
                    _ => return Err(Error::Internal),
                }
            }
            Err(error) => return Err(error),
        };
        self.remember_header(header.clone());
        let work = match self.meta_read(catalog::Query::Work).await? {
            catalog::Reply::Work(Some(work)) => work,
            catalog::Reply::Work(None) => return Ok(()),
            _ => return Err(Error::Internal),
        };
        let fence = Fence {
            id: work.id,
            step: work.step,
            coordinator: self.config.broker_id,
        };
        self.apply_effect(&header, fence, work.effect).await?;
        self.meta_write(catalog::Command::Advance {
            id: work.id,
            step: work.step,
        })
        .await?;
        Ok(())
    }

    async fn apply_effect(
        &self,
        header: &catalog::Header,
        fence: Fence,
        effect: catalog::Effect,
    ) -> Result<(), Error> {
        match effect {
            catalog::Effect::Initialize {
                topic,
                partition,
                target,
            } => {
                let group = partition_id(topic, partition);
                for &id in &target.voters {
                    self.peer_operation::<()>(
                        id,
                        &Rpc::Open {
                            group: group.clone(),
                            election_after: None,
                            fence,
                        },
                    )
                    .await?;
                }
                self.peer_operation::<()>(
                    target.preferred,
                    &Rpc::Initialize {
                        group,
                        members: voter_addresses(header, &target.voters)?,
                        fence,
                    },
                )
                .await?;
                self.partition_step(
                    topic,
                    partition,
                    &target.voters,
                    partition::Command::Init {
                        topic_id: topic,
                        partition,
                    },
                    fence,
                )
                .await
            }
            catalog::Effect::Subscribe {
                topic,
                partition,
                voters,
                name,
                start,
            } => {
                self.partition_step(
                    topic,
                    partition,
                    &voters,
                    partition::Command::OpenSubscription { name, start },
                    fence,
                )
                .await
            }
            catalog::Effect::Place {
                group,
                previous,
                target,
            } => {
                self.replace(header, &group, &previous, &target, fence)
                    .await
            }
            catalog::Effect::Reset {
                topic,
                partition,
                voters,
                name,
            } => {
                self.partition_step(
                    topic,
                    partition,
                    &voters,
                    partition::Command::ResetExpired { name },
                    fence,
                )
                .await
            }
            catalog::Effect::Skip | catalog::Effect::Finish => Ok(()),
        }
    }

    async fn partition_admin<T: serde::de::DeserializeOwned>(
        &self,
        topic: Uuid,
        part: u32,
        voters: &BTreeSet<u64>,
        call: GroupCall,
    ) -> Result<T, Error> {
        let group = partition_id(topic, part);
        self.call_group(
            &group,
            voters,
            &Rpc::Group {
                group: group.clone(),
                call,
            },
        )
        .await
    }

    async fn partition_step(
        &self,
        topic: Uuid,
        part: u32,
        voters: &BTreeSet<u64>,
        command: partition::Command,
        fence: Fence,
    ) -> Result<(), Error> {
        let reply: partition::Mutation = self
            .partition_admin(
                topic,
                part,
                voters,
                GroupCall::PartitionStep { command, fence },
            )
            .await?;
        reply.map(|_| ()).map_err(|_| Error::Conflict)
    }

    async fn replace(
        &self,
        header: &catalog::Header,
        group: &str,
        previous: &BTreeSet<u64>,
        target: &BTreeSet<u64>,
        fence: Fence,
    ) -> Result<(), Error> {
        let candidates = previous.union(target).copied().collect::<BTreeSet<_>>();
        let inspect = Rpc::Group {
            group: group.into(),
            call: GroupCall::Membership,
        };
        let membership: Membership = self.call_group(group, &candidates, &inspect).await?;
        if membership.joint || &membership.voters != target {
            let election_after = membership.membership_index.ok_or(Error::Unavailable)?;
            for &id in target.difference(&membership.voters) {
                self.peer_operation::<()>(
                    id,
                    &Rpc::Open {
                        group: group.into(),
                        election_after: Some(election_after),
                        fence,
                    },
                )
                .await?;
                let address = header
                    .roster
                    .get(&id)
                    .ok_or(Error::NotFound)?
                    .peer_url
                    .clone();
                self.call_group::<()>(
                    group,
                    &candidates,
                    &Rpc::Group {
                        group: group.into(),
                        call: GroupCall::AddLearner { id, address, fence },
                    },
                )
                .await?;
            }
            self.call_group::<()>(
                group,
                &candidates,
                &Rpc::Group {
                    group: group.into(),
                    call: GroupCall::Place {
                        previous: previous.clone(),
                        target: target.clone(),
                        fence,
                    },
                },
            )
            .await?;
        }

        // The new leader must confirm the exact final membership before a
        // source can retire or the metadata placement can become visible.
        let membership: Membership = self.call_group(group, target, &inspect).await?;
        if membership.joint || &membership.voters != target {
            return Err(Error::Unavailable);
        }
        let removed_at = membership.membership_index.ok_or(Error::Unavailable)?;
        for &id in previous.difference(target) {
            // If a failed source cannot respond, it still has a copy of the
            // old Raft state. Its membership has already been fenced.
            if group != METADATA || id != self.config.broker_id {
                let _ = self
                    .peer_operation::<()>(
                        id,
                        &Rpc::Retire {
                            group: group.into(),
                            removed_at,
                            fence,
                        },
                    )
                    .await;
            }
        }
        Ok(())
    }

    pub(super) async fn reopen(&self, restoring: bool) -> Result<()> {
        for id in self.broker.stored_group_ids()? {
            self.open_group(&id)
                .await
                .with_context(|| format!("open stored group {id}"))?;
        }
        let initial = &self.config.bootstrap.metadata_voters;
        if initial.contains(&self.config.broker_id) && !self.broker.was_retired(METADATA)? {
            let group = self.open_group(METADATA).await?;
            if initial.first() == Some(&self.config.broker_id)
                && !group.is_initialized()
                && (restoring
                    || self
                        .config
                        .initialize_new_cluster
                        .unwrap_or(self.config.allow_plaintext))
            {
                let addresses = initial
                    .iter()
                    .map(|id| (*id, self.bootstrap[id].peer_url.clone()))
                    .collect();
                group.initialize(addresses).await?;
            }
        }
        Ok(())
    }

    pub(super) async fn recover_seeded(self: Arc<Self>) -> Result<()> {
        // The restore CLI installs an identical backup cut on every assigned
        // voter before these groups form new Raft terms.
        let mut retry = tokio::time::interval(HEALTH_INTERVAL);
        let header = loop {
            retry.tick().await;
            if let Ok(header) = self.read_header().await {
                break header;
            }
        };
        let mut after = None;
        loop {
            let page = match self
                .meta_read(catalog::Query::Topics {
                    after: after.clone(),
                    limit: 4,
                })
                .await
            {
                Ok(catalog::Reply::Topics(page)) => page,
                Ok(_) => anyhow::bail!("unexpected topology page during restore"),
                Err(_) => {
                    retry.tick().await;
                    continue;
                }
            };
            if page.is_empty() {
                return Ok(());
            }
            for topic in page {
                for (part, placement) in topic.partitions.iter().enumerate() {
                    if placement.preferred != self.config.broker_id {
                        continue;
                    }
                    let id = partition_id(topic.id, part as u32);
                    let Some(group) = self.broker.group(&id) else {
                        continue;
                    };
                    if !group.is_initialized() {
                        let addresses = voter_addresses(&header, &placement.voters)?;
                        loop {
                            match group.initialize(addresses.clone()).await {
                                Ok(()) => break,
                                Err(
                                    GroupError::Unavailable
                                    | GroupError::OutcomeUnknown
                                    | GroupError::Overloaded,
                                ) => {
                                    retry.tick().await;
                                }
                                Err(error) => return Err(error.into()),
                            }
                        }
                    }
                }
                after = Some(topic.name);
            }
        }
    }
}

fn voter_addresses(
    header: &catalog::Header,
    members: &BTreeSet<u64>,
) -> Result<BTreeMap<u64, String>, Error> {
    members
        .iter()
        .map(|id| {
            header
                .roster
                .get(id)
                .map(|entry| (*id, entry.peer_url.clone()))
                .ok_or(Error::NotFound)
        })
        .collect()
}
