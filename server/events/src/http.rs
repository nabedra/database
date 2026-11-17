//! Public HTTP routes. The backend resolves and dispatches requests to Raft groups.

mod admission;
mod auth;
mod error;
mod wire;

use std::{future::Future, pin::Pin, sync::Arc};

use axum::{
    Router,
    extract::{
        DefaultBodyLimit, Path, Query, Request, State,
        rejection::{PathRejection, QueryRejection},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use serde::{Deserialize, Serialize};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::{
    sync::OwnedSemaphorePermit,
    time::{Instant, timeout_at},
};
use uuid::Uuid;

use self::{
    admission::{Admission, Lane, PublishQueue},
    auth::{Access, Delivery, Session},
    wire::InputBatch,
};
use crate::partition::{
    self, Command, Event, Lease, MAX_BATCH_BYTES, MAX_EVENT_BYTES, MAX_LEASE_MS, MAX_PAGE_BYTES,
    MAX_PAGE_EVENTS, QueryReply, Reply, valid_name,
};

pub use admission::HttpLimits;
pub use auth::{
    Grant, GrantScope, Keyring, KeyringConfig, SigningKeyConfig, SigningKeysConfig,
    authorize_operator,
};
pub use error::ApiError;

const SMALL_BODY_BYTES: usize = 1024;
const CHECKPOINT_BODY_BYTES: usize = 4096;
const DISCOVERY_PAGE_COUNT: usize = 100;

pub type BackendFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, BackendError>> + Send + 'a>>;

pub trait Backend: Send + Sync + 'static {
    fn partitions<'a>(
        &'a self,
        topic: &'a str,
        after: Option<u32>,
        limit: usize,
    ) -> BackendFuture<'a, PartitionPage>;

    fn resolve<'a>(&'a self, topic: &'a str, partition: u32) -> BackendFuture<'a, PartitionRoute>;

    fn write<'a>(
        &'a self,
        route: &'a PartitionRoute,
        command: Command,
    ) -> BackendFuture<'a, partition::Mutation>;

    fn read<'a>(
        &'a self,
        route: &'a PartitionRoute,
        query: partition::Query,
    ) -> BackendFuture<'a, Result<QueryReply, partition::Error>>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionPage {
    pub topic_id: Uuid,
    pub partitions: Vec<u32>,
    pub next_after: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct PartitionRoute {
    pub topic_id: Uuid,
    pub partition: u32,
    pub group_id: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendError {
    NotFound,
    Unavailable,
    Overloaded,
    InsufficientStorage,
    OutcomeUnknown,
    Internal,
}

struct Api {
    admission: Admission,
    keyring: Arc<Keyring>,
    backend: Arc<dyn Backend>,
    publishes: Arc<PublishQueue>,
}

pub fn router(
    backend: Arc<dyn Backend>,
    keyring: Arc<Keyring>,
    limits: HttpLimits,
) -> anyhow::Result<Router> {
    let limits = limits.validate()?;
    let state = Arc::new(Api {
        admission: Admission::new(limits),
        keyring,
        publishes: Arc::new(PublishQueue::new(limits, backend.clone())),
        backend,
    });

    Ok(Router::new()
        .route("/v1/topics/{topic}/partitions", get(discover))
        .route(
            "/v1/topics/{topic}/partitions/{partition}/events",
            post(publish_one).layer(DefaultBodyLimit::max(MAX_EVENT_BYTES)),
        )
        .route(
            "/v1/topics/{topic}/partitions/{partition}/batches",
            post(publish_batch).layer(DefaultBodyLimit::max(MAX_BATCH_BYTES)),
        )
        .route(
            "/v1/topics/{topic}/partitions/{partition}/subscriptions/{name}/claim",
            post(claim).layer(DefaultBodyLimit::max(SMALL_BODY_BYTES)),
        )
        .route(
            "/v1/topics/{topic}/partitions/{partition}/subscriptions/{name}/renew",
            post(renew).layer(DefaultBodyLimit::max(SMALL_BODY_BYTES)),
        )
        .route(
            "/v1/topics/{topic}/partitions/{partition}/subscriptions/{name}/release",
            post(release).layer(DefaultBodyLimit::max(SMALL_BODY_BYTES)),
        )
        .route(
            "/v1/topics/{topic}/partitions/{partition}/subscriptions/{name}/events",
            get(poll),
        )
        .route(
            "/v1/topics/{topic}/partitions/{partition}/subscriptions/{name}/checkpoint",
            put(ack).layer(DefaultBodyLimit::max(CHECKPOINT_BODY_BYTES)),
        )
        .fallback(|| async { ApiError::from(BackendError::NotFound) })
        .method_not_allowed_fallback(|| async {
            ApiError::new(
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "method does not match the route",
            )
        })
        .with_state(state))
}

pub fn now_ms() -> i64 {
    let millis = OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
    i64::try_from(millis).unwrap_or(i64::MAX)
}

impl Api {
    async fn resolve(
        &self,
        topic: &str,
        partition: u32,
        deadline: Instant,
    ) -> Result<PartitionRoute, ApiError> {
        let route = read_before(deadline, self.backend.resolve(topic, partition)).await?;
        if route.partition != partition || route.topic_id.is_nil() {
            return Err(BackendError::Internal.into());
        }
        Ok(route)
    }

    async fn write(
        &self,
        route: &PartitionRoute,
        command: Command,
        deadline: Instant,
    ) -> Result<Reply, ApiError> {
        let result = timeout_at(deadline, self.backend.write(route, command))
            .await
            .map_err(|_| ApiError::unknown())?
            .map_err(|error| match error {
                BackendError::Internal => ApiError::unknown(),
                _ => error.into(),
            })?;
        Ok(result?)
    }

    fn authorize(
        &self,
        headers: &HeaderMap,
        access: Access<'_>,
        now: i64,
    ) -> Result<Grant, ApiError> {
        self.keyring.authorize(headers, access, now)
    }

    fn lease_ttl(&self, grant: &Grant, now: i64) -> Result<i64, ApiError> {
        let expires = self
            .keyring
            .session_expiry(now.saturating_add(self.admission.limits.lease_ms), grant);
        if expires <= now {
            return Err(BackendError::Unavailable.into());
        }
        Ok(expires - now)
    }

    fn lease_response(
        &self,
        route: &PartitionRoute,
        name: String,
        grant: &Grant,
        lease: Lease,
        permit: Arc<OwnedSemaphorePermit>,
    ) -> Result<Response, ApiError> {
        let now = now_ms();
        let expires_ms = self.keyring.session_expiry(lease.expires_ms, grant);
        let session = Session::new(route, name, grant.owner(), lease.generation);
        let credential = self
            .keyring
            .issue_lease(session, now, expires_ms)
            .map_err(|_| ApiError::unknown())?;
        let expires_at =
            OffsetDateTime::from_unix_timestamp_nanos(i128::from(expires_ms) * 1_000_000)
                .map_err(|_| ApiError::unknown())?
                .format(&Rfc3339)
                .map_err(|_| ApiError::unknown())?;
        let body = wire::encode(
            &LeaseResponse {
                lease: credential,
                expires_at,
            },
            CHECKPOINT_BODY_BYTES,
        )
        .map_err(|_| ApiError::unknown())?;
        Ok(wire::response(StatusCode::OK, body, permit))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DiscoveryQuery {
    #[serde(default)]
    after: Option<u32>,
    #[serde(default = "discovery_limit")]
    limit: usize,
}

fn discovery_limit() -> usize {
    DISCOVERY_PAGE_COUNT
}

async fn discover(
    State(api): State<Arc<Api>>,
    path: Result<Path<String>, PathRejection>,
    query: Result<Query<DiscoveryQuery>, QueryRejection>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let topic = read_path(path)?;
    check_name(&topic)?;
    let grant = api.authorize(&headers, Access::Discover(&topic), now_ms())?;
    let permit = api.admission.admit(Lane::Read)?;
    let deadline = api.admission.deadline();
    let DiscoveryQuery { after, limit } = read_query(query)?;
    check_limit(limit, DISCOVERY_PAGE_COUNT)?;
    let page = read_before(deadline, api.backend.partitions(&topic, after, limit)).await?;
    let body = wire::encode(&page, CHECKPOINT_BODY_BYTES)?;
    api.admission
        .charge(&grant.tenant, Lane::Read, body.len())?;
    Ok(wire::response(StatusCode::OK, body, permit))
}

async fn publish_one(
    State(api): State<Arc<Api>>,
    path: Result<Path<(String, u32)>, PathRejection>,
    request: Request,
) -> Result<Response, ApiError> {
    publish(api, read_path(path)?, request, true).await
}

async fn publish_batch(
    State(api): State<Arc<Api>>,
    path: Result<Path<(String, u32)>, PathRejection>,
    request: Request,
) -> Result<Response, ApiError> {
    publish(api, read_path(path)?, request, false).await
}

async fn publish(
    api: Arc<Api>,
    (topic, partition): (String, u32),
    request: Request,
    single: bool,
) -> Result<Response, ApiError> {
    check_name(&topic)?;
    let grant = api.authorize(request.headers(), Access::Publish(&topic), now_ms())?;
    let permit = api.admission.admit(Lane::Publish)?;
    let deadline = api.admission.deadline();
    let body = wire::read_body(request, deadline, false).await?;
    api.admission
        .charge(&grant.tenant, Lane::Publish, body.len())?;
    let input = if single {
        vec![
            serde_json::from_slice::<&serde_json::value::RawValue>(&body)
                .map_err(|_| ApiError::invalid("invalid JSON request"))?,
        ]
    } else {
        wire::decode::<InputBatch<'_>>(&body)?.events
    };
    let (events, bytes) = wire::publish_events(input)?;
    drop(body);
    let route = api.resolve(&topic, partition, deadline).await?;
    if now_ms() >= grant.expires_at_ms {
        return Err(ApiError::unauthorized());
    }
    let outcome = api
        .publishes
        .submit(route, events, bytes, deadline, permit.clone())
        .await?;
    let (status, body) = if single {
        let event = outcome.events.first().ok_or_else(ApiError::unknown)?;
        let status = if outcome.inserted == 0 {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        };
        (status, wire::encode(event, MAX_BATCH_BYTES))
    } else {
        (
            StatusCode::OK,
            wire::encode(
                &PublishedBatch {
                    events: outcome.events,
                },
                MAX_BATCH_BYTES,
            ),
        )
    };
    Ok(wire::response(
        status,
        body.map_err(|_| ApiError::unknown())?,
        permit,
    ))
}

#[derive(Serialize)]
struct PublishedBatch {
    events: Vec<Event>,
}

type SubscriptionPath = Result<Path<(String, u32, String)>, PathRejection>;

struct SubscriptionRequest {
    topic: String,
    partition: u32,
    name: String,
    grant: Grant,
    deadline: Instant,
    permit: Arc<OwnedSemaphorePermit>,
}

impl SubscriptionRequest {
    fn new(
        api: &Api,
        path: SubscriptionPath,
        headers: &HeaderMap,
        lane: Lane,
    ) -> Result<Self, ApiError> {
        let (topic, partition, name) = read_path(path)?;
        check_name(&topic)?;
        check_name(&name)?;
        let grant = api.authorize(headers, Access::Read(&topic, &name), now_ms())?;
        let deadline = api.admission.deadline();
        let permit = api.admission.admit(lane)?;
        Ok(Self {
            topic,
            partition,
            name,
            grant,
            deadline,
            permit,
        })
    }

    async fn resolve(&self, api: &Api) -> Result<PartitionRoute, ApiError> {
        api.resolve(&self.topic, self.partition, self.deadline)
            .await
    }

    fn session(
        &self,
        api: &Api,
        route: &PartitionRoute,
        headers: &HeaderMap,
        now: i64,
    ) -> Result<Session, ApiError> {
        if now >= self.grant.expires_at_ms {
            return Err(ApiError::unauthorized());
        }
        let session = api.keyring.lease(headers, now)?;
        if !session.matches(route, &self.name, &self.grant.owner()) {
            return Err(ApiError::lease_lost());
        }
        Ok(session)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Claim {
    claim_id: Uuid,
}

#[derive(Serialize)]
struct LeaseResponse {
    lease: String,
    expires_at: String,
}

async fn claim(
    State(api): State<Arc<Api>>,
    path: SubscriptionPath,
    request: Request,
) -> Result<Response, ApiError> {
    let sub = SubscriptionRequest::new(&api, path, request.headers(), Lane::Control)?;
    let body = wire::read_body(request, sub.deadline, false).await?;
    api.admission
        .charge(&sub.grant.tenant, Lane::Control, body.len())?;
    let Claim { claim_id } = wire::decode(&body)?;
    let route = sub.resolve(&api).await?;
    let now = now_ms();
    let ttl_ms = api.lease_ttl(&sub.grant, now)?;
    let command = Command::Claim {
        name: sub.name.clone(),
        owner: sub.grant.owner(),
        claim_id,
        now_ms: now,
        ttl_ms,
    };
    let Reply::Lease(lease) = api.write(&route, command, sub.deadline).await? else {
        return Err(ApiError::unknown());
    };
    api.lease_response(&route, sub.name, &sub.grant, lease, sub.permit)
}

async fn renew(
    State(api): State<Arc<Api>>,
    path: SubscriptionPath,
    request: Request,
) -> Result<Response, ApiError> {
    update_lease(api, path, request, true).await
}

async fn release(
    State(api): State<Arc<Api>>,
    path: SubscriptionPath,
    request: Request,
) -> Result<Response, ApiError> {
    update_lease(api, path, request, false).await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}

async fn update_lease(
    api: Arc<Api>,
    path: SubscriptionPath,
    request: Request,
    renew: bool,
) -> Result<Response, ApiError> {
    let sub = SubscriptionRequest::new(&api, path, request.headers(), Lane::Control)?;
    let headers = request.headers().clone();
    let body = wire::read_body(request, sub.deadline, true).await?;
    if !body.is_empty() {
        let _: Empty = wire::decode(&body)?;
    }
    api.admission
        .charge(&sub.grant.tenant, Lane::Control, body.len().max(1))?;

    let route = sub.resolve(&api).await?;
    let now = now_ms();
    let Session {
        owner, generation, ..
    } = sub.session(&api, &route, &headers, now)?;
    let command = if renew {
        Command::Renew {
            name: sub.name.clone(),
            owner,
            generation,
            now_ms: now,
            ttl_ms: api.lease_ttl(&sub.grant, now)?,
        }
    } else {
        Command::Release {
            name: sub.name.clone(),
            owner,
            generation,
            now_ms: now,
        }
    };

    match api.write(&route, command, sub.deadline).await? {
        Reply::Lease(lease) if renew => {
            api.lease_response(&route, sub.name, &sub.grant, lease, sub.permit)
        }
        Reply::Done if !renew => Ok(StatusCode::NO_CONTENT.into_response()),
        _ => Err(ApiError::unknown()),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PollQuery {
    #[serde(default = "poll_limit")]
    limit: usize,
}

fn poll_limit() -> usize {
    MAX_PAGE_EVENTS
}

#[derive(Serialize)]
struct PollResponse {
    after: String,
    events: Vec<Event>,
    delivery: Option<String>,
}

async fn poll(
    State(api): State<Arc<Api>>,
    path: SubscriptionPath,
    query: Result<Query<PollQuery>, QueryRejection>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let sub = SubscriptionRequest::new(&api, path, &headers, Lane::Read)?;
    let PollQuery { limit } = read_query(query)?;
    check_limit(limit, MAX_PAGE_EVENTS)?;
    let route = sub.resolve(&api).await?;
    let now = now_ms();
    let session = sub.session(&api, &route, &headers, now)?;
    let query = partition::Query::Poll {
        name: sub.name,
        owner: session.owner.clone(),
        generation: session.generation,
        now_ms: now,
        limit,
    };
    let result = read_before(sub.deadline, api.backend.read(&route, query)).await??;
    let QueryReply::Poll(poll) = result else {
        return Err(BackendError::Internal.into());
    };

    let delivery = if let Some(last) = poll.last {
        let now = now_ms();
        let expires = api
            .keyring
            .session_expiry(now.saturating_add(MAX_LEASE_MS), &sub.grant);
        let token = api
            .keyring
            .issue_delivery(
                Delivery {
                    session,
                    after: poll.after,
                    last,
                },
                now,
                expires,
            )
            .map_err(|_| BackendError::Unavailable)?;
        Some(token)
    } else {
        None
    };
    let body = wire::encode(
        &PollResponse {
            after: poll.after.to_string(),
            events: poll.events,
            delivery,
        },
        MAX_PAGE_BYTES,
    )?;
    api.admission
        .charge(&sub.grant.tenant, Lane::Read, body.len())?;
    Ok(wire::response(StatusCode::OK, body, sub.permit))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Ack {
    delivery: String,
    through: String,
}

#[derive(Serialize)]
struct Checkpoint {
    offset: String,
}

async fn ack(
    State(api): State<Arc<Api>>,
    path: SubscriptionPath,
    request: Request,
) -> Result<Response, ApiError> {
    let sub = SubscriptionRequest::new(&api, path, request.headers(), Lane::Control)?;
    let headers = request.headers().clone();
    let body = wire::read_body(request, sub.deadline, false).await?;
    api.admission
        .charge(&sub.grant.tenant, Lane::Control, body.len())?;
    let Ack { delivery, through } = wire::decode(&body)?;
    let through = wire::decode_offset(&through)?;

    let route = sub.resolve(&api).await?;
    let now = now_ms();
    let session = sub.session(&api, &route, &headers, now)?;
    let delivery = api.keyring.delivery(&delivery, now)?;
    if session != delivery.session {
        return Err(ApiError::invalid_delivery());
    }
    if through <= delivery.after || through > delivery.last {
        return Err(ApiError::invalid(
            "acknowledgment is outside the delivered prefix",
        ));
    }
    let command = Command::Ack {
        name: sub.name,
        owner: session.owner,
        generation: session.generation,
        after: delivery.after,
        last: delivery.last,
        through,
        now_ms: now,
    };
    let Reply::Checkpoint { offset } = api.write(&route, command, sub.deadline).await? else {
        return Err(ApiError::unknown());
    };
    let body = wire::encode(
        &Checkpoint {
            offset: offset.to_string(),
        },
        SMALL_BODY_BYTES,
    )
    .map_err(|_| ApiError::unknown())?;
    Ok(wire::response(StatusCode::OK, body, sub.permit))
}

async fn read_before<T>(deadline: Instant, read: BackendFuture<'_, T>) -> Result<T, ApiError> {
    Ok(timeout_at(deadline, read)
        .await
        .map_err(|_| BackendError::Unavailable)??)
}

fn read_path<T>(path: Result<Path<T>, PathRejection>) -> Result<T, ApiError> {
    path.map(|Path(path)| path)
        .map_err(|_| ApiError::invalid("invalid Events route"))
}

fn read_query<T>(query: Result<Query<T>, QueryRejection>) -> Result<T, ApiError> {
    query
        .map(|Query(query)| query)
        .map_err(|_| ApiError::invalid("invalid query parameters"))
}

fn check_name(name: &str) -> Result<(), ApiError> {
    if valid_name(name) {
        Ok(())
    } else {
        Err(ApiError::invalid("invalid topic or subscription name"))
    }
}

fn check_limit(limit: usize, maximum: usize) -> Result<(), ApiError> {
    if (1..=maximum).contains(&limit) {
        Ok(())
    } else {
        Err(ApiError::invalid("page limit is out of range"))
    }
}
