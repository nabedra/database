use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Result, ensure};
use serde::Deserialize;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, oneshot},
    time::{Instant, sleep, timeout_at},
};

use super::{
    ApiError, Backend, BackendError, PartitionRoute, now_ms,
    partition::{Command, MAX_BATCH_BYTES, MAX_BATCH_EVENTS, PublishEvent, Published, Reply},
};

const MAX_LANES: usize = 1024;
const MAX_BUCKETS: usize = 65_536;
const TENANT_IDLE_TTL: Duration = Duration::from_secs(300);

#[derive(Clone, Copy, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HttpLimits {
    pub publish_in_flight: usize,
    pub read_in_flight: usize,
    pub control_in_flight: usize,
    pub max_partition_queues: usize,
    pub max_pending_per_partition: usize,
    pub max_tenants: usize,
    pub tenant_bytes_per_second: u32,
    pub tenant_burst_bytes: u32,
    pub coalesce_ms: u64,
    pub request_timeout_ms: u64,
    pub lease_ms: i64,
}

impl Default for HttpLimits {
    fn default() -> Self {
        Self {
            publish_in_flight: 64,
            read_in_flight: 32,
            control_in_flight: 32,
            max_partition_queues: 32,
            max_pending_per_partition: 16,
            max_tenants: 4096,
            tenant_bytes_per_second: 8 * 1024 * 1024,
            tenant_burst_bytes: 16 * 1024 * 1024,
            coalesce_ms: 2,
            request_timeout_ms: 5_000,
            lease_ms: 30_000,
        }
    }
}

impl HttpLimits {
    pub(super) fn validate(self) -> Result<Self> {
        ensure!(
            (1..=MAX_LANES).contains(&self.publish_in_flight)
                && (1..=MAX_LANES).contains(&self.read_in_flight)
                && (1..=MAX_LANES).contains(&self.control_in_flight)
                && (1..=MAX_LANES).contains(&self.max_partition_queues)
                && (1..=MAX_LANES).contains(&self.max_pending_per_partition),
            "invalid Events request or publish queue limits"
        );
        ensure!(
            (1..=MAX_BUCKETS).contains(&self.max_tenants)
                && self.tenant_bytes_per_second > 0
                && self.tenant_burst_bytes > 0,
            "invalid Events per-tenant byte rate"
        );
        ensure!(
            (100..=60_000).contains(&self.request_timeout_ms)
                && self.coalesce_ms <= 25
                && (1..=super::partition::MAX_LEASE_MS).contains(&self.lease_ms),
            "invalid Events request, coalescing, or lease timeout"
        );
        Ok(self)
    }
}

#[derive(Clone, Copy)]
pub(super) enum Lane {
    Publish = 0,
    Read = 1,
    Control = 2,
}

pub(super) struct Admission {
    pub limits: HttpLimits,
    lanes: [Arc<Semaphore>; 3],
    tenants: Mutex<HashMap<String, Tenant>>,
}

struct Tenant {
    last_seen: Instant,
    buckets: [Bucket; 3],
}

struct Bucket {
    last_refill: Instant,
    remaining_bytes: f64,
}

impl Admission {
    pub fn new(limits: HttpLimits) -> Self {
        Self {
            limits,
            lanes: [
                Arc::new(Semaphore::new(limits.publish_in_flight)),
                Arc::new(Semaphore::new(limits.read_in_flight)),
                Arc::new(Semaphore::new(limits.control_in_flight)),
            ],
            tenants: Mutex::new(HashMap::new()),
        }
    }

    pub fn admit(&self, lane: Lane) -> Result<Arc<OwnedSemaphorePermit>, ApiError> {
        Ok(Arc::new(
            self.lanes[lane as usize]
                .clone()
                .try_acquire_owned()
                .map_err(|_| BackendError::Overloaded)?,
        ))
    }

    pub fn deadline(&self) -> Instant {
        Instant::now() + Duration::from_millis(self.limits.request_timeout_ms)
    }

    pub fn charge(&self, tenant: &str, lane: Lane, bytes: usize) -> Result<(), ApiError> {
        let now = Instant::now();
        let mut tenants = self.tenants.lock().map_err(|_| BackendError::Internal)?;
        let burst = f64::from(self.limits.tenant_burst_bytes);
        let rate = f64::from(self.limits.tenant_bytes_per_second);

        if !tenants.contains_key(tenant) && tenants.len() >= self.limits.max_tenants {
            // Wait for idle accounts to refill before reclaiming the entry. Eviction
            // cannot give a tenant extra burst capacity when it immediately returns.
            let ttl = TENANT_IDLE_TTL.max(Duration::from_secs_f64(burst / rate));
            tenants.retain(|_, account| now.duration_since(account.last_seen) < ttl);
            if tenants.len() >= self.limits.max_tenants {
                return Err(BackendError::Overloaded.into());
            }
        }

        let account = tenants.entry(tenant.into()).or_insert_with(|| Tenant {
            last_seen: now,
            buckets: std::array::from_fn(|_| Bucket {
                last_refill: now,
                remaining_bytes: burst,
            }),
        });
        account.last_seen = now;
        let bucket = &mut account.buckets[lane as usize];
        let earned = now.duration_since(bucket.last_refill).as_secs_f64() * rate;
        bucket.remaining_bytes = (bucket.remaining_bytes + earned).min(burst);
        bucket.last_refill = now;
        if bucket.remaining_bytes < bytes as f64 {
            return Err(BackendError::Overloaded.into());
        }
        bucket.remaining_bytes -= bytes as f64;
        Ok(())
    }
}

type PublishReceipt = Result<Published, ApiError>;

struct Pending {
    events: Vec<PublishEvent>,
    bytes: usize,
    deadline: Instant,
    reply: oneshot::Sender<PublishReceipt>,
    // After dispatch, a disconnected client must still occupy its queue slot
    // until the write settles or exhausts its deadline.
    _permit: Arc<OwnedSemaphorePermit>,
}

pub(super) struct PublishQueue {
    limits: HttpLimits,
    backend: Arc<dyn Backend>,
    partitions: Mutex<HashMap<PartitionRoute, VecDeque<Pending>>>,
}

impl PublishQueue {
    pub fn new(limits: HttpLimits, backend: Arc<dyn Backend>) -> Self {
        Self {
            limits,
            backend,
            partitions: Mutex::new(HashMap::new()),
        }
    }

    pub async fn submit(
        self: &Arc<Self>,
        route: PartitionRoute,
        events: Vec<PublishEvent>,
        bytes: usize,
        deadline: Instant,
        permit: Arc<OwnedSemaphorePermit>,
    ) -> PublishReceipt {
        let (reply, result) = oneshot::channel();
        {
            let mut partitions = self.partitions.lock().map_err(|_| BackendError::Internal)?;
            let spawn = !partitions.contains_key(&route);
            if spawn && partitions.len() >= self.limits.max_partition_queues {
                return Err(BackendError::Overloaded.into());
            }
            let queue = partitions.entry(route.clone()).or_default();
            if queue.len() >= self.limits.max_pending_per_partition {
                return Err(BackendError::Overloaded.into());
            }
            queue.push_back(Pending {
                events,
                bytes,
                deadline,
                reply,
                _permit: permit,
            });
            if spawn {
                let queue = self.clone();
                tokio::spawn(async move { queue.drain(route).await });
            }
        }
        timeout_at(deadline, result)
            .await
            .map_err(|_| ApiError::unknown())?
            .map_err(|_| ApiError::unknown())?
    }

    async fn drain(&self, route: PartitionRoute) {
        loop {
            sleep(Duration::from_millis(self.limits.coalesce_ms)).await;
            let mut pending = match self.take(&route) {
                Some(pending) => pending,
                None => return,
            };

            let now = Instant::now();
            pending.retain(|request| {
                // The response waiter already observed a timeout or disconnect.
                !request.reply.is_closed() && now < request.deadline
            });
            if pending.is_empty() {
                continue;
            }

            let now_ms = now_ms();
            let deadline = pending
                .iter()
                .map(|request| request.deadline)
                .max()
                .unwrap_or(now);
            let command = if pending.len() == 1 {
                Command::Publish {
                    now_ms,
                    events: std::mem::take(&mut pending[0].events),
                }
            } else {
                Command::PublishMany {
                    now_ms,
                    batches: pending
                        .iter_mut()
                        .map(|request| std::mem::take(&mut request.events))
                        .collect(),
                }
            };

            let response = timeout_at(deadline, self.backend.write(&route, command)).await;
            match response {
                Ok(Ok(Ok(Reply::Published(receipt)))) if pending.len() == 1 => {
                    let request = pending.remove(0);
                    let _ = request.reply.send(Ok(receipt));
                }
                Ok(Ok(Ok(Reply::Coalesced { results }))) if results.len() == pending.len() => {
                    for (request, result) in pending.into_iter().zip(results) {
                        let _ = request.reply.send(result.map_err(ApiError::from));
                    }
                }
                Ok(Ok(Err(error))) => fail(pending, error.into()),
                Ok(Err(BackendError::Internal)) | Ok(Ok(Ok(_))) | Err(_) => {
                    fail(pending, ApiError::unknown())
                }
                Ok(Err(error)) => fail(pending, error.into()),
            }
        }
    }

    fn take(&self, route: &PartitionRoute) -> Option<Vec<Pending>> {
        let mut partitions = self.partitions.lock().ok()?;
        let queue = partitions.get_mut(route)?;
        if queue.is_empty() {
            partitions.remove(route);
            return None;
        }

        let mut commands = Vec::new();
        let mut events = 0;
        let mut bytes = 2;

        while let Some(next) = queue.front() {
            let batch_bytes = next.bytes + usize::from(!commands.is_empty());
            if events + next.events.len() > MAX_BATCH_EVENTS
                || bytes + batch_bytes > MAX_BATCH_BYTES
            {
                break;
            }
            events += next.events.len();
            bytes += batch_bytes;
            commands.push(queue.pop_front().expect("front exists"));
        }

        Some(commands)
    }
}

fn fail(pending: Vec<Pending>, error: ApiError) {
    for request in pending {
        let _ = request.reply.send(Err(error.clone()));
    }
}
