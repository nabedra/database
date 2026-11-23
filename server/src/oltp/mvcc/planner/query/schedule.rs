use super::*;
use crate::{
    oltp::{pool::WorkerSharedResources, read_attempt::UnsettledRead},
    scheduling::Reservation,
};
use nabedra_oltp_types::authority::MAX_READ_GRANTS;
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::{
    future::Future,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::task::JoinHandle;

// Keep fan-out within the local Storage transport lane, which also serves
// independent requests and other participant operations.
const MAX_SCAN_PAGES: usize = MAX_READ_GRANTS;
const PAGE_METADATA_BYTES: u64 = 16 << 10;

#[derive(Clone)]
pub(crate) struct QueryScheduler {
    width: usize,
    recovering: Arc<AtomicBool>,
    active: Arc<Mutex<BTreeMap<(Uuid, Uuid), Active>>>,
}

impl QueryScheduler {
    pub(crate) fn new(
        max_workers: u64,
        shared: &WorkerSharedResources,
    ) -> Result<Self, ProtocolError> {
        let width = usize::try_from(max_workers.saturating_sub(1).max(1))
            .map_err(|_| unavailable())?
            .min(MAX_SCAN_PAGES);
        shared
            .scheduler()
            .set_io_capacity(shared.scheduler_node_id(), MAX_SCAN_PAGES as u64)?;
        Ok(Self {
            width,
            active: Arc::new(Mutex::new(BTreeMap::new())),
            recovering: Arc::new(AtomicBool::new(false)),
        })
    }

    pub(super) fn begin(
        &self,
        request: RequestBinding,
        shared: &Arc<WorkerSharedResources>,
    ) -> Result<Arc<QueryExecution>, ProtocolError> {
        self.recover();
        let id = (request.run_id, request.request_id);
        let job = shared.next_native_request_id()?;
        let owner = shared.reserve_scratch(ByteClass::Data, PAGE_METADATA_BYTES as usize)?;
        let mut active = self.active.lock().map_err(|_| unavailable())?;
        if active.contains_key(&id) {
            return Err(failure(
                ErrorCode::Overloaded,
                "a prior scan of this request is still settling",
            ));
        }
        active.insert(
            id,
            Active {
                running: true,
                outstanding: Vec::new(),
            },
        );
        Ok(Arc::new(QueryExecution {
            id,
            job,
            active: self.active.clone(),
            shared: shared.clone(),
            _owner: owner,
        }))
    }

    /// Try another exact settlement when new work arrives. The quarantined
    /// grants stay owned even if this reaper or its transport is cancelled.
    fn recover(&self) {
        let Ok(active) = self.active.lock() else {
            return;
        };
        if !active.values().any(|query| !query.outstanding.is_empty())
            || self.recovering.swap(true, Ordering::Acquire)
        {
            return;
        }
        let recovering = Recovery(self.recovering.clone());
        let outstanding: Vec<_> = active
            .iter()
            .flat_map(|(&id, active)| {
                active
                    .outstanding
                    .iter()
                    .map(move |read| (id, read.clone()))
            })
            .collect();
        drop(active);
        let active = self.active.clone();
        nabedra_rt::task::spawn(async move {
            let _recovering = recovering;
            for (id, pending) in outstanding {
                if !pending.read.settle().await {
                    continue;
                }
                #[cfg(test)]
                pending.read.record("coordinator-confirmed");
                let mut active = active
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(query) = active.get_mut(&id) {
                    query
                        .outstanding
                        .retain(|other| !Arc::ptr_eq(other, &pending));
                    if !query.running && query.outstanding.is_empty() {
                        active.remove(&id);
                    }
                }
            }
        });
    }

    pub(super) fn pages<T>(&self, parallel: bool) -> Pages<T> {
        Pages {
            width: if parallel { self.width } else { 1 },
            jobs: FuturesUnordered::new(),
        }
    }
}

struct Outstanding {
    read: UnsettledRead,
    _owner: Reservation,
}

struct Active {
    running: bool,
    outstanding: Vec<Arc<Outstanding>>,
}

struct Recovery(Arc<AtomicBool>);
impl Drop for Recovery {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

pub(super) struct QueryExecution {
    id: (Uuid, Uuid),
    job: u64,
    active: Arc<Mutex<BTreeMap<(Uuid, Uuid), Active>>>,
    shared: Arc<WorkerSharedResources>,
    _owner: ScratchLease,
}
impl QueryExecution {
    pub(super) async fn reserve_page(&self) -> Result<Reservation, ProtocolError> {
        nabedra_rt::time::timeout(
            std::time::Duration::from_secs(10),
            self.shared
                .scheduler()
                .reserve_remote_memory(self.job, 1, PAGE_METADATA_BYTES),
        )
        .await
        .map_err(|_| unavailable())?
    }

    pub(super) fn quarantine(&self, read: UnsettledRead, owner: Reservation) {
        #[cfg(test)]
        read.record("coordinator-quarantined");
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        active
            .get_mut(&self.id)
            .expect("query owns its active entry")
            .outstanding
            .push(Arc::new(Outstanding {
                read,
                _owner: owner,
            }));
    }
}
impl Drop for QueryExecution {
    fn drop(&mut self) {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(query) = active.get_mut(&self.id) {
            query.running = false;
            if query.outstanding.is_empty() {
                active.remove(&self.id);
            }
        }
    }
}

pub(super) struct Pages<T> {
    width: usize,
    jobs: FuturesUnordered<JoinHandle<Result<T, PlanError>>>,
}

impl<T: Send + 'static> Pages<T> {
    pub(super) fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    pub(super) fn can_start(&self) -> bool {
        self.jobs.len() < self.width
    }

    pub(super) fn start(
        &mut self,
        page: impl Future<Output = Result<T, PlanError>> + Send + 'static,
    ) {
        self.jobs.push(nabedra_rt::task::spawn(page));
    }

    pub(super) async fn next(&mut self) -> Result<T, PlanError> {
        self.jobs
            .next()
            .await
            .expect("at least one Data page is in flight")
            .map_err(|_| unavailable())?
    }

    pub(super) async fn settle(&mut self) {
        while self.jobs.next().await.is_some() {}
    }
}

fn unavailable() -> ProtocolError {
    failure(ErrorCode::StorageUnavailable, "Data scan could not settle")
}

#[cfg(test)]
mod tests {
    use super::*;
    use nabedra_resources::config::WorkerResources;
    use std::task::Poll;
    use tokio::sync::{Notify, oneshot};
    const DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

    fn shared() -> Arc<WorkerSharedResources> {
        WorkerSharedResources::new(1, WorkerResources::default(), Default::default()).unwrap()
    }

    fn request() -> RequestBinding {
        let digest = nabedra_oltp_types::types::Digest::from_bytes([1; 32]);
        RequestBinding {
            run_id: nabedra_rt::ids::uuid_v4(),
            request_id: nabedra_rt::ids::uuid_v4(),
            digest,
            accepted_context: digest,
        }
    }

    #[tokio::test]
    async fn o22_c04_c06_delayed_page_refills_under_caps_one_two_and_three() {
        for cap in 1..=3 {
            nabedra_rt::time::timeout(DEADLINE, async {
                let shared = shared();
                let baseline = shared.scheduler().usage();
                let scheduler = QueryScheduler::new(cap, &shared).unwrap();
                let run = scheduler.begin(request(), &shared).unwrap();
                let mut pages = scheduler.pages(true);
                let (entered, held) = oneshot::channel();
                let (release, released) = oneshot::channel();
                let delayed = run.clone();
                pages.start(async move {
                    let _grant = delayed.reserve_page().await?;
                    entered.send(()).unwrap();
                    released.await.unwrap();
                    Ok(0)
                });
                held.await.unwrap();
                assert_eq!(shared.scheduler().usage()[0].1, 0);
                let mut seen = Vec::new();
                if cap == 3 {
                    for partition in 1..100 {
                        assert!(pages.can_start());
                        let next = run.clone();
                        pages.start(async move {
                            let _grant = next.reserve_page().await?;
                            Ok(partition)
                        });
                        assert!(!pages.can_start());
                        seen.push(pages.next().await.unwrap());
                        assert_eq!(seen.last(), Some(&partition));
                    }
                } else {
                    assert!(!pages.can_start());
                }
                release.send(()).unwrap();
                seen.push(pages.next().await.unwrap());
                if cap < 3 {
                    for partition in 1..100 {
                        let next = run.clone();
                        pages.start(async move {
                            let _grant = next.reserve_page().await?;
                            Ok(partition)
                        });
                        seen.push(pages.next().await.unwrap());
                    }
                }
                seen.sort_unstable();
                assert_eq!(seen, (0..100).collect::<Vec<_>>());
                drop(run);
                assert_eq!(shared.scheduler().usage(), baseline);
            })
            .await
            .expect("query scheduling watchdog");
        }
    }

    #[tokio::test]
    async fn o22_c05_c07_m02_m03_f03_cancelled_receiver_leaves_grant_with_job() {
        nabedra_rt::time::timeout(DEADLINE, async {
            let shared = shared();
            let baseline = shared.scheduler().usage();
            let scheduler = QueryScheduler::new(3, &shared).unwrap();
            let identity = request();
            let run = scheduler.begin(identity, &shared).unwrap();
            let mut pages = scheduler.pages(true);
            let released = Arc::new(Notify::new());
            let worker = released.clone();
            let (entered, held) = oneshot::channel();
            let (settled, done) = oneshot::channel();
            pages.start(async move {
                let grant = run.reserve_page().await?;
                entered.send(()).unwrap();
                worker.notified().await;
                drop((grant, run));
                settled.send(()).unwrap();
                Ok(())
            });
            held.await.unwrap();
            drop(pages);
            assert_eq!(
                shared.scheduler().usage()[0].2,
                baseline[0].2 + 2 * PAGE_METADATA_BYTES
            );
            assert!(scheduler.begin(identity, &shared).is_err());
            let cpu = shared.scheduler().reserve_on(9, 1, 1, 0).await.unwrap();
            let blocked = shared.scheduler().usage()[0].2;
            let available = WorkerResources::default().working_bytes() - blocked;
            let pressure = shared.scheduler().charge_memory(1, available).unwrap();
            let wiring = shared.scheduler();
            let mut waiting = Box::pin(wiring.reserve_remote_memory(10, 1, 1024));
            assert!(
                std::future::poll_fn(|cx| Poll::Ready(waiting.as_mut().poll(cx).is_pending()))
                    .await
            );
            drop(pressure);
            drop(waiting.await.unwrap());
            drop(cpu);
            released.notify_one();
            done.await.unwrap();
            drop(scheduler.begin(identity, &shared).unwrap());
            assert_eq!(shared.scheduler().usage(), baseline);
        })
        .await
        .expect("query settlement watchdog");
    }
}
