//! R07 owns issuance after configured backend/catalog validation settles.
use super::auth::ReceiverIdentity;
use std::sync::Arc;
use uuid::Uuid;
/// Configuration evidence for one retained boot/init. No production issuer yet.
pub(super) struct InitializedWorker {
    boot: Arc<ReceiverIdentity>,
    initialization: Uuid,
}
impl std::fmt::Debug for InitializedWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InitializedWorker")
    }
}
impl InitializedWorker {
    pub(super) async fn validate(
        boot: Arc<ReceiverIdentity>,
        initialization: Uuid,
        ready: Arc<crate::cluster::startup_material::composition::BootstrapReady>,
        storage: Arc<crate::storage::Storage<crate::storage::HttpStore>>,
        pool: &crate::worker_pool::WorkerPool,
    ) -> super::http::Result<Self> {
        super::http::cpu(pool, move |runtime| {
            let control =
                runtime
                    .block_on(storage.load_control(
                        tokio::time::Instant::now() + std::time::Duration::from_secs(10),
                    ))
                    .map_err(|_| super::http::unavailable())?;
            let root = control.root();
            let local = &ready.prepared.input.value;
            if root.cluster_id != local.cluster_id
                || root.config.storage_fingerprint != ready.backend.storage.as_str()
                || root.template.execution != local.template.execution
                || root
                    .template
                    .partition_size
                    .parse::<crate::partition_size::PartitionSize>()
                    .map_err(|_| super::http::invalid())?
                    != local
                        .template
                        .partition_size
                        .parse::<crate::partition_size::PartitionSize>()
                        .map_err(|_| super::http::invalid())?
            {
                return Err(super::http::invalid());
            }
            let member = boot
                .check_current(root)
                .map_err(|_| super::http::invalid())?;
            if let Some(resources) = member.resources {
                if resources != local.template.worker_resources()
                    || member.partition_slots
                        != super::queue_runtime::job_coordination_capacity(&local.template)? as u64
                    || member.operation_slots
                        != super::queue_runtime::coordination_capacity(&local.template)? as u64
                {
                    return Err(super::http::invalid());
                }
            } else if local.template.resources.is_some() {
                return Err(super::http::invalid());
            }
            runtime
                .block_on(storage.load_cluster_catalog())
                .map_err(|_| super::http::unavailable())?;
            Ok(Self {
                boot,
                initialization,
            })
        })
        .await
    }
    pub(super) fn matches(&self, boot: &ReceiverIdentity, initialization: Uuid) -> bool {
        self.boot.same_boot(boot) && self.initialization == initialization
    }
    #[cfg(test)]
    fn issue_for_test(boot: Arc<ReceiverIdentity>, initialization: Uuid) -> Self {
        Self {
            boot,
            initialization,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        node::{
            admission::{ReceiverState, distributed_fixture},
            payload_owner::PayloadOwners,
            spool::SpoolLimits,
        },
        worker_pool::WorkerPool,
    };
    use std::{num::NonZeroUsize, sync::Mutex, time::Duration};
    const LIMIT: Duration = Duration::from_secs(10);
    #[tokio::test]
    async fn configured_identity_and_original_process_are_both_required() {
        for case in 0..5 {
            let state = Arc::new(Mutex::new(distributed_fixture(1, 1, 0)));
            let boot = state.lock().unwrap().local().unwrap().boot();
            let foreign = distributed_fixture(1, 1, 0).local().unwrap().boot();
            let (pool, threads) = WorkerPool::start(
                NonZeroUsize::new(1).unwrap(),
                tokio::runtime::Handle::current(),
            )
            .unwrap();
            let owners = PayloadOwners::from_receiver(pool, state.clone());
            let path = std::env::temp_dir().join(format!("meshdb-c1-{}", Uuid::new_v4()));
            std::fs::create_dir(&path).unwrap();
            let mut process = owners
                .start_boot(
                    &boot,
                    Uuid::from_u128(30),
                    path.clone(),
                    SpoolLimits::new(4096, 2).unwrap(),
                )
                .unwrap();
            let receipt = tokio::time::timeout(LIMIT, process.process_ready()).await;
            let mut outcome = None;
            let mut duplicate = None;
            let mut redacted = None;
            if let Ok(Ok(receipt)) = receipt {
                let configured = InitializedWorker::issue_for_test(
                    if case == 1 { foreign } else { boot.clone() },
                    Uuid::from_u128(if case == 2 { 31 } else { 30 }),
                );
                redacted = Some(format!("{configured:?}"));
                if case == 3 {
                    drop(process.ready());
                }
                if case == 4 {
                    let mut legacy = ReceiverState::legacy(
                        crate::node::admission::distributed_fixture(1, 1, 0).payload,
                    );
                    outcome = Some(legacy.complete_initialization(receipt, configured));
                } else {
                    outcome = Some(
                        state
                            .lock()
                            .unwrap()
                            .complete_initialization(receipt, configured),
                    );
                }
                duplicate = Some(
                    tokio::time::timeout(LIMIT, process.process_ready())
                        .await
                        .is_ok_and(|v| v.is_err()),
                );
            }
            let retained = state.lock().unwrap().snapshot_locked().payload.accepted;
            let initialized = state
                .lock()
                .unwrap()
                .snapshot_locked()
                .local
                .unwrap()
                .initialized();
            drop(process);
            let drained = tokio::time::timeout(LIMIT, owners.close_and_drain()).await;
            let joined = tokio::time::timeout(LIMIT, threads.shutdown()).await;
            let empty = std::fs::read_dir(&path).unwrap().count() == 0;
            std::fs::remove_dir(path).unwrap();
            assert!(matches!(drained, Ok(Ok(()))) && matches!(joined, Ok(Ok(()))) && empty);
            assert_eq!(duplicate, Some(true));
            assert_eq!(retained, 1);
            assert_eq!(redacted.as_deref(), Some("InitializedWorker"));
            assert_eq!(initialized, case == 0);
            assert_eq!(
                outcome,
                Some(if case == 0 {
                    Ok(())
                } else {
                    Err(crate::node::admission::Refusal::Stale)
                })
            );
        }
    }
}

#[cfg(test)]
#[test]
fn checked_capability_is_opaque() {
    trait NoClone<M> {
        fn marker() {}
    }
    impl<T> NoClone<()> for T {}
    impl<T: Clone> NoClone<u8> for T {}
    trait NoSerialize<M> {
        fn marker() {}
    }
    impl<T> NoSerialize<()> for T {}
    impl<T: serde::Serialize> NoSerialize<u8> for T {}
    trait NoDeserialize<M> {
        fn marker() {}
    }
    impl<T> NoDeserialize<()> for T {}
    impl<T: serde::Deserialize<'static>> NoDeserialize<u8> for T {}
    let _ = <InitializedWorker as NoClone<_>>::marker;
    let _ = <InitializedWorker as NoSerialize<_>>::marker;
    let _ = <InitializedWorker as NoDeserialize<_>>::marker;
}
