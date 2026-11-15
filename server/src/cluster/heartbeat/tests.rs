use super::*;
use crate::{storage::Namespace, test_storage::Fake};
use std::time::Duration;
fn binding() -> HeartbeatBinding {
    HeartbeatBinding {
        resources: None,
        cluster_id: Uuid::from_u128(1),
        node_id: Uuid::from_u128(2),
        incarnation: Uuid::from_u128(3),
        session_id: Uuid::from_u128(4),
        verifier: "ab".repeat(32),
        operation_slots: 2,
        partition_slots: 3,
        queue_size: 4,
    }
}
fn sample() -> HeartbeatSample {
    HeartbeatSample {
        available_threads: None,
        available_memory_bytes: None,
        running_operations: 2,
        running_partitions: 3,
        queued_operations: 4,
        queued_partitions: 4,
        drain_id: None,
        accepting: true,
    }
}
#[test]
fn c01_c02_availability_is_bound_to_advertised_resource_limits() {
    let mut binding = binding();
    binding.resources = Some(crate::config::WorkerResources {
        max_threads: 3,
        memory_limit_bytes: 1000,
        task_memory_limit_bytes: 800,
        control_memory_reserve_bytes: 100,
    });
    let mut sample = sample();
    sample.available_threads = Some(2);
    sample.available_memory_bytes = Some(700);
    let record = binding.record(&sample, 1);
    assert!(record.validate().is_ok());
    let decoded = Heartbeat::decode(&record.encode().unwrap()).unwrap();
    assert_eq!(decoded.available_threads, Some(2));
    assert_eq!(decoded.available_memory_bytes, Some(700));
    for (threads, bytes) in [
        (Some(4), Some(700)),
        (Some(2), Some(901)),
        (None, Some(700)),
    ] {
        sample.available_threads = threads;
        sample.available_memory_bytes = bytes;
        assert!(binding.record(&sample, 2).validate().is_err());
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
#[tokio::test]
async fn create_then_match_preserves_independent_capacities() {
    let storage = Storage::new(Fake::default(), Namespace::new("test").unwrap());
    let mut writer = HeartbeatWriter::new(binding()).unwrap();
    assert!(matches!(
        writer
            .publish(&storage, sample(), deadline())
            .await
            .unwrap(),
        HeartbeatWriteResult::Acknowledged(_)
    ));
    assert!(matches!(
        writer
            .publish(&storage, sample(), deadline())
            .await
            .unwrap(),
        HeartbeatWriteResult::Acknowledged(_)
    ));
    let state = storage.backend().state.lock().unwrap();
    assert_eq!(state.puts, 2);
    assert_eq!(state.gets, 0);
    let record = Heartbeat::decode(&state.objects.values().next().unwrap().0).unwrap();
    assert_eq!(
        (
            record.sequence,
            record.operation_slots,
            record.partition_slots,
            record.queue_size
        ),
        (2, 2, 3, 4)
    );
}

#[tokio::test]
async fn invalid_inputs_are_independent_and_pre_io() {
    let storage = Storage::new(Fake::default(), Namespace::new("test").unwrap());
    for index in 0..4 {
        let mut b = binding();
        match index {
            0 => b.operation_slots = 0,
            1 => b.partition_slots = 257,
            2 => b.queue_size = 4097,
            _ => b.verifier = "secret-invalid".into(),
        }
        assert!(matches!(
            HeartbeatWriter::new(b),
            Err(StorageError::InvalidCatalog)
        ));
    }
    let mut writer = HeartbeatWriter::new(binding()).unwrap();
    for index in 0..5 {
        let mut s = sample();
        match index {
            0 => s.running_operations = 3,
            1 => s.running_partitions = 4,
            2 => s.queued_operations = 5,
            3 => s.queued_partitions = 5,
            _ => s.drain_id = Some(Uuid::from_u128(9)),
        }
        assert!(matches!(
            writer.publish(&storage, s, deadline()).await,
            Err(StorageError::InvalidCatalog)
        ));
    }
    assert!(matches!(
        writer.publish(&storage, sample(), Instant::now()).await,
        Err(StorageError::Unavailable)
    ));
    let state = storage.backend().state.lock().unwrap();
    assert_eq!((state.puts, state.gets), (0, 0));
}

fn key() -> String {
    format!(
        "test/{}",
        ObjectKey::heartbeat(binding().incarnation).as_str()
    )
}
fn storage() -> Storage<Fake> {
    Storage::new(Fake::default(), Namespace::new("test").unwrap())
}
fn put_record(storage: &Storage<Fake>, record: Heartbeat, token: &str) {
    storage
        .backend()
        .state
        .lock()
        .unwrap()
        .objects
        .insert(key(), (record.encode().unwrap(), token.into()));
}
#[tokio::test]
async fn exact_readback_and_failed_reconciliation_never_mint_acknowledgement() {
    for failure in 0..4 {
        let storage = storage();
        {
            let mut state = storage.backend().state.lock().unwrap();
            state.lose_put_response = true;
            match failure {
                1 => state.read_failure = true,
                2 => {
                    state
                        .put_faults
                        .insert(key(), (StorageError::Ambiguous, false));
                }
                3 => state.race = Some(b"corrupt-secret".to_vec()),
                _ => {}
            }
        }
        let mut writer = HeartbeatWriter::new(binding()).unwrap();
        let outcome = writer
            .publish(&storage, sample(), deadline())
            .await
            .unwrap();
        if failure == 0 {
            assert!(matches!(outcome, HeartbeatWriteResult::ObservedExact));
        } else if failure == 3 {
            assert!(matches!(outcome, HeartbeatWriteResult::Conflict));
        } else {
            assert!(matches!(outcome, HeartbeatWriteResult::Unknown));
        }
        assert!(outcome.into_acknowledged().is_none());
        assert!(writer.reconcile);
        let state = storage.backend().state.lock().unwrap();
        assert_eq!((state.puts, state.gets), (1, 1));
    }
    let storage = storage();
    storage
        .backend()
        .state
        .lock()
        .unwrap()
        .put_faults
        .insert(key(), (StorageError::Unavailable, false));
    let mut writer = HeartbeatWriter::new(binding()).unwrap();
    assert!(matches!(
        writer.publish(&storage, sample(), deadline()).await,
        Err(StorageError::Unavailable)
    ));
    assert_eq!(storage.backend().state.lock().unwrap().gets, 0);
}

#[tokio::test]
async fn loaded_receipt_binding_bounds_and_sequence_exhaustion() {
    let storage = storage();
    assert!(
        load_heartbeat(&storage, &binding(), deadline())
            .await
            .unwrap()
            .is_none()
    );
    let mut writer = HeartbeatWriter::new(binding()).unwrap();
    let receipt = writer
        .publish(&storage, sample(), deadline())
        .await
        .unwrap()
        .into_acknowledged()
        .unwrap();
    assert_eq!(receipt.binding(), &binding());
    assert_eq!(receipt.sample(), &sample());
    assert_eq!(receipt.sequence(), 1);
    let loaded = load_heartbeat(&storage, &binding(), deadline())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.binding(), receipt.binding());
    assert_eq!(loaded.heartbeat().sequence, receipt.sequence());
    assert_eq!(
        format!("{writer:?} {receipt:?} {loaded:?} {:?}", binding()),
        "HeartbeatWriter AcknowledgedHeartbeat LoadedHeartbeat HeartbeatBinding"
    );
    for index in 0..6 {
        let mut record = binding().record(&sample(), 2);
        match index {
            0 => record.cluster_id = Uuid::from_u128(90),
            1 => record.node_id = Uuid::from_u128(90),
            2 => record.incarnation = Uuid::from_u128(90),
            3 => record.operation_slots = 3,
            4 => record.partition_slots = 4,
            _ => record.queue_size = 5,
        }
        put_record(&storage, record, "private-token");
        assert!(matches!(
            load_heartbeat(&storage, &binding(), deadline()).await,
            Err(StorageError::InvalidCatalog)
        ));
    }
    put_record(
        &storage,
        binding().record(&sample(), u64::MAX),
        "private-token",
    );
    writer.reconcile = true;
    assert!(matches!(
        writer
            .publish(&storage, sample(), deadline())
            .await
            .unwrap(),
        HeartbeatWriteResult::Exhausted
    ));
    assert_eq!(storage.backend().state.lock().unwrap().puts, 1);
    put_record(&storage, binding().record(&sample(), 2), "");
    assert!(matches!(
        load_heartbeat(&storage, &binding(), deadline()).await,
        Err(StorageError::Unavailable)
    ));
    storage.backend().state.lock().unwrap().objects.insert(
        key(),
        (vec![b' '; MAX_HEARTBEAT_BYTES + 1], "secret-token".into()),
    );
    assert!(matches!(
        load_heartbeat(&storage, &binding(), deadline()).await,
        Err(StorageError::LimitExceeded)
    ));
    storage
        .backend()
        .state
        .lock()
        .unwrap()
        .objects
        .insert(key(), (b"secret malformed".to_vec(), "secret-token".into()));
    let error = load_heartbeat(&storage, &binding(), deadline())
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "Invalid catalog");
    assert_eq!(format!("{error:?}"), "InvalidCatalog");
}

// The provider queue owns the original effect even after the borrowing publish
// future is dropped. No task is detached and no timeout is used as cancellation.
struct Effect {
    key: String,
    bytes: Vec<u8>,
    token: Option<String>,
}
struct Provider {
    fake: Fake,
    mode: std::sync::atomic::AtomicU8,
    effect: std::sync::Mutex<Option<Effect>>,
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
    attempts: std::sync::atomic::AtomicUsize,
    conditions: std::sync::Mutex<Vec<Option<String>>>,
}
impl Default for Provider {
    fn default() -> Self {
        Self {
            fake: Fake::default(),
            mode: std::sync::atomic::AtomicU8::new(0),
            effect: std::sync::Mutex::new(None),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
            attempts: std::sync::atomic::AtomicUsize::new(0),
            conditions: std::sync::Mutex::new(Vec::new()),
        }
    }
}
impl Provider {
    async fn settle(&self) -> Result<String, StorageError> {
        tokio::time::timeout(Duration::from_secs(5), self.release.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        let effect = self.effect.lock().unwrap().take().unwrap();
        self.fake
            .put(
                &effect.key,
                effect.bytes,
                effect
                    .token
                    .as_deref()
                    .map_or(WriteCondition::Create, WriteCondition::Match),
            )
            .await
    }
}
impl ObjectStore for Provider {
    async fn get(&self, key: &str) -> Result<crate::storage::Download, StorageError> {
        self.fake.get(key).await
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        use std::sync::atomic::Ordering::SeqCst;
        self.attempts.fetch_add(1, SeqCst);
        let token = match condition {
            WriteCondition::Create => None,
            WriteCondition::Match(token) => Some(token.to_owned()),
        };
        self.conditions.lock().unwrap().push(token.clone());
        let mode = self.mode.swap(0, SeqCst);
        if mode == 1 || mode == 2 {
            *self.effect.lock().unwrap() = Some(Effect {
                key: key.into(),
                bytes,
                token,
            });
            self.entered.notify_one();
            if mode == 2 {
                std::future::pending::<()>().await;
            }
            return Err(StorageError::Ambiguous);
        }
        if mode == 5 {
            let _ = self.settle().await;
        }
        let result = self.fake.put(key, bytes, condition).await?;
        if mode == 4 {
            std::future::pending::<()>().await;
        }
        if mode == 3 {
            Ok(String::new())
        } else {
            Ok(result)
        }
    }
}
#[tokio::test]
async fn retained_old_effect_competes_with_fresh_proposal_in_both_orders() {
    use std::sync::atomic::Ordering::SeqCst;
    for existing in [false, true] {
        for old_first in [false, true] {
            let storage = Storage::new(Provider::default(), Namespace::new("test").unwrap());
            let mut writer = HeartbeatWriter::new(binding()).unwrap();
            if existing {
                writer
                    .publish(&storage, sample(), deadline())
                    .await
                    .unwrap();
            }
            storage.backend().mode.store(1, SeqCst);
            let unknown = writer
                .publish(&storage, sample(), deadline())
                .await
                .unwrap();
            let attempted = writer.attempted;
            let after_unknown = storage.backend().attempts.load(SeqCst);

            // The first reload saw missing or lower bytes, while the owned old
            // condition is still eligible to commit. Settle before final assertions.
            storage.backend().release.add_permits(1);
            let mut fresh = sample();
            fresh.accepting = false;
            let (old, new) = if old_first {
                let old = storage.backend().settle().await;
                let new = writer.publish(&storage, fresh.clone(), deadline()).await;
                (old, new)
            } else {
                let new = writer.publish(&storage, fresh.clone(), deadline()).await;
                let old = storage.backend().settle().await;
                (old, new)
            };
            let new = new.unwrap();
            assert!(matches!(unknown, HeartbeatWriteResult::Unknown));
            assert!(matches!(new, HeartbeatWriteResult::Acknowledged(_)));
            assert_eq!(old.is_ok(), old_first);
            assert_eq!(storage.backend().attempts.load(SeqCst), after_unknown + 1);
            let state = storage.backend().fake.state.lock().unwrap();
            let h = Heartbeat::decode(&state.objects[&key()].0).unwrap();
            assert_eq!(h.sequence, attempted + 1);
            assert!(!h.accepting);
            assert_eq!(state.gets, 2);
            let conditions = storage.backend().conditions.lock().unwrap();
            if existing {
                assert!(conditions[1].as_ref().is_some_and(|s| s == "\"opaque-1\""));
            }
            assert_eq!(writer.confirmed.as_ref(), Some(&fresh));
        }
    }
}
#[tokio::test]
async fn dropped_borrowed_publish_preserves_high_water_and_pending_provider_effect() {
    use std::sync::atomic::Ordering::SeqCst;
    let storage = Storage::new(Provider::default(), Namespace::new("test").unwrap());
    let mut writer = HeartbeatWriter::new(binding()).unwrap();
    storage.backend().mode.store(2, SeqCst);
    {
        let future = writer.publish(&storage, sample(), deadline());
        tokio::pin!(future);
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! { result = &mut future => panic!("unexpected {result:?}"), () = storage.backend().entered.notified() => {} }
        }).await.unwrap();
    }
    // Keep assertions until the original owned effect has actually settled.
    let attempted = writer.attempted;
    let reconciliation = writer.reconcile;
    let outcome = writer.publish(&storage, sample(), deadline()).await;
    storage.backend().release.add_permits(1);
    let old = storage.backend().settle().await;
    let outcome = outcome.unwrap();
    assert_eq!(attempted, 1);
    assert!(reconciliation);
    assert!(matches!(outcome, HeartbeatWriteResult::Acknowledged(_)));
    assert!(matches!(old, Err(StorageError::Conflict)));
    assert_eq!(storage.backend().attempts.load(SeqCst), 2);
    assert_eq!(storage.backend().fake.state.lock().unwrap().gets, 1);
}

#[tokio::test]
async fn reload_budget_conflict_and_empty_acknowledgement() {
    use std::sync::atomic::Ordering::SeqCst;
    let storage = Storage::new(Provider::default(), Namespace::new("test").unwrap());
    let mut writer = HeartbeatWriter::new(binding()).unwrap();
    storage.backend().mode.store(3, SeqCst);
    let empty = writer
        .publish(&storage, sample(), deadline())
        .await
        .unwrap();
    assert!(matches!(empty, HeartbeatWriteResult::ObservedExact));
    assert!(empty.into_acknowledged().is_none());
    storage
        .backend()
        .fake
        .state
        .lock()
        .unwrap()
        .lose_put_response = true;
    let uncertain = writer
        .publish(&storage, sample(), deadline())
        .await
        .unwrap();
    assert!(matches!(uncertain, HeartbeatWriteResult::Unknown));
    assert!(uncertain.into_acknowledged().is_none());
    assert_eq!(storage.backend().fake.state.lock().unwrap().gets, 2);
    storage.backend().fake.state.lock().unwrap().read_failure = true;
    let failed = writer
        .publish(&storage, sample(), deadline())
        .await
        .unwrap();
    assert!(matches!(failed, HeartbeatWriteResult::Unknown));
    assert_eq!(storage.backend().attempts.load(SeqCst), 2);
    assert_eq!(storage.backend().fake.state.lock().unwrap().gets, 3);
    // A definite conditional conflict remains distinct even when its read fails.
    let storage = self::storage();
    put_record(&storage, binding().record(&sample(), 9), "private");
    storage.backend().state.lock().unwrap().read_failure = true;
    let mut writer = HeartbeatWriter::new(binding()).unwrap();
    let conflict = writer
        .publish(&storage, sample(), deadline())
        .await
        .unwrap();
    assert!(matches!(conflict, HeartbeatWriteResult::Conflict));
    assert!(conflict.into_acknowledged().is_none());
    assert_eq!(storage.backend().state.lock().unwrap().gets, 1);
}

struct FailedTail;
impl tokio::io::AsyncRead for FailedTail {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        _: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Err(std::io::Error::other("secret backend failure")))
    }
}
struct BrokenRead(Fake);
impl ObjectStore for BrokenRead {
    async fn get(&self, key: &str) -> Result<crate::storage::Download, StorageError> {
        use tokio::io::AsyncReadExt;
        let download = self.0.get(key).await?;
        Ok(crate::storage::Download::new(
            download.body.chain(FailedTail),
            download.etag,
        ))
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        self.0.put(key, bytes, condition).await
    }
}
#[tokio::test]
async fn complete_valid_prefix_requires_clean_eof() {
    let fake = Fake::default();
    fake.state.lock().unwrap().objects.insert(
        key(),
        (
            binding().record(&sample(), 1).encode().unwrap(),
            "secret-token".into(),
        ),
    );
    let storage = Storage::new(BrokenRead(fake), Namespace::new("test").unwrap());
    assert!(matches!(
        load_heartbeat(&storage, &binding(), deadline()).await,
        Err(StorageError::Unavailable)
    ));
    assert_eq!(storage.backend().0.state.lock().unwrap().gets, 1);
}
#[test]
fn receipts_and_writer_are_non_clone_and_readonly() {
    trait AmbiguousIfClone<A> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfClone<()> for T {}
    impl<T: Clone> AmbiguousIfClone<u8> for T {}
    let _ = <HeartbeatWriter as AmbiguousIfClone<_>>::marker;
    let _ = <AcknowledgedHeartbeat as AmbiguousIfClone<_>>::marker;
    let _ = <LoadedHeartbeat as AmbiguousIfClone<_>>::marker;
    let _: fn(&LoadedHeartbeat) -> &Heartbeat = LoadedHeartbeat::heartbeat;
    let _: fn(&LoadedHeartbeat) -> &HeartbeatBinding = LoadedHeartbeat::binding;
    let _: fn(&AcknowledgedHeartbeat) -> &HeartbeatBinding = AcknowledgedHeartbeat::binding;
    let _: fn(&AcknowledgedHeartbeat) -> &HeartbeatSample = AcknowledgedHeartbeat::sample;
}

#[tokio::test]
async fn old_write_wins_after_fresh_condition_capture_and_late_ack_has_no_receipt() {
    use std::sync::atomic::Ordering::SeqCst;
    for existing in [false, true] {
        let storage = Storage::new(Provider::default(), Namespace::new("test").unwrap());
        let mut writer = HeartbeatWriter::new(binding()).unwrap();
        if existing {
            writer
                .publish(&storage, sample(), deadline())
                .await
                .unwrap();
        }
        storage.backend().mode.store(1, SeqCst);
        let first = writer
            .publish(&storage, sample(), deadline())
            .await
            .unwrap();
        let first_sequence = writer.attempted;
        // Next PUT evaluates the retained old condition after the new reload.
        storage.backend().release.add_permits(1);
        storage.backend().mode.store(5, SeqCst);
        let second = writer
            .publish(&storage, sample(), deadline())
            .await
            .unwrap();
        let state_counts = {
            let state = storage.backend().fake.state.lock().unwrap();
            (state.gets, state.puts)
        };
        let third = writer
            .publish(&storage, sample(), deadline())
            .await
            .unwrap();
        assert!(matches!(first, HeartbeatWriteResult::Unknown));
        assert!(matches!(second, HeartbeatWriteResult::Conflict));
        assert_eq!(state_counts, (2, if existing { 3 } else { 2 }));
        assert_eq!(
            third.into_acknowledged().unwrap().sequence(),
            first_sequence + 2
        );
        assert!(storage.backend().effect.lock().unwrap().is_none());
    }
    let storage = Storage::new(Provider::default(), Namespace::new("test").unwrap());
    let mut writer = HeartbeatWriter::new(binding()).unwrap();
    storage.backend().mode.store(4, SeqCst);
    // Provider applies the bytes but withholds its response beyond the invocation
    // deadline. This timeout makes no claim that provider application stopped.
    let late = writer
        .publish(
            &storage,
            sample(),
            Instant::now() + Duration::from_millis(10),
        )
        .await
        .unwrap();
    assert!(matches!(late, HeartbeatWriteResult::Unknown));
    assert!(late.into_acknowledged().is_none());
    assert!(writer.reconcile);
    let state = storage.backend().fake.state.lock().unwrap();
    assert_eq!((state.puts, state.gets), (1, 0));
    assert_eq!(
        Heartbeat::decode(&state.objects[&key()].0)
            .unwrap()
            .sequence,
        1
    );
}

#[tokio::test]
async fn definite_max_conflict_exhausts_while_ambiguous_max_remains_unknown() {
    use std::sync::atomic::Ordering::SeqCst;
    for definite_conflict in [true, false] {
        let storage = Storage::new(Provider::default(), Namespace::new("test").unwrap());
        let baseline = u64::MAX - if definite_conflict { 2 } else { 1 };
        storage.backend().fake.state.lock().unwrap().objects.insert(
            key(),
            (
                binding().record(&sample(), baseline).encode().unwrap(),
                "prior-generation".into(),
            ),
        );
        let mut writer = HeartbeatWriter::new(binding()).unwrap();
        // Start at a complete retained record near exhaustion without issuing
        // billions of writes. All subsequent conditions use the real Fake CAS.
        writer.reconcile = true;
        storage.backend().mode.store(1, SeqCst);
        let first = writer.publish(&storage, sample(), deadline()).await;
        if definite_conflict {
            // The older MAX-1 effect wins only after MAX captures its condition.
            storage.backend().release.add_permits(1);
            storage.backend().mode.store(5, SeqCst);
        }
        let second = writer.publish(&storage, sample(), deadline()).await;
        let attempts_before = storage.backend().attempts.load(SeqCst);
        let repeated = writer.publish(&storage, sample(), deadline()).await;
        let attempts_after = storage.backend().attempts.load(SeqCst);
        let pending_before_settlement = writer.pending.as_ref().map(|h| h.sequence);
        let counts_before_settlement = {
            let state = storage.backend().fake.state.lock().unwrap();
            (state.gets, state.puts)
        };

        // A genuinely ambiguous MAX still owns an unapplied provider effect.
        // Settle it before assertions; the next reload may then prove exhaustion.
        let settled = if definite_conflict {
            None
        } else {
            storage.backend().release.add_permits(1);
            Some(storage.backend().settle().await)
        };
        let resolved = writer.publish(&storage, sample(), deadline()).await;
        let final_attempts = storage.backend().attempts.load(SeqCst);
        let no_effect = storage.backend().effect.lock().unwrap().is_none();

        assert!(matches!(first.unwrap(), HeartbeatWriteResult::Unknown));
        if definite_conflict {
            assert!(matches!(second.unwrap(), HeartbeatWriteResult::Conflict));
            assert!(matches!(repeated.unwrap(), HeartbeatWriteResult::Exhausted));
            assert_eq!(pending_before_settlement, None);
            assert_eq!(counts_before_settlement, (3, 2));
            assert_eq!(attempts_before, 2);
        } else {
            assert!(matches!(second.unwrap(), HeartbeatWriteResult::Unknown));
            assert!(matches!(repeated.unwrap(), HeartbeatWriteResult::Unknown));
            assert_eq!(pending_before_settlement, Some(u64::MAX));
            assert_eq!(counts_before_settlement, (3, 0));
            assert_eq!(attempts_before, 1);
            assert!(settled.unwrap().is_ok());
        }
        assert!(matches!(resolved.unwrap(), HeartbeatWriteResult::Exhausted));
        assert_eq!(attempts_after, attempts_before);
        assert_eq!(final_attempts, attempts_before);
        assert_eq!(writer.attempted, u64::MAX);
        assert!(writer.reconcile);
        assert!(no_effect);
    }
}

struct ReconciliationFault {
    fake: Fake,
    fault: u8,
}
impl ObjectStore for ReconciliationFault {
    async fn get(&self, key: &str) -> Result<crate::storage::Download, StorageError> {
        use tokio::io::AsyncReadExt;
        let mut download = self.fake.get(key).await?;
        match self.fault {
            0 => download.etag.clear(),
            1 => download.body = Box::pin(std::io::Cursor::new(b"private corrupt record".to_vec())),
            2 => {
                let mut bytes = Vec::new();
                download.body.read_to_end(&mut bytes).await.unwrap();
                let mut record = Heartbeat::decode(&bytes).unwrap();
                record.partition_slots += 1;
                download.body = Box::pin(std::io::Cursor::new(record.encode().unwrap()));
            }
            _ => return Err(StorageError::NotFound),
        }
        Ok(download)
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        self.fake.put(key, bytes, condition).await
    }
}
#[tokio::test]
async fn applied_write_with_invalid_or_missing_readback_remains_unknown() {
    for fault in 0..4 {
        let fake = Fake::default();
        fake.state.lock().unwrap().lose_put_response = true;
        let storage = Storage::new(
            ReconciliationFault { fake, fault },
            Namespace::new("test").unwrap(),
        );
        let mut writer = HeartbeatWriter::new(binding()).unwrap();
        let first = writer
            .publish(&storage, sample(), deadline())
            .await
            .unwrap();
        assert!(matches!(first, HeartbeatWriteResult::Unknown));
        assert!(first.into_acknowledged().is_none());
        if fault != 3 {
            let second = writer
                .publish(&storage, sample(), deadline())
                .await
                .unwrap();
            assert!(matches!(second, HeartbeatWriteResult::Unknown));
            let state = storage.backend().fake.state.lock().unwrap();
            assert_eq!((state.puts, state.gets), (1, 2));
        }
        assert!(writer.reconcile);
        assert_eq!(writer.attempted, 1);
    }
}
