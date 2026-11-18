use super::*;
use crate::oltp::{OltpEngine, OltpResources};
use uuid::Uuid;

struct Directory(std::path::PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("nabedra-raft-log-{}", Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn peer(cluster_id: Uuid, group_id: Uuid) -> Peer {
    Peer {
        node_id: 1,
        engine: "oltp".into(),
        format_version: 2,
        database: "default".into(),
        cluster_id,
        group_id,
        incarnation: Uuid::new_v4(),
        address: "127.0.0.1:1".into(),
        server_name: "localhost".into(),
        certificate_sha256: "0".repeat(64),
    }
}

/// F03: cancellation of the async owner does not release native work's I/O or
/// ordering ownership. The barrier observes real synchronous RocksDB writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f03_cancelled_log_owner_retains_native_resources_until_settled() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let directory = Directory::new();
        let cluster = Uuid::new_v4();
        let group = Uuid::new_v4();
        let engine =
            OltpEngine::open_replica(&directory.0, OltpResources::default(), cluster, group)
                .unwrap();
        let local = peer(cluster, group);
        let io = Arc::new(Semaphore::new(1));
        let mut store = LogStore::new(
            engine.shared_db(),
            &local,
            io.clone(),
            engine.recovery_marker(),
        )
        .unwrap();
        let (started, observed) = tokio::sync::oneshot::channel();
        let (release, gated) = std::sync::mpsc::sync_channel(0);
        let worker = store.clone();
        let pending = tokio::spawn(async move {
            worker
                .write(move |db, prefix| {
                    let _ = started.send(());
                    gated.recv().unwrap();
                    db.put_opt(
                        key(prefix, b"/meta/", b"vote"),
                        encode(&VoteOf::<TypeConfig>::new_committed(7, 1))?,
                        &sync_options(),
                    )
                    .map_err(disk)
                })
                .await
        });
        observed.await.unwrap();
        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());
        assert_eq!(io.available_permits(), 0);
        assert!(store.writes.try_lock().is_err());
        release.send(()).unwrap();
        let settled = store.writes.lock().await;
        drop(settled);
        assert_eq!(io.available_permits(), 1);
        assert_eq!(
            store.read_vote().await.unwrap(),
            Some(VoteOf::<TypeConfig>::new_committed(7, 1))
        );
        store
            .save_vote(&VoteOf::<TypeConfig>::new_committed(8, 1))
            .await
            .unwrap();
    })
    .await
    .expect("owned native work watchdog");
}

/// O10/O11: a successful native write followed by an injected lost observation
/// models uncertainty. This is not a native WAL/fsync fault injection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o10_o11_uncertain_log_write_blocks_every_handle_until_reopen() {
    let directory = Directory::new();
    let cluster = Uuid::new_v4();
    let group = Uuid::new_v4();
    let local = peer(cluster, group);
    {
        let engine =
            OltpEngine::open_replica(&directory.0, OltpResources::default(), cluster, group)
                .unwrap();
        let io = Arc::new(Semaphore::new(1));
        let recovery = engine.recovery_marker();
        let mut store =
            LogStore::new(engine.shared_db(), &local, io.clone(), recovery.clone()).unwrap();
        let result: io::Result<()> = store
            .write(|db, prefix| {
                db.put_opt(
                    key(prefix, b"/meta/", b"vote"),
                    encode(&VoteOf::<TypeConfig>::new_committed(9, 1))?,
                    &sync_options(),
                )
                .map_err(disk)?;
                Err(io::Error::other(
                    "injected uncertain observation after native sync",
                ))
            })
            .await;
        assert!(result.is_err());
        assert!(recovery.load(Ordering::SeqCst));
        assert!(store.read_vote().await.is_err());
        assert!(
            store
                .save_vote(&VoteOf::<TypeConfig>::new_committed(10, 1))
                .await
                .is_err()
        );
        assert!(
            LogStore::new(
                engine.shared_db(),
                &local,
                io.clone(),
                Arc::new(AtomicBool::new(false))
            )
            .is_err()
        );
        drop(store);
        assert!(LogStore::new(engine.shared_db(), &local, io, recovery).is_err());
    }
    let engine =
        OltpEngine::open_replica(&directory.0, OltpResources::default(), cluster, group).unwrap();
    let mut recovered = LogStore::new(
        engine.shared_db(),
        &local,
        Arc::new(Semaphore::new(1)),
        engine.recovery_marker(),
    )
    .unwrap();
    assert_eq!(
        recovered.read_vote().await.unwrap(),
        Some(VoteOf::<TypeConfig>::new_committed(9, 1))
    );
}

/// O11: a purge between snapshot capture and metadata/tail observation cannot
/// combine the old purge marker with the new log tail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn o11_log_state_uses_one_native_snapshot_across_concurrent_purge() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let directory = Directory::new();
        let cluster = Uuid::new_v4();
        let group = Uuid::new_v4();
        let engine =
            OltpEngine::open_replica(&directory.0, OltpResources::default(), cluster, group)
                .unwrap();
        let local = peer(cluster, group);
        let mut store = LogStore::new(
            engine.shared_db(),
            &local,
            Arc::new(Semaphore::new(2)),
            engine.recovery_marker(),
        )
        .unwrap();
        let vote = VoteOf::<TypeConfig>::new_committed(1, 1);
        let last = LogIdOf::<TypeConfig>::new(*vote.leader_id(), 3);
        store
            .append(
                (0..4).map(|index| {
                    EntryOf::<TypeConfig>::new_blank(LogIdOf::<TypeConfig>::new(
                        *vote.leader_id(),
                        index,
                    ))
                }),
                IOFlushed::noop(),
            )
            .await
            .unwrap();
        let (captured, observed) = tokio::sync::oneshot::channel();
        let (release, gated) = std::sync::mpsc::sync_channel(0);
        let reader = store.clone();
        let pending = tokio::spawn(async move {
            reader
                .read(move |db, prefix| {
                    let snapshot = db.snapshot();
                    let _ = captured.send(());
                    gated.recv().unwrap();
                    snapshot_state(&snapshot, prefix)
                })
                .await
        });
        observed.await.unwrap();
        store.purge(last).await.unwrap();
        release.send(()).unwrap();
        let prior = pending.await.unwrap().unwrap();
        assert_eq!(prior.last_purged_log_id, None);
        assert_eq!(prior.last_log_id, Some(last));
        let current = store.get_log_state().await.unwrap();
        assert_eq!(current.last_purged_log_id, Some(last));
        assert_eq!(current.last_log_id, Some(last));
    })
    .await
    .expect("consistent log view watchdog");
}
