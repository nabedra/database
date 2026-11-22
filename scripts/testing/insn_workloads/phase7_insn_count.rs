// Installed as a Cargo example in each measured checkout.
fn main() {
    let workload = std::env::args().nth(1).expect("workload name");
    let operations = match workload.as_str() {
        #[cfg(feature = "oltp")]
        "oltp-storage" => oltp_storage(),
        #[cfg(feature = "oltp")]
        "kv-write-flush" => kv_write_flush(),
        #[cfg(feature = "cache")]
        "cache-set" => current_thread().block_on(nabedra_database_server::cache::run_insn_cache()),
        #[cfg(feature = "olap")]
        "olap-queue" => olap_queue(),
        _ => panic!("unknown workload: {workload}"),
    };
    println!("nabedra-insn-count: {workload} {operations}");
}

#[cfg(any(feature = "cache", feature = "oltp"))]
fn current_thread() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
}

#[cfg(feature = "oltp")]
fn oltp_storage() -> u64 {
    use nabedra_database_server::{
        config::WorkerResources,
        oltp::pool::{
            AppliedPosition, ApplyMeta, OltpPool, POOL_FORMAT_VERSION, PoolIdentity,
            SharedResourceConfig, TransactionBudget, Transition, WorkerSharedResources,
        },
        oltp_replication::types::{Digest, GroupIdentity, GroupKind, Text},
    };
    use std::path::Path;
    use uuid::Uuid;

    const OPERATIONS: u64 = 512;
    const KEY: &[u8] = b"row";
    current_thread().block_on(async {
        let id = Uuid::from_u128;
        let resources = WorkerSharedResources::new(
            1,
            WorkerResources::default(),
            SharedResourceConfig::default(),
        )
        .unwrap();
        let pool = OltpPool::open_development(
            Path::new("pool"),
            PoolIdentity {
                engine: "oltp".into(),
                pool_format_version: POOL_FORMAT_VERSION,
                cluster_id: id(1),
                worker_id: id(2),
                pool_id: id(3),
            },
            resources,
        )
        .unwrap();
        let group = pool
            .group(GroupIdentity {
                cluster_id: id(1),
                database: Text::try_from("default").unwrap(),
                group_id: id(4),
                kind: GroupKind::Data {
                    table_id: id(5),
                    shard_id: id(6),
                },
                group_format_version: 1,
            })
            .await
            .unwrap();
        let budget = || TransactionBudget {
            scratch_bytes: 64 << 10,
            batch_bytes: 64 << 10,
            output_bytes: 64 << 10,
        };
        for index in 1..=OPERATIONS {
            let value = index.to_be_bytes();
            let mut digest = [0; 32];
            digest[..8].copy_from_slice(&value);
            let reply = group
                .transaction(
                    ApplyMeta {
                        position: AppliedPosition {
                            term: 1,
                            node_id: 1,
                            index,
                        },
                        command_digest: Digest::from_bytes(digest),
                        membership: None,
                    },
                    budget(),
                    move |tx| {
                        tx.put(KEY, &value)?;
                        Ok(Transition::Commit(tx.copy_output(&value)?))
                    },
                )
                .await
                .unwrap();
            assert_eq!(reply.as_slice(), value);
        }
        let value = group
            .read(budget(), |read| Ok(read.get(KEY)?.unwrap()))
            .await
            .unwrap();
        assert_eq!(value.as_slice(), OPERATIONS.to_be_bytes());
    });
    OPERATIONS
}

#[cfg(feature = "oltp")]
fn kv_write_flush() -> u64 {
    use nabedra_kv::{
        Background, BlockCache, Durability, MemtableBudget, Options, Store, WriteBatch, fs::RealFs,
    };
    use std::{path::Path, sync::Arc};

    const OPERATIONS: u64 = 1024;
    const VALUE: &[u8] = &[b'v'; 100];
    let background = Background::new(1).unwrap();
    let options = Options::new(
        Arc::new(RealFs),
        Arc::new(BlockCache::new(16 << 20)),
        Arc::new(MemtableBudget::new(16 << 20)),
        Arc::clone(&background),
    );
    let store = Store::open(Path::new("kv"), options).unwrap().store;
    for key in 0..OPERATIONS {
        let mut batch = WriteBatch::new();
        batch.put(&key.to_be_bytes(), VALUE);
        store
            .submit(batch, Durability::Sync, Box::new(()))
            .unwrap()
            .wait()
            .unwrap();
    }
    store.flush().unwrap();
    let stats = store.stats();
    assert_eq!(stats.commits, OPERATIONS);
    assert!(stats.flush_bytes > 0);
    let key = (OPERATIONS - 1).to_be_bytes();
    assert_eq!(store.snapshot().get(&key).unwrap().unwrap().as_ref(), VALUE);
    store.close().unwrap();
    OPERATIONS
}

#[cfg(feature = "olap")]
fn olap_queue() -> u64 {
    use nabedra_database_server::{
        node::insn_queue_now_ms,
        request_queue::{QueueLimits, RequestQueue, RequestState},
    };
    use uuid::Uuid;

    const OPERATIONS: u64 = 512;
    let run = Uuid::from_u128(1);
    let member = Uuid::from_u128(2);
    let mut queue = RequestQueue::new(
        run,
        1,
        1,
        QueueLimits {
            max_pending_requests: OPERATIONS as usize,
            max_queue_bytes_per_replica: 8 << 20,
            max_retained_result_bytes_per_worker: 1 << 20,
            result_retention_ms: 3600_000,
        },
        [member].into(),
    )
    .unwrap();
    for n in 0..OPERATIONS {
        let request = Uuid::from_u128(u128::from(n) + 3);
        let body =
            format!(r#"{{"request_id":"{request}","operation":"table.list","payload":{{}}}}"#);
        queue
            .propose(body.into_bytes(), insn_queue_now_ms())
            .unwrap();
        queue.acknowledge(request, member).unwrap();
        queue.start(request, Some(b"{}".to_vec())).unwrap();
        queue.acknowledge(request, member).unwrap();
        let body = format!(r#"{{"request_id":"{request}","data":[]}}"#);
        queue
            .finish_retained(request, 200, body.into_bytes(), insn_queue_now_ms())
            .unwrap();
        queue.acknowledge(request, member).unwrap();
        assert_eq!(
            queue
                .lookup(run, request, insn_queue_now_ms())
                .unwrap()
                .state,
            RequestState::Succeeded
        );
    }
    assert_eq!(queue.pending(), 0);
    OPERATIONS
}
