//! O29 recovery assertions over the native pool and replication journal gate.
use crate::{
    oltp::{
        pool::{
            OltpPool, PoolIdentity, SharedResourceConfig, Transition,
            tests::{
                Dir, budget, get, group_identity,
                kv::{journal_dir, put_entry, reopen},
                meta, pool_identity, shared,
            },
        },
        recovery_unknown,
    },
    oltp_acceptance::clause,
    oltp_replication::typed_log::tests::GatedSync,
};
use nabedra_kv::fs::{FaultFs, FsOp};
use nabedra_rt::fs::os as fs;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

pub(crate) async fn o29_pool_recovery() {
    let dir = Dir::new();
    let disk = FaultFs::new();
    fs::create_dir_all(&dir.0).unwrap();
    disk.create_dir_all_durable(&dir.0);
    let fs = GatedSync::new().over(disk.clone());
    let resources = shared(SharedResourceConfig::default());
    let identity = pool_identity();
    let pool = OltpPool::open_development_on(
        &dir.0,
        identity.clone(),
        resources.clone(),
        Arc::new(fs.clone()),
    )
    .unwrap();
    let group_id = group_identity(&pool);
    let group = pool.group(group_id.clone()).await.unwrap();
    let alias_pool = pool.clone();
    let alias = alias_pool.group(group_id.clone()).await.unwrap();

    let healthy_dir = Dir::new();
    let healthy = OltpPool::open_development(
        &healthy_dir.0,
        PoolIdentity {
            pool_id: nabedra_rt::ids::uuid_v4(),
            ..identity.clone()
        },
        resources.clone(),
    )
    .unwrap();
    let proof = healthy.group(group_identity(&healthy)).await.unwrap();
    let decided = nabedra_rt::ids::uuid_v4().to_string().into_bytes();
    proof
        .transaction(meta(0), budget(), put_entry(0, b"prior", decided.clone()))
        .await
        .unwrap();

    let table_id = nabedra_rt::ids::uuid_v4().to_string().into_bytes();
    let original = table_id.clone();
    let live = pool.kv_store().visible_seq();
    disk.clear_calls();
    fs.fail_after_sync();
    let ambiguous = group
        .transaction(meta(0), budget(), move |tx| {
            tx.put(b"table", &table_id)?;
            Ok(Transition::Commit(tx.copy_output(&table_id)?))
        })
        .await;
    assert_eq!(ambiguous.err(), Some(recovery_unknown()));
    assert_eq!(pool.kv_store().visible_seq(), live);
    let syncs = || {
        disk.calls()
            .into_iter()
            .filter(|c| c.op == FsOp::SyncData && c.path.starts_with(journal_dir(&dir)) && c.ok)
            .count()
    };
    assert_eq!(syncs(), 1, "the failed live write must already be durable");

    let attempted = Arc::new(AtomicUsize::new(0));
    for handle in [&group, &alias] {
        let observed = attempted.clone();
        let strong_read = handle.read(budget(), move |view| {
            observed.fetch_add(1, Ordering::Relaxed);
            view.copy_output(b"unexpected read")
        });
        assert_eq!(strong_read.await.err(), Some(recovery_unknown()));
        let observed = attempted.clone();
        let same_request = handle.transaction(meta(0), budget(), move |tx| {
            observed.fetch_add(1, Ordering::Relaxed);
            let regenerated = nabedra_rt::ids::uuid_v4().to_string();
            tx.put(b"table", regenerated.as_bytes())?;
            Ok(Transition::Commit(tx.copy_output(regenerated.as_bytes())?))
        });
        assert_eq!(same_request.await.err(), Some(recovery_unknown()));
    }
    assert_eq!(attempted.load(Ordering::Relaxed), 0);
    assert_eq!(syncs(), 1, "no second decision was appended");
    assert!(pool.requires_recovery() && alias_pool.requires_recovery());
    assert_eq!(
        alias_pool.group(group_id.clone()).await.err(),
        Some(recovery_unknown())
    );
    clause(
        "O29",
        "shared-recovery-state",
        "passed",
        "The original pool and both existing and newly requested group aliases shared a recovery latch after post-sync EIO.",
    );

    let replay = proof
        .transaction(meta(0), budget(), |_| panic!("reran a known decision"))
        .await
        .unwrap();
    assert_eq!(replay.as_slice(), 0u64.to_be_bytes());
    assert_eq!(get(&proof, b"prior").await, decided);
    proof
        .transaction(
            meta(1),
            budget(),
            put_entry(1, b"healthy", b"usable".to_vec()),
        )
        .await
        .unwrap();
    assert_eq!(get(&proof, b"healthy").await, b"usable");
    assert!(!healthy.requires_recovery());
    clause(
        "O29",
        "healthy-pool-isolation",
        "passed",
        "The unrelated pool provided a prior applied reply and served an additional write and read while the first pool was latched.",
    );

    drop((alias, alias_pool, group, pool));
    let disk = disk.crash();
    let recovered = reopen(&dir, &disk, identity, resources).unwrap();
    assert!(!recovered.requires_recovery());
    let group = recovered.retained_group(group_id).await.unwrap();
    let applied = group.applied_state(budget()).await.unwrap().unwrap();
    assert_eq!(applied.position, meta(0).position);
    assert_eq!(applied.command_digest, meta(0).command_digest);
    assert_eq!(get(&group, b"table").await, original);
    let recovered_reply = group
        .transaction(meta(0), budget(), |_| panic!("re-executed recovered work"))
        .await
        .unwrap();
    assert_eq!(recovered_reply.as_slice(), original);
    clause(
        "O29",
        "recovery-without-pending-replay",
        "passed",
        "Native recovery exposed the persisted UUID and exact reply at the original position. Retrying that entry did not execute its transition.",
    );
    clause(
        "O29",
        "fault-model-label",
        "passed",
        "FaultFs reports EIO after persisting the journal sync. The assertion is deterministic fault-model evidence.",
    );
}
