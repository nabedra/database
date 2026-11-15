use super::*;

#[tokio::test]
async fn c06_remote_io_balances_by_compute_capacity_without_reserving_cpu() {
    let limits = |max_threads| WorkerResources {
        max_threads,
        memory_limit_bytes: 1 << 20,
        task_memory_limit_bytes: 1 << 19,
        control_memory_reserve_bytes: 1 << 16,
    };
    let scheduler =
        ResourceScheduler::new(vec![(1, limits(1)), (2, limits(2)), (3, limits(4))]).unwrap();
    for node in 1..=3 {
        scheduler.set_io_capacity(node, 16).unwrap();
    }
    let mut grants = Vec::new();
    let mut counts = std::collections::BTreeMap::new();
    for _ in 0..7 {
        let grant = scheduler.reserve_remote_memory(42, 3, 1024).await.unwrap();
        *counts.entry(grant.node_id()).or_insert(0) += 1;
        grants.push(grant);
    }
    assert_eq!(counts, [(1, 1), (2, 2), (3, 4)].into());
    assert!(
        scheduler
            .usage()
            .iter()
            .all(|(_, threads, _)| *threads == 0)
    );
    drop(grants);
    assert!(
        scheduler
            .usage()
            .iter()
            .all(|(_, threads, bytes)| *threads == 0 && *bytes == 0)
    );
}
