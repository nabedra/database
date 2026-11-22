//! The seam outside a simulated world equals the direct calls. These tests run in a
//! product build and, unchanged, with the `sim` feature: outside an installed world the
//! simulation forwards to the product calls too.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{ids, task, time};

#[tokio::test(start_paused = true)]
async fn now_is_the_tokio_clock() {
    assert_eq!(time::now(), tokio::time::Instant::now());
    tokio::time::advance(Duration::from_millis(1_234)).await;
    assert_eq!(time::now(), tokio::time::Instant::now());
}

#[tokio::test(start_paused = true)]
async fn timers_take_the_requested_time() {
    let start = tokio::time::Instant::now();
    time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        tokio::time::Instant::now() - start,
        Duration::from_millis(300)
    );

    let deadline = start + Duration::from_millis(1_000);
    time::sleep_until(deadline).await;
    assert_eq!(tokio::time::Instant::now(), deadline);

    let elapsed = time::timeout(Duration::from_millis(50), std::future::pending::<()>()).await;
    assert!(elapsed.is_err());
    assert_eq!(
        tokio::time::Instant::now() - deadline,
        Duration::from_millis(50)
    );
    let at = tokio::time::Instant::now() + Duration::from_millis(20);
    assert!(
        time::timeout_at(at, std::future::pending::<()>())
            .await
            .is_err()
    );
    assert_eq!(tokio::time::Instant::now(), at);
    assert_eq!(
        time::timeout(Duration::from_secs(1), async { 7 })
            .await
            .unwrap(),
        7
    );

    let mut ticks = time::interval(Duration::from_millis(10));
    let first = ticks.tick().await;
    let second = ticks.tick().await;
    assert_eq!(second - first, Duration::from_millis(10));
    let mut later = time::interval_at(at + Duration::from_millis(100), Duration::from_millis(5));
    assert_eq!(later.tick().await, at + Duration::from_millis(100));
}

#[test]
fn std_and_wall_clocks_are_the_system_clocks() {
    let std_before = std::time::Instant::now();
    let wall_before = SystemTime::now();
    let std_now = time::now_std();
    let wall_now = time::wall_now();
    let ms = time::unix_ms();
    let micros = time::unix_micros();
    let nanos = time::unix_nanos();
    let std_after = std::time::Instant::now();
    let wall_after = SystemTime::now();
    assert!(std_before <= std_now && std_now <= std_after);
    assert!(wall_before <= wall_now && wall_now <= wall_after);
    let low = wall_before.duration_since(UNIX_EPOCH).unwrap();
    let high = wall_after.duration_since(UNIX_EPOCH).unwrap();
    assert!(low.as_millis() as u64 <= ms && ms <= high.as_millis() as u64);
    assert!(low.as_micros() as u64 <= micros && micros <= high.as_micros() as u64);
    assert!(low.as_nanos() <= nanos && nanos <= high.as_nanos());
}

#[test]
fn thread_sleep_sleeps() {
    let start = std::time::Instant::now();
    time::thread_sleep(Duration::from_millis(20));
    assert!(start.elapsed() >= Duration::from_millis(20));
}

#[tokio::test]
async fn spawned_work_runs_on_the_runtime() {
    assert_eq!(task::spawn(async { 3 }).await.unwrap(), 3);
    let handle = tokio::runtime::Handle::current();
    assert_eq!(task::spawn_on(&handle, async { 4 }).await.unwrap(), 4);

    // spawn_blocking runs on one of tokio's blocking threads, not inline.
    let caller = std::thread::current().id();
    let ran_on = task::spawn_blocking(|| std::thread::current().id())
        .await
        .unwrap();
    assert_ne!(ran_on, caller);
    assert!(
        task::spawn_blocking(|| -> u8 { panic!("planted") })
            .await
            .unwrap_err()
            .is_panic()
    );

    let mut set = task::JoinSet::new();
    set.spawn(async { 5 });
    assert_eq!(set.join_next().await.unwrap().unwrap(), 5);
}

#[test]
fn services_are_named_threads() {
    let started = Arc::new(AtomicBool::new(false));
    let flag = started.clone();
    let service = task::spawn_service("nabedra-test-service", move || {
        flag.store(true, Ordering::SeqCst);
        std::thread::current().name().map(str::to_owned)
    })
    .unwrap();
    assert_eq!(
        service.join().unwrap().as_deref(),
        Some("nabedra-test-service")
    );
    assert!(started.load(Ordering::SeqCst));

    let caller = std::thread::current().id();
    let stacked = task::spawn_service_with_stack("nabedra-test-stack", 128 * 1024, move || {
        // Use part of the stack to show the requested size is usable.
        let buffer = [7u8; 32 * 1024];
        (
            std::thread::current().id(),
            buffer.iter().map(|&b| u32::from(b)).sum::<u32>(),
        )
    })
    .unwrap();
    let (id, sum) = stacked.join().unwrap();
    assert_ne!(id, caller);
    assert_eq!(sum, 7 * 32 * 1024);
}

#[test]
fn ids_are_random_version_4() {
    let first = ids::uuid_v4();
    let second = ids::uuid_v4();
    assert_ne!(first, second);
    assert_eq!(first.get_version_num(), 4);
    assert_eq!(first.get_variant(), uuid::Variant::RFC4122);
    // 64 random bits: 32 draws are distinct and set the top bits at some point.
    let draws: Vec<u64> = (0..32).map(|_| ids::random_u64()).collect();
    let mut unique = draws.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), draws.len());
    let union = draws.iter().fold(0, |acc, value| acc | value);
    assert_ne!(union >> 62, 0, "the top bits are never set");
}
