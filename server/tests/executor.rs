use meshdb_server::protocol;
#[path = "../src/executor.rs"]
mod executor;

use executor::Executor;
use protocol::{ErrorCode, Outcome};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc, oneshot};

async fn bounded<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("hung lifecycle test")
}

#[tokio::test]
async fn validates_capacity_and_releases_unused_admissions_with_zero_queue() {
    assert!(Executor::new(0, 0).is_err());
    assert!(Executor::new(1, usize::MAX).is_err());
    assert!(Executor::new(Semaphore::MAX_PERMITS + 1, 0).is_err());
    assert!(Executor::new(1, Semaphore::MAX_PERMITS).is_err());
    let executor = Executor::new(1, 0).unwrap();
    let admission = executor.try_admit().unwrap();
    let error = executor.try_admit().err().unwrap();
    assert_eq!(
        (error.code, error.outcome),
        (ErrorCode::Overloaded, Outcome::NotApplied)
    );
    drop(admission);
    let admission = executor.try_admit().unwrap();
    executor.stop_accepting();
    assert_eq!(
        executor.try_admit().err().unwrap().code,
        ErrorCode::Overloaded
    );
    let drain = executor.drain();
    tokio::pin!(drain);
    tokio::select! { biased; _ = &mut drain => panic!("unused admission was ignored"), _ = tokio::task::yield_now() => {} }
    drop(admission);
    bounded(drain).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn limits_active_closures_and_drains_disconnected_queued_work() {
    bounded(async {
        let executor = Executor::new(2, 2).unwrap();
        let gate = Arc::new(Semaphore::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicUsize::new(0));
        let (started, mut starts) = mpsc::unbounded_channel();
        let mut handles = Vec::new();
        for id in 0..4 {
            let (gate, active, peak, finished, started) = (gate.clone(), active.clone(), peak.clone(), finished.clone(), started.clone());
            handles.push(executor.try_admit().unwrap().spawn(move || {
                let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(count, Ordering::SeqCst);
                started.send(id).unwrap();
                async move {
                    gate.acquire().await.unwrap().forget();
                    active.fetch_sub(1, Ordering::SeqCst);
                    finished.fetch_add(1, Ordering::SeqCst);
                    Ok(id)
                }
            }).unwrap());
            if id == 1 {
                starts.recv().await.unwrap();
                starts.recv().await.unwrap();
            }
        }
        assert!(starts.try_recv().is_err(), "queued closure was invoked");
        assert_eq!(active.load(Ordering::SeqCst), 2);
        assert_eq!(executor.try_admit().err().unwrap().code, ErrorCode::Overloaded);
        drop(handles); // Includes both running and queued receivers.
        executor.stop_accepting();
        assert!(executor.try_admit().is_err());
        let drain = executor.drain();
        let other_drain = executor.drain();
        tokio::pin!(drain, other_drain);
        tokio::select! { biased; _ = &mut drain => panic!("drain returned early"), _ = tokio::task::yield_now() => {} }
        tokio::select! { biased; _ = &mut other_drain => panic!("second drain returned early"), _ = tokio::task::yield_now() => {} }
        gate.add_permits(1);
        starts.recv().await.unwrap();
        assert_eq!(active.load(Ordering::SeqCst), 2);
        gate.add_permits(3);
        tokio::join!(drain, other_drain);
        assert_eq!(finished.load(Ordering::SeqCst), 4);
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disconnect_keeps_permits_until_awaited_blocking_work_finishes() {
    bounded(async {
        let executor = Executor::new(1, 0).unwrap();
        let (started, running) = oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let operation = executor.try_admit().unwrap().spawn(move || async move {
            tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                blocked.recv().unwrap();
            }).await.unwrap();
            Ok(())
        }).unwrap();
        running.await.unwrap();
        drop(operation);
        assert_eq!(executor.try_admit().err().unwrap().code, ErrorCode::Overloaded);
        let drain = executor.drain();
        tokio::pin!(drain);
        tokio::select! { biased; _ = &mut drain => panic!("released blocking work early"), _ = tokio::task::yield_now() => {} }
        release.send(()).unwrap();
        drain.await;
    }).await;
}

#[tokio::test]
async fn results_and_panics_are_visible_and_release_capacity() {
    bounded(async {
        let executor = Executor::new(1, 0).unwrap();
        assert_eq!(
            executor
                .try_admit()
                .unwrap()
                .spawn(|| async { Ok(42) })
                .unwrap()
                .receive()
                .await
                .unwrap(),
            42
        );
        let expected = protocol::ProtocolError::new(
            ErrorCode::ValidationError,
            "invalid body",
            Outcome::NotApplied,
        );
        let error = expected.clone();
        assert_eq!(
            executor
                .try_admit()
                .unwrap()
                .spawn(|| async { Err::<(), _>(error) })
                .unwrap()
                .receive()
                .await
                .unwrap_err(),
            expected
        );
        let operation = executor
            .try_admit()
            .unwrap()
            .spawn(
                || -> std::future::Ready<Result<(), protocol::ProtocolError>> {
                    panic!("closure panic")
                },
            )
            .unwrap();
        let error = operation.receive().await.unwrap_err();
        assert_eq!(
            (error.code, error.outcome),
            (ErrorCode::InternalError, Outcome::Unknown)
        );
        let operation = executor
            .try_admit()
            .unwrap()
            .spawn(|| async {
                panic!("future panic");
                #[allow(unreachable_code)]
                Ok(())
            })
            .unwrap();
        assert_eq!(
            operation.receive().await.unwrap_err().code,
            ErrorCode::InternalError
        );
        drop(executor.try_admit().unwrap());
        executor.drain().await;
    })
    .await;
}
