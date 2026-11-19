#![cfg(any(feature = "olap", feature = "oltp"))]

use nabedra_database_server::{
    protocol::{ErrorCode, Outcome},
    worker_pool::WorkerPool,
};
use std::{
    cell::RefCell,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot},
    time::timeout,
};

thread_local! {
    // A sender kept until OS thread exit proves teardown reached thread exit.
    static EXIT_NOTICE: RefCell<Option<oneshot::Sender<()>>> = const { RefCell::new(None) };
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_threads_refill_any_free_executor_and_bound_work() {
    timeout(Duration::from_secs(15), async {
        let (pool, owner) =
            WorkerPool::start(NonZeroUsize::new(3).unwrap(), Handle::current()).unwrap();
        let (started, mut starts) = mpsc::unbounded_channel();
        let mut gates = Vec::new();
        let mut jobs = Vec::new();
        for index in 0..3 {
            let (release, gate) = oneshot::channel();
            gates.push(release);
            let started = started.clone();
            jobs.push(
                pool.submit(index, move |id, runtime| {
                    assert!(Handle::try_current().is_err());
                    let before = thread::current().id();
                    assert_eq!(
                        thread::current().name(),
                        Some(format!("nabedra-worker-{id}").as_str())
                    );
                    started.send((id, before)).unwrap();
                    runtime.block_on(async {
                        gate.await.unwrap();
                        tokio::task::yield_now().await;
                    });
                    assert_eq!(before, thread::current().id());
                    Ok((id, thread::current().id()))
                })
                .await
                .unwrap(),
            );
        }
        let mut ids = [None; 3];
        for _ in 0..3 {
            let (id, tid) = starts.recv().await.unwrap();
            ids[id] = Some(tid);
        }
        assert_ne!(ids[0], ids[1]);
        assert_ne!(ids[0], ids[2]);
        assert_ne!(ids[1], ids[2]);
        assert_eq!(pool.scheduler().usage()[0].1, 3);
        let next = pool.submit(0, |id, _| Ok((id, thread::current().id())));
        tokio::pin!(next);
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(next.as_mut().poll(cx).is_pending()))
                .await
        );
        // Partition index zero can use whichever real executor becomes free.
        gates.remove(1).send(()).unwrap();
        let (released_id, released_thread) = jobs.remove(1).receive().await.unwrap();
        let (next_id, next_thread) = next.await.unwrap().receive().await.unwrap();
        assert_eq!((next_id, next_thread), (released_id, released_thread));
        assert_eq!(Some(next_thread), ids[next_id]);
        for gate in gates {
            gate.send(()).unwrap();
        }
        for job in jobs {
            let (id, tid) = job.receive().await.unwrap();
            assert_eq!(Some(tid), ids[id]);
        }
        assert_eq!(pool.scheduler().usage()[0].1, 0);
        owner.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_drains_abandoned_and_panicking_jobs_and_joins() {
    timeout(Duration::from_secs(15), async {
        let (pool, owner) =
            WorkerPool::start(NonZeroUsize::new(3).unwrap(), Handle::current()).unwrap();
        let completed = Arc::new(Mutex::new(Vec::new()));
        let mut exits = Vec::new();
        let (started, mut starts) = mpsc::unbounded_channel();
        let mut gates = Vec::new();
        let mut jobs = Vec::new();
        for index in 0..3 {
            let (release, gate) = oneshot::channel();
            gates.push(release);
            let started = started.clone();
            let completed = completed.clone();
            let (exit_tx, exit_rx) = oneshot::channel();
            exits.push(exit_rx);
            jobs.push(
                pool.submit(index, move |id, runtime| {
                    EXIT_NOTICE.with(|notice| *notice.borrow_mut() = Some(exit_tx));
                    started.send((id, thread::current().id())).unwrap();
                    runtime.block_on(async {
                        gate.await.unwrap();
                    });
                    if index == 1 {
                        panic!("private panic payload");
                    }
                    completed.lock().unwrap().push(index);
                    Ok(thread::current().id())
                })
                .await
                .unwrap(),
            );
        }
        let mut original_ids = [None; 3];
        for _ in 0..3 {
            let (id, tid) = starts.recv().await.unwrap();
            original_ids[id] = Some(tid);
        }
        // Free only the executor assigned the panicking logical job. Its
        // identity is observed rather than inferred from a partition index.
        let mut jobs: Vec<_> = jobs.into_iter().map(Some).collect();
        let mut gates: Vec<_> = gates.into_iter().map(Some).collect();
        gates[1].take().unwrap().send(()).unwrap();
        let panic_error = jobs[1].take().unwrap().receive().await.unwrap_err();
        assert_eq!(panic_error.code, ErrorCode::InternalError);
        assert_eq!(panic_error.outcome, Outcome::NotApplied);
        assert!(!panic_error.message.contains("private"));
        let completed_copy = completed.clone();
        let recovered = pool
            .submit(0, move |id, _| {
                completed_copy.lock().unwrap().push(3);
                Ok((id, thread::current().id()))
            })
            .await
            .unwrap()
            .receive()
            .await
            .unwrap();
        assert_eq!(Some(recovered.1), original_ids[recovered.0]);
        let completed_copy = completed.clone();
        let (done, done_rx) = oneshot::channel();
        let abandoned = pool
            .submit(0, move |_, _| {
                completed_copy.lock().unwrap().push(4);
                let _ = done.send(());
                Ok(())
            })
            .await
            .unwrap();
        drop(abandoned);
        done_rx.await.unwrap();
        let (started_last, last_started) = oneshot::channel();
        let (release_last, last_gate) = oneshot::channel();
        let completed_copy = completed.clone();
        let accepted = pool
            .submit(0, move |_, runtime| {
                let _ = started_last.send(());
                runtime.block_on(last_gate).unwrap();
                completed_copy.lock().unwrap().push(5);
                Ok(())
            })
            .await
            .unwrap();
        last_started.await.unwrap();
        drop(accepted);
        drop(jobs[0].take());
        let blocked = pool.submit(6, |_, _| Ok(()));
        tokio::pin!(blocked);
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(
                blocked.as_mut().poll(cx).is_pending()
            ))
            .await
        );
        let shutdown = owner.shutdown();
        assert!(pool.submit(0, |_, _| Ok(())).await.is_err());
        tokio::pin!(shutdown);
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(
                shutdown.as_mut().poll(cx).is_pending()
            ))
            .await
        );
        gates[0].take().unwrap().send(()).unwrap();
        gates[2].take().unwrap().send(()).unwrap();
        release_last.send(()).unwrap();
        jobs[2].take().unwrap().receive().await.unwrap();
        shutdown.await.unwrap();
        assert!(blocked.await.is_err());
        for mut exit in exits {
            assert_eq!(exit.try_recv(), Err(oneshot::error::TryRecvError::Closed));
        }
        let mut done = completed.lock().unwrap().clone();
        done.sort();
        assert_eq!(done, vec![0, 2, 3, 4, 5]);

        // Worker 0 drains first, but must keep driving IO needed by worker 1.
        let (pool, owner) =
            WorkerPool::start(NonZeroUsize::new(2).unwrap(), Handle::current()).unwrap();
        let (io_go, io_gate) = oneshot::channel();
        let (io_done, io_result) = oneshot::channel();
        let (finish_first, first_gate) = oneshot::channel();
        let first = pool
            .submit(0, move |_, runtime| {
                runtime.spawn(async move {
                    io_gate.await.unwrap();
                    io_done.send(()).unwrap();
                });
                runtime.block_on(first_gate).unwrap();
                Ok(())
            })
            .await
            .unwrap();
        let (release, gate) = oneshot::channel();
        let (second_started, second_entered) = oneshot::channel();
        let last_dispatch = Arc::new(pool.clone());
        let job = pool
            .submit(1, move |_, runtime| {
                second_started.send(()).unwrap();
                runtime.block_on(async {
                    gate.await.unwrap();
                    io_go.send(()).unwrap();
                    io_result.await.unwrap();
                });
                drop(last_dispatch); // Last Engine-like dispatch Arc drops on worker.
                Ok(())
            })
            .await
            .unwrap();
        second_entered.await.unwrap();
        finish_first.send(()).unwrap();
        first.receive().await.unwrap();
        drop(pool);
        let shutdown = owner.shutdown();
        release.send(()).unwrap();
        job.receive().await.unwrap();
        shutdown.await.unwrap();

        // Fallback Drop returns while accepted work is gated, then tears down.
        let (pool, owner) =
            WorkerPool::start(NonZeroUsize::new(1).unwrap(), Handle::current()).unwrap();
        let (release, gate) = oneshot::channel();
        let (exit_tx, exit_rx) = oneshot::channel();
        let job = pool
            .submit(0, move |_, runtime| {
                EXIT_NOTICE.with(|notice| *notice.borrow_mut() = Some(exit_tx));
                runtime.block_on(async {
                    gate.await.unwrap();
                });
                Ok(())
            })
            .await
            .unwrap();
        drop(owner);
        assert!(pool.submit(0, |_, _| Ok(())).await.is_err());
        release.send(()).unwrap();
        job.receive().await.unwrap();
        assert!(exit_rx.await.is_err());
    })
    .await
    .unwrap();
}
