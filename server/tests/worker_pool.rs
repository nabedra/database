use meshdb_server::{
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
async fn fixed_threads_route_and_bound_work() {
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
                        Some(format!("meshdb-worker-{id}").as_str())
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
        for index in 3..6 {
            jobs.push(
                pool.submit(index, |id, _| Ok((id, thread::current().id())))
                    .await
                    .unwrap(),
            );
        }
        let seventh = pool.submit(6, |id, _| Ok((id, thread::current().id())));
        tokio::pin!(seventh);
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(
                seventh.as_mut().poll(cx).is_pending()
            ))
            .await
        );
        gates.remove(0).send(()).unwrap();
        jobs.push(seventh.await.unwrap());
        for gate in gates {
            gate.send(()).unwrap();
        }
        for (index, job) in jobs.into_iter().enumerate() {
            let (id, tid) = job.receive().await.unwrap();
            assert_eq!(id, index % 3);
            assert_eq!(Some(tid), ids[id]);
        }
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
                    if id == 1 {
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
        let mut successors = Vec::new();
        for index in 3..6 {
            let completed = completed.clone();
            successors.push(
                pool.submit(index, move |_, _| {
                    completed.lock().unwrap().push(index);
                    Ok(thread::current().id())
                })
                .await
                .unwrap(),
            );
        }
        drop(jobs.remove(0)); // Receiving is optional, execution is not.
        drop(successors.remove(0));
        let blocked = pool.submit(6, |_, _| Ok(()));
        tokio::pin!(blocked);
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(
                blocked.as_mut().poll(cx).is_pending()
            ))
            .await
        );
        let shutdown = owner.shutdown(); // Must stop synchronously, before polling.
        assert!(pool.submit(0, |_, _| Ok(())).await.is_err());
        tokio::pin!(shutdown);
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(
                shutdown.as_mut().poll(cx).is_pending()
            ))
            .await
        );
        for gate in gates {
            gate.send(()).unwrap();
        }
        let panic_error = jobs.remove(0).receive().await.unwrap_err();
        assert_eq!(panic_error.code, ErrorCode::InternalError);
        assert_eq!(panic_error.outcome, Outcome::NotApplied);
        assert!(!panic_error.message.contains("private"));
        let original_thread = jobs.remove(0).receive().await.unwrap();
        assert_eq!(
            Some(successors.remove(0).receive().await.unwrap()),
            original_ids[1]
        );
        assert_eq!(
            successors.remove(0).receive().await.unwrap(),
            original_thread
        );
        shutdown.await.unwrap(); // Also ignores unpolled, unaccepted reservations.
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
        pool.submit(0, move |_, runtime| {
            runtime.spawn(async move {
                io_gate.await.unwrap();
                io_done.send(()).unwrap();
            });
            Ok(())
        })
        .await
        .unwrap()
        .receive()
        .await
        .unwrap();
        let (release, gate) = oneshot::channel();
        let last_dispatch = Arc::new(pool.clone());
        let job = pool
            .submit(1, move |_, runtime| {
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
