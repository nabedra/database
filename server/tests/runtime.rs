use meshdb_server::{
    engine::Engine,
    executor::Executor,
    runtime,
    storage::{Namespace, Storage},
    worker_pool::WorkerPool,
};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
#[path = "support/storage.rs"]
mod support;
use support::{Fake, ReadGate};

#[tokio::test]
async fn shutdown_closes_listener_and_waits_for_detached_engine_work() {
    for signal_error in [false, true] {
        tokio::time::timeout(Duration::from_secs(10), async {
            let storage = Storage::new(Fake::default(), Namespace::new("runtime/").unwrap());
            storage.initialize_catalog().await.unwrap();
            let (pool, workers) = WorkerPool::start(
                NonZeroUsize::new(2).unwrap(),
                tokio::runtime::Handle::current(),
            )
            .unwrap();
            let engine = Engine::new(storage, pool, "2 MiB".parse().unwrap());
            let executor = Executor::new(1, 1).unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (stop, stopped) = oneshot::channel();
            let serving_engine = engine.clone();
            let serving_executor = executor.clone();
            let serving = tokio::spawn(async move {
                let result = runtime::serve(
                    listener,
                    serving_engine,
                    serving_executor,
                    "test-token".into(),
                    None,
                    async move {
                        stopped.await.unwrap();
                        if signal_error {
                            Err(std::io::Error::other("signal failed"))
                        } else {
                            Ok(())
                        }
                    },
                )
                .await;
                workers.shutdown().await.unwrap();
                result
            });
            // A real HTTP connection proves the initialized router is being served.
            let mut client = TcpStream::connect(address).await.unwrap();
            client
                .write_all(
                    b"GET /health/live HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            let mut response = String::new();
            client.read_to_string(&mut response).await.unwrap();
            assert!(response.starts_with("HTTP/1.1 200"));
            let gate = Arc::new(ReadGate::default());
            engine.storage().backend().state.lock().unwrap().read_gate = Some(gate.clone());
            let detached_engine = engine.clone();
            let operation = executor
                .try_admit()
                .unwrap()
                .spawn(move || async move {
                    // The response receiver is dropped below; admitted work must still finish.
                    let worker_engine = detached_engine.clone();
                    detached_engine
                        .worker_pool()
                        .submit(5, move |id, runtime| {
                            assert_eq!(id, 1);
                            runtime
                                .block_on(worker_engine.storage().load_catalog())
                                .unwrap();
                            Ok(())
                        })
                        .await
                        .unwrap()
                        .receive()
                        .await
                        .unwrap();
                    Ok(())
                })
                .unwrap();
            drop(operation);
            // This queued request submits only after the first request finishes.
            // Shutting the pool before draining the executor would reject its job.
            let queued_engine = engine.clone();
            let (completed, completion) = oneshot::channel();
            let queued = executor
                .try_admit()
                .unwrap()
                .spawn(move || async move {
                    queued_engine
                        .worker_pool()
                        .submit(0, |id, _| Ok(id))
                        .await?
                        .receive()
                        .await?;
                    completed.send(()).unwrap();
                    Ok(())
                })
                .unwrap();
            drop(queued);
            gate.entered.acquire().await.unwrap().forget();
            stop.send(()).unwrap();
            // Probe admission without retaining any successful guard.
            while executor.try_admit().is_ok() {
                tokio::task::yield_now().await;
            }
            // Ingress must close even while detached work is still gated.
            loop {
                if TcpStream::connect(address).await.is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert!(!serving.is_finished());
            gate.release.add_permits(1);
            let result = serving.await.unwrap();
            assert_eq!(result.is_err(), signal_error);
            completion.await.unwrap();
            assert!(executor.try_admit().is_err());
            assert!(engine.worker_pool().submit(0, |_, _| Ok(())).await.is_err());
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn engine_clones_retain_config_and_share_worker_threads() {
    let storage = Storage::new(Fake::default(), Namespace::new("wiring/").unwrap());
    let (pool, workers) = WorkerPool::start(
        NonZeroUsize::new(3).unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let size = "7 KiB".parse().unwrap();
    let engine = Engine::new(storage, pool.clone(), size);
    let clone = engine.clone();
    assert_eq!(clone.partition_size(), size);
    assert_eq!(clone.worker_pool().worker_count().get(), 3);
    assert!(Arc::ptr_eq(engine.coordinator(), clone.coordinator()));
    for index in 0..7 {
        let original = engine
            .worker_pool()
            .submit(index, |id, _| Ok((id, std::thread::current().id())))
            .await
            .unwrap()
            .receive()
            .await
            .unwrap();
        let cloned = clone
            .worker_pool()
            .submit(index, |id, _| Ok((id, std::thread::current().id())))
            .await
            .unwrap()
            .receive()
            .await
            .unwrap();
        assert_eq!(original.0, index % 3);
        assert_eq!(original, cloned);
    }
    workers.shutdown().await.unwrap();
    // Shutdown through the injected owner closes every handle, including clones.
    assert!(pool.submit(0, |_, _| Ok(())).await.is_err());
    assert!(clone.worker_pool().submit(0, |_, _| Ok(())).await.is_err());
}
