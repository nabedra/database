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
                let result = runtime::serve_configured(
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
                    Default::default(),
                    false,
                    true,
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
                    detached_engine
                        .storage()
                        .load_cluster_catalog()
                        .await
                        .unwrap();
                    detached_engine
                        .worker_pool()
                        .submit(5, |id, _| {
                            assert!(id < 2);
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
    let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
    let mut releases = Vec::new();
    let mut jobs = Vec::new();
    for index in 0..3 {
        let shared = if index % 2 == 0 { &engine } else { &clone };
        let (release, gate) = oneshot::channel();
        releases.push(release);
        let started = started.clone();
        jobs.push(
            shared
                .worker_pool()
                .submit(77, move |id, runtime| {
                    started.send((id, std::thread::current().id())).unwrap();
                    runtime.block_on(gate).unwrap();
                    Ok(())
                })
                .await
                .unwrap(),
        );
    }
    let mut ids = std::collections::BTreeSet::new();
    for _ in 0..3 {
        ids.insert(starts.recv().await.unwrap().0);
    }
    assert_eq!(ids.len(), 3, "clones share exactly three actual executors");
    assert_eq!(pool.scheduler().usage()[0].1, 3);
    let pending = clone.worker_pool().submit(77, |id, _| Ok(id));
    tokio::pin!(pending);
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(pending.as_mut().poll(cx).is_pending()))
            .await
    );
    releases.remove(0).send(()).unwrap();
    assert!(pending.await.unwrap().receive().await.unwrap() < 3);
    for release in releases {
        release.send(()).unwrap();
    }
    for job in jobs {
        job.receive().await.unwrap();
    }
    workers.shutdown().await.unwrap();
    // Shutdown through the injected owner closes every handle, including clones.
    assert!(pool.submit(0, |_, _| Ok(())).await.is_err());
    assert!(clone.worker_pool().submit(0, |_, _| Ok(())).await.is_err());
}

#[tokio::test]
async fn u01_startup_initializes_the_same_local_run_before_readiness() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for cold in [false, true] {
            let storage = Storage::new(Fake::default(), Namespace::new("eager-startup/").unwrap());
            let previous = if cold {
                let fence = meshdb_server::storage::CatalogFence {
                    run_id: uuid::Uuid::new_v4(),
                    scheduler_term: 1,
                };
                storage
                    .initialize_fenced_cluster_catalog(fence, false, false)
                    .await
                    .unwrap();
                Some(fence)
            } else {
                None
            };
            let (pool, workers) = WorkerPool::start(
                NonZeroUsize::new(1).unwrap(),
                tokio::runtime::Handle::current(),
            )
            .unwrap();
            let engine = Engine::new(storage, pool, "2 MiB".parse().unwrap());
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (stop, stopped) = oneshot::channel();
            let served_engine = engine.clone();
            let serving = tokio::spawn(async move {
                runtime::serve_configured(
                    listener,
                    served_engine,
                    Executor::new(2, 0).unwrap(),
                    "test-token".into(),
                    None,
                    async move {
                        stopped.await.unwrap();
                        Ok(())
                    },
                    Default::default(),
                    cold,
                    false,
                )
                .await
            });
            let mut client = TcpStream::connect(address).await.unwrap();
            client
                .write_all(
                    b"GET /health/ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            let mut response = String::new();
            client.read_to_string(&mut response).await.unwrap();
            assert!(response.starts_with("HTTP/1.1 200"));
            let catalog = engine.storage().load_cluster_catalog().await.unwrap();
            assert_eq!(catalog.catalog().format_version, 4);
            assert!(catalog.catalog().fence.is_some());
            if let Some(previous) = previous {
                assert_ne!(catalog.catalog().fence.unwrap().run_id, previous.run_id);
            }
            stop.send(()).unwrap();
            serving.await.unwrap().unwrap();
            workers.shutdown().await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn r04_startup_rejects_missing_migration_and_cold_confirmation_before_ingress() {
    tokio::time::timeout(Duration::from_secs(10), async {
        for legacy in [true, false] {
            let storage =
                Storage::new(Fake::default(), Namespace::new("eager-rejection/").unwrap());
            if legacy {
                storage.initialize_catalog().await.unwrap();
            } else {
                storage
                    .initialize_fenced_cluster_catalog(
                        meshdb_server::storage::CatalogFence {
                            run_id: uuid::Uuid::new_v4(),
                            scheduler_term: 1,
                        },
                        false,
                        false,
                    )
                    .await
                    .unwrap();
            }
            let before = storage
                .read(
                    &meshdb_server::storage::ObjectKey::catalog(),
                    meshdb_server::protocol::MAX_CATALOG_BYTES,
                )
                .await
                .unwrap()
                .bytes;
            let (pool, workers) = WorkerPool::start(
                NonZeroUsize::new(1).unwrap(),
                tokio::runtime::Handle::current(),
            )
            .unwrap();
            let engine = Engine::new(storage, pool, "2 MiB".parse().unwrap());
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let error = runtime::serve_configured(
                listener,
                engine.clone(),
                Executor::new(2, 0).unwrap(),
                "test-token".into(),
                None,
                std::future::pending::<std::io::Result<()>>(),
                Default::default(),
                false,
                false,
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains(if legacy {
                "migrate-catalog"
            } else {
                "cold-start"
            }));
            assert!(TcpStream::connect(address).await.is_err());
            assert_eq!(
                engine
                    .storage()
                    .read(
                        &meshdb_server::storage::ObjectKey::catalog(),
                        meshdb_server::protocol::MAX_CATALOG_BYTES
                    )
                    .await
                    .unwrap()
                    .bytes,
                before
            );
            workers.shutdown().await.unwrap();
        }
    })
    .await
    .unwrap();
}
