use meshdb_server::{
    engine::Engine,
    executor::Executor,
    runtime,
    storage::{Namespace, Storage},
};
use std::{sync::Arc, time::Duration};
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
    tokio::time::timeout(Duration::from_secs(10), async {
        let storage = Storage::new(Fake::default(), Namespace::new("runtime/").unwrap());
        storage.initialize_catalog().await.unwrap();
        let engine = Engine::new(storage);
        let executor = Executor::new(1, 1).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel();
        let serving = tokio::spawn(runtime::serve(
            listener,
            engine.clone(),
            executor.clone(),
            "test-token".into(),
            None,
            async {
                stopped.await.unwrap();
                Ok(())
            },
        ));
        // A real HTTP connection proves the initialized router is being served.
        let mut client = TcpStream::connect(address).await.unwrap();
        client
            .write_all(b"GET /health/live HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
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
                detached_engine.storage().load_catalog().await.unwrap();
                Ok(())
            })
            .unwrap();
        drop(operation);
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
        serving.await.unwrap().unwrap();
        assert!(executor.try_admit().is_err());
    })
    .await
    .unwrap();
}
