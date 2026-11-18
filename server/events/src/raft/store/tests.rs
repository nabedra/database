use super::*;

#[tokio::test]
async fn group_close_fences_detached_storage_work() -> Result<()> {
    let disk = tempfile::tempdir()?;
    let database = Database::open(disk.path(), Uuid::new_v4(), 1)?;
    let old = GroupStore::new(database.clone(), "close-fence");
    let key = old.key(b"applied-marker");
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, parked) = std::sync::mpsc::channel();
    let waiter = {
        let old = old.clone();
        let key = key.clone();
        tokio::spawn(async move {
            old.run(move |db| {
                entered
                    .send(())
                    .map_err(|_| anyhow::anyhow!("test observer stopped"))?;
                parked.recv()?;
                let mut write = WriteBatch::default();
                write.put(&key, b"first-generation");
                sync_write(&db.db, write)
            })
            .await
        })
    };
    started.await?;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert!(
        old.sealed.try_write().is_err(),
        "detached disk job released its permit"
    );

    release.send(())?;
    old.seal().await;
    let new = GroupStore::new(database, "close-fence");
    let attempted_key = key.clone();
    assert!(
        old.run(move |db| {
            let mut write = WriteBatch::default();
            write.put(&attempted_key, b"late-old-write");
            sync_write(&db.db, write)
        })
        .await
        .is_err()
    );
    assert_eq!(
        new.run(move |db| Ok(db.db.get(&key)?)).await?,
        Some(b"first-generation".to_vec())
    );
    Ok(())
}
