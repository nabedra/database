//! RV-EV regression tests at the broker's control layer: a quarantined metadata group does
//! not stop the broker (M3), and a store that requires recovery fails readiness and is
//! reported (M2).

use std::path::{Path, PathBuf};
use std::time::Duration;

use nabedra_kv::fs::{Fault, FaultError, FaultFs, Fs, FsOp};
use serde_json::json;

use super::*;
use crate::raft::{BrokerConfig, test_options};

const CLUSTER: Uuid = Uuid::from_u128(0xe7f1);

fn config(dir: &Path) -> Config {
    let keys = |id: &str, byte: &str| {
        json!({
            "active": id,
            "keys": [{ "id": id, "secret_hex": byte.repeat(32), "accept_until_ms": i64::MAX }],
        })
    };
    // The control layer is tested without listeners, so the ports are never bound.
    serde_json::from_value(json!({
        "broker_id": 1,
        "cluster_id": CLUSTER,
        "data_dir": dir,
        "public_bind": "127.0.0.1:9",
        "peer_bind": "127.0.0.1:10",
        "peer_token": "local-testing-peer-token-with-32-bytes",
        "allow_plaintext": true,
        "bootstrap": {
            "metadata_voters": [1],
            "brokers": [{ "id": 1, "zone": "zone-a", "peer_url": "http://127.0.0.1:10" }],
        },
        "initialize_new_cluster": false,
        "keyring": { "cluster_id": CLUSTER, "grant": keys("g1", "a1"), "session": keys("s1", "b2") },
    }))
    .expect("test broker config")
}

fn open(dir: &Path, fs: &FaultFs) -> Result<Arc<Broker>> {
    Broker::open_with(
        BrokerConfig {
            id: 1,
            cluster_id: CLUSTER,
            data_dir: dir.into(),
            peer_token: "local-testing-peer-token-with-32-bytes".into(),
            peer_ca: None,
            peer_identity: None,
            allow_plaintext: true,
        },
        test_options(fs),
    )
}

fn journal(dir: &Path) -> PathBuf {
    dir.join("kv").join("journal")
}

/// Every `(file, offset)` under `dir` that holds `needle`.
fn find(fs: &FaultFs, dir: &Path, needle: &[u8]) -> Vec<(PathBuf, u64)> {
    let mut hits = Vec::new();
    for path in fs.list(dir).unwrap_or_default() {
        let Ok(data) = fs.contents(&path) else {
            continue;
        };
        for (offset, window) in data.windows(needle.len()).enumerate() {
            if window == needle {
                hits.push((path.clone(), offset as u64));
            }
        }
    }
    hits
}

/// NS27 (RV-EV M3; plan section 3, rule 1): on a bootstrap metadata voter, a quarantined
/// metadata group stays stopped and reported while the rest of the broker starts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ns27_rv_ev_m3_quarantined_metadata_group_does_not_stop_the_broker() -> Result<()> {
    let disk = tempfile::tempdir()?;
    let fs = FaultFs::new();
    fs.create_dir_all_durable(disk.path());
    let partition = partition_id(Uuid::from_u128(7), 0);
    let broker = open(disk.path(), &fs)?;
    let control = Control::new(config(disk.path()), broker.clone())?;
    control.open_group(&partition).await?;
    broker.close_group(&partition).await?;
    broker.test_write(b"hosted/metadata", b"").await?;
    broker
        .test_write(b"group/metadata/election-floor/v1", b"<<rv-ev-m3-record>>")
        .await?;
    // A later synced record: evidence that the damaged one was acknowledged.
    broker.test_write(b"retired/v1/unrelated", b"1").await?;
    let image = fs.kill();
    drop(control);
    drop(broker);
    let hits = find(&image, &journal(disk.path()), b"<<rv-ev-m3-record>>");
    assert_eq!(hits.len(), 1, "{hits:?}");
    image.flip_bit(&hits[0].0, hits[0].1 + 4, 2)?;

    let broker = open(disk.path(), &image)?;
    assert!(broker.is_quarantined(METADATA));
    let control = Control::new(config(disk.path()), broker.clone())?;
    control.reopen(false).await?;
    let started = broker
        .group(&partition)
        .expect("the partition group started");
    assert!(started.status().running);
    assert!(
        broker.group(METADATA).is_none(),
        "the damaged group is not started"
    );
    let report = control.broker_report();
    assert_eq!(
        report["quarantined_groups"][0]["group"], METADATA,
        "{report}"
    );
    assert_eq!(report["store_recovery_required"], false, "{report}");
    // RV-EV L5: the quarantined group still counts as hosted (a restore's fresh-volume
    // check sees it).
    assert!(broker.hosted_group_ids()?.contains(&METADATA.to_owned()));
    assert!(!broker.stored_group_ids()?.contains(&METADATA.to_owned()));
    broker.close_group(&partition).await?;
    Ok(())
}

/// NS27 (RV-EV M2): an EIO on a journal sync latches `RecoveryRequired`. The broker then
/// fails readiness and reports the latch (and `serve` stops with `RECOVERY_EXIT_CODE`);
/// the next open of the volume recovers under R1-R3 and keeps every acknowledged write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ns27_rv_ev_m2_store_recovery_required_fails_readiness_and_is_reported() -> Result<()> {
    let disk = tempfile::tempdir()?;
    let fs = FaultFs::new();
    fs.create_dir_all_durable(disk.path());
    let broker = open(disk.path(), &fs)?;
    let control = Control::new(config(disk.path()), broker.clone())?;
    broker.test_write(b"group/g/acknowledged", b"1").await?;
    control.mark_ready();
    assert!(control.ready());
    assert_eq!(control.broker_report()["store_recovery_required"], false);

    fs.inject(
        Fault::on(FsOp::SyncData)
            .path_prefix(journal(disk.path()))
            .error(FaultError::Eio),
    );
    assert!(broker.test_write(b"group/g/lost", b"1").await.is_err());
    tokio::time::timeout(Duration::from_secs(5), broker.recovery_required())
        .await
        .expect("the latch is signalled");
    assert!(broker.requires_recovery());
    assert!(!control.ready(), "readiness turns 503");
    let report = control.broker_report();
    assert_eq!(report["store_recovery_required"], true, "{report}");
    assert_eq!(report["ready"], false, "{report}");
    // Every later write fails until the store is reopened.
    assert!(broker.test_write(b"group/g/later", b"1").await.is_err());

    let image = fs.crash();
    drop(control);
    drop(broker);
    let reopened = open(disk.path(), &image)?;
    assert!(!reopened.requires_recovery());
    assert!(reopened.test_read(b"group/g/acknowledged")?.is_some());
    assert!(reopened.test_read(b"group/g/lost")?.is_none());
    reopened.test_write(b"group/g/after", b"1").await?;
    Ok(())
}
