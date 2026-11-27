//! RV-EV regression tests at the broker's control layer: a quarantined metadata group does
//! not stop the broker (M3), a store that requires recovery fails readiness and is
//! reported (M2), and a broker on an empty volume does not rejoin the metadata group under
//! its old membership (EVENTS-1).

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

/// A broker config with `peers` as extra bootstrap brokers at their peer URLs. Broker 1
/// is the only metadata voter, and it initializes a new cluster.
fn config_with_peers(dir: &Path, id: u64, peers: &[(u64, String)]) -> Config {
    let keys = |key: &str, byte: &str| {
        json!({
            "active": key,
            "keys": [{ "id": key, "secret_hex": byte.repeat(32), "accept_until_ms": i64::MAX }],
        })
    };
    let mut brokers = vec![json!({ "id": 1, "zone": "zone-a", "peer_url": "http://127.0.0.1:10" })];
    for (index, (peer, url)) in peers.iter().enumerate() {
        brokers.push(json!({ "id": peer, "zone": format!("zone-{}", index + 2), "peer_url": url }));
    }
    serde_json::from_value(json!({
        "broker_id": id,
        "cluster_id": CLUSTER,
        "data_dir": dir,
        "public_bind": "127.0.0.1:9",
        "peer_bind": "127.0.0.1:10",
        "peer_token": "local-testing-peer-token-with-32-bytes",
        "allow_plaintext": true,
        "bootstrap": { "metadata_voters": [1], "brokers": brokers },
        "keyring": { "cluster_id": CLUSTER, "grant": keys("g1", "a1"), "session": keys("s1", "b2") },
    }))
    .expect("test broker config")
}

fn open_as(dir: &Path, fs: &FaultFs, id: u64) -> Result<Arc<Broker>> {
    Broker::open_with(
        BrokerConfig {
            id,
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

/// Serves `control`'s private control routes on a loopback port; returns its peer URL.
async fn serve_peer(control: Arc<Control>) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/", listener.local_addr()?);
    let routes = super::forward::router(control);
    let task = tokio::spawn(async move {
        let _ = axum::serve(listener, routes).await;
    });
    Ok((url, task))
}

/// Waits until the group's metrics show the initial membership or a vote.
async fn wait_initialized(group: &Group) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !group.is_initialized() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}

/// What `nabedra-events init` writes: the authorization to create a new cluster.
fn authorize(dir: &Path) -> Result<()> {
    std::fs::write(dir.join(INITIALIZE_CLUSTER), b"")?;
    Ok(())
}

fn empty_volume() -> Result<(tempfile::TempDir, FaultFs)> {
    let disk = tempfile::tempdir()?;
    let fs = FaultFs::new();
    fs.create_dir_all_durable(disk.path());
    Ok((disk, fs))
}

/// EVENTS-1: the rule that decides whether a broker whose volume lacks a group may open it
/// as a voter.
#[test]
fn events_1_rejoin_rule() {
    use crate::raft::{DiskState, Rejoin, rejoin};
    let voters = BTreeSet::from([1, 2, 3]);
    let empty = DiskState::default();
    let open = DiskState {
        hosted: true,
        ..DiskState::default()
    };
    let formed = DiskState {
        hosted: true,
        formed: true,
        other_groups: false,
    };
    let partitions = DiskState {
        other_groups: true,
        ..DiskState::default()
    };
    let answers =
        |states: [Option<DiskState>; 2]| BTreeMap::from([(2, states[0]), (3, states[1])]);
    // A new cluster: nobody holds anything.
    assert_eq!(
        rejoin(false, &voters, &answers([Some(empty), Some(empty)])),
        Rejoin::Genesis
    );
    // The initializer waits until every other initial voter opened the group.
    assert_eq!(
        rejoin(true, &voters, &answers([Some(open), Some(empty)])),
        Rejoin::Wait
    );
    assert_eq!(
        rejoin(true, &voters, &answers([Some(open), Some(open)])),
        Rejoin::Genesis
    );
    // One peer that holds the group, or any other group, proves the cluster exists.
    assert_eq!(
        rejoin(false, &voters, &answers([Some(formed), None])),
        Rejoin::Lost
    );
    assert_eq!(
        rejoin(true, &voters, &answers([None, Some(partitions)])),
        Rejoin::Lost
    );
    // A missing answer can hide a surviving replica: wait.
    assert_eq!(
        rejoin(false, &voters, &answers([Some(empty), None])),
        Rejoin::Wait
    );
    // A broker alone in its bootstrap roster decides alone.
    assert_eq!(
        rejoin(true, &BTreeSet::from([1]), &BTreeMap::new()),
        Rejoin::Genesis
    );
}

/// EVENTS-1: what a volume reports for a group: nothing, hosted, formed, other groups.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_1_disk_state_reports_what_the_volume_holds() -> Result<()> {
    let (disk, fs) = empty_volume()?;
    let broker = open_as(disk.path(), &fs, 1)?;
    let control = Control::new(config_with_peers(disk.path(), 1, &[]), broker.clone())?;
    let state = broker.disk_state(METADATA)?;
    assert_eq!(state, crate::raft::DiskState::default());
    let group = control.open_group(METADATA).await?;
    let state = broker.disk_state(METADATA)?;
    assert!(
        state.hosted && !state.formed && !state.other_groups,
        "{state:?}"
    );
    group
        .initialize(BTreeMap::from([(1, "http://127.0.0.1:10".to_owned())]))
        .await?;
    wait_initialized(&group).await?;
    let state = broker.disk_state(METADATA)?;
    assert!(state.hosted && state.formed, "{state:?}");
    let partition = partition_id(Uuid::from_u128(9), 0);
    let other = broker.disk_state(&partition)?;
    assert!(
        !other.hosted && !other.formed && other.other_groups,
        "{other:?}"
    );
    broker.close_group(METADATA).await?;
    let closed = broker.disk_state(METADATA)?;
    assert!(
        closed.hosted && closed.formed,
        "a hosted group that is not running counts as formed"
    );
    Ok(())
}

/// EVENTS-1, before the fix: the initializing broker restarted on an empty volume, found
/// no metadata on its own disk, and initialized a new metadata group although a peer still
/// held the cluster's groups. Now a peer that holds any group makes the broker report its
/// replica lost and stay out of the group.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_1_empty_volume_does_not_reinitialize_metadata_while_a_peer_holds_groups()
-> Result<()> {
    let (peer_disk, peer_fs) = empty_volume()?;
    let peer_broker = open_as(peer_disk.path(), &peer_fs, 2)?;
    let peer = Control::new(
        config_with_peers(peer_disk.path(), 2, &[(2, "http://127.0.0.1:11/".into())]),
        peer_broker.clone(),
    )?;
    let partition = partition_id(Uuid::from_u128(7), 0);
    peer.open_group(&partition).await?;
    let (url, server) = serve_peer(peer.clone()).await?;

    let (disk, fs) = empty_volume()?;
    authorize(disk.path())?;
    let broker = open_as(disk.path(), &fs, 1)?;
    let control = Control::new(
        config_with_peers(disk.path(), 1, &[(2, url)]),
        broker.clone(),
    )?;
    control.reopen(false).await?;
    assert!(
        broker.group(METADATA).is_none(),
        "the probe runs after the listeners start"
    );
    control.join_metadata().await?;
    assert!(
        broker.group(METADATA).is_none(),
        "a broker that lost its metadata replica must not open it as a voter"
    );
    let report = control.broker_report();
    assert_eq!(report["metadata_replica_lost"], true, "{report}");
    assert_eq!(report["metadata_join_pending"], false, "{report}");
    server.abort();
    peer_broker.close_group(&partition).await?;
    Ok(())
}

/// EVENTS-1: in a new cluster every bootstrap peer reports an empty volume, so the
/// initializer opens and initializes the metadata group; while a peer does not answer it
/// waits instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_1_new_cluster_initializes_once_every_peer_answers_empty() -> Result<()> {
    let (disk, fs) = empty_volume()?;
    let broker = open_as(disk.path(), &fs, 1)?;
    // Port 1 refuses connections: the peer never answers.
    let silent = Control::new(
        config_with_peers(disk.path(), 1, &[(2, "http://127.0.0.1:1/".into())]),
        broker.clone(),
    )?;
    silent.reopen(false).await?;
    silent.join_metadata().await?;
    assert!(
        broker.group(METADATA).is_none(),
        "a silent peer can hide a surviving replica"
    );
    assert_eq!(silent.broker_report()["metadata_join_pending"], true);
    drop(silent);

    let (peer_disk, peer_fs) = empty_volume()?;
    let peer_broker = open_as(peer_disk.path(), &peer_fs, 2)?;
    let peer = Control::new(
        config_with_peers(peer_disk.path(), 2, &[(2, "http://127.0.0.1:11/".into())]),
        peer_broker,
    )?;
    let (url, server) = serve_peer(peer).await?;
    authorize(disk.path())?;
    let control = Control::new(
        config_with_peers(disk.path(), 1, &[(2, url)]),
        broker.clone(),
    )?;
    control.reopen(false).await?;
    control.join_metadata().await?;
    let group = broker
        .group(METADATA)
        .expect("a new cluster opens its metadata group");
    wait_initialized(&group).await?;
    assert!(
        !disk.path().join(INITIALIZE_CLUSTER).exists(),
        "the one-shot authorization is consumed"
    );
    assert_eq!(control.broker_report()["metadata_replica_lost"], false);
    server.abort();
    broker.close_group(METADATA).await?;
    Ok(())
}

/// EVENTS-3: retention removed at most 128 events per visit of a partition (one `Retain`
/// entry), so a partition publishing faster outgrew it without bound. One visit now trims
/// the whole expired backlog within the tick's budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_3_one_retention_visit_trims_a_backlog_larger_than_one_page() -> Result<()> {
    // A full-size store: the small test options cannot hold 100-event batches.
    let disk = tempfile::tempdir()?;
    let broker = Broker::open(BrokerConfig {
        id: 1,
        cluster_id: CLUSTER,
        data_dir: disk.path().into(),
        peer_token: "local-testing-peer-token-with-32-bytes".into(),
        peer_ca: None,
        peer_identity: None,
        allow_plaintext: true,
    })?;
    let control = Control::new(config_with_peers(disk.path(), 1, &[]), broker.clone())?;
    let id = partition_id(Uuid::from_u128(5), 0);
    let group = control.open_group(&id).await?;
    group
        .initialize(BTreeMap::from([(1, "http://127.0.0.1:10".to_owned())]))
        .await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while group.leader() != Some(1) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let write = |command: partition::Command| {
        let group = group.clone();
        async move {
            let reply = group
                .write(serde_json::to_value(command)?)
                .await
                .map_err(|error| anyhow::anyhow!("{error}: {:?}", group.status()))?;
            serde_json::from_value::<partition::Mutation>(reply)?
                .map_err(|error| anyhow::anyhow!("{:?}", error.code))
        }
    };
    write(partition::Command::Init {
        topic_id: Uuid::from_u128(5),
        partition: 0,
    })
    .await?;
    for batch in 0..10 {
        let events = (0..100)
            .map(|n| partition::PublishEvent {
                event_id: Uuid::from_u128(1_000 * batch + n + 1),
                data: json!(n),
            })
            .collect();
        write(partition::Command::Publish { now_ms: 1, events }).await?;
    }
    let deadline = crate::rt::now() + Duration::from_secs(30);
    let removed = control.retain_group(&group, 1_000, deadline).await;
    assert_eq!(removed, 1_000, "one visit trims the whole expired backlog");
    let status = group
        .read(|tx| partition::query(tx, partition::Query::Status))
        .await?;
    let Ok(partition::QueryReply::Status(status)) = status else {
        anyhow::bail!("expected the partition status");
    };
    assert_eq!(status.earliest_offset, 1_001);
    broker.close_group(&id).await?;
    Ok(())
}

/// Review S1 of #176: after every bootstrap disk is lost, the first metadata voter found
/// every peer empty and, with `initialize_new_cluster` defaulting to `allow_plaintext`,
/// re-formed an empty cluster under the old identity. Without `nabedra-events init` it
/// now waits: no metadata group, the join stays pending.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_1_s1_uninitialized_first_voter_waits_when_every_peer_is_empty() -> Result<()> {
    let (peer_disk, peer_fs) = empty_volume()?;
    let peer = Control::new(
        config_with_peers(peer_disk.path(), 2, &[(2, "http://127.0.0.1:11/".into())]),
        open_as(peer_disk.path(), &peer_fs, 2)?,
    )?;
    let (url, server) = serve_peer(peer).await?;
    let (disk, fs) = empty_volume()?;
    let broker = open_as(disk.path(), &fs, 1)?;
    let control = Control::new(
        config_with_peers(disk.path(), 1, &[(2, url)]),
        broker.clone(),
    )?;
    control.reopen(false).await?;
    control.join_metadata().await?;
    assert!(broker.group(METADATA).is_none(), "no cluster forms without init");
    assert_eq!(control.broker_report()["metadata_join_pending"], true);
    server.abort();
    Ok(())
}

/// Review S1 for RF1: a broker alone in its bootstrap roster forms no cluster without
/// `nabedra-events init`, and forms one with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_1_s1_single_broker_needs_init() -> Result<()> {
    let (disk, fs) = empty_volume()?;
    let broker = open_as(disk.path(), &fs, 1)?;
    let control = Control::new(config_with_peers(disk.path(), 1, &[]), broker.clone())?;
    control.reopen(false).await?;
    assert!(broker.group(METADATA).is_none(), "no cluster forms without init");
    assert_eq!(control.broker_report()["metadata_join_pending"], true);
    authorize(disk.path())?;
    control.join_metadata().await?;
    assert_eq!(control.broker_report()["metadata_join_pending"], true, "the start decides");
    drop(control);
    let control = Control::new(config_with_peers(disk.path(), 1, &[]), broker.clone())?;
    control.reopen(false).await?;
    let group = broker.group(METADATA).expect("init authorizes the new cluster");
    wait_initialized(&group).await?;
    broker.close_group(METADATA).await?;
    Ok(())
}
