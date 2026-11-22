use super::*;
use crate::{
    oltp::{
        mvcc::{
            CanonicalJson,
            store::direct::{DirectCommit, DirectReply, DirectWrite},
        },
        pool::{ByteClass, is_disk_reservation_refusal},
    },
    oltp_replication::owned::OwnedCommand,
};
use std::os::fd::AsRawFd;

// The O08 process fixture sets TMPDIR to a 2 GiB tmpfs. Poll each proposal
// once on the same Tokio thread as Raft so all reservations overlap before
// Raft has a chance to apply or discard any of them.
#[tokio::test]
#[ignore = "needs the O08 Linux tmpfs fixture"]
async fn o08_o13_small_data_proposals_share_two_gib() {
    const WRITES: i64 = 256;
    let cluster = Cluster::new(Options::default()).await;
    let leader = cluster.leader().await;
    let voter = &cluster.voters[leader];
    let disk = || {
        let stats = voter.shared.storage_reservations();
        assert_eq!(stats.len(), 1);
        serde_json::to_value(stats[0]).unwrap()
    };
    let read = |stats: &serde_json::Value, field: &str| stats[field].as_u64().unwrap();
    let before = disk();
    let available = read(&before, "available_bytes");
    assert!(((1 << 30)..=(2 << 30)).contains(&available), "{before}");
    assert_eq!(read(&before, "reserved_bytes"), 0, "{before}");

    let command = |id, tag: &str| {
        let fixed = |value: serde_json::Value| {
            CanonicalJson::from_serializable(&voter.shared, &value, 1 << 20).unwrap()
        };
        OwnedCommand::new_owned(
            ParticipantCommand::Direct(DirectCommit {
                authority: cluster.authority,
                table_id: cluster.table,
                schema_epoch: 1,
                routing_epoch: 1,
                request_id: nabedra_rt::ids::uuid_v4(),
                digest: Digest::from_bytes([0; 32]),
                commit_ts: CommitTs::new(1_000 + id as u64).unwrap(),
                writes: vec![DirectWrite {
                    key: cluster.row_key(id),
                    expected_version: None,
                    value: Some(fixed(
                        serde_json::json!({"id": id, "value": id, "tag": tag, "score": 0}),
                    )),
                    index: Vec::new(),
                }],
                result: fixed(serde_json::Value::Null),
                applied_unix_ms: 1,
            }),
            &voter.shared,
            ByteClass::Data,
        )
        .unwrap()
    };
    let mut pending = Vec::new();
    for id in 0..WRITES {
        let mut proposal = Box::pin(voter.handle.propose(command(id, "small")));
        assert!(
            futures_util::poll!(&mut proposal).is_pending(),
            "proposal {id} refused before Raft could apply: {}",
            disk()
        );
        pending.push(proposal);
    }
    let held = disk();
    let reserved = read(&held, "reserved_bytes");
    assert_eq!(read(&held, "reservations"), WRITES as u64, "{held}");
    assert!(reserved > 0 && reserved < 16 << 20, "{held}");

    // Leave enough space for the held writes and protected headroom, but
    // only 64 KiB of room for one more proposal. The payload alone is larger.
    let filler = std::fs::File::create(cluster._dir.0.join("disk-pressure")).unwrap();
    let keep = read(&held, "protected_bytes") + reserved + (64 << 10);
    let fill = read(&held, "available_bytes").checked_sub(keep).unwrap();
    // SAFETY: fallocate borrows the live fd and fill fits in off_t (<= 2 GiB).
    assert_eq!(
        unsafe { libc::posix_fallocate(filler.as_raw_fd(), 0, fill as libc::off_t) },
        0
    );
    let pressure = disk();
    let remaining =
        read(&pressure, "available_bytes") - read(&pressure, "protected_bytes") - reserved;
    assert!((32 << 10..=64 << 10).contains(&remaining), "{pressure}");
    {
        let large = "x".repeat(128 << 10);
        let proposal = voter.handle.propose(command(WRITES, &large));
        tokio::pin!(proposal);
        match futures_util::poll!(&mut proposal) {
            std::task::Poll::Ready(Err(error)) => {
                assert!(is_disk_reservation_refusal(&error), "{error:?}");
            }
            other => panic!("expected a definite storage refusal, got {other:?}"),
        }
    }
    assert_eq!(read(&disk(), "reserved_bytes"), reserved);
    // Consuming each response releases its decoded-reply charge while Raft
    // is still applying the rest of the entries.
    let replies = pending
        .into_iter()
        .map(|proposal| async move { proposal.await.unwrap().reply });
    let replies = nabedra_rt::time::timeout(
        Duration::from_secs(45),
        futures_util::future::join_all(replies),
    )
    .await
    .expect("small proposals did not settle");
    for (id, reply) in replies.into_iter().enumerate() {
        assert_eq!(
            reply,
            ParticipantReply::Direct(DirectReply::Applied {
                commit_ts: CommitTs::new(1_000 + id as u64).unwrap(),
            })
        );
    }
    let settled = disk();
    assert_eq!(read(&settled, "reserved_bytes"), 0, "{settled}");
    assert_eq!(read(&settled, "reservations"), 0, "{settled}");
    drop(filler);
    std::fs::remove_file(cluster._dir.0.join("disk-pressure")).unwrap();
    cluster.converge(leader).await;
    for voter in 0..3 {
        for id in 0..WRITES {
            let (version, value) = cluster.latest(voter, cluster.row_key(id)).await;
            assert_eq!(version, Some(CommitTs::new(1_000 + id as u64).unwrap()));
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&value.unwrap()).unwrap()["id"],
                id
            );
        }
        assert_eq!(
            cluster.latest(voter, cluster.row_key(WRITES)).await,
            (None, None)
        );
    }
    eprintln!(
        "O08 concurrent Data proposals: before={before} held={held} pressure={pressure} settled={settled}"
    );
    cluster.shutdown().await;
}
