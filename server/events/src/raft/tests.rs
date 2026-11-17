use super::*;

#[tokio::test]
async fn obsolete_self_vote_cannot_resume_after_reopening_as_learner() -> Result<()> {
    let disk = tempfile::tempdir()?;
    let broker = Broker::open(BrokerConfig {
        id: 1,
        cluster_id: Uuid::new_v4(),
        data_dir: disk.path().into(),
        peer_token: "local-testing-peer-token-with-32-bytes".into(),
        peer_ca: None,
        peer_identity: None,
        allow_plaintext: true,
    })?;
    let machine = Arc::new(crate::partition::Machine);
    let members = BTreeMap::from([(1, "http://127.0.0.1:8081".into())]);
    let old = broker.open_group("rejoin", machine.clone()).await?;
    old.initialize(members.clone()).await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while old.leader() != Some(1) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let obsolete_membership = old
        .committed_membership()
        .await?
        .membership_index
        .expect("RF1 membership has a committed log");
    broker.close_group("rejoin").await?;

    // The placement controller supplies the membership index that removed this
    // broker ID. This disk still holds the previous RF1 vote and membership.
    let removal = obsolete_membership + 1;
    let learner = broker
        .open_group_after("rejoin", machine.clone(), Some(removal))
        .await?;
    assert_eq!(learner.leader(), None);
    assert!(!learner.is_initialized());
    assert!(!learner.accepts_votes());
    assert!(matches!(
        learner.initialize(members).await,
        Err(GroupError::Unavailable)
    ));
    assert!(!broker.retire_group("rejoin", removal).await?);
    assert!(broker.group("rejoin").is_some());
    broker.close_group("rejoin").await?;

    let restarted = broker.open_group("rejoin", machine).await?;
    assert_eq!(restarted.election_after, Some(removal));
    assert_eq!(restarted.leader(), None);
    assert!(!restarted.accepts_votes());
    broker.close_group("rejoin").await?;
    Ok(())
}

#[tokio::test]
async fn delayed_retirement_preserves_reopened_replica_data() -> Result<()> {
    let disk = tempfile::tempdir()?;
    let broker = Broker::open(BrokerConfig {
        id: 1,
        cluster_id: Uuid::new_v4(),
        data_dir: disk.path().into(),
        peer_token: "local-testing-peer-token-with-32-bytes".into(),
        peer_ca: None,
        peer_identity: None,
        allow_plaintext: true,
    })?;
    let machine = Arc::new(crate::partition::Machine);
    let old = broker.open_group("delayed-retire", machine.clone()).await?;
    old.initialize(BTreeMap::from([(1, "http://127.0.0.1:8081".into())]))
        .await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while old.leader() != Some(1) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;

    use crate::partition::{Command, Mutation, PublishEvent, Reply};
    old.write(serde_json::to_value(Command::Init {
        topic_id: Uuid::new_v4(),
        partition: 0,
    })?)
    .await?;
    let publish = serde_json::to_value(Command::Publish {
        now_ms: 1,
        events: vec![PublishEvent {
            event_id: Uuid::new_v4(),
            data: serde_json::json!({"saved": true}),
        }],
    })?;
    let original: Mutation = serde_json::from_value(old.write(publish.clone()).await?)?;
    let Reply::Published(original) = original.expect("the test event was committed") else {
        anyhow::bail!("expected a publish receipt");
    };
    let delayed_cut = old
        .committed_membership()
        .await?
        .membership_index
        .expect("group committed a membership")
        + 1;
    let (release, delayed) = tokio::sync::oneshot::channel();
    let delayed_retire = {
        let broker = broker.clone();
        tokio::spawn(async move {
            delayed.await.map_err(anyhow::Error::from)?;
            broker.retire_group("delayed-retire", delayed_cut).await
        })
    };

    // Advance real committed membership before letting the older retired-copy
    // cleanup reach the broker, as when an ID is selected again after a move.
    old.raft
        .add_learner(2, Node::new("http://127.0.0.1:8082"), false)
        .await?;
    old.raft
        .change_membership(BTreeSet::from([1]), false)
        .await?;
    assert!(old.committed_membership().await?.membership_index.unwrap() > delayed_cut);
    broker.close_group("delayed-retire").await?;
    let current = broker
        .open_group_after("delayed-retire", machine, Some(delayed_cut))
        .await?;
    release.send(()).expect("retirement task is waiting");

    assert!(!delayed_retire.await??);
    assert!(!broker.was_retired("delayed-retire")?);
    let retry: Mutation = serde_json::from_value(current.write(publish).await?)?;
    let Reply::Published(retry) = retry.expect("retirement preserved the partition") else {
        anyhow::bail!("expected the original receipt");
    };
    assert_eq!(retry.inserted, 0);
    assert_eq!(retry.events, original.events);
    broker.close_group("delayed-retire").await?;
    Ok(())
}
