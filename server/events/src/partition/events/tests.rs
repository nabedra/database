use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::{Result, ensure};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    partition::{Command, HEAD_KEY, Head, Machine, Mutation, PublishEvent, Reply, load, store},
    raft::{Broker, BrokerConfig, Group, StateMachine, Transaction},
};

use super::{ErrorCode, MAX_RETAINED_EVENT_BYTES, dedup_key};

struct CapacityFixture;

impl StateMachine for CapacityFixture {
    fn apply(&self, tx: &mut Transaction<'_>, command: Value) -> Result<Value> {
        if command != json!("fill_retention_capacity") {
            return Machine.apply(tx, command);
        }

        // Model a full partition without writing 63 GiB of unrelated records.
        let mut head = load::<Head>(tx, HEAD_KEY)?.expect("partition is initialized");
        head.retained_event_bytes = MAX_RETAINED_EVENT_BYTES;
        store(tx, HEAD_KEY, &head)?;
        Ok(Value::Null)
    }
}

async fn apply(group: &Group, command: Command) -> Result<Mutation> {
    Ok(serde_json::from_value(
        group.write(serde_json::to_value(command)?).await?,
    )?)
}

async fn committed(group: &Group, command: Command) -> Result<Reply> {
    apply(group, command)
        .await?
        .map_err(|error| anyhow::anyhow!("{:?}: {}", error.code, error.message))
}

#[tokio::test]
async fn retention_frees_capacity_without_consuming_rejected_offsets() -> Result<()> {
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
    let group = broker
        .open_group("capacity", Arc::new(CapacityFixture))
        .await?;
    group
        .initialize(BTreeMap::from([(1, "http://127.0.0.1:8081".into())]))
        .await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while group.leader() != Some(1) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;

    committed(
        &group,
        Command::Init {
            topic_id: Uuid::new_v4(),
            partition: 0,
        },
    )
    .await?;
    let old = PublishEvent {
        event_id: Uuid::new_v4(),
        data: json!("a".repeat(1024)),
    };
    let new = PublishEvent {
        event_id: Uuid::new_v4(),
        data: json!("b"),
    };
    let publish = |now_ms, events| Command::Publish { now_ms, events };
    let Reply::Published(original) = committed(&group, publish(1, vec![old.clone()])).await? else {
        anyhow::bail!("expected a publish receipt");
    };
    group.write(json!("fill_retention_capacity")).await?;

    let Reply::Published(retry) = committed(&group, publish(2, vec![old.clone()])).await? else {
        anyhow::bail!("expected a retry receipt");
    };
    assert_eq!(retry.inserted, 0);
    assert_eq!(retry.events, original.events);

    let rejected = apply(&group, publish(2, vec![old, new.clone()]))
        .await?
        .unwrap_err();
    assert_eq!(rejected.code, ErrorCode::InsufficientStorage);
    let new_id = new.event_id;
    let (tail, dedup) = group
        .read(move |tx| {
            let head = load::<Head>(tx, HEAD_KEY)?.expect("partition is initialized");
            Ok((head.tail(), load::<u64>(tx, &dedup_key(new_id))?))
        })
        .await?;
    assert_eq!(tail, 1);
    assert_eq!(dedup, None);

    let retained = committed(
        &group,
        Command::Retain {
            cutoff_ms: 2,
            limit: 1,
        },
    )
    .await?;
    ensure!(matches!(
        retained,
        Reply::Retained {
            removed: 1,
            earliest_offset: 2
        }
    ));
    let Reply::Published(accepted) = committed(&group, publish(2, vec![new])).await? else {
        anyhow::bail!("expected a new publish receipt after retention");
    };
    assert_eq!(accepted.inserted, 1);
    assert_eq!(accepted.events[0].offset, 2);
    broker.close_group("capacity").await?;
    Ok(())
}
