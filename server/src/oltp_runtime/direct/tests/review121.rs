//! Independent review test for RD1 (PR #121 review). Written from
//! f2e21999 and its bug record only. The Catalog answers the descriptor
//! lookup with the old descriptor, and the answer reaches the executor only
//! after the DDL fence applied and the new descriptor was published. The
//! oracle is the stored state: an acknowledged update must have its entry in
//! the index the fence introduced.
use super::super::schema::{DirectSchema, SchemaLookup};
use super::*;
use crate::oltp::mvcc::store::direct::{DirectSchema as GroupSchema, DirectSchemaChange};
use tokio::sync::Notify;

struct LateSchema {
    inner: Arc<StaticSchema>,
    hold: AtomicBool,
    answered: Notify,
    release: Notify,
}
impl DirectSchema for LateSchema {
    fn table(
        &self,
        table_id: Uuid,
        schema_epoch: u64,
        group: Option<GroupSchema>,
        authority: Authority,
    ) -> ConsensusFuture<'_, SchemaLookup> {
        Box::pin(async move {
            let answer = self
                .inner
                .table(table_id, schema_epoch, group, authority)
                .await;
            if self.hold.swap(false, Ordering::AcqRel) {
                self.answered.notify_one();
                self.release.notified().await;
            }
            answer
        })
    }
    fn lifecycle(&self, table_id: Uuid, authority: Authority) -> ConsensusFuture<'_, Option<Uuid>> {
        self.inner.lifecycle(table_id, authority)
    }
}

async fn scenario(publish: bool) {
    let cluster = Cluster::new(Options::default()).await;
    let leader = cluster.leader().await;
    let handle = cluster.voters[leader].handle.clone();
    let late = Arc::new(LateSchema {
        inner: cluster.schema.clone(),
        hold: AtomicBool::new(false),
        answered: Notify::new(),
        release: Notify::new(),
    });
    let executor = DirectExecutor::new(
        cluster.voters[leader].shared.clone(),
        DirectConfig::default(),
        cluster.gate.clone(),
        late.clone(),
        DirectTimestampClient::new(None, Arc::new(Counter(AtomicU64::new(1_000)))),
    );
    let run = |operation: &'static str, payload: serde_json::Value| {
        let id = nabedra_rt::ids::uuid_v4();
        let body = cluster.body(id, operation, payload);
        let request = cluster.request(leader, &body, id);
        let (executor, handle) = (executor.clone(), handle.clone());
        async move { executor.execute(&handle, request).await }
    };
    assert_eq!(
        run(
            "table.insert",
            serde_json::json!({"row": {"id": 1, "value": 1, "tag": "a", "score": 0.0}}),
        )
        .await,
        done("null")
    );
    late.hold.store(true, Ordering::Release);
    let before = cluster.last_index(leader);
    let update = nabedra_rt::task::spawn(run(
        "table.update",
        serde_json::json!({"key": 1, "values": {"tag": "b"}}),
    ));
    nabedra_rt::time::timeout(Duration::from_secs(20), late.answered.notified())
        .await
        .expect("the update looked its descriptor up");
    // The DDL: fence the Data group, then publish the descriptor that adds
    // an index on `tag`.
    let published = nabedra_rt::ids::uuid_v4();
    handle
        .change_direct_schema(DirectSchemaChange::Fence {
            lifecycle: Some(published),
        })
        .await
        .unwrap();
    let fenced = cluster.last_index(leader);
    assert!(fenced > before, "the fence is an entry of its own");
    let by_tag = nabedra_rt::ids::uuid_v4();
    if publish {
        let single = |index_id, name: &str, column: &str| IndexDefinition {
            index_id,
            table_id: cluster.table,
            name: Text::try_from(name).unwrap(),
            columns: Page::new(vec![Text::try_from(column).unwrap()]).unwrap(),
            covering: Page::new(Vec::new()).unwrap(),
        };
        cluster.schema.insert(DirectTable {
            descriptor: descriptor(cluster.table),
            schema_epoch: 1,
            lifecycle_id: published,
            indexes: vec![
                single(cluster.index, "by_value", "value"),
                single(by_tag, "by_tag", "tag"),
            ],
        });
    }
    late.release.notify_one();
    let answer = nabedra_rt::time::timeout(Duration::from_secs(20), update)
        .await
        .expect("the update ended")
        .unwrap();
    let row = cluster.latest(leader, cluster.row_key(1)).await.1.unwrap();
    let entry = cluster
        .latest(
            leader,
            MvccKey::IndexEntry {
                table_id: cluster.table,
                index_id: by_tag,
                values: Page::new(vec![IndexValue::Utf8("b".into())]).unwrap(),
                primary_key: Key::Int64(1),
            },
        )
        .await
        .0;
    if publish {
        assert_eq!(answer, done("true"), "the update is served either way");
        assert!(row.contains(r#""tag":"b""#), "{row}");
        assert!(
            entry.is_some(),
            "an update acknowledged after the fence has no entry in the published index: {row}"
        );
    } else {
        assert!(
            row.contains(r#""tag":"a""#) && cluster.last_index(leader) == fenced,
            "a row was written from a descriptor the fence retired: {answer:?} {row}"
        );
        assert!(
            matches!(answer, DirectResponse::Fallback { .. }),
            "{answer:?}"
        );
    }
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r121_rd1_published_index_has_every_acknowledged_update() {
    scenario(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r121_rd1_no_write_between_fence_and_publication() {
    scenario(false).await;
}
