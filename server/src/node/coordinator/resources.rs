//! One remote allocation ledger per coordinator worker, shared across requests.
use super::*;
use crate::{
    cluster::{
        member::{Node, NodeState, Registration},
        state::Control,
    },
    config::WorkerResources,
    scheduling::ResourceScheduler,
};
use std::sync::Mutex;
struct Registry {
    scheduler: Option<ResourceScheduler>,
    ids: BTreeMap<(Uuid, Uuid), u64>,
    active: BTreeSet<u64>,
    next: u64,
    revision: u64,
}
pub(crate) struct RemoteScheduler {
    registry: Mutex<Registry>,
}
pub(super) struct RemoteView {
    pub scheduler: ResourceScheduler,
    pub nodes: BTreeMap<u64, Node>,
}
impl RemoteScheduler {
    pub(crate) fn new() -> Self {
        Self {
            registry: Mutex::new(Registry {
                scheduler: None,
                ids: BTreeMap::new(),
                active: BTreeSet::new(),
                next: 0,
                revision: 0,
            }),
        }
    }
    pub(super) fn view(&self, root: &Control) -> Result<RemoteView> {
        let mut registry = self.registry.lock().map_err(|_| unavailable())?;
        if root.revision < registry.revision {
            return Err(unavailable());
        }
        let mut nodes = BTreeMap::new();
        let mut limits = Vec::new();
        for node in root
            .nodes
            .iter()
            .filter(|n| n.state == NodeState::Ready && n.registration == Registration::Verified)
        {
            let key = (node.node_id, node.incarnation.ok_or_else(unavailable)?);
            let number = if let Some(number) = registry.ids.get(&key) {
                *number
            } else {
                let number = registry.next;
                registry.next = number.checked_add(1).ok_or_else(unavailable)?;
                registry.ids.insert(key, number);
                number
            };
            let resources = node.resources.unwrap_or(WorkerResources {
                max_threads: node.partition_slots,
                ..root.template.worker_resources()
            });
            limits.push((number, resources));
            nodes.insert(number, node.clone());
        }
        if nodes.is_empty() {
            return Err(unavailable());
        }
        let scheduler = match &registry.scheduler {
            Some(s) => s.clone(),
            None => {
                let s = ResourceScheduler::new(limits.clone())?;
                registry.scheduler = Some(s.clone());
                s
            }
        };
        let active = nodes.keys().copied().collect::<BTreeSet<_>>();
        for old in registry.active.difference(&active) {
            scheduler.drain_node(*old)?;
        }
        for (number, resources) in limits {
            scheduler.update_node(number, resources)?;
            scheduler.set_io_capacity(
                number,
                nodes.get(&number).ok_or_else(unavailable)?.partition_slots,
            )?;
        }
        registry.active = active;
        registry.revision = root.revision;
        Ok(RemoteView { scheduler, nodes })
    }
}
/// The worker reserves the same estimate (`crate::engine::job_memory_bytes`).
pub(super) fn task_bytes(job: &JobEnvelope) -> Result<u64> {
    crate::engine::job_memory_bytes(job)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn m01_m03_tiny_legacy_file_is_not_charged_as_both_format_maxima() {
        use crate::{
            storage::{Namespace, ObjectKey, Storage},
            worker_pool::WorkerPool,
        };
        let table = Uuid::from_u128(1);
        let snapshot = Uuid::from_u128(2);
        let object = format!("tables/{table}/{snapshot}.parquet");
        let job=job::parse_job(&serde_json::to_vec(&serde_json::json!({"version":1,
            "cluster_id":Uuid::from_u128(3),"operation_id":Uuid::from_u128(4),"job_id":Uuid::from_u128(5),"attempt_id":Uuid::from_u128(6),"request_digest":"a".repeat(64),
            "snapshot":{"catalog_revision":1,"catalog_commit_id":snapshot,"catalog_etag":"token"},"ordinal":0,
            "table":{"id":table,"name":"items","schema":{"id":"int64"},"key":"id","format":"parquet"},
            "task":{"kind":"get","payload":{"file":{"kind":"legacy","payload":{"object":object,"row_count":1}},"key":1}}})).unwrap()).unwrap();
        assert!(task_bytes(&job).unwrap() <= WorkerResources::default().task_memory_limit_bytes);
        let storage = Storage::new(
            crate::test_storage::Fake::default(),
            Namespace::new("legacy-resource/").unwrap(),
        );
        let rows = vec![[("id".into(), crate::protocol::Scalar::Int64(1))].into()];
        let bytes = crate::snapshot::encode(&job.table, &rows).unwrap();
        storage
            .put(
                &ObjectKey::parse_snapshot(&object, table).unwrap(),
                bytes,
                crate::storage::WriteCondition::Create,
            )
            .await
            .unwrap();
        let (pool, threads) = WorkerPool::start(
            std::num::NonZeroUsize::new(1).unwrap(),
            tokio::runtime::Handle::current(),
        )
        .unwrap();
        let (input, charge) = crate::engine::prepare_captured(&storage, &pool, &job)
            .await
            .unwrap();
        assert_eq!(
            pool.scheduler().usage()[0].1,
            0,
            "bounded download holds memory, never CPU"
        );
        drop((input, charge));
        assert_eq!(pool.scheduler().usage(), vec![(0, 0, 0)]);
        threads.shutdown().await.unwrap();
    }

    fn partition_job(
        object: &str,
        table: Uuid,
        rows: &[crate::protocol::Row],
        bytes: usize,
    ) -> JobEnvelope {
        let file = crate::node::job::FileInput::from(crate::storage::PartitionFile {
            object: object.into(),
            range: crate::storage::KeyRange {
                lower: None,
                upper: None,
            },
            row_count: rows.len() as u64,
            byte_size: bytes as u64,
            payload_bytes: rows.iter().map(crate::snapshot::row_payload).sum::<usize>() as u64,
        });
        job::parse_job(&serde_json::to_vec(&serde_json::json!({"version":1,
            "cluster_id":Uuid::from_u128(3),"operation_id":Uuid::from_u128(4),"job_id":Uuid::from_u128(5),"attempt_id":Uuid::from_u128(6),"request_digest":"a".repeat(64),
            "snapshot":{"catalog_revision":1,"catalog_commit_id":Uuid::from_u128(2),"catalog_etag":"token"},"ordinal":0,
            "table":{"id":table,"name":"items","schema":{"id":"int64","payload":"utf8"},"key":"id","format":"parquet"},
            "task":{"kind":"collect","payload":{"file":file,"query":{"select":null,"filter":null,"order_by":null,"limit":null}}}})).unwrap()).unwrap()
    }

    /// The bench's one-thread worker: before the fix every partition read
    /// reserved the whole 256 MiB task budget, so a third concurrent read of
    /// a few hundred bytes waited for memory that the coordinator's ledger,
    /// which charges the estimate, believed free.
    #[tokio::test]
    async fn m01_concurrent_small_partition_reads_fit_a_one_thread_worker() {
        use crate::{
            storage::{Namespace, ObjectKey, Storage},
            worker_pool::WorkerPool,
        };
        let table = Uuid::from_u128(1);
        let storage = Storage::new(
            crate::test_storage::Fake::default(),
            Namespace::new("small-partitions/").unwrap(),
        );
        let rows: Vec<crate::protocol::Row> = (0..16)
            .map(|id| {
                [
                    ("id".into(), crate::protocol::Scalar::Int64(id)),
                    (
                        "payload".into(),
                        crate::protocol::Scalar::Utf8("x".repeat(48)),
                    ),
                ]
                .into()
            })
            .collect();
        let key = ObjectKey::partition(table, Uuid::from_u128(9), 0);
        let descriptor: TableDescriptor = serde_json::from_value(serde_json::json!({
            "id": table, "name": "items", "schema": {"id": "int64", "payload": "utf8"},
            "key": "id", "format": "parquet"
        }))
        .unwrap();
        let bytes = crate::snapshot::encode(&descriptor, &rows).unwrap();
        let job = partition_job(key.as_str(), table, &rows, bytes.len());
        storage
            .put(&key, bytes, crate::storage::WriteCondition::Create)
            .await
            .unwrap();
        let resources = WorkerResources {
            max_threads: 1,
            memory_limit_bytes: 768 << 20,
            task_memory_limit_bytes: 256 << 20,
            control_memory_reserve_bytes: 128 << 20,
        };
        let (pool, threads) =
            WorkerPool::start_with_resources(resources, tokio::runtime::Handle::current()).unwrap();
        let mut held = Vec::new();
        for _ in 0..3 {
            let prepared = nabedra_rt::time::timeout(
                std::time::Duration::from_secs(5),
                crate::engine::prepare_captured(&storage, &pool, &job),
            )
            .await
            .expect("a small read must not wait for a whole task budget")
            .unwrap();
            assert!(prepared.1.bytes() <= task_bytes(&job).unwrap());
            held.push(prepared);
        }
        assert!(pool.scheduler().usage()[0].2 < 64 << 20);
        drop(held);
        threads.shutdown().await.unwrap();
    }

    #[test]
    fn m01_memory_refusal_of_a_job_is_a_definite_retryable_overload() {
        use crate::{node::payload_owner::OwnerError, protocol::ErrorCode};
        // The worker did not execute the job: overloaded, not invalid.
        assert_eq!(
            crate::node::payload_owner::prepare_refusal(ErrorCode::LimitExceeded),
            OwnerError::Overloaded
        );
        assert_eq!(
            crate::node::payload_owner::prepare_refusal(ErrorCode::ValidationError),
            OwnerError::Invalid
        );
        let answered = crate::node::runtime::job_command_error(OwnerError::Overloaded);
        assert_eq!(answered.code, ErrorCode::Overloaded);
        assert_eq!(answered.outcome, crate::protocol::Outcome::NotApplied);
        assert_eq!(answered.status_code(), 429);
        // The coordinator keeps that definite answer instead of unknown.
        let observed = job_status_error(429);
        assert_eq!(observed.code, ErrorCode::Overloaded);
        assert_eq!(observed.outcome, crate::protocol::Outcome::NotApplied);
        assert_eq!(job_status_error(503).code, ErrorCode::StorageUnavailable);
        assert_eq!(
            crate::node::runtime::job_command_error(OwnerError::Invalid).code,
            ErrorCode::StorageUnavailable
        );
    }

    /// OLAP-7: a filtered collect over a 100,000-row, 7-column table retains
    /// only its matched rows; charging every scanned row (537.6 MB at 768 B a
    /// cell) exceeded the 512 MiB default task limit.
    #[test]
    fn olap_7_collect_charges_retained_rows_not_scanned_rows() {
        let schema: serde_json::Map<String, serde_json::Value> = (0..7)
            .map(|c| {
                if c == 0 {
                    ("id".to_string(), serde_json::json!("int64"))
                } else {
                    (format!("c{c}"), serde_json::json!("utf8"))
                }
            })
            .collect();
        let table: TableDescriptor = serde_json::from_value(serde_json::json!({
            "id": Uuid::from_u128(1), "name": "wide", "schema": schema, "key": "id", "format": "parquet"
        }))
        .unwrap();
        let limit = WorkerResources::default().task_memory_limit_bytes;
        let scanned = crate::snapshot::memory_shape(&table, 100_000, 100_000 * 6 * 8, 0).unwrap();
        assert!(scanned > limit, "{scanned} against {limit}");
        let matched: Vec<crate::protocol::Row> = (0..1_000)
            .map(|id| {
                (0..7)
                    .map(|c| {
                        if c == 0 {
                            ("id".to_string(), crate::protocol::Scalar::Int64(id))
                        } else {
                            (
                                format!("c{c}"),
                                crate::protocol::Scalar::Utf8("v".repeat(8)),
                            )
                        }
                    })
                    .collect()
            })
            .collect();
        let retained = retained_charge(&table, &matched).unwrap();
        assert!(retained < limit / 64, "{retained}");
        let payload = matched
            .iter()
            .map(crate::snapshot::row_payload)
            .sum::<usize>();
        assert_eq!(
            retained,
            crate::snapshot::memory_shape(&table, 1_000, payload, 0).unwrap()
        );
    }
}
