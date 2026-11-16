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
pub(super) fn task_bytes(job: &JobEnvelope) -> Result<u64> {
    let file = match &job.task {
        Task::Get { file, .. } | Task::Collect { file, .. } | Task::Count { file, .. } => {
            Some(file)
        }
        Task::Prepare { file, .. } => file.as_ref(),
    };
    let (mut count, mut payload, encoded) = match file {
        Some(FileInput::Partition {
            row_count,
            payload_bytes,
            byte_size,
            ..
        }) => (*row_count, *payload_bytes, *byte_size),
        // A legacy manifest contains no encoded or decoded byte counts. Use
        // only its known lower bound for advisory placement. The worker bounds
        // the download and checks actual metadata/payload before allocating it.
        Some(FileInput::Legacy { row_count, .. }) => (*row_count, 0, 0),
        None => (0, 0, 0),
    };
    if let Task::Prepare { mutation, .. } = &job.task {
        let rows = match mutation {
            RowMutation::Insert(p) | RowMutation::Upsert(p) => std::slice::from_ref(&p.row),
            RowMutation::InsertMany(p) => p.rows.as_slice(),
            RowMutation::Update(p) => std::slice::from_ref(&p.values),
            _ => &[],
        };
        count = count
            .checked_add(rows.len() as u64)
            .ok_or_else(|| error(ErrorCode::LimitExceeded))?;
        for row in rows {
            payload = payload
                .checked_add(crate::snapshot::row_payload(row) as u64)
                .ok_or_else(|| error(ErrorCode::LimitExceeded))?;
        }
    }
    crate::snapshot::memory_shape(
        &job.table,
        usize::try_from(count).map_err(|_| invalid())?,
        usize::try_from(payload).map_err(|_| invalid())?,
        usize::try_from(encoded).map_err(|_| invalid())?,
    )
    .map(|n| n.max(4096))
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
}
