//! In-process holds for one Catalog's index metadata commits (IDX1). A test
//! registers a Catalog group and then receives every metadata transaction of
//! that Catalog twice: after Prepare (intents written, no decision) and after
//! the decision, before the coordinator resolves its intents. The transaction waits until
//! the test releases it. Other Catalogs, and so other tests, are never held.
use crate::oltp_replication::types::TxnId;
use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stage {
    Prepared,
    Decided,
}

pub(crate) struct Held {
    pub stage: Stage,
    pub txn: TxnId,
    pub release: oneshot::Sender<()>,
}

#[derive(Default)]
struct Registry {
    catalogs: HashMap<Uuid, mpsc::UnboundedSender<Held>>,
    txns: HashMap<Uuid, Uuid>,
    checkpoints: HashMap<(Uuid, Checkpoint), oneshot::Sender<BuildHold>>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum Checkpoint {
    Backfill,
    Refresh,
    RefreshAdmissionRefused,
    RefreshPrepare,
    Validation,
    ReadPin,
    Reclaim,
}

#[derive(Debug)]
pub(crate) struct PageBuffers {
    pub rows: usize,
    pub source_rows: usize,
    pub page_bytes: usize,
    pub cursor_bytes: usize,
    pub exhausted: bool,
}

pub(crate) struct BuildHold {
    pub buffers: Option<PageBuffers>,
    pub release: oneshot::Sender<bool>,
}

pub(crate) fn register_checkpoint(id: Uuid, stage: Checkpoint) -> oneshot::Receiver<BuildHold> {
    let (send, receive) = oneshot::channel();
    registry()
        .lock()
        .unwrap()
        .checkpoints
        .insert((id, stage), send);
    receive
}

pub(crate) async fn checkpoint(
    id: Uuid,
    stage: Checkpoint,
    buffers: impl FnOnce() -> Option<PageBuffers>,
) -> bool {
    let send = registry().lock().unwrap().checkpoints.remove(&(id, stage));
    let Some(send) = send else { return false };
    let (release, decision) = oneshot::channel();
    if send
        .send(BuildHold {
            buffers: buffers(),
            release,
        })
        .is_err()
    {
        return false;
    }
    decision.await.unwrap_or(false)
}

pub(crate) fn unregister_checkpoint(id: Uuid, stage: Checkpoint) {
    registry().lock().unwrap().checkpoints.remove(&(id, stage));
}

fn registry() -> &'static Mutex<Registry> {
    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(Default::default)
}

/// Hold every later index metadata commit of `catalog`. Dropping the receiver
/// stops new holds; an already held transaction continues when its `release`
/// sender is used or dropped.
pub(crate) fn register(catalog: Uuid) -> mpsc::UnboundedReceiver<Held> {
    let (sender, receiver) = mpsc::unbounded_channel();
    registry().lock().unwrap().catalogs.insert(catalog, sender);
    receiver
}

pub(crate) fn unregister(catalog: Uuid) {
    let mut registry = registry().lock().unwrap();
    registry.catalogs.remove(&catalog);
    registry.txns.retain(|_, held| *held != catalog);
}

/// Bind an index metadata transaction to its Catalog before Prepare, so the
/// generic transaction path can find the hold by transaction ID.
pub(crate) fn bind(catalog: Uuid, txn: TxnId) {
    let mut registry = registry().lock().unwrap();
    if registry.catalogs.contains_key(&catalog) {
        registry.txns.insert(txn.request_id, catalog);
    }
}

pub(crate) async fn prepared(txn: TxnId) {
    let catalog = registry()
        .lock()
        .unwrap()
        .txns
        .get(&txn.request_id)
        .copied();
    if let Some(catalog) = catalog {
        hold(catalog, Stage::Prepared, txn).await;
    }
}

pub(crate) async fn decided(txn: TxnId) {
    let catalog = registry().lock().unwrap().txns.remove(&txn.request_id);
    if let Some(catalog) = catalog {
        hold(catalog, Stage::Decided, txn).await;
    }
}

async fn hold(catalog: Uuid, stage: Stage, txn: TxnId) {
    let (release, released) = oneshot::channel();
    let sent = registry()
        .lock()
        .unwrap()
        .catalogs
        .get(&catalog)
        .is_some_and(|sender| {
            sender
                .send(Held {
                    stage,
                    txn,
                    release,
                })
                .is_ok()
        });
    if sent {
        // A dropped sender also releases the transaction.
        let _ = released.await;
    }
}
