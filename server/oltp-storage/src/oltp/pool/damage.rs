//! NS14: quarantined groups at pool level (plan section 3, "Raft safety of a
//! quarantined group", rules 1, 2 and 4, and R6).
//!
//! - **R6, sticky.** The pool keeps the set of quarantined groups it has
//!   accepted in a synced record (`ACCEPTED_KEY`, store-wide scope). An open
//!   counts as new damage every quarantined group outside that set. When the
//!   new damage passes the R6 threshold the open is refused and the record is
//!   left unchanged, so the next open counts the same groups and refuses again.
//!   Groups quarantined at run time are accepted as soon as they are found, so
//!   independent damage found weeks apart never adds up (RV-NS6 F2). An
//!   operator accepts a refused pool's damage by creating
//!   [`ACCEPT_DAMAGE_FILE`] in the pool directory; the next open accepts every
//!   quarantined group and removes the file.
//! - **Rule 1.** Run-time damage is published on a watch channel, so the Raft
//!   host of that group stops (see `manager/lifecycle.rs`). Every write of a
//!   quarantined group is refused and counted.
//! - **Rule 4.** [`OltpPool::release_damaged_group`] deletes a group's bytes
//!   only with [`DamagedReplicaRemoved`], the evidence that the membership
//!   change removing the damaged replica committed. It runs
//!   `Store::release_scope` off the runtime, then retires the group's snapshot
//!   files. A crash between the two leaves the group quarantined or its files
//!   behind; repeating the release finishes it.
use super::{
    OltpPool, PoolInner, failure,
    storage::{commit_error, group_scope, storage_error, wait_commit},
};
use crate::protocol::{ErrorCode, Outcome, ProtocolError};
use nabedra_kv::{Damage, DamageReason, Durability, Store, WriteBatch};
use nabedra_oltp_types::types::{AppliedPosition, GroupIdentity, ReplicaMember};
use nabedra_rt::fs::os as fs;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    path::Path,
    sync::{
        Mutex, OnceLock, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::sync::watch;
use uuid::Uuid;

/// The synced set of quarantined groups this pool accepted (JSON `[Uuid]`).
/// Store-wide scope: it sorts before every group key.
const ACCEPTED_KEY: &[u8] = b"\0typed-pool-quarantine-v1";
/// Created by an operator to accept the damage of a pool that R6 refused.
pub const ACCEPT_DAMAGE_FILE: &str = "NABEDRA_OLTP_POOL_ACCEPT_DAMAGE";
const MAX_ACCEPTED_BYTES: usize = 1 << 20;
/// The synced set of groups quarantined for damaged vector values (L6, JSON
/// `[Uuid]`). Store-wide scope, like the accepted record.
const VALUE_DAMAGE_KEY: &[u8] = b"\0typed-pool-value-damage-v1";

/// The groups an earlier run quarantined for damaged vector values.
pub(super) fn read_value_damage(store: &Store) -> Result<Vec<Uuid>, ProtocolError> {
    let Some(raw) = store.snapshot().get(VALUE_DAMAGE_KEY).map_err(storage_error)? else {
        return Ok(Vec::new());
    };
    if raw.len() > MAX_ACCEPTED_BYTES {
        return Err(accepted_unreadable());
    }
    serde_json::from_slice(&raw).map_err(|_| accepted_unreadable())
}
fn write_value_damage(store: &Store, groups: &BTreeSet<Uuid>) -> Result<(), ProtocolError> {
    let bytes = serde_json::to_vec(groups).map_err(|_| accepted_unreadable())?;
    if bytes.len() > MAX_ACCEPTED_BYTES {
        return Err(accepted_unreadable());
    }
    let mut batch = WriteBatch::new();
    batch.put(VALUE_DAMAGE_KEY, &bytes);
    let commit = store
        .submit(batch, Durability::Sync, Box::new(()))
        .map_err(|error| commit_error(error).1)?;
    wait_commit(commit).map_err(|error| commit_error(error).1)?;
    Ok(())
}
/// R6: more than this many groups quarantined by one open refuses the pool.
const R6_MAX_GROUPS: usize = 8;

/// Why a group is quarantined (`nabedra_kv::DamageReason`), as Control records it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DamageCause {
    JournalRecord,
    RunFile,
    RunBlock,
    JournalValue,
}
impl From<DamageReason> for DamageCause {
    fn from(reason: DamageReason) -> Self {
        match reason {
            DamageReason::JournalRecord => Self::JournalRecord,
            DamageReason::RunFile => Self::RunFile,
            DamageReason::RunBlock => Self::RunBlock,
            DamageReason::JournalValue => Self::JournalValue,
        }
    }
}

/// One quarantined group of this pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupDamage {
    pub(crate) group: Uuid,
    pub cause: DamageCause,
    pub first_bad_seq: Option<u64>,
}

/// Rule 4 evidence: the membership change that removed `removed` from `group`
/// committed, so the damaged replica's bytes are no longer needed by anyone.
/// Built only from a quorum observation of the group (`manager/lifecycle.rs`)
/// or from Control's applied `GroupDamaged` record in its replaced phase.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DamagedReplicaRemoved {
    pub group: GroupIdentity,
    pub removed: ReplicaMember,
    pub committed_at: AppliedPosition,
}

/// What [`OltpPool::release_damaged_group`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReleaseOutcome {
    Released,
    /// The group retained nothing here any more (an earlier release finished).
    AlreadyReleased,
}

/// Per-pool damage state, owned by `PoolInner`.
pub(super) struct DamageState {
    /// Groups found damaged at run time since this open. The sender drops with
    /// the pool, which ends every watcher.
    events: watch::Sender<BTreeSet<Uuid>>,
    pool: OnceLock<Weak<PoolInner>>,
    /// Serializes rewrites of the accepted record.
    accept: Mutex<()>,
    refused_writes: AtomicU64,
    releases: AtomicU64,
    /// Groups whose stored vector values failed their own checksum, which kv
    /// cannot see. Loaded from the pool's synced record at open.
    values: Mutex<BTreeSet<Uuid>>,
    any_value: AtomicBool,
    /// Orders the rewrites of the value damage record.
    value_writes: Mutex<()>,
}
impl DamageState {
    pub(super) fn new() -> Self {
        Self {
            events: watch::Sender::new(BTreeSet::new()),
            pool: OnceLock::new(),
            accept: Mutex::new(()),
            refused_writes: AtomicU64::new(0),
            releases: AtomicU64::new(0),
            values: Mutex::new(BTreeSet::new()),
            any_value: AtomicBool::new(false),
            value_writes: Mutex::new(()),
        }
    }
    fn values(&self) -> std::sync::MutexGuard<'_, BTreeSet<Uuid>> {
        self.values.lock().unwrap_or_else(|poison| poison.into_inner())
    }
    /// Groups quarantined for damaged values, at open or at run time.
    pub(super) fn mark_values(&self, groups: impl IntoIterator<Item = Uuid>) {
        let mut values = self.values();
        values.extend(groups);
        if !values.is_empty() {
            self.any_value.store(true, Ordering::Release);
        }
    }
    pub(super) fn damaged_values(&self) -> Vec<Uuid> {
        self.values().iter().copied().collect()
    }
    /// One atomic load while no value is damaged.
    pub(super) fn value_damaged(&self, group: Uuid) -> bool {
        self.any_value.load(Ordering::Acquire) && self.values().contains(&group)
    }
    pub(super) fn bind(&self, pool: Weak<PoolInner>) {
        let _ = self.pool.set(pool);
    }
    pub(super) fn count_refusal(&self) {
        self.refused_writes.fetch_add(1, Ordering::Relaxed);
    }
    /// The kv damage hook: publish the group and accept it durably off the
    /// caller's thread (the hook may run inside a read or a compaction).
    pub(super) fn found(&self, damage: &Damage) {
        let Some(group) = group_of_scope(&damage.scope) else {
            return;
        };
        let added = self.events.send_if_modified(|groups| groups.insert(group));
        if !added {
            return;
        }
        let Some(pool) = self.pool.get().cloned() else {
            return;
        };
        let spawned =
            nabedra_rt::task::spawn_oneshot("oltp-damage", nabedra_kv::THREAD_STACK_BYTES, move || {
                let Some(inner) = pool.upgrade() else {
                    return;
                };
                let pool = OltpPool { inner };
                if let Err(error) = pool.accept_current() {
                    // The next open counts this group as new damage, which can
                    // only make R6 stricter.
                    tracing::warn!(%group, %error, "could not record run-time quarantine");
                }
            });
        if spawned.is_err() {
            tracing::warn!(%group, "could not record run-time quarantine: no thread");
        }
    }
}

/// R6 at open (plan section 3), sticky. Returns the accepted set to persist,
/// when it changes. A pool without the record has accepted nothing, so a pool
/// that never had damage writes nothing at open.
pub(super) fn admit(
    path: &Path,
    store: &Store,
    quarantined: &[Damage],
    healthy_groups: usize,
) -> Result<Option<BTreeSet<Uuid>>, ProtocolError> {
    let current: BTreeSet<Uuid> = quarantined
        .iter()
        .filter_map(|damage| group_of_scope(&damage.scope))
        .collect();
    let accepted = read_accepted(store)?.unwrap_or_default();
    let override_file = path.join(ACCEPT_DAMAGE_FILE);
    let operator_accepts = fs::try_exists(&override_file).unwrap_or(false);
    let new: BTreeSet<Uuid> = if operator_accepts {
        BTreeSet::new()
    } else {
        current.difference(&accepted).copied().collect()
    };
    let groups = current.len() + healthy_groups;
    if new.len() > R6_MAX_GROUPS || (new.len() > 1 && new.len() * 10 > groups) {
        tracing::error!(
            path = %path.display(),
            damaged = new.len(),
            groups,
            "refusing OLTP pool: too many damaged groups; it stays refused until an operator \
             creates {ACCEPT_DAMAGE_FILE} or the pool is replaced"
        );
        return Err(refused_by_r6());
    }
    if operator_accepts {
        tracing::warn!(
            path = %path.display(),
            quarantined = current.len(),
            "operator accepted the pool's quarantined groups"
        );
    }
    for damage in quarantined {
        tracing::warn!(
            path = %path.display(),
            group = ?group_of_scope(&damage.scope),
            reason = ?damage.reason,
            file = ?damage.file,
            first_bad_seq = ?damage.first_bad_seq,
            "OLTP group quarantined; it will not start until Control replaces it"
        );
    }
    Ok((accepted != current || operator_accepts).then_some(current))
}

/// Persists the accepted set at open, then removes the operator's override.
pub(super) fn persist_accepted(
    path: &Path,
    store: &Store,
    accepted: &BTreeSet<Uuid>,
) -> Result<(), ProtocolError> {
    write_accepted(store, accepted)?;
    let override_file = path.join(ACCEPT_DAMAGE_FILE);
    if fs::try_exists(&override_file).unwrap_or(false) {
        fs::remove_file(&override_file)
            .and_then(|_| fs::sync_dir(path))
            .map_err(|_| {
                failure(
                    ErrorCode::StorageUnavailable,
                    "could not remove the accepted-damage marker",
                )
            })?;
    }
    Ok(())
}

fn read_accepted(store: &Store) -> Result<Option<BTreeSet<Uuid>>, ProtocolError> {
    let Some(raw) = store.snapshot().get(ACCEPTED_KEY).map_err(storage_error)? else {
        return Ok(None);
    };
    if raw.len() > MAX_ACCEPTED_BYTES {
        return Err(accepted_unreadable());
    }
    serde_json::from_slice::<BTreeSet<Uuid>>(&raw)
        .map(Some)
        .map_err(|_| accepted_unreadable())
}

fn write_accepted(store: &Store, accepted: &BTreeSet<Uuid>) -> Result<(), ProtocolError> {
    let bytes = serde_json::to_vec(accepted).map_err(|_| accepted_unreadable())?;
    if bytes.len() > MAX_ACCEPTED_BYTES {
        return Err(accepted_unreadable());
    }
    let mut batch = WriteBatch::new();
    batch.put(ACCEPTED_KEY, &bytes);
    let commit = store
        .submit(batch, Durability::Sync, Box::new(()))
        .map_err(|error| commit_error(error).1)?;
    wait_commit(commit).map_err(|error| commit_error(error).1)?;
    Ok(())
}

impl OltpPool {
    /// Quarantines a group with a missing or damaged vector value (L6).
    /// Publish the quarantine after persisting the pool's set. Serialize the
    /// complete rewrite with other reports and releases.
    pub(crate) async fn report_value_damage(&self, group: Uuid) {
        if self.inner.damage.value_damaged(group) {
            return;
        }
        let pool = self.clone();
        let saved = nabedra_rt::task::spawn_blocking(move || {
            let damage = &pool.inner.damage;
            let _serial = damage.value_writes.lock().unwrap_or_else(|poison| poison.into_inner());
            let mut groups = damage.values().clone();
            if !groups.insert(group) {
                return;
            }
            if let Err(error) = pool
                .healthy()
                .and_then(|()| write_value_damage(&pool.inner.store, &groups))
            {
                pool.latch();
                tracing::error!(%group, %error, "could not persist a vector damage quarantine");
            }
            damage.mark_values([group]);
            damage.events.send_if_modified(|groups| groups.insert(group));
            tracing::error!(%group, "OLTP group quarantined: a stored vector value is damaged");
        })
        .await;
        if let Err(error) = saved {
            self.latch();
            tracing::error!(%group, %error, "vector damage quarantine was interrupted");
        }
    }

    /// Rewrites the accepted record to the store's current quarantined groups.
    /// Blocks on a synced write: call it off the async runtime.
    fn accept_current(&self) -> Result<(), ProtocolError> {
        let _serial = self
            .inner
            .damage
            .accept
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let current: BTreeSet<Uuid> = self
            .inner
            .store
            .quarantined()
            .iter()
            .filter_map(|damage| group_of_scope(&damage.scope))
            .collect();
        if read_accepted(&self.inner.store)?.unwrap_or_default() == current {
            return Ok(());
        }
        self.healthy()?;
        write_accepted(&self.inner.store, &current).inspect_err(|_| self.latch())
    }

    /// Every quarantined group, at open or at run time, with its first damage.
    pub(crate) fn damaged_groups(&self) -> Vec<GroupDamage> {
        let mut seen = BTreeSet::new();
        self.inner
            .store
            .quarantined()
            .into_iter()
            .filter_map(|damage| {
                let group = group_of_scope(&damage.scope)?;
                seen.insert(group).then_some(GroupDamage {
                    group,
                    cause: damage.reason.into(),
                    first_bad_seq: damage.first_bad_seq,
                })
            })
            .chain(
                self.inner
                    .damage
                    .damaged_values()
                    .into_iter()
                    .map(|group| GroupDamage {
                        group,
                        cause: DamageCause::RunBlock,
                        first_bad_seq: None,
                    }),
            )
            .collect()
    }
    pub fn group_damage(&self, group: Uuid) -> Option<GroupDamage> {
        self.damaged_groups()
            .into_iter()
            .find(|damage| damage.group == group)
    }
    /// True while the group is quarantined. One atomic load when nothing is.
    pub fn is_group_quarantined(&self, group: Uuid) -> bool {
        self.group_quarantined(group)
    }
    /// Groups found damaged at run time since this open. The channel closes
    /// when the pool does.
    pub fn watch_damage(&self) -> watch::Receiver<BTreeSet<Uuid>> {
        self.inner.damage.events.subscribe()
    }
    /// Writes of quarantined groups refused since this open (rule 1 evidence).
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    pub fn quarantine_refusals(&self) -> u64 {
        self.inner.damage.refused_writes.load(Ordering::Relaxed)
    }
    /// Damaged groups released since this open.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    pub fn damage_releases(&self) -> u64 {
        self.inner.damage.releases.load(Ordering::Relaxed)
    }

    /// Rule 4: deletes a damaged group's bytes once the membership change that
    /// removed its replica committed, then retires its snapshot files. Refused
    /// while any handle of the group is still open. Idempotent.
    pub async fn release_damaged_group(
        &self,
        removed: DamagedReplicaRemoved,
    ) -> Result<ReleaseOutcome, ProtocolError> {
        let id = removed.group.group_id;
        if removed.group.cluster_id != self.identity().cluster_id
            || removed.removed.pool_id != self.identity().pool_id
            || removed.removed.worker_id != self.identity().worker_id
        {
            return Err(failure(
                ErrorCode::ValidationError,
                "damaged replica is not on this pool",
            ));
        }
        // Holding the handle registry excludes a concurrent open of the group.
        let groups = self.inner.groups.clone().lock_owned().await;
        let pool = self.clone();
        nabedra_rt::task::spawn_blocking(move || {
            if groups
                .get(&id)
                .is_some_and(|group| group.strong_count() > 0)
            {
                return Err(ProtocolError::new(
                    ErrorCode::Conflict,
                    "a handle of the damaged group is still open",
                    Outcome::NotApplied,
                ));
            }
            // A replica given up by its supervisor (RV-APPLY F4) is not
            // quarantined, but its bytes are just as unneeded once the
            // change removing it committed (RV-FINAL M4).
            let retained = pool.group_quarantined(id)
                || pool
                    .snapshot()
                    .get(&super::identity_key(id))
                    .map_err(storage_error)?
                    .is_some();
            let outcome = if retained {
                pool.healthy()?;
                pool.inner
                    .store
                    .release_scope(&group_scope(id))
                    .map_err(|error| pool.failed(error))?;
                pool.inner.damage.releases.fetch_add(1, Ordering::Relaxed);
                let damage = &pool.inner.damage;
                let _serial = damage.value_writes.lock().unwrap_or_else(|poison| poison.into_inner());
                if damage.values().remove(&id) {
                    let rest = damage.values().clone();
                    write_value_damage(&pool.inner.store, &rest).inspect_err(|_| pool.latch())?;
                }
                ReleaseOutcome::Released
            } else {
                ReleaseOutcome::AlreadyReleased
            };
            super::snapshot::retire_group_files(&pool.inner.path, id)?;
            pool.accept_current()?;
            drop(groups);
            tracing::warn!(
                group = %id,
                removed = removed.removed.node_id,
                committed_at = removed.committed_at.index,
                ?outcome,
                "released a damaged OLTP group after its replacement committed"
            );
            Ok(outcome)
        })
        .await
        .map_err(|_| {
            failure(
                ErrorCode::StorageUnavailable,
                "damaged group release was interrupted",
            )
        })?
    }
}

/// The group a quarantined scope names (`0x40 ‖ id`).
pub(super) fn group_of_scope(scope: &[u8]) -> Option<Uuid> {
    (scope.len() == super::storage::GROUP_SCOPE_BYTES && scope[0] == 0x40)
        .then(|| Uuid::from_slice(&scope[1..]).ok())
        .flatten()
}

/// Section 3 rule 5 names this `StorageCorrupt`. The protocol has no such code
/// (adding one changes every ORM and old peer), so it is `StorageUnavailable`
/// with a message that starts with "storage corrupt".
pub fn quarantined_group() -> ProtocolError {
    failure(
        ErrorCode::StorageUnavailable,
        "storage corrupt: OLTP group is quarantined after storage damage",
    )
}

/// R1 to R4 at open (NS24): the refusal names what kv found. Corruption (R1,
/// R3, and kv's own limit of 8 damaged scopes) and an unreadable store (R2)
/// stay `StorageUnavailable`, so callers and ORMs see the code they already
/// handle; the message tells an operator which case it is.
pub(super) fn open_refused(error: nabedra_kv::Error) -> ProtocolError {
    match error {
        nabedra_kv::Error::Corruption { what, .. } => failure(
            ErrorCode::StorageUnavailable,
            &format!("OLTP pool refused: storage corrupt: {what}"),
        ),
        nabedra_kv::Error::Io(kind) => failure(
            ErrorCode::StorageUnavailable,
            &format!("OLTP pool refused: storage I/O error: {kind}"),
        ),
        other => storage_error(other),
    }
}

fn refused_by_r6() -> ProtocolError {
    failure(
        ErrorCode::StorageUnavailable,
        "OLTP pool refused: damage spans too many groups",
    )
}

fn accepted_unreadable() -> ProtocolError {
    failure(
        ErrorCode::StorageUnavailable,
        "OLTP pool quarantine record unreadable",
    )
}
