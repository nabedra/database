//! Receiver-side cache of Control `service_role` checks.
//!
//! The cache records only that a caller incarnation passed a role check
//! (action, target group and optional replica placement). It never serves the
//! grant body: owner, effective authority and ready view are what receiving
//! services compare against a request's fences, so [`GrantCache::grant`]
//! always reads them fresh from Control. An authority change for the same
//! owner (an owner-term bump or a cold fence) therefore never meets a stale
//! cached authority.
//!
//! A role entry is dropped [`GRANT_TTL`] after the Control read that produced
//! it started, when the locally applied Control enrollment revision changes,
//! or when a fresh Control read reports a different ready view or owner. A
//! revoked caller can therefore pass a role check for at most five seconds.
//! Data safety does not depend on this cache: every write checks its fence in
//! the Data log.
//!
//! Only roles whose check reflects stable membership are cached. Request-,
//! transfer- and transition-scoped actions always read Control.
use crate::{
    oltp_replication::{
        authority::control::{ServiceAction, ServiceAuthorization},
        types::{GroupIdentity, ReadyMembership, WorkerIdentity},
    },
    protocol::ProtocolError,
};
use sha2::{Digest as _, Sha256};
use std::{
    collections::HashMap,
    sync::{Mutex, PoisonError},
    time::Duration,
};
use tokio::time::Instant;
use uuid::Uuid;

pub(crate) const GRANT_TTL: Duration = Duration::from_secs(5);
const MAX_GRANTS: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct GrantKey([u8; 32]);
impl GrantKey {
    /// `None` for an action this cache never holds.
    pub(crate) fn new(
        remote: WorkerIdentity,
        action: ServiceAction,
        target: &GroupIdentity,
        placement: Option<(u64, Uuid)>,
    ) -> Option<Self> {
        let cacheable = match placement {
            Some(_) => action == ServiceAction::Owner,
            None => matches!(
                action,
                ServiceAction::Owner
                    | ServiceAction::DirectLeader
                    | ServiceAction::StatusProof { .. }
                    | ServiceAction::TimestampProof { .. }
                    | ServiceAction::ParticipantProof { .. }
            ),
        };
        if !cacheable {
            return None;
        }
        let encoded = serde_json::to_vec(&(remote, action, target, placement)).ok()?;
        let mut hash = Sha256::new();
        hash.update(b"nabedra/oltp/service-grant/v1\0");
        hash.update(&encoded);
        Some(Self(hash.finalize().into()))
    }
}

impl GrantKey {
    /// An API process's `ApiIngress` role for `Method::Direct` and
    /// `Method::DirectForward` at a range leader (T7b). The check compares
    /// no owner or authority (the leader checks its fence in the Data log),
    /// so it is cached under the same rules as the stable roles: dropped
    /// after [`GRANT_TTL`], on an enrollment revision change, or when a
    /// fresh read sees another ready view or owner. Other `ApiIngress`
    /// checks stay uncached.
    pub(crate) fn direct_ingress(remote: WorkerIdentity, target: &GroupIdentity) -> Option<Self> {
        let encoded = serde_json::to_vec(&(remote, ServiceAction::ApiIngress, target)).ok()?;
        let mut hash = Sha256::new();
        hash.update(b"nabedra/oltp/direct-ingress-grant/v1\0");
        hash.update(&encoded);
        Some(Self(hash.finalize().into()))
    }
}

/// The parts of a fresh grant that invalidate every cached role when they move.
type Observed = (ReadyMembership, Option<WorkerIdentity>);
fn observed(grant: &ServiceAuthorization) -> Observed {
    (grant.ready, grant.owner)
}

#[derive(Default)]
struct Grants {
    /// Role checks passed, by key, with their expiry.
    entries: HashMap<GrantKey, Instant>,
    revision: Option<u64>,
    observed: Option<Observed>,
}
impl Grants {
    fn align(&mut self, revision: Option<u64>) {
        if self.revision != revision {
            self.entries.clear();
            self.observed = None;
            self.revision = revision;
        }
    }
}

#[derive(Default)]
pub(crate) struct GrantCache(Mutex<Grants>);
impl GrantCache {
    fn grants(&self) -> std::sync::MutexGuard<'_, Grants> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
    /// Whether a role check for `key` passed recently at this applied revision.
    pub(crate) fn allowed(&self, key: GrantKey, revision: Option<u64>) -> bool {
        let mut grants = self.grants();
        grants.align(revision);
        match grants.entries.get(&key) {
            Some(&expires) if expires > Instant::now() => true,
            Some(_) => {
                grants.entries.remove(&key);
                false
            }
            None => false,
        }
    }
    /// Record a passed role check from a Control read that started at
    /// `started` with the applied revision `revision`. A read that raced a
    /// newer revision is not recorded.
    pub(crate) fn insert(
        &self,
        key: Option<GrantKey>,
        revision: Option<u64>,
        started: Instant,
        observed: Option<Observed>,
    ) {
        let mut grants = self.grants();
        match grants.revision.cmp(&revision) {
            std::cmp::Ordering::Greater => return,
            std::cmp::Ordering::Less => grants.align(revision),
            std::cmp::Ordering::Equal => {}
        }
        if let Some(observed) = observed {
            if grants.observed.is_some_and(|previous| previous != observed) {
                grants.entries.clear();
            }
            grants.observed = Some(observed);
        }
        let Some(key) = key else {
            return;
        };
        let expires = started + GRANT_TTL;
        let now = Instant::now();
        if expires <= now {
            return;
        }
        if grants.entries.len() >= MAX_GRANTS {
            grants.entries.retain(|_, expires| *expires > now);
            if grants.entries.len() >= MAX_GRANTS {
                grants.entries.clear();
            }
        }
        grants.entries.insert(key, expires);
    }
    /// A role check: served from the cache while valid, otherwise `read`
    /// (which reports the fresh view it saw, if any) runs and is recorded.
    pub(crate) async fn role<F>(
        &self,
        key: Option<GrantKey>,
        revision: Option<u64>,
        read: impl FnOnce() -> F,
    ) -> Result<(), ProtocolError>
    where
        F: Future<Output = Result<Option<ServiceAuthorization>, ProtocolError>>,
    {
        if key.is_some_and(|key| self.allowed(key, revision)) {
            return Ok(());
        }
        let started = Instant::now();
        let grant = read().await?;
        self.insert(key, revision, started, grant.as_ref().map(observed));
        Ok(())
    }
    /// A full grant, always read fresh because callers compare its owner and
    /// authority with the request. The passed role check is recorded.
    pub(crate) async fn grant<F>(
        &self,
        key: Option<GrantKey>,
        revision: Option<u64>,
        read: impl FnOnce() -> F,
    ) -> Result<ServiceAuthorization, ProtocolError>
    where
        F: Future<Output = Result<ServiceAuthorization, ProtocolError>>,
    {
        let started = Instant::now();
        let grant = read().await?;
        self.insert(key, revision, started, Some(observed(&grant)));
        Ok(grant)
    }
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.grants().entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oltp_replication::types::{AuthorityView, Digest, GroupKind, Text};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn worker() -> WorkerIdentity {
        WorkerIdentity {
            worker_id: Uuid::new_v4(),
            incarnation: Uuid::new_v4(),
            certificate_sha256: Digest::from_bytes([7; 32]),
            enrollment_revision: 1,
        }
    }
    fn group() -> GroupIdentity {
        GroupIdentity {
            cluster_id: Uuid::new_v4(),
            database: Text::try_from("grants").unwrap(),
            group_id: Uuid::new_v4(),
            kind: GroupKind::Timestamps,
            group_format_version: 1,
        }
    }
    fn grant_at(owner: WorkerIdentity, owner_term: u64, revision: u64) -> ServiceAuthorization {
        let ready = ReadyMembership {
            view: AuthorityView {
                run_id: Uuid::from_u128(1),
                owner_term,
                membership_revision: revision,
            },
            ready_root: Digest::from_bytes([1; 32]),
            ready_count: 3,
        };
        ServiceAuthorization {
            ready,
            owner: Some(owner),
            effective_authority: ready.view.authority(),
            pending_ready: None,
            scoped_ingress: None,
        }
    }
    fn grant(owner: WorkerIdentity, revision: u64) -> ServiceAuthorization {
        grant_at(owner, 1, revision)
    }
    fn key(caller: WorkerIdentity, target: &GroupIdentity) -> Option<GrantKey> {
        GrantKey::new(caller, ServiceAction::Owner, target, None)
    }

    /// D10/O14: a passed role check serves repeat checks for the same caller
    /// incarnation and role, and expires five seconds after its Control read
    /// started, however long that read took.
    #[tokio::test(start_paused = true)]
    async fn d10_o14_grant_cache_expires_five_seconds_after_the_read_started() {
        let cache = GrantCache::default();
        let caller = worker();
        let target = group();
        let key = key(caller, &target);
        let reads = AtomicUsize::new(0);
        let read = || async {
            reads.fetch_add(1, Ordering::Relaxed);
            Ok(Some(grant(caller, 1)))
        };
        cache.role(key, Some(4), read).await.unwrap();
        cache.role(key, Some(4), read).await.unwrap();
        assert_eq!(reads.load(Ordering::Relaxed), 1);
        tokio::time::advance(GRANT_TTL - Duration::from_millis(1)).await;
        assert!(cache.allowed(key.unwrap(), Some(4)));
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(!cache.allowed(key.unwrap(), Some(4)));
        assert_eq!(cache.len(), 0);

        // A slow Control read: the window starts when the read started.
        let slow = || async {
            tokio::time::sleep(Duration::from_secs(3)).await;
            Ok(Some(grant(caller, 1)))
        };
        cache.role(key, Some(4), slow).await.unwrap();
        assert!(cache.allowed(key.unwrap(), Some(4)));
        tokio::time::advance(GRANT_TTL - Duration::from_secs(3)).await;
        assert!(!cache.allowed(key.unwrap(), Some(4)));
        // A read slower than the whole window is not recorded at all.
        let slower = || async {
            tokio::time::sleep(GRANT_TTL).await;
            Ok(None)
        };
        cache.role(key, Some(4), slower).await.unwrap();
        assert_eq!(cache.len(), 0);

        // A replaced incarnation has its own key and never sees the old grant.
        cache.insert(key, Some(4), Instant::now(), None);
        let replacement = WorkerIdentity {
            incarnation: Uuid::new_v4(),
            enrollment_revision: 2,
            ..caller
        };
        let replaced = GrantKey::new(replacement, ServiceAction::Owner, &target, None).unwrap();
        assert_ne!(Some(replaced), key);
        assert!(!cache.allowed(replaced, Some(4)));
        // The role is part of the key, as is the target group.
        let proof = ServiceAction::StatusProof {
            source_group: Uuid::new_v4(),
        };
        assert_ne!(GrantKey::new(caller, proof, &target, None), key);
        assert_ne!(
            GrantKey::new(caller, ServiceAction::Owner, &group(), None),
            key
        );
        assert_ne!(
            GrantKey::new(
                caller,
                ServiceAction::Owner,
                &target,
                Some((2, Uuid::new_v4()))
            ),
            key
        );
    }

    /// D10/O14: a change in the locally applied Control revision drops every
    /// cached role, and so does a fresh read that reports a new ready view or
    /// owner. A read that raced a newer revision cannot roll the cache back.
    #[tokio::test(start_paused = true)]
    async fn d10_o14_grant_cache_drops_entries_on_control_revision_change() {
        let cache = GrantCache::default();
        let caller = worker();
        let other = worker();
        let target = group();
        let key = key(caller, &target);
        let other_key = GrantKey::new(
            other,
            ServiceAction::TimestampProof {
                source_group: Uuid::new_v4(),
            },
            &target,
            None,
        );
        let now = Instant::now();
        cache.insert(key, Some(4), now, Some(observed(&grant(caller, 1))));
        cache.insert(other_key, Some(4), now, Some(observed(&grant(caller, 1))));
        assert_eq!(cache.len(), 2);
        assert!(!cache.allowed(key.unwrap(), Some(5)));
        assert!(!cache.allowed(other_key.unwrap(), Some(5)));
        assert_eq!(cache.len(), 0);
        // A read made at revision 4 finishes after the cache moved to 5.
        cache.insert(key, Some(4), now, None);
        assert_eq!(cache.len(), 0);
        assert!(!cache.allowed(key.unwrap(), Some(5)));

        // A directory without an applied revision relies on the TTL and the
        // observed Control view.
        let cache = GrantCache::default();
        cache.insert(key, None, now, Some(observed(&grant(caller, 1))));
        cache.insert(other_key, None, now, Some(observed(&grant(caller, 1))));
        assert_eq!(cache.len(), 2);
        let moved = grant(other, 2);
        cache.insert(other_key, None, now, Some(observed(&moved)));
        assert!(!cache.allowed(key.unwrap(), None));
        assert!(cache.allowed(other_key.unwrap(), None));
    }

    /// O14/R series: the same owner keeps its role while Control moves its
    /// authority (an owner-term bump). With the role check warm, the next
    /// grant still carries the new authority, so a request at the current
    /// authority is accepted, not rejected for up to five seconds.
    #[tokio::test(start_paused = true)]
    async fn o14_same_owner_authority_change_with_warm_cache_is_not_rejected() {
        let cache = GrantCache::default();
        let owner = worker();
        let target = group();
        let key = key(owner, &target);
        let control = std::sync::Mutex::new(grant_at(owner, 1, 1));
        let reads = AtomicUsize::new(0);
        let read = || async {
            reads.fetch_add(1, Ordering::Relaxed);
            Ok(*control.lock().unwrap())
        };
        let before = cache.grant(key, Some(4), read).await.unwrap();
        // Warm: the role check alone needs no Control read.
        cache
            .role(key, Some(4), || async {
                panic!("a warm role check reads Control")
            })
            .await
            .unwrap();
        assert_eq!(reads.load(Ordering::Relaxed), 1);

        // Control fences the next owner term for the same owner. The applied
        // enrollment revision does not move.
        *control.lock().unwrap() = grant_at(owner, 2, 1);
        let request_authority = grant_at(owner, 2, 1).effective_authority;
        assert_ne!(before.effective_authority, request_authority);
        tokio::time::advance(Duration::from_secs(1)).await;
        let after = cache.grant(key, Some(4), read).await.unwrap();
        assert_eq!(reads.load(Ordering::Relaxed), 2);
        // The receiving services' fence check passes.
        assert_eq!(after.effective_authority, request_authority);
        assert_eq!(after.owner, Some(owner));
        // The moved view dropped the old entry; the fresh read recorded one.
        assert!(cache.allowed(key.unwrap(), Some(4)));
    }

    #[test]
    fn d10_o14_request_scoped_roles_are_never_cached() {
        let caller = worker();
        let target = group();
        for action in [
            ServiceAction::QueueRetirement,
            ServiceAction::ApiIngress,
            ServiceAction::Operator,
            ServiceAction::ControlFence {
                transition_id: Uuid::new_v4(),
            },
            ServiceAction::ScopedQueueFenceReceipt {
                queue_group_id: Uuid::new_v4(),
                transfer_id: Uuid::new_v4(),
            },
        ] {
            assert_eq!(GrantKey::new(caller, action, &target, None), None);
        }
        assert_eq!(
            GrantKey::new(
                caller,
                ServiceAction::StatusProof {
                    source_group: Uuid::new_v4()
                },
                &target,
                Some((1, Uuid::new_v4()))
            ),
            None
        );
    }

    #[tokio::test(start_paused = true)]
    async fn d10_o05_grant_cache_stays_bounded() {
        let cache = GrantCache::default();
        let target = group();
        for _ in 0..MAX_GRANTS + 10 {
            let caller = worker();
            cache.insert(key(caller, &target), Some(1), Instant::now(), None);
            assert!(cache.len() <= MAX_GRANTS);
        }
    }

    /// T7b (T6 concern 4): an API process's `ApiIngress` role for
    /// `Method::Direct` is cached under the stable-role rules: one Control
    /// read per caller incarnation per `GRANT_TTL`, dropped on a revision
    /// change or a moved ready view or owner, so a revoked API passes for at
    /// most five seconds. Its key differs from every other role's, and the
    /// general `ApiIngress` key stays uncached.
    #[tokio::test(start_paused = true)]
    async fn d10_o14_t7b_direct_ingress_role_is_cached_with_a_five_second_bound() {
        let cache = GrantCache::default();
        let api = worker();
        let control = group();
        let key = GrantKey::direct_ingress(api, &control);
        assert!(key.is_some());
        assert_eq!(
            GrantKey::new(api, ServiceAction::ApiIngress, &control, None),
            None
        );
        assert_ne!(
            key,
            GrantKey::new(api, ServiceAction::Owner, &control, None)
        );
        assert_ne!(key, GrantKey::direct_ingress(worker(), &control));
        let reads = AtomicUsize::new(0);
        let owner = worker();
        let read = || async {
            reads.fetch_add(1, Ordering::Relaxed);
            Ok(Some(grant(owner, 1)))
        };
        for _ in 0..100 {
            cache.role(key, Some(7), read).await.unwrap();
        }
        assert_eq!(
            reads.load(Ordering::Relaxed),
            1,
            "one Control read per window"
        );
        // Revocation: at most five seconds later the role is read again.
        tokio::time::advance(GRANT_TTL).await;
        assert!(!cache.allowed(key.unwrap(), Some(7)));
        let revoked = || async {
            Err(crate::protocol::ProtocolError::new(
                crate::protocol::ErrorCode::Unauthorized,
                "api enrollment revoked",
                crate::protocol::Outcome::NotApplied,
            ))
        };
        assert!(cache.role(key, Some(7), revoked).await.is_err());
        assert!(!cache.allowed(key.unwrap(), Some(7)));
        // An enrollment revision change drops the cached role at once.
        cache.role(key, Some(7), read).await.unwrap();
        assert!(cache.allowed(key.unwrap(), Some(7)));
        assert!(!cache.allowed(key.unwrap(), Some(8)));
    }
}
