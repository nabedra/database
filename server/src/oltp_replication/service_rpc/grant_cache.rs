//! Receiver-side cache of Control `service_role` grants.
//!
//! A grant is cached per caller incarnation and role (action, target group and
//! optional replica placement). An entry is dropped after [`GRANT_TTL`], when
//! the locally applied Control enrollment revision changes, or when a fresh
//! Control read reports a different ready view or owner. A revoked caller can
//! therefore act for at most five seconds. Data safety does not depend on this
//! cache: every write checks its fence in the Data log.
//!
//! Only roles whose grant reflects stable membership are cached. Request-,
//! transfer- and transition-scoped actions always read Control.
use crate::oltp_replication::{
    authority::control::{ServiceAction, ServiceAuthorization},
    types::{GroupIdentity, ReadyMembership, WorkerIdentity},
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

struct Entry {
    /// `None` records a successful role check that returns no grant body.
    grant: Option<ServiceAuthorization>,
    expires: Instant,
}

#[derive(Default)]
struct Grants {
    entries: HashMap<GrantKey, Entry>,
    revision: Option<u64>,
    observed: Option<(ReadyMembership, Option<WorkerIdentity>)>,
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
    /// A cached grant, if one is still valid at this applied revision.
    pub(crate) fn get(
        &self,
        key: GrantKey,
        revision: Option<u64>,
    ) -> Option<Option<ServiceAuthorization>> {
        let mut grants = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        grants.align(revision);
        let now = Instant::now();
        match grants.entries.get(&key) {
            Some(entry) if entry.expires > now => Some(entry.grant),
            Some(_) => {
                grants.entries.remove(&key);
                None
            }
            None => None,
        }
    }
    /// Record a grant from a fresh Control read made at `revision`.
    pub(crate) fn insert(
        &self,
        key: GrantKey,
        revision: Option<u64>,
        grant: Option<ServiceAuthorization>,
    ) {
        let mut grants = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        grants.align(revision);
        if let Some(grant) = grant {
            let observed = (grant.ready, grant.owner);
            if grants.observed.is_some_and(|previous| previous != observed) {
                grants.entries.clear();
            }
            grants.observed = Some(observed);
        }
        let now = Instant::now();
        if grants.entries.len() >= MAX_GRANTS {
            grants.entries.retain(|_, entry| entry.expires > now);
            if grants.entries.len() >= MAX_GRANTS {
                grants.entries.clear();
            }
        }
        grants.entries.insert(
            key,
            Entry {
                grant,
                expires: now + GRANT_TTL,
            },
        );
    }
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entries
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oltp_replication::types::{AuthorityView, Digest, GroupKind, Text};

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
    fn grant(owner: WorkerIdentity, revision: u64) -> ServiceAuthorization {
        let ready = ReadyMembership {
            view: AuthorityView {
                run_id: Uuid::from_u128(1),
                owner_term: 1,
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

    /// D10/O14: a cached grant serves repeat checks for the same caller
    /// incarnation and role, and expires five seconds after the Control read.
    #[tokio::test(start_paused = true)]
    async fn d10_o14_grant_cache_expires_after_five_seconds() {
        let cache = GrantCache::default();
        let caller = worker();
        let target = group();
        let key = GrantKey::new(caller, ServiceAction::Owner, &target, None).unwrap();
        assert_eq!(cache.get(key, Some(4)), None);
        cache.insert(key, Some(4), Some(grant(caller, 1)));
        assert_eq!(cache.get(key, Some(4)), Some(Some(grant(caller, 1))));
        tokio::time::advance(GRANT_TTL - Duration::from_millis(1)).await;
        assert_eq!(cache.get(key, Some(4)), Some(Some(grant(caller, 1))));
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(cache.get(key, Some(4)), None);
        assert_eq!(cache.len(), 0);

        // A replaced incarnation has its own key and never sees the old grant.
        cache.insert(key, Some(4), Some(grant(caller, 1)));
        let replacement = WorkerIdentity {
            incarnation: Uuid::new_v4(),
            enrollment_revision: 2,
            ..caller
        };
        let replaced = GrantKey::new(replacement, ServiceAction::Owner, &target, None).unwrap();
        assert_ne!(replaced, key);
        assert_eq!(cache.get(replaced, Some(4)), None);
        // The role is part of the key, as is the target group.
        let proof = ServiceAction::StatusProof {
            source_group: Uuid::new_v4(),
        };
        assert_ne!(GrantKey::new(caller, proof, &target, None).unwrap(), key);
        assert_ne!(
            GrantKey::new(caller, ServiceAction::Owner, &group(), None).unwrap(),
            key
        );
        assert_ne!(
            GrantKey::new(
                caller,
                ServiceAction::Owner,
                &target,
                Some((2, Uuid::new_v4()))
            )
            .unwrap(),
            key
        );
    }

    /// D10/O14: a change in the locally applied Control revision drops every
    /// cached grant, and so does a fresh read that reports a new ready view or
    /// owner.
    #[tokio::test(start_paused = true)]
    async fn d10_o14_grant_cache_drops_entries_on_control_revision_change() {
        let cache = GrantCache::default();
        let caller = worker();
        let other = worker();
        let target = group();
        let key = GrantKey::new(caller, ServiceAction::Owner, &target, None).unwrap();
        let other_key = GrantKey::new(
            other,
            ServiceAction::TimestampProof {
                source_group: Uuid::new_v4(),
            },
            &target,
            None,
        )
        .unwrap();
        cache.insert(key, Some(4), Some(grant(caller, 1)));
        cache.insert(other_key, Some(4), Some(grant(caller, 1)));
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(key, Some(5)), None);
        assert_eq!(cache.get(other_key, Some(5)), None);
        assert_eq!(cache.len(), 0);

        // A directory without an applied revision relies on the TTL and the
        // observed Control view.
        cache.insert(key, None, Some(grant(caller, 1)));
        cache.insert(other_key, None, Some(grant(caller, 1)));
        assert_eq!(cache.len(), 2);
        let moved = grant(other, 2);
        cache.insert(other_key, None, Some(moved));
        assert_eq!(cache.get(key, None), None);
        assert_eq!(cache.get(other_key, None), Some(Some(moved)));
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
            let key = GrantKey::new(caller, ServiceAction::Owner, &target, None).unwrap();
            cache.insert(key, Some(1), None);
            assert!(cache.len() <= MAX_GRANTS);
        }
    }
}
