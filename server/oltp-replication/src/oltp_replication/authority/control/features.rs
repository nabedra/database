//! CAP1: a worker's view of the cluster feature registry
//! (`direct::CLUSTER_FEATURES`), as ScyllaDB's feature service keeps one
//! node-local set of enabled cluster features.
//!
//! - [`EnabledFeatures`]: the features this worker observed enabled. Each one
//!   is sticky in Control, so a flag never turns off. Data leaders read it
//!   before proposing a gated entry (`ReclaimIndex`), and the worker's
//!   background tasks before sending the gated service calls.
//! - [`watch`]: advertises each watched feature for the worker's enrolled
//!   identity until Control records the advert, asks Control to re-evaluate
//!   now and then, and sets the flag once Control reports the feature
//!   enabled.
//!
//! Enablement means every enrolled worker advertised the feature, so every
//! Control and Data replica host runs a build that decodes the gated entries,
//! and Control refuses later joiners without the advert. A per-connection
//! capability (the former ALPN `nabedra-otp/3`) cannot give the second half:
//! an older build could still join a group and then fail to decode an entry
//! already in its log.
use super::{ControlClient, DirectCapabilityOperation, direct::CLUSTER_FEATURES};
use crate::oltp_replication::types::WorkerIdentity;
use std::{
    sync::{
        Arc, Weak,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

/// Poll period while a watched feature is not enabled.
const POLL: Duration = Duration::from_secs(5);
/// Every this many polls the worker asks Control to re-evaluate enablement
/// (a member that never advertised may have left). The first poll does too:
/// it runs when `data.direct.v1` activation is observed, and an advert that
/// reached Control before then could not enable the feature.
const REEVALUATE_EVERY: u32 = 12;

/// The registered features this worker observed enabled (one bit per
/// registry entry). Starts empty.
#[derive(Debug, Default)]
#[doc(hidden)]
pub struct EnabledFeatures {
    bits: AtomicU32,
}
const _: () = assert!(CLUSTER_FEATURES.len() <= 32);

impl EnabledFeatures {
    fn bit(name: &str) -> Option<u32> {
        CLUSTER_FEATURES
            .iter()
            .position(|feature| feature.name == name)
            .map(|index| 1 << index)
    }
    /// Control enabled `name` (as last observed; never reverts). An
    /// unregistered name is never enabled.
    #[doc(hidden)]
    pub fn enabled(&self, name: &str) -> bool {
        Self::bit(name).is_some_and(|bit| self.bits.load(Ordering::Acquire) & bit != 0)
    }
    /// Records that Control reported `name` enabled.
    #[doc(hidden)]
    pub fn enable(&self, name: &str) {
        if let Some(bit) = Self::bit(name) {
            self.bits.fetch_or(bit, Ordering::AcqRel);
        }
    }
}

/// One round for `name`: true once Control reports it enabled. An older
/// Control (which rejects the read) or an unreachable one answers false.
async fn observe(
    control: &ControlClient,
    local: WorkerIdentity,
    name: &str,
    reevaluate: bool,
) -> bool {
    let Ok(state) = control.cluster_feature(name).await else {
        return false;
    };
    if state.enabled.is_some() {
        return true;
    }
    if !state.advertised {
        control.advertise_direct_feature(name, local).await;
    } else if reevaluate {
        let _ = control
            .change_direct_capability(nabedra_rt::ids::uuid_v4(), DirectCapabilityOperation::Activate)
            .await;
    } else {
        return false;
    }
    control
        .cluster_feature(name)
        .await
        .is_ok_and(|state| state.enabled.is_some())
}

/// Watches `names` for `local` until each one is enabled, then returns. It
/// holds neither the Control client nor the flags between rounds, and stops
/// once either is dropped.
pub(crate) async fn watch(
    control: Weak<ControlClient>,
    local: WorkerIdentity,
    names: &'static [&'static str],
    flags: Weak<EnabledFeatures>,
) {
    let mut polls = 0u32;
    loop {
        {
            let (Some(control), Some(flags)) = (control.upgrade(), flags.upgrade()) else {
                return;
            };
            let reevaluate = polls.is_multiple_of(REEVALUATE_EVERY);
            let mut pending = false;
            for &name in names {
                if flags.enabled(name) {
                    continue;
                }
                if observe(&control, local, name, reevaluate).await {
                    flags.enable(name);
                } else {
                    pending = true;
                }
            }
            if !pending {
                return;
            }
        }
        polls = polls.wrapping_add(1);
        nabedra_rt::time::sleep(POLL).await;
    }
}

/// Starts [`watch`] on the current Tokio runtime, if there is one.
#[doc(hidden)]
pub fn spawn_watch(
    control: &Arc<ControlClient>,
    local: WorkerIdentity,
    names: &'static [&'static str],
    flags: &Arc<EnabledFeatures>,
) {
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        nabedra_rt::task::spawn_on(&runtime, watch(
            Arc::downgrade(control),
            local,
            names,
            Arc::downgrade(flags),
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cap1_registry_names_are_unique_and_keys_follow_the_names() {
        for (index, feature) in CLUSTER_FEATURES.iter().enumerate() {
            assert_eq!(
                feature.enabled_key,
                format!("ct/feature/{}", feature.name).as_bytes()
            );
            assert!(
                CLUSTER_FEATURES[..index]
                    .iter()
                    .all(|other| other.name != feature.name)
            );
        }
        let flags = EnabledFeatures::default();
        assert!(!flags.enabled(super::super::INDEX_RECLAIM_FEATURE));
        flags.enable("not.registered");
        flags.enable(super::super::INDEX_RECLAIM_FEATURE);
        assert!(flags.enabled(super::super::INDEX_RECLAIM_FEATURE));
        assert!(!flags.enabled(super::super::DIRECT_BATCH_FEATURE));
        assert!(!flags.enabled("not.registered"));
    }
}
