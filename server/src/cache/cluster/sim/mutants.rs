//! Directed witnesses and seeded searches for the Cache fault model (K03).

use nabedra_check::{
    checker::relaxed_kv,
    history::{reindex, write_jsonl},
};
use nabedra_sim::{PlantedSearch, runtime};

use super::super::super::protocol::{Admin, Reply};
use super::super::super::{
    clock::ClockMode,
    controller::{MAX_CUTOVER_LAG, RECONCILE_MS, STAGED_RESET_GRACE_MS, SourceFence},
    protocol::{LEASE_MS, RENEW_MS},
    worker::{PROBES, REPLICATION_POLL},
};
use super::super::harness::{Cluster, Config, Partitions, memory_limit};
use super::super::history::Op;
use super::super::hooks::Knobs;
use crate::cache::{
    command::{SLOT_COUNT, key_slot},
    store::EvictionPolicy,
    wire::Response,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Case {
    BoundIgnored,
    DeleteIgnored,
    ExpiryDeleteIgnored,
    EpochReused,
    LiveReset,
    ExcessDrift(i64),
    LocalSkew,
    UnfencedStaged,
    RetiredStaged,
    RetiredEarlyCommit,
    StaleReport,
    LastCopyGate,
    HandoverShort,
    ColdRetains,
    StaleSample,
}

impl Case {
    fn name(self) -> &'static str {
        match self {
            Self::BoundIgnored => "P1",
            Self::DeleteIgnored | Self::ExpiryDeleteIgnored => "P2-drops-deletes",
            Self::EpochReused => "P3",
            Self::LiveReset => "P4",
            Self::ExcessDrift(_) => "E1",
            Self::LocalSkew => "E2",
            Self::UnfencedStaged => "P5",
            Self::RetiredStaged => "P7",
            Self::RetiredEarlyCommit => "last-live-grant",
            Self::StaleReport => "stale-source-report",
            Self::LastCopyGate => "last-copy-gate",
            Self::HandoverShort => "handover-short-prefix",
            Self::ColdRetains => "cold-reset-retains-data",
            Self::StaleSample => "SIM-7-stale-sample",
        }
    }

    fn invariant(self) -> &'static str {
        match self {
            // P1 bends the product's own lease rule. The closed-form check
            // on each served turn catches it; the views stay exclusive.
            Self::BoundIgnored => "ServedWithinLease",
            Self::LiveReset
            | Self::ExcessDrift(_)
            | Self::LocalSkew
            | Self::UnfencedStaged
            | Self::RetiredStaged => "LeaseExclusive",
            Self::RetiredEarlyCommit => "AckAfterHandover",
            Self::StaleReport => "TransferLag",
            Self::LastCopyGate => "LastCopyDiscarded",
            Self::HandoverShort => "lost-write",
            Self::ColdRetains => "resurrection",
            Self::StaleSample => "ServedWithinLease",
            Self::DeleteIgnored | Self::ExpiryDeleteIgnored => "ReplicaPrefix",
            Self::EpochReused => "SlotOwnership",
        }
    }

    fn budget(self) -> u64 {
        match self {
            Self::ExcessDrift(_) | Self::LocalSkew => 200,
            _ => 500,
        }
    }

    fn config(self, enabled: bool) -> Config {
        Config {
            partitions: Partitions::Loss,
            nodes: 4,
            groups: 3,
            actors: 1,
            slot_bytes: memory_limit(1),
            eviction: EvictionPolicy::NoEviction,
            clock_mode: if enabled && self == Self::LocalSkew {
                ClockMode::LocalDocker
            } else {
                ClockMode::ClockBound
            },
            base_error_ns: 4_450_000,
            declared_ppm: 10,
            skew: matches!(
                self,
                Self::BoundIgnored
                    | Self::LocalSkew
                    | Self::ExcessDrift(_)
                    | Self::RetiredStaged
                    | Self::RetiredEarlyCommit
            ),
            gradual_drift: false,
            knobs: Knobs {
                lease_ignores_error_bound: enabled && self == Self::BoundIgnored,
                replica_drops_deletes: enabled
                    && matches!(self, Self::DeleteIgnored | Self::ExpiryDeleteIgnored),
                owner_change_keeps_epoch: enabled && self == Self::EpochReused,
                cold_reset_ignores_live_lease: enabled && self == Self::LiveReset,
                staged_ignores_fence: enabled && self == Self::UnfencedStaged,
                staged_retire_is_fence: enabled && self == Self::RetiredStaged,
                retired_promotion_commits_early: enabled && self == Self::RetiredEarlyCommit,
                controller_keeps_older_report: enabled && self == Self::StaleReport,
                last_copy_lag_gate: enabled && self == Self::LastCopyGate,
                handover_short_prefix: enabled && self == Self::HandoverShort,
                cold_reset_retains_data: enabled && self == Self::ColdRetains,
                publish_reuses_first_sample: enabled && self == Self::StaleSample,
                ..Knobs::default()
            },
            check_replica_prefix: true,
            keep_full_trace: false,
        }
    }
}

pub(super) fn write_ok(reply: Option<Response>) -> bool {
    matches!(reply, Some(Response::Simple(ref message)) if message.as_ref() == b"OK")
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Trial {
    pub trace_hash: u64,
    pub trace_events: u64,
    pub history: Vec<u8>,
    pub violations: Vec<String>,
}

fn run_case(case: Case, seed: u64, generated: bool, enabled: bool, history_only: bool) -> Trial {
    let partitions = Partitions::Loss;
    run_case_with(partitions, case, seed, generated, enabled, history_only)
}

fn run_case_with(
    partitions: Partitions,
    case: Case,
    seed: u64,
    generated: bool,
    enabled: bool,
    history_only: bool,
) -> Trial {
    runtime::run(seed, |world| async move {
        let mut choices = nabedra_sim::sweep::workload_gen(seed);
        let staged = matches!(
            case,
            Case::UnfencedStaged
                | Case::RetiredStaged
                | Case::RetiredEarlyCommit
                | Case::StaleReport
                | Case::LastCopyGate
        );
        let slot_max = if generated && staged {
            1 + choices.below(63) as u16
        } else if generated {
            1 + choices.below(250) as u16
        } else {
            32
        };
        let key = (0..)
            .map(|i| format!("{{lease-{i}}}"))
            .find(|key| key_slot(key.as_bytes()) < slot_max)
            .unwrap();
        let slot = key_slot(key.as_bytes());
        let mut config = case.config(enabled);
        config.check_replica_prefix = !history_only;
        config.partitions = partitions;
        let mut cluster = Cluster::boot(world, vec![key.clone()], config).await;
        {
            let mut observed = cluster.observed.lock().unwrap();
            observed.check_transfer_lag = case == Case::StaleReport;
            observed.check_last_copy = case == Case::LastCopyGate;
        }
        if matches!(case, Case::RetiredStaged | Case::RetiredEarlyCommit) {
            let jitter = generated.then(|| choices.below(30));
            retired_source_schedule(&mut cluster, &key, enabled, jitter).await;
            return trial(&cluster, true);
        }
        if case == Case::StaleReport {
            stale_report_schedule(&mut cluster, &key).await;
            return trial(&cluster, false);
        }
        if case == Case::HandoverShort {
            handover_schedule(&mut cluster, &key).await;
            return trial(&cluster, true);
        }
        if case == Case::ColdRetains {
            scoped_reset_schedule(&mut cluster, &key).await;
            return trial(&cluster, true);
        }
        if staged {
            staged_schedule(
                &mut cluster,
                &key,
                case,
                enabled,
                generated.then(|| choices.below(30)),
            )
            .await;
            return trial(&cluster, case == Case::LastCopyGate);
        }
        let before = cluster.slot(slot);
        let old = before.primary.unwrap();
        let replica = before.replica.unwrap();
        assert_ne!(old.node, replica.node);
        let ttl = (case == Case::ExpiryDeleteIgnored).then_some(1_500);
        assert!(write_ok(
            cluster
                .send(&key, Op::Set("old-value".into(), ttl), Some(old.node))
                .await
        ));
        for _ in 0..30 {
            if cluster
                .sample(Some(replica), slot)
                .await
                .is_some_and(|sample| sample.sequence > 0)
            {
                break;
            }
            cluster.step(100).await;
        }
        assert!(
            cluster
                .sample(Some(replica), slot)
                .await
                .is_some_and(|sample| sample.sequence > 0),
            "replica did not receive first write"
        );
        if case == Case::DeleteIgnored {
            assert!(write_ok(
                cluster
                    .send(&key, Op::Set("overwritten".into(), None), Some(old.node))
                    .await
            ));
            assert_eq!(
                cluster.send(&key, Op::Get, Some(old.node)).await,
                Some(Response::bulk(b"overwritten"))
            );
            assert!(matches!(
                cluster.send(&key, Op::Del, Some(old.node)).await,
                Some(Response::Integer(1))
            ));
            cluster.step(1_000).await;
        }
        if case == Case::ExpiryDeleteIgnored {
            cluster.step(2_000).await;
            assert_eq!(
                cluster.send(&key, Op::Get, Some(old.node)).await,
                Some(Response::Bulk(None))
            );
        }
        if let Case::ExcessDrift(ppm) = case {
            let ppm = if enabled {
                ppm
            } else {
                -i64::from(config.declared_ppm)
            };
            cluster.drift(old.node, ppm);
            // Age the bound before cutting renewals so 100x declared drift
            // crosses the fence even with the controller's conservative cutoff.
            cluster.step(8_000).await;
        }
        if generated {
            cluster.step(choices.below(30)).await;
        }
        cluster.isolate_controller_links(old.node);
        if case == Case::StaleSample {
            // SIM-7: the old primary pauses right after the clock sample of a
            // write's turn, just before its grant ends, and resumes after the
            // replica took over. The final check must take a fresh sample.
            let grant = cluster
                .sample(Some(old), slot)
                .await
                .unwrap()
                .lease
                .unwrap();
            let deadline = grant.expires_ms - nabedra_rt::sim::EPOCH_MS;
            cluster
                .step((deadline - 100).saturating_sub(cluster.now()))
                .await;
            cluster.freeze_after_next_clock_sample(old.node);
            let _ = cluster
                .send(&key, Op::Set("stale-sample".into(), None), Some(old.node))
                .await;
            cluster.step(LEASE_MS).await;
            cluster.thaw(old.node);
            cluster.step(RENEW_MS).await;
        }
        if case == Case::LiveReset {
            assert!(matches!(
                cluster.admin(Admin::Drain { worker: old.node }).await,
                Reply::Applied { .. }
            ));
            let reset = cluster.admin(Admin::ColdReset { slot: Some(slot) }).await;
            if enabled {
                assert!(matches!(reset, Reply::Applied { .. }), "{reset:?}");
            } else {
                assert!(
                    matches!(reset, Reply::Refused(reason) if reason == "prior authority is still live")
                );
            }
        }
        if matches!(
            case,
            Case::BoundIgnored | Case::LocalSkew | Case::ExcessDrift(_)
        ) {
            let grant = cluster
                .sample(Some(old), slot)
                .await
                .unwrap()
                .lease
                .unwrap();
            let guard_ms = if case == Case::LocalSkew { 0 } else { 5 };
            let deadline = grant.expires_ms - nabedra_rt::sim::EPOCH_MS;
            cluster
                .step((deadline + guard_ms).saturating_sub(cluster.now()))
                .await;
            cluster.immediate_delivery();
            cluster.reconcile_once().await;
            // This is also the target on the clean twin, where the
            // controller may refuse promotion at this instant.
            cluster.renew_once(replica.node).await.unwrap();
            cluster.sample_leases().await;
            for (node, value) in [
                (replica.node, "new-at-boundary"),
                (old.node, "old-at-boundary"),
            ] {
                let _ = cluster
                    .send(&key, Op::Set(value.into(), None), Some(node))
                    .await;
            }
        }
        let end = cluster.now() + 5_000;
        while cluster.now() < end {
            cluster.step(5).await;
            let placement = cluster.slot(slot);
            if placement.primary.is_some_and(|new| new != old) {
                let successor = placement.primary.unwrap();
                if cluster
                    .sample(Some(successor), slot)
                    .await
                    .is_some_and(|sample| sample.authorized)
                {
                    if case == Case::DeleteIgnored {
                        let _ = cluster.send(&key, Op::Get, Some(successor.node)).await;
                    } else if case != Case::EpochReused {
                        let _ = cluster
                            .send(
                                &key,
                                Op::Set("successor".into(), None),
                                Some(successor.node),
                            )
                            .await;
                        let _ = cluster
                            .send(&key, Op::Set("previous".into(), None), Some(old.node))
                            .await;
                    }
                    break;
                }
            }
        }
        trial(&cluster, history_only)
    })
}

async fn staged_schedule(
    cluster: &mut Cluster,
    key: &str,
    case: Case,
    enabled: bool,
    jitter: Option<u64>,
) {
    let slot = key_slot(key.as_bytes());
    let initial = cluster.slot(slot);
    let old = initial.primary.unwrap();
    let replica = initial.replica.unwrap();
    assert!(write_ok(
        cluster
            .send(key, Op::Set("retained".into(), None), Some(old.node))
            .await
    ));
    cluster.crash(replica.node);
    let deadline = cluster.now() + 2 * LEASE_MS;
    let target = loop {
        if let Some(moving) = cluster.slot(slot).moving {
            assert!(moving.replica_only);
            break moving.target;
        }
        assert!(cluster.now() < deadline, "staging did not start");
        cluster.step(10).await;
    };
    cluster.immediate_delivery();
    // Install the move on the target before withholding its progress reports.
    cluster.renew_once(target.node).await.unwrap();
    cluster.isolate_controller_links(target.node);
    let deadline = cluster.now() + LEASE_MS;
    loop {
        if cluster
            .sample(Some(target), slot)
            .await
            .is_some_and(|copy| copy.baseline)
        {
            break;
        }
        assert!(cluster.now() < deadline, "staged copy did not finish");
        cluster.step(10).await;
    }
    let lagged = case == Case::LastCopyGate;
    if lagged {
        cluster.link(target.node, old.node, true);
        for i in 0..=MAX_CUTOVER_LAG {
            assert!(write_ok(
                cluster
                    .send(key, Op::Set(format!("update-{i}"), None), Some(old.node))
                    .await
            ));
        }
    }
    cluster.renew_once(old.node).await.unwrap();
    if let Some(jitter) = jitter {
        cluster.step(jitter).await;
    }
    cluster.heal();
    cluster.isolate_controller_links(old.node);
    if lagged {
        cluster.crash(old.node);
    }
    cluster.renew_once(target.node).await.unwrap();
    cluster.reconcile_once().await;
    cluster.renew_once(target.node).await.unwrap();
    cluster.sample_leases().await;

    // This fixture fits in one reconciliation batch. Allow the prior
    // lease, the report grace period, placement and two grant heartbeats.
    let deadline = cluster.now() + LEASE_MS + STAGED_RESET_GRACE_MS + RECONCILE_MS + 2 * RENEW_MS;
    let successor = loop {
        let placement = cluster.slot(slot);
        if placement.epoch > initial.epoch
            && cluster
                .sample(placement.primary, slot)
                .await
                .is_some_and(|sample| sample.authorized)
        {
            break placement;
        }
        assert!(
            cluster.now() < deadline,
            "no authorized successor for {case:?}"
        );
        cluster.step(25).await;
    };
    // CACHE-4: the target is the last verified copy of a crashed (still
    // enrolled) source, so it is promoted even 33 mutations behind. Only the
    // planted lag gate resets the slot.
    let reset = case == Case::LastCopyGate && enabled;
    assert_eq!(successor.cold, reset, "{case:?} enabled {enabled}");
    let expected = if reset {
        Response::Bulk(None)
    } else {
        Response::bulk(b"retained")
    };
    assert_eq!(cluster.send(key, Op::Get, None).await, Some(expected));
}

/// R-cache-14: the source of a replica-only move is cut off from the
/// controllers but still serves clients, and its incarnation retires while
/// its last grant is live. The controller may commit the staged promotion at
/// once, but the target must not serve before that grant's expiry.
async fn retired_source_schedule(
    cluster: &mut Cluster,
    key: &str,
    enabled: bool,
    jitter: Option<u64>,
) {
    let slot = key_slot(key.as_bytes());
    let initial = cluster.slot(slot);
    let old = initial.primary.unwrap();
    let replica = initial.replica.unwrap();
    assert!(write_ok(
        cluster
            .send(key, Op::Set("retained".into(), None), Some(old.node))
            .await
    ));
    cluster.crash(replica.node);
    let deadline = cluster.now() + 2 * LEASE_MS;
    let target = loop {
        if let Some(moving) = cluster.slot(slot).moving {
            assert!(moving.replica_only);
            break moving.target;
        }
        assert!(cluster.now() < deadline, "staging did not start");
        cluster.step(10).await;
    };
    cluster.immediate_delivery();
    // Keep the move staged: the controller must not hear the verified copy
    // while the source is still reporting.
    cluster.renew_once(target.node).await.unwrap();
    cluster.isolate_controller_links(target.node);
    let deadline = cluster.now() + LEASE_MS;
    loop {
        if cluster
            .sample(Some(target), slot)
            .await
            .is_some_and(|copy| copy.baseline)
        {
            break;
        }
        assert!(cluster.now() < deadline, "staged copy did not finish");
        cluster.step(10).await;
    }
    cluster.renew_once(old.node).await.unwrap();
    if let Some(jitter) = jitter {
        cluster.step(jitter).await;
    }
    cluster.heal();
    // The source keeps its client and peer links. Only the controllers lose it.
    cluster.isolate_controller_links(old.node);
    let reply = cluster.enroll_replacement(old.node).await;
    assert!(matches!(reply, Reply::Applied { .. }), "{reply:?}");
    cluster.trace(format_args!(
        "retired source {} staged target {}",
        old.node, target.node
    ));
    let grant = cluster.slot(slot).lease.expect("source grant");
    let expiry = grant.expires_ms - nabedra_rt::sim::EPOCH_MS;
    assert!(
        cluster.now() + 500 < expiry,
        "source grant already near expiry"
    );
    assert!(
        cluster
            .sample(Some(old), slot)
            .await
            .is_some_and(|sample| sample.authorized),
        "retired source must still hold a live grant"
    );

    // The clean path must leave the source writable until the fence.
    if !enabled {
        assert!(write_ok(
            cluster
                .send(
                    key,
                    Op::Set("source-before-fence".into(), None),
                    Some(old.node)
                )
                .await
        ));
    }

    // Let the target report its verified copy and the leader plan.
    let deadline = expiry + 2 * RENEW_MS;
    let promoted = loop {
        let _ = cluster.renew_once(target.node).await;
        cluster.reconcile_once().await;
        let placement = cluster.slot(slot);
        if placement.epoch > initial.epoch {
            break placement;
        }
        assert!(cluster.now() < deadline, "staged promotion did not commit");
        cluster.step(25).await;
    };
    let _ = cluster.renew_once(target.node).await;
    cluster.sample_leases().await;
    assert_eq!(promoted.primary, Some(target));
    assert!(!promoted.cold);
    if !enabled {
        // Retirement prevents renewals. Publication still waits for the
        // last committed grant to expire, which apply checks again.
        let (at, _, proof) = cluster
            .observed
            .lock()
            .unwrap()
            .staged
            .iter()
            .copied()
            .find(|(_, index, _)| *index == slot)
            .expect("committed staged proof");
        assert_eq!(proof.fence, SourceFence::Retired);
        assert!(proof.last_copy);
        assert_eq!(proof.source, old);
        assert_eq!(proof.prior_lease, Some(grant));
        assert!(
            at >= expiry,
            "promotion committed at {at}, grant ends {expiry}"
        );
        assert_eq!(promoted.lease.unwrap().not_before_ms, grant.expires_ms);
    }

    if enabled {
        // The planted early cutover can discard a subsequent acknowledgement.
        assert!(write_ok(
            cluster
                .send(
                    key,
                    Op::Set("source-after-retire".into(), None),
                    Some(old.node),
                )
                .await
        ));
    }
    let deadline = expiry + STAGED_RESET_GRACE_MS + 2 * RENEW_MS;
    let first_authorized = loop {
        if cluster
            .sample(Some(target), slot)
            .await
            .is_some_and(|sample| sample.authorized)
        {
            break cluster.now();
        }
        if cluster
            .sample(Some(old), slot)
            .await
            .is_some_and(|sample| sample.authorized)
        {
            let _ = cluster
                .send(key, Op::Set("source-live".into(), None), Some(old.node))
                .await;
        }
        assert!(cluster.now() < deadline, "target never became writable");
        cluster.step(5).await;
    };
    cluster.trace(format_args!(
        "staged target writable at {first_authorized} ms, source grant ends at {expiry} ms"
    ));
    if !enabled {
        assert!(
            first_authorized >= expiry,
            "target writable at {first_authorized}, before {expiry}"
        );
    }
    // Write on both owners. Only the planted defect lets both grants overlap.
    for (node, value) in [(target.node, "successor"), (old.node, "previous")] {
        let _ = cluster
            .send(key, Op::Set(value.into(), None), Some(node))
            .await;
    }
    cluster.sample_leases().await;
    cluster.step(LEASE_MS).await;
    let _ = cluster.send(key, Op::Get, Some(target.node)).await;
}

/// Stale source reports (record cache-older-source-heartbeat): a staged copy
/// stops 40 mutations behind its source while the controller has the
/// source's newest report. An older source heartbeat then arrives late. The
/// controller must still measure the transfer against the newest progress.
async fn stale_report_schedule(cluster: &mut Cluster, key: &str) {
    let slot = key_slot(key.as_bytes());
    let initial = cluster.slot(slot);
    let old = initial.primary.unwrap();
    let replica = initial.replica.unwrap();
    assert!(write_ok(
        cluster
            .send(key, Op::Set("base".into(), None), Some(old.node))
            .await
    ));
    cluster.crash(replica.node);
    let deadline = cluster.now() + 2 * LEASE_MS;
    let target = loop {
        if let Some(moving) = cluster.slot(slot).moving {
            assert!(moving.replica_only);
            break moving.target;
        }
        assert!(cluster.now() < deadline, "staging did not start");
        cluster.step(10).await;
    };
    let deadline = cluster.now() + 2 * LEASE_MS;
    loop {
        let source = cluster.sample(Some(old), slot).await.unwrap();
        if cluster
            .sample(Some(target), slot)
            .await
            .is_some_and(|copy| copy.baseline && copy.sequence == source.sequence)
        {
            break;
        }
        assert!(cluster.now() < deadline, "staged copy did not catch up");
        cluster.step(10).await;
    }
    cluster.immediate_delivery();
    // The source's report at the copy's sequence, kept for late delivery.
    let older = cluster.capture_heartbeat(old.node).await;
    cluster.link(target.node, old.node, true);
    cluster.link(old.node, target.node, true);
    for i in 0..MAX_CUTOVER_LAG + 8 {
        assert!(write_ok(
            cluster
                .send(key, Op::Set(format!("update-{i}"), None), Some(old.node))
                .await
        ));
    }
    // The controller hears the newest progress of both and must keep the
    // copy staged: it trails by 40, beyond the cutover limit.
    cluster.renew_once(old.node).await.unwrap();
    cluster.renew_once(target.node).await.unwrap();
    cluster.reconcile_once().await;
    assert!(
        cluster
            .slot(slot)
            .moving
            .is_some_and(|moving| moving.target == target),
        "the transfer completed 40 mutations behind its source"
    );
    // The older heartbeat arrives after the newer one.
    cluster.isolate_controller_links(old.node);
    assert!(matches!(
        cluster.deliver_heartbeat(old.node, older).await,
        Some(Reply::Update(_))
    ));
    cluster.reconcile_once().await;
    cluster.reconcile_once().await;
    cluster.trace(format_args!(
        "after the late heartbeat slot {slot} moving {:?} replica {:?}",
        cluster.slot(slot).moving,
        cluster.slot(slot).replica
    ));
    cluster.heal();
    cluster.step(2 * RENEW_MS).await;
}

/// Planned handover: an online move whose target misses the source's last
/// acknowledged write. The cutover needs the exact frozen prefix, so the
/// source resumes under a new epoch instead and keeps every write.
async fn handover_schedule(cluster: &mut Cluster, key: &str) {
    let slot = key_slot(key.as_bytes());
    let initial = cluster.slot(slot);
    let old = initial.primary.unwrap();
    let replica = initial.replica.unwrap();
    let target = *cluster
        .workers
        .keys()
        .find(|&&node| node != old.node && node != replica.node)
        .unwrap();
    assert!(write_ok(
        cluster
            .send(key, Op::Set("first".into(), None), Some(old.node))
            .await
    ));
    let order = cluster
        .admin(Admin::Rebalance {
            first: slot,
            last: slot,
            target,
        })
        .await;
    assert!(matches!(order, Reply::Applied { .. }), "{order:?}");
    let deadline = cluster.now() + 4 * LEASE_MS;
    let target = loop {
        if let Some(moving) = cluster.slot(slot).moving
            && !moving.replica_only
        {
            let source = cluster.sample(Some(old), slot).await.unwrap();
            if cluster
                .sample(Some(moving.target), slot)
                .await
                .is_some_and(|copy| copy.baseline && copy.sequence == source.sequence)
            {
                break moving.target;
            }
        }
        assert!(cluster.now() < deadline, "move target did not catch up");
        cluster.step(10).await;
    };
    // The target stops one write short of the source.
    cluster.link(target.node, old.node, true);
    cluster.link(old.node, target.node, true);
    assert!(write_ok(
        cluster
            .send(key, Op::Set("last".into(), None), Some(old.node))
            .await
    ));
    let deadline = cluster.now() + 3 * LEASE_MS;
    let successor = loop {
        let placement = cluster.slot(slot);
        if placement.epoch > initial.epoch
            && cluster
                .sample(placement.primary, slot)
                .await
                .is_some_and(|sample| sample.authorized)
        {
            break placement;
        }
        assert!(cluster.now() < deadline, "no owner after the handover");
        cluster.step(25).await;
    };
    cluster.trace(format_args!(
        "handover of slot {slot} to {:?} at epoch {}",
        successor.primary, successor.epoch
    ));
    let _ = cluster.send(key, Op::Get, None).await;
    cluster.heal();
    cluster.step(2 * RENEW_MS).await;
    let _ = cluster.send(key, Op::Get, None).await;
}

/// Cold reset: an operator resets a slot whose primary still holds data.
/// The controllers lose every report in a new term while the slot's two
/// workers are cut off, so no copy can take over and the operator resets it.
/// The reset epoch starts empty even on the worker that held the slot.
async fn scoped_reset_schedule(cluster: &mut Cluster, key: &str) {
    let slot = key_slot(key.as_bytes());
    let initial = cluster.slot(slot);
    let old = initial.primary.unwrap();
    let replica = initial.replica.unwrap();
    assert!(write_ok(
        cluster
            .send(key, Op::Set("before-reset".into(), None), Some(old.node))
            .await
    ));
    cluster.isolate_controller_links(old.node);
    cluster.isolate_controller_links(replica.node);
    // A new term clears every report, so neither worker is live to it.
    let leader = cluster.leader().unwrap();
    cluster.crash(leader);
    let grant = cluster.slot(slot).lease.expect("committed grant");
    let expiry = grant.expires_ms - nabedra_rt::sim::EPOCH_MS;
    cluster
        .step((expiry + RENEW_MS).saturating_sub(cluster.now()))
        .await;
    let deadline = cluster.now() + 4 * LEASE_MS;
    loop {
        match cluster.admin(Admin::ColdReset { slot: Some(slot) }).await {
            Reply::Applied { .. } => break,
            reply => {
                assert!(cluster.now() < deadline, "scoped reset refused: {reply:?}");
                cluster.step(RENEW_MS).await;
            }
        }
    }
    cluster.start_controller(leader).await;
    cluster.heal();
    let deadline = cluster.now() + 3 * LEASE_MS;
    let successor = loop {
        let placement = cluster.slot(slot);
        if placement.epoch > initial.epoch
            && cluster
                .sample(placement.primary, slot)
                .await
                .is_some_and(|sample| sample.authorized)
        {
            break placement;
        }
        assert!(cluster.now() < deadline, "no owner after the scoped reset");
        cluster.step(25).await;
    };
    cluster.trace(format_args!(
        "scoped reset of slot {slot}: owner {:?} epoch {}",
        successor.primary, successor.epoch
    ));
    let _ = cluster.send(key, Op::Get, None).await;
}

/// K03 and CACHE-1, assigned promotion: after installing the retirement
/// view, an existing replica must still pull an acknowledgement from the
/// exact incarnation it follows. Returns whether it did.
fn retired_tail(enabled: bool) -> bool {
    runtime::run(1, |world| async move {
        let key = (0..)
            .map(|i| format!("{{retired-tail-{i}}}"))
            .find(|key| key_slot(key.as_bytes()) < 63)
            .unwrap();
        let slot = key_slot(key.as_bytes());
        let mut config = Case::RetiredStaged.config(false);
        config.knobs.copy_drops_retired_source = enabled;
        let mut cluster = Cluster::boot(world, vec![key.clone()], config).await;
        let initial = cluster.slot(slot);
        let old = initial.primary.unwrap();
        let replica = initial.replica.unwrap();
        assert!(write_ok(
            cluster
                .send(&key, Op::Set("before".into(), None), Some(old.node))
                .await
        ));
        let deadline = cluster.now() + LEASE_MS;
        let copied = loop {
            if let Some(sample) = cluster.sample(Some(replica), slot).await
                && sample.baseline
                && sample.sequence > 0
            {
                break sample.sequence;
            }
            assert!(cluster.now() < deadline, "assigned copy did not catch up");
            cluster.step(25).await;
        };
        cluster.immediate_delivery();
        cluster.renew_once(old.node).await.unwrap();
        cluster.isolate_controller_links(old.node);
        assert!(matches!(
            cluster.enroll_replacement(old.node).await,
            Reply::Applied { .. }
        ));
        cluster.renew_once(replica.node).await.unwrap();
        assert!(
            cluster.workers[&replica.node]
                .topology()
                .member(old)
                .is_none()
        );
        assert!(write_ok(
            cluster
                .send(&key, Op::Set("after".into(), None), Some(old.node))
                .await
        ));
        // Allow a full poll cycle across the biggest possible source lane.
        let poll_cycle = SLOT_COUNT.div_ceil(PROBES) as u64 * REPLICATION_POLL.as_millis() as u64;
        let expires = cluster.slot(slot).lease.unwrap().expires_ms - nabedra_rt::sim::EPOCH_MS;
        let deadline = (cluster.now() + poll_cycle + 250).min(expires);
        loop {
            let sample = cluster.sample(Some(replica), slot).await.unwrap();
            assert_eq!(sample.epoch, initial.epoch);
            if sample.sequence == copied + 1 {
                return true;
            }
            if cluster.now() >= deadline {
                eprintln!(
                    "replica stopped pulling the retired tail at sequence {} instead of {}",
                    sample.sequence,
                    copied + 1
                );
                return false;
            }
            cluster.step(25).await;
        }
    })
}

pub(super) fn trial(cluster: &Cluster, history_only: bool) -> Trial {
    let observed = cluster.observed.lock().unwrap();
    let mut violations: Vec<_> = observed
        .violations
        .iter()
        .map(|(name, detail)| format!("{name}: {detail}"))
        .collect();
    if history_only {
        let verdict = relaxed_kv::check_with(&observed.history, &super::history_options());
        violations.extend(
            verdict
                .anomalies
                .into_iter()
                .map(|anomaly| format!("{}: {}", anomaly.kind, anomaly.detail)),
        );
    }
    let mut events = observed.history.clone();
    reindex(&mut events);
    let mut history = Vec::new();
    write_jsonl(&mut history, &events).expect("serialize trial history");
    Trial {
        trace_hash: observed.trace.hash(),
        trace_events: observed.trace.events(),
        history,
        violations,
    }
}

pub(super) fn catches(violations: &[String], name: &str) -> bool {
    violations.iter().any(|violation| {
        violation
            .split_once(':')
            .is_some_and(|(kind, _)| kind == name)
    })
}

fn assert_clean(case: Case, seed: u64, clean: &Trial) {
    assert!(
        clean.violations.is_empty(),
        "{} seed {seed} clean twin: {:?}",
        case.name(),
        clean.violations
    );
}

fn assert_clock_assumption(case: Case, trial: &Trial) {
    if matches!(case, Case::ExcessDrift(_)) {
        assert!(catches(&trial.violations, "ClockAssumption"));
    }
}

fn scripted(case: Case) {
    let seed = 1;
    let trial = run_case(case, seed, false, true, false);
    let clean = run_case(case, seed, false, false, false);
    assert_clean(case, seed, &clean);
    eprintln!(
        "Cache fault model {} scripted seed {seed}: {:?}",
        case.name(),
        trial.violations
    );
    assert!(
        catches(&trial.violations, case.invariant()),
        "expected {}: {:?}",
        case.invariant(),
        trial.violations
    );
    assert_clock_assumption(case, &trial);
}

fn search(case: Case) {
    use std::{collections::BTreeMap, sync::Mutex};

    let mut search = PlantedSearch::from_env(case.name(), 1, case.budget(), &[case.invariant()]);
    search.budget = search.budget.min(case.budget());
    eprintln!(
        "Cache fault model {} budget: {}",
        case.name(),
        search.budget
    );
    let clean_failures = Mutex::new(Vec::new());
    let witnesses = Mutex::new(BTreeMap::new());
    let found = search
        .run(|seed| {
            let clean = run_case(case, seed, true, false, false);
            if !clean.violations.is_empty() {
                clean_failures
                    .lock()
                    .unwrap()
                    .push((seed, clean.violations));
                return Vec::new();
            }
            let trial = run_case(case, seed, true, true, false);
            if catches(&trial.violations, case.invariant()) {
                assert_clock_assumption(case, &trial);
                let violations = trial.violations.clone();
                witnesses.lock().unwrap().insert(seed, (trial, clean));
                return violations;
            }
            trial.violations
        })
        .unwrap();
    let clean_failures = clean_failures.into_inner().unwrap();
    assert!(clean_failures.is_empty(), "clean twins: {clean_failures:?}");
    assert!(found.tried <= case.budget());

    let seed = found.seed;
    let (trial, clean) = witnesses.into_inner().unwrap().remove(&seed).unwrap();
    assert_replay(seed, &trial, &run_case(case, seed, true, true, false));
    assert_replay(seed, &clean, &run_case(case, seed, true, false, false));
    println!(
        "Cache fault model {} seed {seed} budget {} tried {} invariant {} trace {:016x} clean {:016x}",
        case.name(),
        search.budget,
        found.tried,
        case.invariant(),
        trial.trace_hash,
        clean.trace_hash
    );
}

pub(super) fn assert_replay(seed: u64, trial: &Trial, replay: &Trial) {
    assert_eq!(
        trial.trace_hash, replay.trace_hash,
        "seed {seed} trace hash"
    );
    assert_eq!(
        trial.trace_events, replay.trace_events,
        "seed {seed} trace events"
    );
    assert_eq!(trial.history, replay.history, "seed {seed} history");
    assert_eq!(trial.violations, replay.violations, "seed {seed} verdict");
}

#[test]
fn planted_lease_ignores_error_bound_scripted() {
    scripted(Case::BoundIgnored);
}
#[test]
fn planted_replica_drops_deletes_scripted() {
    scripted(Case::DeleteIgnored);
}
#[test]
fn planted_replica_drops_synthesized_expiry_delete_scripted() {
    scripted(Case::ExpiryDeleteIgnored);
}
#[test]
fn planted_owner_change_keeps_epoch_scripted() {
    scripted(Case::EpochReused);
}
#[test]
fn planted_cold_reset_ignores_live_lease_scripted() {
    scripted(Case::LiveReset);
}
#[test]
fn drift_beyond_bound_scripted() {
    scripted(Case::ExcessDrift(-1000));
}
#[test]
fn drift_sensitivity_scripted() {
    let seed = 1;
    for ppm in [-20, -100, -500_000] {
        let trial = run_case(Case::ExcessDrift(ppm), seed, false, true, false);
        println!(
            "Cache fault model E1 sensitivity seed {seed}, {ppm} ppm, trace {:016x}, {:?}",
            trial.trace_hash, trial.violations,
        );
        assert!(catches(&trial.violations, "ClockAssumption"));
        assert_eq!(
            catches(&trial.violations, "LeaseExclusive"),
            ppm == -500_000
        );
    }
}
#[test]
fn local_docker_skew_scripted() {
    scripted(Case::LocalSkew);
}

/// The directed schedules partition their workers. Late delivery changes what
/// a peer hears after the heal, not which defects the checks catch.
#[test]
fn planted_cases_under_late_partitions() {
    let seed = 1;
    for case in [
        Case::BoundIgnored,
        Case::DeleteIgnored,
        Case::ExpiryDeleteIgnored,
        Case::EpochReused,
        Case::LiveReset,
        Case::ExcessDrift(-1000),
        Case::LocalSkew,
        Case::UnfencedStaged,
        Case::RetiredStaged,
        Case::RetiredEarlyCommit,
        Case::StaleReport,
        Case::LastCopyGate,
        Case::HandoverShort,
        Case::ColdRetains,
        Case::StaleSample,
    ] {
        let run = |enabled| run_case_with(Partitions::Late, case, seed, false, enabled, false);
        let (trial, clean) = (run(true), run(false));
        assert_clean(case, seed, &clean);
        assert!(
            catches(&trial.violations, case.invariant()),
            "{} late: {:?}",
            case.name(),
            trial.violations
        );
        assert_replay(seed, &trial, &run(true));
        assert_replay(seed, &clean, &run(false));
        println!(
            "Cache fault model {} late scripted seed {seed}: invariant {} trace {:016x} clean {:016x}",
            case.name(),
            case.invariant(),
            trial.trace_hash,
            clean.trace_hash
        );
    }
}

#[test]
fn planted_lease_ignores_error_bound_search() {
    search(Case::BoundIgnored);
}
#[test]
fn planted_replica_drops_deletes_search() {
    search(Case::DeleteIgnored);
}
#[test]
fn planted_owner_change_keeps_epoch_search() {
    search(Case::EpochReused);
}
#[test]
fn planted_cold_reset_ignores_live_lease_search() {
    search(Case::LiveReset);
}
#[test]
fn drift_beyond_bound_search() {
    search(Case::ExcessDrift(-1000));
}
#[test]
fn local_docker_skew_search() {
    search(Case::LocalSkew);
}

#[test]
fn planted_replica_delete_history() {
    let trial = run_case(Case::DeleteIgnored, 1, false, true, true);
    assert!(
        catches(&trial.violations, "resurrection"),
        "{:?}",
        trial.violations
    );
    let clean = run_case(Case::DeleteIgnored, 1, false, false, true);
    assert_clean(Case::DeleteIgnored, 1, &clean);
    if let Some(dir) = std::env::var_os("NABEDRA_SIM_HISTORY_DIR") {
        let dir = std::path::PathBuf::from(dir);
        std::fs::create_dir_all(&dir).expect("create Cache history directory");
        std::fs::write(dir.join("cache-delete-clean.jsonl"), clean.history)
            .expect("write clean delete history");
    }
}

#[test]
fn planted_staged_promotion_before_fence_scripted_and_search() {
    scripted(Case::UnfencedStaged);
    search(Case::UnfencedStaged);
}

#[test]
fn planted_staged_promotion_on_retirement_scripted_and_search() {
    scripted(Case::RetiredStaged);
    search(Case::RetiredStaged);
}

/// R-cache-14 and decision 6: retirement alone does not let the staged target
/// serve while the retired source's grant is live. Clocks sit at the edges of
/// their bound. Node 11 runs slow and the other workers run fast.
#[test]
fn staged_promotion_after_live_source_retirement_waits_for_grant() {
    let runs = [(1, false), (2, false), (3, false)]
        .into_iter()
        .chain((1..=12).map(|seed| (seed, true)));
    for (seed, generated) in runs {
        let clean = run_case(Case::RetiredStaged, seed, generated, false, true);
        assert_clean(Case::RetiredStaged, seed, &clean);
        let replay = run_case(Case::RetiredStaged, seed, generated, false, true);
        assert_replay(seed, &clean, &replay);
        println!(
            "Cache staged promotion after retirement seed {seed} generated {generated}: trace {:016x} events {}",
            clean.trace_hash, clean.trace_events
        );
    }
}

/// K03 and CACHE-1, assigned promotion: after installing the retirement
/// view, an existing replica must still pull an acknowledgement from the
/// exact incarnation it follows. The planted member gates stop it.
#[test]
fn assigned_replica_keeps_pulling_the_retired_source_tail() {
    assert!(
        retired_tail(false),
        "clean twin stopped pulling the retired tail"
    );
    assert!(
        !retired_tail(true),
        "planted member gate still pulled the tail"
    );
}

/// CACHE-1, last live grant: a planner that commits a retired source's staged
/// promotion before its grant ends lets the source acknowledge writes the
/// successor never holds.
#[test]
fn planted_retired_promotion_commits_early_scripted() {
    scripted(Case::RetiredEarlyCommit);
}

/// Stale source reports (cache-older-source-heartbeat): an older source
/// heartbeat delivered late must not complete a transfer beyond the limit.
#[test]
fn planted_older_source_report_scripted() {
    scripted(Case::StaleReport);
}

/// CACHE-4: the last verified staged copy of a crashed source is promoted
/// beyond the cutover limit. The planted lag gate resets it instead.
#[test]
fn planted_last_copy_lag_gate_scripted() {
    scripted(Case::LastCopyGate);
}

/// Planned handover: no acknowledged write is lost across a cutover. The
/// planted planner cuts over to a target one write short of the freeze.
#[test]
fn planted_handover_short_prefix_scripted() {
    scripted(Case::HandoverShort);
}

/// Cold reset: a scoped reset starts the slot empty on every worker. The
/// planted worker keeps its old store when it installs the reset epoch.
#[test]
fn planted_cold_reset_retains_data_scripted() {
    scripted(Case::ColdRetains);
}

/// SIM-7: a pause between a turn's clock sample and its use. The final lease
/// check samples again; the planted check reuses the first sample and serves
/// after the grant ended.
#[test]
fn planted_publish_reuses_first_sample_scripted() {
    scripted(Case::StaleSample);
}
