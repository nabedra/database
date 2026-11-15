use super::*;
use crate::cluster::attachment::{
    check_attach,
    tests::{Fixture, candidate, deadline, store},
};
fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn owner() -> Owner {
    Owner {
        process_id: id(90),
        epoch: 7,
    }
}
async fn setup() -> (Fixture, Control, AttachedConfig) {
    let f = Fixture::new();
    let local = f.ready(&f.input());
    let mut root = candidate(&local, true);
    root.controller.owner_id = Some(owner().process_id);
    root.controller.epoch = owner().epoch;
    root.controller.lease_state = LeaseState::Held;
    root.controller.lease_until_ms = Some(1000);
    root.policy.desired = 1;
    root.policy.min_nodes = 1;
    root.policy.max_nodes = 1;
    let root = allocate_start(&root, owner(), id(30), id(40), 10, id(50))
        .unwrap()
        .into_parts()
        .2;
    let s = store(Some(&root));
    let attached = check_attach(
        s.load_control(deadline()).await.unwrap(),
        local,
        "Fleet_1",
        Provisioning::Managed,
    )
    .unwrap();
    (f, root, attached)
}

use crate::cluster::{
    callback::{CallbackReply, InventoryCommand, InventoryEvidence, decode_reply},
    store::PreparedProvision,
};
use serde_json::{Value, json};
fn evidence(root: &Control, records: Value) -> InventoryEvidence {
    let command = InventoryCommand::build(root, owner_for(root), id(200)).unwrap();
    let (_, ctx) = command.into_parts();
    let reply =
        serde_json::to_vec(&json!({"version":1,"invocation_id":id(200),"result":records})).unwrap();
    let CallbackReply::Inventory(e) = decode_reply(200, &reply, &ctx).unwrap() else {
        panic!("inventory")
    };
    e
}
fn owner_for(c: &Control) -> Owner {
    Owner {
        process_id: c.controller.owner_id.unwrap(),
        epoch: c.controller.epoch,
    }
}
fn record(c: &Control, resource: &str, state: &str) -> Value {
    json!({"cluster_id":c.cluster_id,"node_id":id(30),"request_id":id(40),"resource_id":resource,"state":state})
}
fn unresolved(c: &Control, a: &AttachedConfig) -> Control {
    let (p, _) = claim_start(c, owner(), id(40), id(60), a, 10, id(70))
        .unwrap()
        .into_parts();
    let mut c = p.into_parts().2;
    c.intents[0].state = IntentState::Reconciling;
    c.intents[0].error_code = Some("callback_uncertain".into());
    c.intents[0].retry_at_ms = 100;
    c.scheduler.failures = 3;
    c.scheduler.backoff_until_ms = 200;
    c.scheduler.cooldown_until_ms = 777;
    c.scheduler.low_since_ms = Some(6);
    Control::decode(&c.encode().unwrap()).unwrap()
}
fn change(plan: ReconcilePlan, class: ReconcileClass) -> ProvisionCandidate {
    let ReconcilePlan::Change {
        candidate,
        classification,
    } = plan
    else {
        panic!("expected change")
    };
    assert_eq!(classification, class);
    candidate
}
#[tokio::test]
async fn exact_positive_recovers_retained_launch_and_submits() {
    let (_f, c, a) = setup().await;
    let c = unresolved(&c, &a);
    let e = evidence(&c, json!([record(&c, "resource/one", "running")]));
    let candidate = change(
        reconcile_start(&c, owner(), id(40), e, 999999, id(80)).unwrap(),
        ReconcileClass::Recovered,
    );
    let s = store(Some(&c));
    let result = PreparedProvision::prepare(s.load_control(deadline()).await.unwrap(), candidate)
        .unwrap()
        .submit(&s, deadline())
        .await
        .unwrap();
    let crate::cluster::store::OwnershipOutcome::Acknowledged(done) = result else {
        panic!("ack")
    };
    let mut expected = c.clone();
    expected.revision += 1;
    expected.change_id = id(80);
    expected.nodes[0].resource_id = Some("resource/one".into());
    expected.intents[0].resource_id = Some("resource/one".into());
    expected.intents[0].state = IntentState::Completed;
    expected.intents[0].outcome = Some(IntentOutcome::ResourceKnown);
    expected.intents[0].error_code = None;
    assert_eq!(done.root(), &expected);
    assert_eq!(managed_capacity(done.root()).unwrap(), 1);
    assert_eq!(
        done.root().capacity().unwrap().credits,
        c.capacity().unwrap().credits
    );
}
#[tokio::test]
async fn exact_absence_allows_only_explicit_same_identity_retry() {
    let (_f, c, a) = setup().await;
    let c = unresolved(&c, &a);
    assert!(matches!(
        reconcile_start(&c, owner(), id(40), evidence(&c, json!([])), 999999, id(80)).unwrap(),
        ReconcilePlan::Unchanged(ReconcileClass::Absent)
    ));
    let (candidate, _) = retry_start(
        &c,
        owner(),
        id(40),
        evidence(&c, json!([])),
        id(61),
        &a,
        200,
        id(81),
    )
    .unwrap()
    .into_parts();
    let next = candidate.into_parts().2;
    let mut expected = c.clone();
    expected.revision += 1;
    expected.change_id = id(81);
    expected.intents[0].state = IntentState::InProgress;
    expected.intents[0].attempts += 1;
    expected.intents[0].delivery = next.intents[0].delivery.clone();
    assert_eq!(next, expected);
    assert_eq!(
        next.intents[0].delivery.as_ref().unwrap().invocation_id,
        id(61)
    );
    assert_eq!(managed_capacity(&next).unwrap(), 1);
    assert!(claim_start(&c, owner(), id(40), id(62), &a, 200, id(82)).is_err());
}

#[tokio::test]
async fn complete_scan_rejects_wrong_request_unknown_collision_and_terminal_independently() {
    let (_f, c, a) = setup().await;
    let c = unresolved(&c, &a);
    for mode in 0..7 {
        let mut c = c.clone();
        let mut r = record(&c, "resource/one", "running");
        let mut records = vec![r.clone()];
        let expected = if mode >= 4 {
            ReconcileClass::TerminalConflict
        } else {
            ReconcileClass::Quarantined
        };
        match mode {
            0 => {
                r["request_id"] = json!(id(41));
                records = vec![r];
            }
            1 => {
                r["node_id"] = json!(id(31));
                records.push(r);
                records[1]["resource_id"] = json!("unknown");
                records[1]["request_id"] = json!(id(41));
            }
            2 => {
                c.nodes[0].resource_id = Some("other".into());
                c.intents[0].resource_id = Some("other".into());
            }
            3 => {
                let mut n = c.nodes[0].clone();
                n.node_id = id(31);
                n.launch_id = Some(id(41));
                n.resource_id = Some("resource/one".into());
                let mut i = c.intents[0].clone();
                i.request_id = id(41);
                i.launch_id = id(41);
                i.node_id = id(31);
                i.resource_id = n.resource_id.clone();
                c.nodes.push(n);
                c.intents.push(i);
            }
            4 => {
                c.intents[0].state = IntentState::Failed;
                c.intents[0].outcome = Some(IntentOutcome::NotInvoked);
            }
            5 => c.nodes[0].state = NodeState::Lost,
            _ => c.nodes[0].state = NodeState::Stopped,
        }
        c.validate().unwrap();
        let next = change(
            reconcile_start(
                &c,
                owner(),
                id(40),
                evidence(&c, json!(records)),
                20,
                id(80),
            )
            .unwrap(),
            expected,
        )
        .into_parts()
        .2;
        assert_eq!(next.nodes, c.nodes, "mode {mode}");
        assert_eq!(next.intents[0].resource_id, c.intents[0].resource_id);
        assert_eq!(next.intents[0].outcome, c.intents[0].outcome);
        assert_eq!(next.intents[0].attempts, c.intents[0].attempts);
        assert_eq!(next.intents[0].delivery, c.intents[0].delivery);
        assert_eq!(next.scheduler.failures, c.scheduler.failures + 1);
        assert!(next.scheduler.backoff_until_ms >= c.scheduler.backoff_until_ms);
        assert_eq!(
            next.scheduler.cooldown_until_ms,
            c.scheduler.cooldown_until_ms
        );
    }
}
#[tokio::test]
async fn retry_refuses_backoffs_exhaustion_phase_evidence_and_reused_identity() {
    let (_f, c, a) = setup().await;
    let c = unresolved(&c, &a);
    for mode in 0..12 {
        let mut c = c.clone();
        let mut invocation = id(61);
        let mut records = json!([]);
        match mode {
            0 => c.intents[0].retry_at_ms = 201,
            1 => c.scheduler.backoff_until_ms = 201,
            2 => c.intents[0].attempts = c.timing.max_attempts,
            3 => {
                c.nodes[0].state = NodeState::Draining;
                c.nodes[0].drain = Some(crate::cluster::member::Drain {
                    id: id(103),
                    requested_at_ms: 11,
                    ack_seq: None,
                    confirmed_idle: false,
                });
            }
            4 => c.nodes[0].state = NodeState::Lost,
            5 => c.nodes[0].state = NodeState::Stopped,
            6 => c.intents[0].state = IntentState::Pending,
            7 => c.intents[0].state = IntentState::InProgress,
            8 => records = json!([record(&c, "one", "running")]),
            9 => invocation = id(60),
            10 => {
                c.nodes[0].resource_id = Some("known".into());
                c.intents[0].resource_id = Some("known".into());
            }
            _ => {
                c.nodes[0].retired_incarnations.push(id(104));
            }
        }
        c.validate().unwrap();
        let saved = c.clone();
        assert!(
            retry_start(
                &c,
                owner(),
                id(40),
                evidence(&c, records),
                invocation,
                &a,
                200,
                id(81)
            )
            .is_err(),
            "mode {mode}"
        );
        assert_eq!(c, saved);
    }
}
#[tokio::test]
async fn takeover_adopts_delivery_before_recovery_and_requires_new_inventory_round() {
    let (_f, c, a) = setup().await;
    let (p, _) = claim_start(&c, owner(), id(40), id(60), &a, 10, id(70))
        .unwrap()
        .into_parts();
    let mut c = p.into_parts().2;
    c.intents[0].attempts = 2;
    c.intents[0].error_code = Some("callback_uncertain".into());
    c.intents[0].retry_at_ms = 888;
    c.scheduler.failures = 5;
    c.scheduler.backoff_until_ms = 999;
    c.validate().unwrap();
    let old = evidence(&c, json!([]));
    let mut taken = c.clone();
    taken.revision += 1;
    taken.change_id = id(71);
    taken.controller.owner_id = Some(id(91));
    taken.controller.epoch += 1;
    let o = owner_for(&taken);
    assert!(reconcile_start(&taken, o, id(40), old, 20, id(80)).is_err());
    let adopted = change(
        reconcile_start(
            &taken,
            o,
            id(40),
            evidence(&taken, json!([record(&taken, "one", "running")])),
            20,
            id(80),
        )
        .unwrap(),
        ReconcileClass::Recovered,
    )
    .into_parts()
    .2;
    assert_eq!(adopted.intents[0].state, IntentState::Reconciling);
    assert_eq!(adopted.intents[0].delivery, c.intents[0].delivery);
    assert_eq!(adopted.intents[0].attempts, 2);
    let mut expected = taken.clone();
    expected.revision += 1;
    expected.change_id = id(80);
    expected.intents[0].state = IntentState::Reconciling;
    assert_eq!(adopted, expected);
    assert_eq!(adopted.intents[0].resource_id, None);
    let done = change(
        reconcile_start(
            &adopted,
            o,
            id(40),
            evidence(&adopted, json!([record(&adopted, "one", "running")])),
            999999,
            id(81),
        )
        .unwrap(),
        ReconcileClass::Recovered,
    )
    .into_parts()
    .2;
    assert_eq!(done.intents[0].outcome, Some(IntentOutcome::ResourceKnown));
    let stale = evidence(&done, json!([]));
    let mut admin = done.clone();
    admin.revision += 1;
    admin.change_id = id(82);
    admin.policy.revision += 1;
    assert!(reconcile_start(&admin, o, id(40), stale, 30, id(83)).is_err());
}
#[tokio::test]
async fn terminated_is_exact_binding_only_and_absence_never_releases_capacity() {
    let (_f, c, a) = setup().await;
    let c = unresolved(&c, &a);
    let plan = reconcile_start(
        &c,
        owner(),
        id(40),
        evidence(&c, json!([record(&c, "one", "terminated")])),
        999999,
        id(80),
    )
    .unwrap();
    let ReconcilePlan::Unchanged(ReconcileClass::NeedsTerminationReconciliation(b)) = plan else {
        panic!("termination classification")
    };
    assert_eq!(b.cluster_id, c.cluster_id);
    assert_eq!(b.node_id, id(30));
    assert_eq!(b.request_id, id(40));
    assert_eq!(b.resource_id, "one");
    assert!(format!("{b:?}").contains(".."));
    assert_eq!(managed_capacity(&c).unwrap(), 1);
    assert!(matches!(
        reconcile_start(
            &c,
            owner(),
            id(40),
            evidence(&c, json!([])),
            u64::MAX,
            id(80)
        )
        .unwrap(),
        ReconcilePlan::Unchanged(ReconcileClass::Absent)
    ));
}
#[tokio::test]
async fn inventory_failure_preserves_root_except_checked_scheduler_backoff() {
    let (_f, c, a) = setup().await;
    let c = unresolved(&c, &a);
    let next = inventory_failed_candidate(&c, owner(), 20, id(80))
        .unwrap()
        .into_parts()
        .2;
    let mut expected = c.clone();
    expected.revision += 1;
    expected.change_id = id(80);
    expected.scheduler.failures += 1;
    expected.scheduler.backoff_until_ms = next.scheduler.backoff_until_ms;
    assert_eq!(next, expected);
    assert!(next.scheduler.backoff_until_ms >= c.scheduler.backoff_until_ms);
    for mode in 0..3 {
        let mut c = c.clone();
        let mut now = 20;
        match mode {
            0 => c.scheduler.failures = u64::MAX,
            1 => now = u64::MAX,
            _ => c.revision = u64::MAX,
        };
        c.validate().unwrap();
        assert!(matches!(
            inventory_failed_candidate(&c, owner(), now, id(80)),
            Err(StorageError::LimitExceeded)
        ));
    }
}

#[tokio::test]
async fn late_registered_and_unregistered_draining_recovery_preserves_entire_history() {
    use crate::cluster::member::{Drain, Session};
    let (_f, c, a) = setup().await;
    let c = unresolved(&c, &a);
    for registered in [false, true] {
        let mut c = c.clone();
        let n = &mut c.nodes[0];
        n.state = NodeState::Draining;
        n.drain = Some(Drain {
            id: id(103),
            requested_at_ms: 11,
            ack_seq: registered.then_some(9),
            confirmed_idle: registered,
        });
        if registered {
            n.registration = Registration::Registered;
            n.incarnation = Some(id(101));
            n.endpoint = Some("http://worker:8000/".into());
            n.session = Some(Session {
                id: id(102),
                verifier: "a".repeat(64),
            });
        }
        c.validate().unwrap();
        let next = change(
            reconcile_start(
                &c,
                owner(),
                id(40),
                evidence(&c, json!([record(&c, "one", "running")])),
                999999,
                id(80),
            )
            .unwrap(),
            ReconcileClass::Recovered,
        )
        .into_parts()
        .2;
        let mut expected = c.clone();
        expected.revision += 1;
        expected.change_id = id(80);
        expected.nodes[0].resource_id = Some("one".into());
        expected.intents[0].resource_id = Some("one".into());
        expected.intents[0].state = IntentState::Completed;
        expected.intents[0].outcome = Some(IntentOutcome::ResourceKnown);
        expected.intents[0].error_code = None;
        assert_eq!(next, expected);
        assert_eq!(
            next.capacity().unwrap().credits,
            c.capacity().unwrap().credits
        );
        assert!(matches!(
            reconcile_start(
                &next,
                owner(),
                id(40),
                evidence(&next, json!([record(&next, "one", "running")])),
                999999,
                id(81)
            )
            .unwrap(),
            ReconcilePlan::Unchanged(ReconcileClass::AlreadyKnown)
        ));
    }
}
#[tokio::test]
async fn known_and_unresolved_unrelated_resources_preserve_collision_refusals() {
    let (_f, c, a) = setup().await;
    let mut c = unresolved(&c, &a);
    let mut n = c.nodes[0].clone();
    n.node_id = id(31);
    n.launch_id = Some(id(41));
    n.resource_id = Some("other".into());
    let mut i = c.intents[0].clone();
    i.node_id = id(31);
    i.request_id = id(41);
    i.launch_id = id(41);
    i.resource_id = Some("other".into());
    c.nodes.push(n);
    c.intents.push(i);
    c.validate().unwrap();
    let mut other = record(&c, "other", "running");
    other["node_id"] = json!(id(31));
    other["request_id"] = json!(id(41));
    let records = json!([record(&c, "one", "running"), other]);
    assert_eq!(
        change(
            reconcile_start(
                &c,
                owner(),
                id(40),
                evidence(&c, records.clone()),
                20,
                id(80)
            )
            .unwrap(),
            ReconcileClass::Recovered
        )
        .into_parts()
        .2
        .nodes[1],
        c.nodes[1]
    );
    let mut node_known = c.clone();
    node_known.intents[1].resource_id = None;
    node_known.validate().unwrap();
    let _ = change(
        reconcile_start(
            &node_known,
            owner(),
            id(40),
            evidence(&node_known, records.clone()),
            20,
            id(80),
        )
        .unwrap(),
        ReconcileClass::Recovered,
    );
    // A complete snapshot with multiple terminated resources sharing one launch
    // passes wire structure but must not select just one of them.
    let r1 = record(&c, "one", "terminated");
    let r2 = record(&c, "two", "terminated");
    let _ = change(
        reconcile_start(
            &c,
            owner(),
            id(40),
            evidence(&c, json!([r1, r2])),
            20,
            id(80),
        )
        .unwrap(),
        ReconcileClass::Quarantined,
    );
    c.nodes[1].resource_id = None;
    c.intents[1].resource_id = None;
    c.validate().unwrap();
    let _ = change(
        reconcile_start(&c, owner(), id(40), evidence(&c, records), 20, id(80)).unwrap(),
        ReconcileClass::Recovered,
    );
    let mut retired = c.clone();
    retired.nodes[0].retired_incarnations.push(id(104));
    retired.validate().unwrap();
    let _ = change(
        reconcile_start(
            &retired,
            owner(),
            id(40),
            evidence(&retired, json!([record(&retired, "one", "running")])),
            20,
            id(80),
        )
        .unwrap(),
        ReconcileClass::TerminalConflict,
    );
}
#[tokio::test]
async fn parent_regression_two_unresolved_launches_recover_in_separate_fresh_rounds() {
    use crate::cluster::store::OwnershipOutcome;
    let (_f, c, a) = setup().await;
    let mut c = unresolved(&c, &a);
    c.policy.desired = 2;
    c.policy.min_nodes = 2;
    c.policy.max_nodes = 2;
    c = allocate_start(&c, owner(), id(31), id(41), 200, id(81))
        .unwrap()
        .into_parts()
        .2;
    let (claim, _) = claim_start(&c, owner(), id(41), id(61), &a, 200, id(82))
        .unwrap()
        .into_parts();
    c = claim.into_parts().2;
    c.intents[1].state = IntentState::Reconciling;
    c.intents[1].error_code = Some("callback_uncertain".into());
    c = Control::decode(&c.encode().unwrap()).unwrap();
    let mut second = record(&c, "resource/second", "running");
    second["node_id"] = json!(id(31));
    second["request_id"] = json!(id(41));
    let records = json!([record(&c, "resource/first", "running"), second]);
    let stale_second = evidence(&c, records.clone());
    let s = store(Some(&c));
    let candidate = change(
        reconcile_start(
            &c,
            owner(),
            id(40),
            evidence(&c, records.clone()),
            999999,
            id(83),
        )
        .unwrap(),
        ReconcileClass::Recovered,
    );
    let first = PreparedProvision::prepare(s.load_control(deadline()).await.unwrap(), candidate)
        .unwrap()
        .submit(&s, deadline())
        .await
        .unwrap();
    let OwnershipOutcome::Acknowledged(first) = first else {
        panic!("first recovery must be acknowledged")
    };
    let mut expected = c.clone();
    expected.revision += 1;
    expected.change_id = id(83);
    expected.nodes[0].resource_id = Some("resource/first".into());
    expected.intents[0].resource_id = Some("resource/first".into());
    expected.intents[0].state = IntentState::Completed;
    expected.intents[0].outcome = Some(IntentOutcome::ResourceKnown);
    expected.intents[0].error_code = None;
    assert_eq!(first.root(), &expected);
    assert!(matches!(
        reconcile_start(first.root(), owner(), id(41), stale_second, 999999, id(84)),
        Err(StorageError::Conflict)
    ));
    let candidate = change(
        reconcile_start(
            first.root(),
            owner(),
            id(41),
            evidence(first.root(), records),
            999999,
            id(84),
        )
        .unwrap(),
        ReconcileClass::Recovered,
    );
    let second = PreparedProvision::prepare(s.load_control(deadline()).await.unwrap(), candidate)
        .unwrap()
        .submit(&s, deadline())
        .await
        .unwrap();
    let OwnershipOutcome::Acknowledged(second) = second else {
        panic!("second recovery must be acknowledged")
    };
    expected.revision += 1;
    expected.change_id = id(84);
    expected.nodes[1].resource_id = Some("resource/second".into());
    expected.intents[1].resource_id = Some("resource/second".into());
    expected.intents[1].state = IntentState::Completed;
    expected.intents[1].outcome = Some(IntentOutcome::ResourceKnown);
    expected.intents[1].error_code = None;
    assert_eq!(second.root(), &expected);
    assert_eq!(managed_capacity(second.root()).unwrap(), 2);
    assert_eq!(
        second.root().capacity().unwrap().credits,
        c.capacity().unwrap().credits
    );
}
#[tokio::test]
async fn parent_regression_collision_checks_scan_each_retained_array_once_per_record() {
    let (_f, c, a) = setup().await;
    let base = unresolved(&c, &a);
    for fleet in [32, 1024] {
        let mut c = base.clone();
        let node = c.nodes[0].clone();
        let intent = c.intents[0].clone();
        for k in 1..fleet {
            let mut n = node.clone();
            let mut i = intent.clone();
            n.node_id = id(1000 + 2 * k);
            n.launch_id = Some(id(1001 + 2 * k));
            i.node_id = n.node_id;
            i.request_id = n.launch_id.unwrap();
            i.launch_id = i.request_id;
            i.delivery.as_mut().unwrap().invocation_id = id(4000 + k);
            c.nodes.push(n);
            c.intents.push(i);
        }
        let mut records = Vec::new();
        for (k, (n, i)) in c.nodes.iter_mut().zip(c.intents.iter_mut()).enumerate() {
            let resource = format!("resource/{k}");
            n.resource_id = Some(resource.clone());
            i.resource_id = Some(resource.clone());
            records.push(json!({"cluster_id":c.cluster_id,"node_id":n.node_id,
                "request_id":i.request_id,"resource_id":resource,"state":"running"}));
        }
        c = Control::decode(&c.encode().unwrap()).unwrap();
        let e = evidence(&c, json!(records));
        let before = COLLISION_CHECKS.with(std::cell::Cell::get);
        let class = classify_inventory(&c, owner(), &e).unwrap();
        let checks = COLLISION_CHECKS.with(std::cell::Cell::get) - before;
        assert!(matches!(class, InventoryClass::Clean));
        assert!(checks > 0, "the collision scan observer must be active");
        let bound = e.records().len() * (c.nodes.len() + c.intents.len());
        assert!(
            checks <= bound as u64,
            "{checks} collision checks exceed {bound}"
        );
    }
}
#[tokio::test]
async fn retained_inventory_recovery_and_failure_work_at_full_admission() {
    let (_f, c, a) = setup().await;
    let mut c = unresolved(&c, &a);
    let node = c.nodes[0].clone();
    let intent = c.intents[0].clone();
    for k in 0..1365u128 {
        let mut n = node.clone();
        let mut i = intent.clone();
        n.node_id = id(1000 + 2 * k);
        n.launch_id = Some(id(1001 + 2 * k));
        i.node_id = n.node_id;
        i.request_id = n.launch_id.unwrap();
        i.launch_id = i.request_id;
        i.delivery = None;
        i.attempts = 0;
        c.nodes.push(n);
        c.intents.push(i);
    }
    c = Control::decode(&c.encode().unwrap()).unwrap();
    assert!(c.check_new_admission().is_err());
    let credits = c.capacity().unwrap().credits;
    let next = change(
        reconcile_start(
            &c,
            owner(),
            id(40),
            evidence(&c, json!([record(&c, "one", "running")])),
            999999,
            id(80),
        )
        .unwrap(),
        ReconcileClass::Recovered,
    )
    .into_parts()
    .2;
    assert_eq!(next.capacity().unwrap().credits, credits);
    assert_eq!(managed_capacity(&next).unwrap(), 1366);
    let failed = inventory_failed_candidate(&c, owner(), 20, id(80))
        .unwrap()
        .into_parts()
        .2;
    assert_eq!(failed.capacity().unwrap().credits, credits);
}
#[tokio::test]
async fn new_root_retry_confirmation_and_later_denial_preserve_ambiguity() {
    use crate::cluster::{
        callback::{DeliveryOutcome, validate_start_bytes},
        store::OwnershipOutcome,
    };
    let (_f, c, a) = setup().await;
    let mut c = unresolved(&c, &a);
    c.revision += 1;
    c.change_id = id(72);
    c.policy.revision += 1;
    c.controller.owner_id = Some(id(91));
    c.controller.epoch += 1;
    let o = owner_for(&c);
    c.validate().unwrap();
    let (p, pending) = retry_start(
        &c,
        o,
        id(40),
        evidence(&c, json!([])),
        id(61),
        &a,
        200,
        id(81),
    )
    .unwrap()
    .into_parts();
    let next = p.result.clone();
    let s = store(Some(&c));
    let ack = PreparedProvision::prepare(s.load_control(deadline()).await.unwrap(), p)
        .unwrap()
        .submit(&s, deadline())
        .await
        .unwrap();
    let (bytes, ctx) = pending.confirm(&ack).unwrap().into_parts();
    assert_eq!(
        validate_start_bytes(&bytes, &next, &a).unwrap(),
        next.intents[0].delivery.as_ref().unwrap().command_sha256
    );
    let next = complete_start(
        &next,
        o,
        &ctx,
        CallbackReply::Failure {
            code: "callback_capacity".into(),
            outcome: DeliveryOutcome::NotInvoked,
        },
        201,
        id(82),
    )
    .unwrap()
    .into_parts()
    .2;
    assert_eq!(next.intents[0].state, IntentState::Reconciling);
    assert_eq!(next.intents[0].attempts, 2);
    assert_eq!(next.intents[0].outcome, None);
    assert_eq!(next.scheduler.failures, c.scheduler.failures + 1);
    let (_, pending) = retry_start(
        &c,
        o,
        id(40),
        evidence(&c, json!([])),
        id(62),
        &a,
        200,
        id(83),
    )
    .unwrap()
    .into_parts();
    assert!(
        pending
            .confirm(&OwnershipOutcome::Unknown { current: None })
            .is_none()
    );
}
