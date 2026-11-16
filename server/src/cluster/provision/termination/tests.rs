use super::*;
use crate::cluster::{
    attachment::tests::{deadline, store},
    callback::{CallbackReply, InventoryCommand, decode_reply},
    configuration::{Template, Timing},
    member::{Delivery, Drain, Session},
    store::{OwnershipOutcome, PreparedProvision},
};
use serde_json::{Value, json};
fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn fixture() -> Value {
    let template = json!({"api_urls":["http://api/"],"listen":"worker:0","storage":{"driver":"http","options":{"base_url":"http://store/","namespace":"nabedra/","token_file":"/token","ca_file":null}},"tls":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"});
    let hash = Template::decode(&serde_json::to_vec(&template).unwrap())
        .unwrap()
        .fingerprint()
        .unwrap();
    json!({"version":1,"cluster_id":id(1),"name":"Fleet_1","config":{"generation":2,"worker_semantic_fingerprint":"d".repeat(64),"storage_fingerprint":"a".repeat(64),"auth_fingerprint":"b".repeat(64),"template_fingerprint":hash,"lifecycle_fingerprint":"c".repeat(64),"adapter_contract":"meshdb.callbacks.v1"},"provisioning":"managed","template":template,"timing":Timing::default(),"revision":1,"change_id":id(2),"policy":{"revision":1,"mode":"fixed","desired":0,"min_nodes":0,"max_nodes":0},"controller":{"owner_id":null,"epoch":0,"lease_state":"released","lease_until_ms":null},"scheduler":{"cooldown_until_ms":0,"backoff_until_ms":0,"failures":0,"low_since_ms":null},"admin_seq":0,"admin_head":null,"nodes":[],"intents":[]})
}
fn owner() -> Owner {
    Owner {
        process_id: id(90),
        epoch: 7,
    }
}
fn root() -> Control {
    let mut v = fixture();
    v["controller"] = json!({"owner_id":id(90),"epoch":7,"lease_state":"held","lease_until_ms":1});
    v["policy"] = json!({"revision":9,"mode":"fixed","desired":3,"min_nodes":3,"max_nodes":3});
    Control::decode(&serde_json::to_vec(&v).unwrap()).unwrap()
}
fn allocated() -> Control {
    allocate_start(&root(), owner(), id(30), id(40), 10, id(50))
        .unwrap()
        .into_parts()
        .2
}

fn ambiguous() -> Control {
    let mut c = allocated();
    c.intents[0].state = IntentState::Reconciling;
    c.intents[0].attempts = 2;
    c.intents[0].delivery = Some(Delivery {
        invocation_id: id(60),
        owner_id: owner().process_id,
        epoch: owner().epoch,
        command_sha256: "a".repeat(64),
    });
    c.intents[0].error_code = Some("callback_uncertain".into());
    c.scheduler.failures = 3;
    c.scheduler.backoff_until_ms = 999;
    c.scheduler.low_since_ms = Some(2);
    roundtrip(c)
}
fn roundtrip(c: Control) -> Control {
    Control::decode(&c.encode().unwrap()).unwrap()
}
fn evidence(c: &Control, records: Value) -> InventoryEvidence {
    let (_, ctx) = InventoryCommand::build(
        c,
        Owner {
            process_id: c.controller.owner_id.unwrap(),
            epoch: c.controller.epoch,
        },
        id(200),
    )
    .unwrap()
    .into_parts();
    let bytes =
        serde_json::to_vec(&json!({"version":1,"invocation_id":id(200),"result":records})).unwrap();
    let CallbackReply::Inventory(e) = decode_reply(200, &bytes, &ctx).unwrap() else {
        panic!("inventory")
    };
    e
}
fn record(c: &Control, resource: &str, state: &str) -> Value {
    json!({"cluster_id":c.cluster_id,"node_id":id(30),"request_id":id(40),"resource_id":resource,"state":state})
}
fn change(p: TerminationPlan) -> ProvisionCandidate {
    let TerminationPlan::Change(c) = p else {
        panic!("expected terminal candidate: {p:?}")
    };
    c
}
#[tokio::test]
async fn unique_positive_lost_launch_commits_exact_terminal_witness() {
    let mut c = ambiguous();
    c.nodes[0].state = NodeState::Lost;
    let c = roundtrip(c);
    let p = change(
        reconcile_termination(
            &c,
            owner(),
            id(30),
            evidence(&c, json!([record(&c, "resource/one", "terminated")])),
            999999,
            id(80),
        )
        .unwrap(),
    );
    let s = store(Some(&c));
    let out = PreparedProvision::prepare(s.load_control(deadline()).await.unwrap(), p)
        .unwrap()
        .submit(&s, deadline())
        .await
        .unwrap();
    let OwnershipOutcome::Acknowledged(done) = out else {
        panic!("ack")
    };
    let mut expected = c.clone();
    expected.revision += 1;
    expected.change_id = id(80);
    expected.nodes[0].state = NodeState::Stopped;
    expected.nodes[0].resource_id = Some("resource/one".into());
    expected.intents[0].resource_id = Some("resource/one".into());
    expected.intents[0].state = IntentState::Failed;
    expected.intents[0].outcome = Some(IntentOutcome::Terminated);
    assert_eq!(done.root(), &expected);
    assert_eq!(s.load_control(deadline()).await.unwrap().root(), &expected);
    assert_eq!(managed_capacity(&expected), Ok(0));
    assert_eq!(expected.capacity().unwrap().credits, 0);
}
fn registered_stop() -> Control {
    let mut c = ambiguous();
    let n = &mut c.nodes[0];
    n.resource_id = Some("resource/one".into());
    n.state = NodeState::Draining;
    n.incarnation = Some(id(101));
    n.registration = Registration::Registered;
    n.endpoint = Some("http://worker:8000/".into());
    n.session = Some(Session {
        id: id(102),
        verifier: "b".repeat(64),
    });
    n.drain = Some(Drain {
        id: id(103),
        requested_at_ms: 11,
        ack_seq: Some(9),
        confirmed_idle: true,
    });
    c.intents[0].resource_id = Some("resource/one".into());
    c.intents[0].state = IntentState::Completed;
    c.intents[0].outcome = Some(IntentOutcome::ResourceKnown);
    let mut stop = c.intents[0].clone();
    stop.request_id = id(70);
    stop.action = Action::Stop;
    stop.incarnation_id = Some(id(101));
    stop.template_fingerprint = None;
    stop.state = IntentState::Reconciling;
    stop.outcome = None;
    stop.delivery.as_mut().unwrap().invocation_id = id(61);
    c.intents.push(stop);
    roundtrip(c)
}
fn terminal_candidate(c: &Control, proposal: Uuid) -> ProvisionCandidate {
    change(
        reconcile_termination(
            c,
            owner(),
            id(30),
            evidence(c, json!([record(c, "resource/one", "terminated")])),
            999999,
            proposal,
        )
        .unwrap(),
    )
}
#[tokio::test]
async fn known_stop_commits_preserving_complete_session_drain_delivery_history() {
    let c = registered_stop();
    let s = store(Some(&c));
    let p = terminal_candidate(&c, id(80));
    let out = PreparedProvision::prepare(s.load_control(deadline()).await.unwrap(), p)
        .unwrap()
        .submit(&s, deadline())
        .await
        .unwrap();
    assert!(matches!(out, OwnershipOutcome::Acknowledged(_)));
    let next = s.load_control(deadline()).await.unwrap();
    let mut expected = c.clone();
    expected.revision += 1;
    expected.change_id = id(80);
    expected.nodes[0].state = NodeState::Stopped;
    expected.intents[1].state = IntentState::Completed;
    expected.intents[1].outcome = Some(IntentOutcome::Terminated);
    assert_eq!(next.root(), &expected);
    assert_eq!(managed_capacity(next.root()), Ok(0));
    assert_eq!(
        next.root().capacity().unwrap().credits,
        c.capacity().unwrap().credits
    );
    assert_eq!(next.root().intents.len(), 2);
}
#[tokio::test]
async fn late_unregistered_draining_and_phase_only_stopped_need_positive_record() {
    for state in [NodeState::Draining, NodeState::Stopped] {
        let mut c = ambiguous();
        c.nodes[0].state = state;
        c.nodes[0].drain = Some(Drain {
            id: id(103),
            requested_at_ms: 11,
            ack_seq: None,
            confirmed_idle: false,
        });
        let c = roundtrip(c);
        // Component shapes only: no callback, heartbeat, or provider executed.
        for (records, class) in [
            (json!([]), TerminationClass::Absent),
            (
                json!([record(&c, "resource/one", "running")]),
                TerminationClass::Pending,
            ),
            (
                json!([record(&c, "resource/one", "stopping")]),
                TerminationClass::Pending,
            ),
        ] {
            assert!(
                matches!(reconcile_termination(&c,owner(),id(30),evidence(&c,records),u64::MAX,c.change_id).unwrap(),TerminationPlan::Unchanged(v) if v==class)
            );
            assert_eq!(managed_capacity(&c), Ok(1));
            assert_eq!(c.capacity().unwrap().credits, 1);
        }
        let next = terminal_candidate(&c, id(80)).into_parts().2;
        let mut expected = c.clone();
        expected.revision += 1;
        expected.change_id = id(80);
        expected.nodes[0].state = NodeState::Stopped;
        expected.nodes[0].resource_id = Some("resource/one".into());
        expected.intents[0].resource_id = Some("resource/one".into());
        expected.intents[0].state = IntentState::Failed;
        expected.intents[0].outcome = Some(IntentOutcome::Terminated);
        assert_eq!(next, expected);
        assert_eq!(next.nodes[0].registration, Registration::Unregistered);
    }
}
#[test]
fn callback_null_and_not_invoked_stop_shapes_release_nothing_without_positive_inventory() {
    for failed in [false, true] {
        let mut c = registered_stop();
        if failed {
            c.intents[1].state = IntentState::Failed;
            c.intents[1].outcome = Some(IntentOutcome::NotInvoked);
        }
        let c = roundtrip(c);
        for records in [
            json!([]),
            json!([record(&c, "resource/one", "running")]),
            json!([record(&c, "resource/one", "stopping")]),
        ] {
            assert!(matches!(
                reconcile_termination(&c, owner(), id(30), evidence(&c, records), u64::MAX, id(80))
                    .unwrap(),
                TerminationPlan::Unchanged(_)
            ));
            assert_eq!(managed_capacity(&c), Ok(1));
            assert_eq!(c.capacity().unwrap().credits, 0);
        }
        assert_eq!(
            managed_capacity(&terminal_candidate(&c, id(80)).into_parts().2),
            Ok(0)
        );
    }
}
#[test]
fn exact_replay_precedes_max_revision_and_proposal_growth() {
    for c in [ambiguous(), registered_stop()] {
        let mut done = terminal_candidate(&c, id(80)).into_parts().2;
        done.revision = u64::MAX;
        let done = roundtrip(done);
        for proposal in [done.change_id, id(81)] {
            assert!(matches!(
                reconcile_termination(
                    &done,
                    owner(),
                    id(30),
                    evidence(&done, json!([record(&done, "resource/one", "terminated")])),
                    u64::MAX,
                    proposal
                )
                .unwrap(),
                TerminationPlan::Unchanged(TerminationClass::AlreadyTerminated)
            ));
        }
        let mut c = c;
        c.revision = u64::MAX;
        let c = roundtrip(c);
        assert!(matches!(
            reconcile_termination(
                &c,
                owner(),
                id(30),
                evidence(&c, json!([record(&c, "resource/one", "terminated")])),
                1,
                id(81)
            ),
            Err(StorageError::LimitExceeded)
        ));
    }
}
#[test]
fn mismatches_and_multiple_terminal_resources_degrade_without_attachment() {
    let base = ambiguous();
    for mode in 1..8 {
        let mut c = base.clone();
        let mut r = record(&c, "resource/one", "terminated");
        let mut records = vec![];
        match mode {
            0 => r["cluster_id"] = json!(id(300)),
            1 => r["node_id"] = json!(id(301)),
            2 => r["request_id"] = json!(id(302)),
            3 => {
                c.nodes[0].resource_id = Some("different".into());
            }
            4 => {
                c.intents[0].resource_id = Some("different".into());
            }
            5 => records.push(record(&c, "second", "terminated")),
            6 => {
                let mut n = c.nodes[0].clone();
                n.node_id = id(31);
                n.launch_id = Some(id(41));
                n.resource_id = Some("resource/one".into());
                let mut i = c.intents[0].clone();
                i.node_id = id(31);
                i.request_id = id(41);
                i.launch_id = id(41);
                i.resource_id = Some("resource/one".into());
                i.delivery.as_mut().unwrap().invocation_id = id(62);
                c.nodes.push(n);
                c.intents.push(i);
            }
            _ => {
                let mut stop = registered_stop().intents[1].clone();
                c = registered_stop();
                stop.request_id = id(71);
                stop.delivery.as_mut().unwrap().invocation_id = id(62);
                c.intents.push(stop);
            }
        }
        let c = roundtrip(c);
        records.push(r);
        let next = change(
            reconcile_termination(
                &c,
                owner(),
                id(30),
                evidence(&c, json!(records)),
                20,
                id(80),
            )
            .unwrap(),
        )
        .into_parts()
        .2;
        assert_eq!(next.nodes, c.nodes, "mode {mode}");
        assert_eq!(next.intents[0].resource_id, c.intents[0].resource_id);
        assert_eq!(next.intents[0].outcome, c.intents[0].outcome);
        assert_eq!(next.scheduler.failures, c.scheduler.failures + 1);
        assert_eq!(managed_capacity(&next), managed_capacity(&c));
    }
}
#[test]
fn contradictory_later_inventory_preserves_witness_and_pending_failure_is_global_only() {
    let c = ambiguous();
    let done = terminal_candidate(&c, id(80)).into_parts().2;
    for mut c in [done, c.clone()] {
        if c.intents[0].outcome.is_none() {
            c.intents[0].state = IntentState::Failed;
            c.intents[0].outcome = Some(IntentOutcome::NotInvoked);
        }
        let c = roundtrip(c);
        let next = change(
            reconcile_termination(
                &c,
                owner(),
                id(30),
                evidence(&c, json!([record(&c, "resource/one", "running")])),
                20,
                id(81),
            )
            .unwrap(),
        )
        .into_parts()
        .2;
        assert_eq!(next.nodes, c.nodes);
        assert_eq!(next.intents, c.intents);
        assert_eq!(next.capacity().unwrap().credits, 0);
        assert_eq!(managed_capacity(&next), Ok(0));
    }
    let c = allocated();
    let mut r = record(&c, "foreign", "terminated");
    r["node_id"] = json!(id(999));
    let next = change(
        reconcile_termination(&c, owner(), id(30), evidence(&c, json!([r])), 20, id(81)).unwrap(),
    )
    .into_parts()
    .2;
    assert_eq!(next.intents, c.intents);
    assert_eq!(next.nodes, c.nodes);
    assert_eq!(next.scheduler.failures, c.scheduler.failures + 1);
}
#[tokio::test]
async fn two_independent_unbound_terminal_launches_progress_only_with_fresh_successor_inventory() {
    let mut c = ambiguous();
    let mut n = c.nodes[0].clone();
    n.node_id = id(31);
    n.launch_id = Some(id(41));
    let mut i = c.intents[0].clone();
    i.node_id = id(31);
    i.request_id = id(41);
    i.launch_id = id(41);
    i.delivery.as_mut().unwrap().invocation_id = id(61);
    c.nodes.push(n);
    c.intents.push(i);
    let c = roundtrip(c);
    let mut other = record(&c, "resource/two", "terminated");
    other["node_id"] = json!(id(31));
    other["request_id"] = json!(id(41));
    let records = json!([record(&c, "resource/one", "terminated"), other]);
    let stale = evidence(&c, records.clone());
    let s = store(Some(&c));
    let p = change(
        reconcile_termination(
            &c,
            owner(),
            id(30),
            evidence(&c, records.clone()),
            20,
            id(80),
        )
        .unwrap(),
    );
    let out = PreparedProvision::prepare(s.load_control(deadline()).await.unwrap(), p)
        .unwrap()
        .submit(&s, deadline())
        .await
        .unwrap();
    assert!(matches!(out, OwnershipOutcome::Acknowledged(_)));
    let first = s.load_control(deadline()).await.unwrap();
    assert_eq!(first.root().nodes[1], c.nodes[1]);
    assert_eq!(first.root().intents[1], c.intents[1]);
    assert_eq!(managed_capacity(first.root()), Ok(1));
    assert!(matches!(
        reconcile_termination(first.root(), owner(), id(31), stale, 20, id(81)),
        Err(StorageError::Conflict)
    ));
    let p = change(
        reconcile_termination(
            first.root(),
            owner(),
            id(31),
            evidence(first.root(), records),
            20,
            id(81),
        )
        .unwrap(),
    );
    let out = PreparedProvision::prepare(first, p)
        .unwrap()
        .submit(&s, deadline())
        .await
        .unwrap();
    assert!(matches!(out, OwnershipOutcome::Acknowledged(_)));
    let done = s.load_control(deadline()).await.unwrap();
    assert_eq!(managed_capacity(done.root()), Ok(0));
    assert_eq!(done.root().capacity().unwrap().credits, 0);
    assert_eq!(done.root().intents.len(), 2);
}
#[test]
fn stale_root_owner_external_and_incomplete_evidence_refuse() {
    let c = ambiguous();
    let e = evidence(&c, json!([record(&c, "resource/one", "terminated")]));
    let mut newer = c.clone();
    newer.revision += 1;
    newer.change_id = id(88);
    assert!(reconcile_termination(&newer, owner(), id(30), e, 20, id(80)).is_err());
    let e = evidence(&c, json!([]));
    let mut wrong = owner();
    wrong.epoch += 1;
    assert!(reconcile_termination(&c, wrong, id(30), e, 20, id(80)).is_err());
    let mut ext = c.clone();
    ext.nodes[0].ownership = Ownership::External;
    ext.nodes[0].launch_id = None;
    ext.nodes[0].resource_id = Some(format!("external:{}", id(30)));
    ext.intents.clear();
    ext.provisioning = Provisioning::External;
    ext.config.adapter_contract = None;
    let ext = roundtrip(ext);
    assert!(InventoryCommand::build(&ext, owner(), id(200)).is_err()); // Earlier command refusal.
    assert!(
        reconcile_termination(&ext, owner(), id(30), evidence(&c, json!([])), 20, id(80)).is_err()
    );
    let (_, ctx) = InventoryCommand::build(&c, owner(), id(200))
        .unwrap()
        .into_parts();
    let mut foreign = record(&c, "one", "terminated");
    foreign["cluster_id"] = json!(id(300));
    for result in [
        json!([foreign]),
        Value::Null,
        json!({"partial":true}),
        json!([
            record(&c, "one", "terminated"),
            record(&c, "one", "terminated")
        ]),
    ] {
        let bytes =
            serde_json::to_vec(&json!({"version":1,"invocation_id":id(200),"result":result}))
                .unwrap();
        assert!(decode_reply(200, &bytes, &ctx).is_err()); // Earlier decode refusal, no transition coverage.
    }
    assert_eq!(
        format!(
            "{:?}",
            evidence(&c, json!([record(&c, "private/resource", "terminated")]))
        ),
        "InventoryEvidence { .. }"
    );
    assert_eq!(
        format!(
            "{:?}",
            TerminationPlan::Unchanged(TerminationClass::Pending)
        ),
        "TerminationPlan { .. }"
    );
}
#[tokio::test]
async fn gated_terminal_cas_loses_to_takeover_or_late_resource_recovery() {
    use crate::cluster::{controller::OwnershipChange, store::PreparedOwnership};
    use std::sync::Arc;
    use tokio::time::{Duration, Instant};
    for takeover in [false, true] {
        let c = ambiguous();
        let s = Arc::new(store(Some(&c)));
        let d = Instant::now() + Duration::from_secs(5);
        let stale = evidence(&c, json!([record(&c, "resource/one", "terminated")]));
        let p = terminal_candidate(&c, id(80));
        let prepared = PreparedProvision::prepare(s.load_control(d).await.unwrap(), p).unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let submitted = s.clone();
        let mut task = tokio::spawn(async move {
            let _ = entered_tx.send(());
            let _ = release_rx.await;
            prepared.submit(&submitted, d).await
        });
        let entered = tokio::time::timeout(Duration::from_secs(2), entered_rx).await;
        // Capture every fallible result until release and original-task settlement.
        let competing = async {
            let loaded = s.load_control(d).await?;
            if takeover {
                PreparedOwnership::prepare(
                    loaded,
                    OwnershipChange::Acquire { process_id: id(92) },
                    20000,
                    id(81),
                )?
                .submit(&s, d)
                .await
            } else {
                let plan = super::super::reconcile_start(
                    &c,
                    owner(),
                    id(40),
                    evidence(&c, json!([record(&c, "resource/two", "running")])),
                    20,
                    id(81),
                )?;
                let super::super::ReconcilePlan::Change { candidate, .. } = plan else {
                    return Err(StorageError::Conflict);
                };
                PreparedProvision::prepare(loaded, candidate)?
                    .submit(&s, d)
                    .await
            }
        }
        .await;
        let _ = release_tx.send(());
        let settled = match tokio::time::timeout(Duration::from_secs(6), &mut task).await {
            Ok(v) => Some(v),
            Err(_) => {
                task.abort();
                let _ = task.await;
                None
            }
        };
        let loaded = s
            .load_control(Instant::now() + Duration::from_secs(2))
            .await;
        assert!(entered.unwrap().is_ok());
        assert!(matches!(competing, Ok(OwnershipOutcome::Acknowledged(_))));
        assert!(matches!(
            settled.unwrap().unwrap().unwrap(),
            OwnershipOutcome::Conflict { .. }
        ));
        let loaded = loaded.unwrap();
        let current = loaded.root();
        let o = Owner {
            process_id: current.controller.owner_id.unwrap(),
            epoch: current.controller.epoch,
        };
        assert!(matches!(
            reconcile_termination(current, o, id(30), stale, 20001, id(82)),
            Err(StorageError::Conflict)
        ));
        assert!(
            PreparedProvision::prepare(
                s.load_control(deadline()).await.unwrap(),
                terminal_candidate(&c, id(83))
            )
            .is_err()
        );
        let p = change(
            reconcile_termination(
                current,
                o,
                id(30),
                evidence(
                    current,
                    json!([record(current, "resource/one", "terminated")]),
                ),
                20001,
                id(82),
            )
            .unwrap(),
        );
        let expected_terminal = takeover;
        let out = PreparedProvision::prepare(loaded, p)
            .unwrap()
            .submit(&s, deadline())
            .await
            .unwrap();
        assert!(matches!(out, OwnershipOutcome::Acknowledged(_)));
        let done = s.load_control(deadline()).await.unwrap();
        assert_eq!(
            done.root().intents[0].outcome == Some(IntentOutcome::Terminated),
            expected_terminal
        );
        assert_eq!(
            done.root().nodes[0].resource_id.as_deref(),
            Some(if takeover {
                "resource/one"
            } else {
                "resource/two"
            })
        );
        assert_eq!(done.root().intents.len(), 1);
        // Late active recovery through the real reconciliation path cannot erase a witness.
        if takeover {
            let plan = super::super::reconcile_start(
                done.root(),
                o,
                id(40),
                evidence(
                    done.root(),
                    json!([record(done.root(), "resource/one", "running")]),
                ),
                20002,
                id(84),
            )
            .unwrap();
            let super::super::ReconcilePlan::Change { candidate, .. } = plan else {
                panic!("degradation")
            };
            let later = candidate.into_parts().2;
            assert_eq!(later.nodes, done.root().nodes);
            assert_eq!(later.intents, done.root().intents);
            assert_eq!(managed_capacity(&later), Ok(0));
        }
    }
}
#[test]
fn retained_terminal_recovery_ignores_admission_gate_and_checks_failure_backoff() {
    let mut c = ambiguous();
    for k in 0..1365u128 {
        let mut n = c.nodes[0].clone();
        let mut i = c.intents[0].clone();
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
    let c = roundtrip(c);
    assert!(c.check_new_admission().is_err());
    let next = terminal_candidate(&c, id(80)).into_parts().2;
    assert_eq!(managed_capacity(&next), Ok(1365));
    assert_eq!(next.intents.len(), c.intents.len());
    assert_eq!(next.nodes[1..], c.nodes[1..]);
    assert_eq!(next.intents[1..], c.intents[1..]);
    for now in [20, u64::MAX] {
        let mut c = ambiguous();
        if now == 20 {
            c.scheduler.failures = u64::MAX;
        }
        let mut r = record(&c, "one", "terminated");
        r["node_id"] = json!(id(999));
        assert!(matches!(
            reconcile_termination(&c, owner(), id(30), evidence(&c, json!([r])), now, id(80)),
            Err(StorageError::LimitExceeded)
        ));
    }
}
#[tokio::test]
async fn original_start_reply_cannot_overwrite_committed_terminal_resource() {
    use crate::cluster::attachment::{
        check_attach,
        tests::{Fixture, candidate},
    };
    let f = Fixture::new();
    let local = f.ready(&f.input());
    let mut c = candidate(&local, true);
    c.controller.owner_id = Some(owner().process_id);
    c.controller.epoch = owner().epoch;
    c.controller.lease_state = LeaseState::Held;
    c.controller.lease_until_ms = Some(1000);
    c.policy.desired = 1;
    c.policy.min_nodes = 1;
    c.policy.max_nodes = 1;
    let c = allocate_start(&c, owner(), id(30), id(40), 10, id(50))
        .unwrap()
        .into_parts()
        .2;
    let s = store(Some(&c));
    let attached = check_attach(
        s.load_control(deadline()).await.unwrap(),
        local,
        "Fleet_1",
        Provisioning::Managed,
    )
    .unwrap();
    let (p, pending) = claim_start(&c, owner(), id(40), id(60), &attached, 10, id(70))
        .unwrap()
        .into_parts();
    let out = PreparedProvision::prepare(s.load_control(deadline()).await.unwrap(), p)
        .unwrap()
        .submit(&s, deadline())
        .await
        .unwrap();
    let (_, ctx) = pending.confirm(&out).unwrap().into_parts();
    let c = s.load_control(deadline()).await.unwrap();
    let p = terminal_candidate(c.root(), id(80));
    let out = PreparedProvision::prepare(c, p)
        .unwrap()
        .submit(&s, deadline())
        .await
        .unwrap();
    assert!(matches!(out, OwnershipOutcome::Acknowledged(_)));
    let done = s.load_control(deadline()).await.unwrap();
    let before = done.root().clone();
    let bytes =
        serde_json::to_vec(&json!({"version":1,"invocation_id":id(60),"result":"late/different"}))
            .unwrap();
    let reply = decode_reply(200, &bytes, &ctx).unwrap();
    assert!(complete_start(done.root(), owner(), &ctx, reply, 20, id(81)).is_err());
    assert_eq!(s.load_control(deadline()).await.unwrap().root(), &before);
    assert_eq!(managed_capacity(&before), Ok(0));
    assert_eq!(before.intents.len(), 1);
}
#[test]
fn known_lost_resource_without_stop_retains_start_delivery_and_terminalizes() {
    let mut c = ambiguous();
    c.nodes[0].state = NodeState::Lost;
    c.nodes[0].resource_id = Some("resource/one".into());
    c.intents[0].resource_id = Some("resource/one".into());
    c.intents[0].state = IntentState::Completed;
    c.intents[0].outcome = Some(IntentOutcome::ResourceKnown);
    let c = roundtrip(c);
    let next = terminal_candidate(&c, id(80)).into_parts().2;
    let mut expected = c.clone();
    expected.revision += 1;
    expected.change_id = id(80);
    expected.nodes[0].state = NodeState::Stopped;
    expected.intents[0].state = IntentState::Failed;
    expected.intents[0].outcome = Some(IntentOutcome::Terminated);
    assert_eq!(next, expected);
    assert_eq!(managed_capacity(&next), Ok(0));
    assert_eq!(next.capacity().unwrap().credits, 0);
}
#[test]
fn contradictory_retained_delivery_bindings_quarantine_before_terminal_mutation() {
    for mode in 0..4 {
        let mut c = registered_stop();
        match mode {
            0 => {
                c.intents[1].delivery.as_mut().unwrap().invocation_id =
                    c.intents[0].delivery.as_ref().unwrap().invocation_id
            }
            1 => c.intents[1].delivery.as_mut().unwrap().owner_id = id(999),
            2 => c.intents[1].delivery.as_mut().unwrap().epoch = owner().epoch + 1,
            _ => c.intents[1].attempts = 0,
        }
        let c = roundtrip(c);
        let next = terminal_candidate(&c, id(80)).into_parts().2;
        assert_eq!(next.nodes, c.nodes, "mode {mode}");
        assert_eq!(next.intents, c.intents, "mode {mode}");
        assert_eq!(next.scheduler.failures, c.scheduler.failures + 1);
    }
}
