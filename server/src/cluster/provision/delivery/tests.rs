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
#[tokio::test]
async fn pending_start_can_be_claimed() {
    let (_f, root, attached) = setup().await;
    assert!(claim_start(&root, owner(), id(40), id(60), &attached, 10, id(70)).is_ok());
}
use crate::cluster::{
    callback::{decode_reply, validate_start_bytes},
    member::{Drain, Session},
    store::{OwnershipOutcome, PreparedProvision},
};
use serde_json::json;
async fn claimed(root: &Control, attached: &AttachedConfig) -> (Control, ReplyContext, Vec<u8>) {
    let (candidate, pending) = claim_start(root, owner(), id(40), id(60), attached, 10, id(70))
        .unwrap()
        .into_parts();
    let expected = candidate.result.clone();
    let s = store(Some(root));
    let outcome = PreparedProvision::prepare(s.load_control(deadline()).await.unwrap(), candidate)
        .unwrap()
        .submit(&s, deadline())
        .await
        .unwrap();
    let (bytes, ctx) = pending.confirm(&outcome).unwrap().into_parts();
    (expected, ctx, bytes)
}
fn success(ctx: &ReplyContext) -> CallbackReply {
    decode_reply(
        200,
        &serde_json::to_vec(&json!({"version":1,"invocation_id":id(60),"result":"resource/one"}))
            .unwrap(),
        ctx,
    )
    .unwrap()
}
fn denial(ctx: &ReplyContext, code: &str, status: u16, outcome: &str) -> CallbackReply {
    decode_reply(
        status,
        &serde_json::to_vec(
            &json!({"version":1,"invocation_id":id(60),"error":{"code":code,"outcome":outcome}}),
        )
        .unwrap(),
        ctx,
    )
    .unwrap()
}
#[tokio::test]
async fn exact_claim_hash_confirmation_and_resource_completion() {
    let (_f, root, attached) = setup().await;
    let saved = root.clone();
    let original = StartCommand::build(&root, owner(), id(40), id(60), &attached).unwrap();
    let (next, ctx, bytes) = claimed(&root, &attached).await;
    assert!(bytes == original.bytes());
    assert_eq!(
        validate_start_bytes(&bytes, &next, &attached).unwrap(),
        next.intents[0].delivery.as_ref().unwrap().command_sha256
    );
    assert_eq!(ctx.intent_id(), Some(id(40)));
    assert!(ctx.matches_delivery(&next, owner(), id(40)).is_ok());
    assert_eq!(next.revision, root.revision + 1);
    let mut expected = root.clone();
    expected.revision += 1;
    expected.change_id = id(70);
    expected.intents[0].state = IntentState::InProgress;
    expected.intents[0].attempts = 1;
    expected.intents[0].delivery = next.intents[0].delivery.clone();
    assert_eq!(next, expected);
    let done = complete_start(&next, owner(), &ctx, success(&ctx), 20, id(80))
        .unwrap()
        .result;
    expected = next.clone();
    expected.revision += 1;
    expected.change_id = id(80);
    expected.nodes[0].resource_id = Some("resource/one".into());
    expected.intents[0].resource_id = Some("resource/one".into());
    expected.intents[0].state = IntentState::Completed;
    expected.intents[0].outcome = Some(IntentOutcome::ResourceKnown);
    assert_eq!(done, expected);
    assert_eq!(done.nodes[0].state, NodeState::Starting);
    assert_eq!(managed_capacity(&done).unwrap(), 1);
    assert_eq!(root, saved);
}
#[tokio::test]
async fn confirmation_requires_entire_root_and_rejects_unknown_conflict() {
    let (_f, root, attached) = setup().await;
    for mode in 0..6 {
        let (c, pending) = claim_start(&root, owner(), id(40), id(60), &attached, 10, id(70))
            .unwrap()
            .into_parts();
        let mut expected = c.result;
        if mode == 2 {
            expected.scheduler.failures += 1;
        } // Same UUID and revision.
        let s = store(Some(&expected));
        let loaded = s.load_control(deadline()).await.unwrap();
        let outcome = match mode {
            0 | 2 => OwnershipOutcome::Acknowledged(loaded),
            1 => OwnershipOutcome::ObservedExact(loaded),
            3 => OwnershipOutcome::Unknown {
                current: Some(loaded),
            },
            4 => OwnershipOutcome::Conflict {
                current: Some(loaded),
            },
            _ => OwnershipOutcome::Unknown { current: None },
        };
        assert_eq!(pending.confirm(&outcome).is_some(), mode < 2);
    }
}
#[tokio::test]
async fn claim_rejects_independent_ineligible_valid_roots() {
    let (_f, root, attached) = setup().await;
    for mode in 0..16 {
        let mut c = root.clone();
        let mut o = owner();
        let mut invocation = id(60);
        let mut proposal = id(70);
        match mode {
            0 => o.process_id = id(91),
            1 => o.epoch += 1,
            2 => c.intents[0].retry_at_ms = 11,
            3 => c.scheduler.backoff_until_ms = 11,
            4 => {
                c.nodes[0].state = NodeState::Draining;
                c.nodes[0].drain = Some(Drain {
                    id: id(100),
                    requested_at_ms: 0,
                    ack_seq: None,
                    confirmed_idle: false,
                });
            }
            5 => c.nodes[0].state = NodeState::Stopped,
            6 => {
                c.nodes[0].resource_id = Some("known".into());
                c.intents[0].resource_id = Some("known".into());
            }
            7 => c.intents[0].state = IntentState::Reconciling,
            8 => {
                c.intents[0].state = IntentState::Failed;
                c.intents[0].outcome = Some(IntentOutcome::NotInvoked);
            }
            9 => {
                c.intents[0].state = IntentState::Completed;
                c.intents[0].outcome = Some(IntentOutcome::ResourceKnown);
                c.intents[0].resource_id = Some("known".into());
                c.nodes[0].resource_id = Some("known".into());
            }
            10 => invocation = id(30),
            11 => proposal = root.change_id,
            12 => proposal = invocation,
            13 => c.intents[0].attempts = 1,
            14 => c.intents[0].error_code = Some("prior_uncertain".into()),
            _ => {
                c.intents[0].delivery = Some(crate::cluster::member::Delivery {
                    invocation_id: id(61),
                    owner_id: owner().process_id,
                    epoch: owner().epoch,
                    command_sha256: "a".repeat(64),
                });
            }
        }
        c.validate().unwrap();
        let saved = c.clone();
        assert!(
            claim_start(&c, o, id(40), invocation, &attached, 10, proposal).is_err(),
            "mode {mode}"
        );
        assert_eq!(c, saved);
    }
}
#[tokio::test]
async fn exact_delivery_rejects_owner_epoch_invocation_digest_launch_phase_and_resource() {
    let (_f, root, attached) = setup().await;
    let (c, ctx, _) = claimed(&root, &attached).await;
    for mode in 0..9 {
        let mut changed = c.clone();
        let mut o = owner();
        match mode {
            0 => o.process_id = id(91),
            1 => o.epoch += 1,
            2 => changed.intents[0].delivery.as_mut().unwrap().invocation_id = id(61),
            3 => changed.intents[0].delivery.as_mut().unwrap().command_sha256 = "b".repeat(64),
            4 => {
                changed.intents[0].request_id = id(41);
                changed.intents[0].launch_id = id(41);
                changed.nodes[0].launch_id = Some(id(41));
            }
            5 => changed.intents[0].state = IntentState::Reconciling,
            6 => changed.nodes[0].state = NodeState::Stopped,
            7 => {
                changed.nodes[0].resource_id = Some("different".into());
                changed.intents[0].resource_id = Some("different".into());
            }
            _ => {
                changed.controller.owner_id = Some(id(91));
                changed.controller.epoch += 1;
                o = Owner {
                    process_id: id(91),
                    epoch: 8,
                };
            }
        }
        changed.validate().unwrap();
        assert!(
            complete_start(&changed, o, &ctx, success(&ctx), 20, id(80)).is_err(),
            "mode {mode}"
        );
    }
    assert!(complete_start(&c, owner(), &ctx, success(&ctx), 20, id(80)).is_ok());
}
fn registered_draining(c: &mut Control) {
    let n = &mut c.nodes[0];
    n.state = NodeState::Draining;
    n.registration = Registration::Registered;
    n.incarnation = Some(id(101));
    n.endpoint = Some("http://worker:8000/".into());
    n.session = Some(Session {
        id: id(102),
        verifier: "a".repeat(64),
    });
    n.drain = Some(Drain {
        id: id(103),
        requested_at_ms: 11,
        ack_seq: Some(9),
        confirmed_idle: true,
    });
}
#[tokio::test]
async fn late_registered_draining_success_preserves_bindings_and_failure_history() {
    let (_f, root, attached) = setup().await;
    let (mut c, ctx, _) = claimed(&root, &attached).await;
    registered_draining(&mut c);
    c.scheduler.failures = 8;
    c.scheduler.backoff_until_ms = 999;
    c.scheduler.cooldown_until_ms = 777;
    c.scheduler.low_since_ms = Some(6);
    c.intents[0].retry_at_ms = 888;
    c.validate().unwrap();
    let done = complete_start(&c, owner(), &ctx, success(&ctx), 20, id(80))
        .unwrap()
        .result;
    let mut expected = c.clone();
    expected.revision += 1;
    expected.change_id = id(80);
    expected.nodes[0].resource_id = Some("resource/one".into());
    expected.intents[0].resource_id = Some("resource/one".into());
    expected.intents[0].state = IntentState::Completed;
    expected.intents[0].outcome = Some(IntentOutcome::ResourceKnown);
    assert_eq!(done, expected);
}
#[tokio::test]
async fn definite_first_denial_vs_registration_resource_later_attempt_and_prior_ambiguity() {
    let (_f, root, attached) = setup().await;
    let (c, ctx, _) = claimed(&root, &attached).await;
    for mode in 0..8 {
        let mut changed = c.clone();
        match mode {
            1 => registered_draining(&mut changed),
            2 => {
                changed.nodes[0].resource_id = Some("known".into());
                changed.intents[0].resource_id = Some("known".into());
            }
            3 => changed.intents[0].attempts = 2,
            4 => changed.intents[0].error_code = Some("callback_uncertain".into()),
            _ => (),
        }
        changed.validate().unwrap();
        let reply = match mode {
            5 => denial(&ctx, "identity_conflict", 409, "not_invoked"),
            6 => denial(&ctx, "callback_in_progress", 409, "unknown"),
            7 => denial(&ctx, "callback_timeout", 504, "unknown"),
            _ => denial(&ctx, "callback_capacity", 429, "not_invoked"),
        };
        let next = complete_start(&changed, owner(), &ctx, reply, 20, id(80))
            .unwrap()
            .result;
        assert_eq!(
            next.intents[0].state,
            if mode == 0 {
                IntentState::Failed
            } else {
                IntentState::Reconciling
            }
        );
        assert_eq!(
            next.intents[0].outcome,
            if mode == 0 {
                Some(IntentOutcome::NotInvoked)
            } else {
                None
            }
        );
        assert_eq!(
            managed_capacity(&next).unwrap(),
            if mode == 0 { 0 } else { 1 }
        );
        assert_eq!(next.intents[0].delivery, changed.intents[0].delivery);
        assert_eq!(next.intents[0].attempts, changed.intents[0].attempts);
        assert_eq!(next.scheduler.failures, 1);
    }
    let uncertain = uncertain_start(&c, owner(), &ctx, 20, id(80))
        .unwrap()
        .result;
    assert_eq!(uncertain.intents[0].state, IntentState::Reconciling);
    assert!(
        complete_start(
            &uncertain,
            owner(),
            &ctx,
            denial(&ctx, "callback_capacity", 429, "not_invoked"),
            21,
            id(81)
        )
        .is_err()
    );
}
#[tokio::test]
async fn checked_counters_timestamps_and_bounded_delays_preserve_base() {
    let (_f, root, attached) = setup().await;
    let (c, ctx, _) = claimed(&root, &attached).await;
    for mode in 0..5 {
        let mut changed = c.clone();
        let mut now = 20;
        match mode {
            0 => changed.scheduler.failures = u64::MAX,
            1 => now = u64::MAX,
            2 => changed.revision = u64::MAX,
            3 => {
                changed.scheduler.failures = u64::MAX - 1;
                changed.intents[0].attempts = u64::MAX;
            }
            _ => {
                changed.intents[0].retry_at_ms = 999999;
                changed.scheduler.backoff_until_ms = 999998;
            }
        }
        changed.validate().unwrap();
        let saved = changed.clone();
        let result = uncertain_start(&changed, owner(), &ctx, now, id(80));
        if mode < 3 {
            assert!(matches!(result, Err(StorageError::LimitExceeded)));
        } else {
            let next = result.unwrap().result;
            assert_eq!(next.scheduler.failures, changed.scheduler.failures + 1);
            assert_eq!(
                next.intents[0].retry_at_ms,
                if mode == 4 {
                    999999
                } else {
                    20 + c.timing.retry_max_ms
                }
            );
            assert_eq!(
                next.scheduler.backoff_until_ms,
                if mode == 4 {
                    999998
                } else {
                    20 + c.timing.retry_max_ms
                }
            );
        }
        assert_eq!(changed, saved);
    }
    let mut exhausted = root.clone();
    exhausted.intents[0].attempts = u64::MAX;
    assert!(matches!(
        claim_start(&exhausted, owner(), id(40), id(60), &attached, 10, id(70)),
        Err(StorageError::LimitExceeded)
    ));
    let mut exhausted = root.clone();
    exhausted.revision = u64::MAX;
    assert!(matches!(
        claim_start(&exhausted, owner(), id(40), id(60), &attached, 10, id(70)),
        Err(StorageError::LimitExceeded)
    ));
    for (initial, cap, k, expected) in [
        (1000, 30000, 1, 1000),
        (1000, 30000, 5, 16000),
        (1000, 30000, 6, 30000),
        (1, u64::MAX, u64::MAX, u64::MAX),
        (u64::MAX, u64::MAX, 2, u64::MAX),
    ] {
        assert_eq!(delay(initial, cap, k), expected);
    }
}

#[tokio::test]
async fn resource_collision_terminal_contradiction_and_invalid_evidence_refuse_mutation() {
    let (_f, root, attached) = setup().await;
    let (mut c, ctx, _) = claimed(&root, &attached).await;
    let mut other = c.nodes[0].clone();
    other.node_id = id(31);
    other.launch_id = Some(id(41));
    other.resource_id = Some("resource/one".into());
    let mut i = c.intents[0].clone();
    i.request_id = id(41);
    i.launch_id = id(41);
    i.node_id = id(31);
    i.resource_id = Some("resource/one".into());
    i.state = IntentState::Completed;
    i.outcome = Some(IntentOutcome::ResourceKnown);
    i.delivery = None;
    c.nodes.push(other);
    c.intents.push(i);
    c.validate().unwrap();
    let saved = c.clone();
    assert!(matches!(
        complete_start(&c, owner(), &ctx, success(&ctx), 20, id(80)),
        Err(StorageError::Conflict)
    ));
    assert_eq!(c, saved);
    let mut terminal = saved.clone();
    let mut stop = terminal.intents[0].clone();
    stop.action = Action::Stop;
    stop.request_id = id(42);
    stop.template_fingerprint = None;
    stop.resource_id = Some("resource/one".into());
    stop.state = IntentState::Completed;
    stop.outcome = Some(IntentOutcome::Terminated);
    terminal.nodes[0].resource_id = Some("resource/one".into());
    terminal.intents[0].resource_id = Some("resource/one".into());
    // Give the other launch a distinct resource to isolate the terminal contradiction.
    terminal.nodes[1].resource_id = Some("other".into());
    terminal.intents[1].resource_id = Some("other".into());
    terminal.intents.push(stop);
    terminal.validate().unwrap();
    assert!(complete_start(&terminal, owner(), &ctx, success(&ctx), 20, id(80)).is_err());
    for reply in [
        CallbackReply::Started("".into()),
        CallbackReply::Failure {
            code: "provider_secret_diagnostic".into(),
            outcome: DeliveryOutcome::NotInvoked,
        },
        CallbackReply::Failure {
            code: "callback_failed".into(),
            outcome: DeliveryOutcome::NotInvoked,
        },
    ] {
        assert!(complete_start(&saved, owner(), &ctx, reply, 20, id(80)).is_err());
    }
}
#[tokio::test]
async fn retained_claim_and_recovery_at_full_augmented_admission_preserve_credits() {
    let (_f, mut c, attached) = setup().await;
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
        c.nodes.push(n);
        c.intents.push(i);
    }
    c.validate().unwrap();
    assert!(c.check_new_admission().is_err());
    let credits = c.capacity().unwrap().credits;
    let (next, ctx, _) = claimed(&c, &attached).await;
    let uncertain = uncertain_start(&next, owner(), &ctx, 20, id(80))
        .unwrap()
        .result;
    assert_eq!(uncertain.capacity().unwrap().credits, credits);
    assert!(uncertain.check_new_admission().is_err());
    let done = complete_start(&next, owner(), &ctx, success(&ctx), 20, id(81))
        .unwrap()
        .result;
    assert_eq!(done.capacity().unwrap().credits, credits);
    assert_eq!(managed_capacity(&done).unwrap(), 1366);
}

// The wrapper gates one actual Fake Match; it does not implement storage semantics.
use crate::{
    storage::{Download, Namespace, ObjectStore, Storage, WriteCondition},
    test_storage::{Fake, ReadGate},
};
use std::sync::{Arc, Mutex};
use tokio::time::Duration;
const KEY: &str = "test/cluster/control.json";
#[derive(Default)]
struct Gated {
    fake: Fake,
    gate: Mutex<Option<Arc<ReadGate>>>,
    tokens: Mutex<Vec<String>>,
}
impl ObjectStore for Gated {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        self.fake.get(key).await
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        let WriteCondition::Match(token) = condition else {
            return Err(StorageError::Conflict);
        };
        self.tokens.lock().unwrap().push(token.into());
        let gate = self.gate.lock().unwrap().take();
        if let Some(g) = gate {
            g.entered.add_permits(1);
            tokio::time::timeout(Duration::from_secs(5), g.release.acquire())
                .await
                .map_err(|_| StorageError::Unavailable)?
                .map_err(|_| StorageError::Unavailable)?
                .forget();
        }
        self.fake.put(key, bytes, condition).await
    }
}
#[tokio::test]
async fn prepared_stale_completion_loses_to_takeover_with_exact_token_and_counts() {
    use crate::cluster::controller::{OwnershipChange, ownership_candidate};
    let (_f, root, attached) = setup().await;
    let (c, ctx, _) = claimed(&root, &attached).await;
    let b = Gated::default();
    b.fake
        .state
        .lock()
        .unwrap()
        .objects
        .insert(KEY.into(), (c.encode().unwrap(), "completion-base".into()));
    let s = Arc::new(Storage::new(b, Namespace::new("test").unwrap()));
    let base = s.load_control(deadline()).await.unwrap();
    let p = PreparedProvision::prepare(
        base,
        complete_start(&c, owner(), &ctx, success(&ctx), 20, id(80)).unwrap(),
    )
    .unwrap();
    let takeover = ownership_candidate(
        &c,
        OwnershipChange::Acquire { process_id: id(91) },
        1001,
        id(81),
    )
    .unwrap();
    let g = Arc::new(ReadGate::default());
    *s.backend().gate.lock().unwrap() = Some(g.clone());
    let task = {
        let s = s.clone();
        tokio::spawn(async move { p.submit(&s, deadline()).await })
    };
    let reached = match tokio::time::timeout(Duration::from_secs(5), g.entered.acquire()).await {
        Ok(Ok(p)) => {
            p.forget();
            true
        }
        _ => false,
    };
    let winner = if reached {
        Some(
            s.backend()
                .fake
                .put(
                    KEY,
                    takeover.encode().unwrap(),
                    WriteCondition::Match("completion-base"),
                )
                .await,
        )
    } else {
        None
    };
    g.release.add_permits(1);
    let mut task = task;
    let result = match tokio::time::timeout(Duration::from_secs(5), &mut task).await {
        Ok(r) => Some(r),
        Err(_) => {
            task.abort();
            let _ = task.await;
            None
        }
    };
    drop(_f);
    // Gates are released, scratch removed and the owned task joined before assertions.
    assert!(reached);
    assert!(matches!(winner, Some(Ok(_))));
    let OwnershipOutcome::Conflict {
        current: Some(current),
    } = result.unwrap().unwrap().unwrap()
    else {
        panic!("stale completion won")
    };
    assert_eq!(current.root(), &takeover);
    assert_eq!(*s.backend().tokens.lock().unwrap(), vec!["completion-base"]);
    let f = s.backend().fake.state.lock().unwrap();
    assert_eq!((f.puts, f.gets), (2, 2));
    assert_eq!(Control::decode(&f.objects[KEY].0).unwrap(), takeover);
    assert_ne!(f.objects[KEY].1, "completion-base");
}

#[tokio::test]
async fn retired_boot_prevents_false_no_effect_terminalization() {
    let (_f, root, attached) = setup().await;
    let (mut c, ctx, _) = claimed(&root, &attached).await;
    c.nodes[0].retired_incarnations.push(id(104));
    c.validate().unwrap();
    let next = complete_start(
        &c,
        owner(),
        &ctx,
        denial(&ctx, "callback_capacity", 429, "not_invoked"),
        20,
        id(80),
    )
    .unwrap()
    .result;
    assert_eq!(next.intents[0].state, IntentState::Reconciling);
    assert_eq!(managed_capacity(&next).unwrap(), 1);
}

#[tokio::test]
async fn uncertainty_sanitizes_uncorrelated_evidence_and_checks_global_deadline() {
    let (_f, root, attached) = setup().await;
    let (mut c, ctx, _) = claimed(&root, &attached).await;
    c.scheduler.cooldown_until_ms = 900;
    c.scheduler.low_since_ms = Some(5);
    let uncorrelated = decode_reply(401, br#"{"version":1,"invocation_id":null,"error":{"code":"unauthorized","outcome":"not_invoked"}}"#, &ctx).unwrap();
    let next = complete_start(&c, owner(), &ctx, uncorrelated, 20, id(80))
        .unwrap()
        .result;
    let mut expected = c.clone();
    expected.revision += 1;
    expected.change_id = id(80);
    expected.intents[0].state = IntentState::Reconciling;
    expected.intents[0].error_code = Some("callback_uncertain".into());
    expected.intents[0].retry_at_ms = 20 + c.timing.retry_initial_ms;
    expected.scheduler.failures = 1;
    expected.scheduler.backoff_until_ms = 20 + c.timing.retry_initial_ms;
    assert_eq!(next, expected);
    c.scheduler.failures = 8;
    let saved = c.clone();
    assert!(matches!(
        uncertain_start(
            &c,
            owner(),
            &ctx,
            u64::MAX - c.timing.retry_initial_ms,
            id(81)
        ),
        Err(StorageError::LimitExceeded)
    ));
    assert_eq!(c, saved);
}

#[tokio::test]
async fn shared_claim_construction_keeps_initial_and_reconciled_eligibility_distinct() {
    let (_f, root, attached) = setup().await;
    assert!(claim_reconciled_start(&root, owner(), id(40), id(60), &attached, 10, id(70)).is_err());
    let (claimed, ctx, _) = claimed(&root, &attached).await;
    let uncertain = uncertain_start(&claimed, owner(), &ctx, 20, id(80))
        .unwrap()
        .result;
    assert!(
        claim_start(
            &uncertain,
            owner(),
            id(40),
            id(61),
            &attached,
            999999,
            id(81)
        )
        .is_err()
    );
    let (candidate, _) = claim_reconciled_start(
        &uncertain,
        owner(),
        id(40),
        id(61),
        &attached,
        999999,
        id(81),
    )
    .unwrap()
    .into_parts();
    let next = candidate.result;
    assert_eq!(next.intents[0].attempts, 2);
    assert_eq!(next.intents[0].error_code, uncertain.intents[0].error_code);
    assert_eq!(
        next.intents[0].retry_at_ms,
        uncertain.intents[0].retry_at_ms
    );
    assert_eq!(next.scheduler, uncertain.scheduler);
}

#[test]
fn common_delivery_owners_are_not_clone_or_serde() {
    trait AmbiguousIfClone<A> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfClone<()> for T {}
    impl<T: Clone> AmbiguousIfClone<u8> for T {}
    trait AmbiguousIfDeserialize<A> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfDeserialize<()> for T {}
    impl<T: serde::Deserialize<'static>> AmbiguousIfDeserialize<u8> for T {}
    trait AmbiguousIfSerialize<A> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfSerialize<()> for T {}
    impl<T: serde::Serialize> AmbiguousIfSerialize<u8> for T {}
    macro_rules! opaque {
        ($($t:ty),*) => { $(
            let _ = <$t as AmbiguousIfClone<_>>::marker;
            let _ = <$t as AmbiguousIfDeserialize<_>>::marker;
            let _ = <$t as AmbiguousIfSerialize<_>>::marker;
        )* };
    }
    opaque!(
        ClaimCommand,
        PreparedClaim,
        PendingClaim,
        ConfirmedClaim,
        StartClaim,
        PendingStart,
        ConfirmedStart
    );
}
