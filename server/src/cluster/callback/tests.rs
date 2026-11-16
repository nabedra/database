use super::*;
#[test]
fn resource_requires_bounded_control_free_utf8() {
    assert!(resource("provider/one"));
    assert!(resource("é"));
    assert!(!resource(""));
    assert!(!resource("bad\u{85}"));
    assert!(!resource(&"é".repeat(129)));
}
use crate::cluster::attachment::{
    check_attach,
    tests::{Fixture, candidate, deadline, store},
};
use serde_json::{Value, json};
use std::fs;
fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn owner() -> Owner {
    Owner {
        process_id: id(10),
        epoch: 2,
    }
}
fn held(f: &Fixture) -> Control {
    let mut root = candidate(&f.ready(&f.input()), true);
    root.controller.owner_id = Some(owner().process_id);
    root.controller.epoch = owner().epoch;
    root.controller.lease_state = LeaseState::Held;
    root.controller.lease_until_ms = Some(1000);
    root
}
fn launched(root: Control) -> Control {
    let mut v = serde_json::to_value(&root).unwrap();
    v["nodes"] = json!([{"node_id":id(20),"launch_id":id(21),"resource_id":null,"ownership":"managed","state":"starting","incarnation":null,"retired_incarnations":[],"endpoint":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"registration":"unregistered","session":null,"drain":null}]);
    v["intents"] = json!([{"request_id":id(21),"node_id":id(20),"launch_id":id(21),"incarnation_id":null,"resource_id":null,"action":"start","state":"pending","attempts":0,"created_at_ms":0,"deadline_ms":300000,"retry_at_ms":0,"error_code":null,"template_fingerprint":root.config.template_fingerprint,"delivery":null,"outcome":null}]);
    Control::decode(&serde_json::to_vec(&v).unwrap()).unwrap()
}
fn response(result: Value) -> Vec<u8> {
    serde_json::to_vec(&json!({"version":1,"invocation_id":id(30),"result":result})).unwrap()
}
#[tokio::test]
async fn reconstructs_original_template_after_relocation_and_deletion() {
    use crate::cluster::startup_material::files::observe_reads;
    let a = Fixture::new();
    let b = Fixture::new();
    let mut root = launched(held(&a));
    if let Storage::Http(opts) = &mut root.template.storage {
        opts.token_file = "/foreign/original-token".into();
    }
    root.config.template_fingerprint = root.template.fingerprint().unwrap();
    root.intents[0].template_fingerprint = Some(root.config.template_fingerprint.clone());
    let s = store(Some(&root));
    let local = b.ready(&b.input());
    let attached = check_attach(
        s.load_control(deadline()).await.unwrap(),
        local,
        "Fleet_1",
        Provisioning::Managed,
    )
    .unwrap();
    for f in [&a, &b] {
        for n in ["auth", "token", "api.json"] {
            fs::remove_file(f.0.join(n)).unwrap();
        }
    }
    let (command, reads) =
        observe_reads(|| StartCommand::build(&root, owner(), id(21), id(30), &attached));
    let command = command.unwrap();
    assert!(reads.is_empty());
    assert!(validate_start_bytes(command.bytes(), &root, &attached).is_ok());
    // Literal independently specified bytes and hashlib SHA256, never printed.
    let literal = r#"{"version":1,"invocation_id":"00000000-0000-0000-0000-00000000001e","cluster_id":"12345678-1234-4234-8234-123456789abc","owner_id":"00000000-0000-0000-0000-00000000000a","epoch":2,"intent_id":"00000000-0000-0000-0000-000000000015","action":"start","target":{"node_id":"00000000-0000-0000-0000-000000000014","request_id":"00000000-0000-0000-0000-000000000015","incarnation_id":null},"payload":{"version":1,"cluster_id":"12345678-1234-4234-8234-123456789abc","cluster_name":"Fleet_1","request_id":"00000000-0000-0000-0000-000000000015","node_id":"00000000-0000-0000-0000-000000000014","bootstrap":{"version":1,"cluster_id":"12345678-1234-4234-8234-123456789abc","node_id":"00000000-0000-0000-0000-000000000014","request_id":"00000000-0000-0000-0000-000000000015","api_urls":["http://api.invalid:8000"],"listen":"worker:0","advertise_url":null,"storage":{"driver":"http","options":{"base_url":"http://store.invalid/objects/","ca_file":null,"namespace":"owned/","token_file":"/foreign/original-token"}},"enrollment_token":"__ENROLLMENT__","tls":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"}}}"#.replace("__ENROLLMENT__", &"E".repeat(43));
    assert!(command.bytes() == literal.as_bytes());
    assert_eq!(
        command.digest(),
        "573f6478393e2c166c27798ea73ce65802bc8b673416acdba2e11b9878053cf4"
    );
    let value: Value = serde_json::from_slice(command.bytes()).unwrap();
    assert!(value["payload"]["bootstrap"]["advertise_url"].is_null());
    assert_eq!(
        value["payload"]["bootstrap"]["storage"]["options"]["token_file"],
        "/foreign/original-token"
    );
    // Never assert/print command bytes or the synthetic secret.
    let spaced = serde_json::to_vec_pretty(&value).unwrap();
    assert_ne!(
        command.digest(),
        validate_start_bytes(&spaced, &root, &attached).unwrap()
    );
    for (ptr, replacement) in [
        ("/owner_id", json!(id(99))),
        (
            "/payload/bootstrap/enrollment_token",
            json!("synthetic-change"),
        ),
        ("/payload/bootstrap/queue_size", json!(65)),
        ("/target/node_id", json!(id(99))),
    ] {
        let mut bad = value.clone();
        *bad.pointer_mut(ptr).unwrap() = replacement;
        assert!(
            validate_start_bytes(&serde_json::to_vec(&bad).unwrap(), &root, &attached).is_err(),
            "{ptr}"
        );
    }
    let (_, ctx) = command.into_parts();
    assert!(
        matches!(decode_reply(200, &response(json!("provider/one")), &ctx).unwrap(), CallbackReply::Started(s) if s == "provider/one")
    );
    let mut wrong = response(json!("provider/one"));
    let mut v: Value = serde_json::from_slice(&wrong).unwrap();
    v["invocation_id"] = json!(id(31));
    wrong = serde_json::to_vec(&v).unwrap();
    assert!(decode_reply(200, &wrong, &ctx).is_err());
    assert!(decode_reply(200, &response(Value::Null), &ctx).is_err());
    let mut claimed = root.clone();
    claimed.intents[0].state = member::IntentState::InProgress;
    claimed.intents[0].attempts = 1;
    claimed.intents[0].delivery = Some(member::Delivery {
        invocation_id: id(30),
        owner_id: owner().process_id,
        epoch: owner().epoch,
        command_sha256: "0".repeat(64),
    });
    assert!(StartCommand::build(&claimed, owner(), id(21), id(31), &attached).is_err());
}
#[test]
fn inventory_literal_command_hash_and_stop_null_positive() {
    let f = Fixture::new();
    let root = held(&f);
    let command = InventoryCommand::build(&root, owner(), id(30)).unwrap();
    let literal = br#"{"version":1,"invocation_id":"00000000-0000-0000-0000-00000000001e","cluster_id":"12345678-1234-4234-8234-123456789abc","owner_id":"00000000-0000-0000-0000-00000000000a","epoch":2,"intent_id":null,"action":"inventory","target":null,"payload":{}}"#;
    assert_eq!(command.bytes(), literal);
    assert_eq!(
        command.digest(),
        "ab42317c16f3352d338a3164e6b19e14f6aa6b383a504f542fa5f67cff88e817"
    );
    let (_, ctx) = command.into_parts();
    match decode_reply(200, &response(json!([])), &ctx).unwrap() {
        CallbackReply::Inventory(e) => {
            assert!(e.records.is_empty());
            assert_eq!(e.owner, owner());
            assert_eq!(e.invocation_id, id(30));
            assert!(e.observation == ctx.observation);
        }
        _ => panic!("expected inventory"),
    }
    let (_, stop) = StopCommand::build(&stopping(root.clone()), owner(), id(22), id(30))
        .unwrap()
        .into_parts();
    assert!(matches!(
        decode_reply(200, &response(Value::Null), &stop),
        Ok(CallbackReply::Stopped)
    ));
    assert!(decode_reply(200, &response(json!("resource")), &stop).is_err());
}
#[test]
fn inventory_rejects_conflicts_but_retains_unknown_resources() {
    let f = Fixture::new();
    let root = held(&f);
    let (_, ctx) = InventoryCommand::build(&root, owner(), id(30))
        .unwrap()
        .into_parts();
    let record = json!({"resource_id":"unknown/one","cluster_id":root.cluster_id,"node_id":id(50),"request_id":id(51),"state":"running"});
    let baseline = response(json!([record]));
    assert!(
        matches!(decode_reply(200, &baseline, &ctx), Ok(CallbackReply::Inventory(e)) if e.records.len() == 1)
    );
    for kind in 0..4 {
        let mut second = record.clone();
        match kind {
            0 => second["node_id"] = json!(id(52)),
            1 => {
                second["resource_id"] = json!("other");
                second["request_id"] = json!(id(52));
            }
            2 => {
                second["resource_id"] = json!("other");
                second["node_id"] = json!(id(52));
            }
            _ => second["cluster_id"] = json!(id(99)),
        }
        assert!(decode_reply(200, &response(json!([record, second])), &ctx).is_err());
    }
    let mut terminated = record.clone();
    terminated["resource_id"] = json!("old");
    terminated["state"] = json!("terminated");
    assert!(decode_reply(200, &response(json!([record, terminated])), &ctx).is_ok());
    let records: Vec<_> = (0..4097).map(|n| json!({"resource_id":format!("r{n}"),"cluster_id":root.cluster_id,"node_id":id(100+n),"request_id":id(10000+n),"state":"running"})).collect();
    let complete = response(json!(&records[..4096]));
    assert!(
        matches!(decode_reply(200, &complete, &ctx), Ok(CallbackReply::Inventory(e)) if e.records.len() == 4096)
    );
    let body = response(json!(records));
    assert!(body.len() < MAX_BODY);
    assert!(matches!(
        decode_reply(200, &body, &ctx),
        Err(StorageError::LimitExceeded)
    ));
}
#[test]
fn replies_enforce_status_outcome_correlation_objects_and_limits() {
    let f = Fixture::new();
    let root = held(&f);
    let ctx = context(&root, owner(), id(30), Some(id(21)), ReplyAction::Start).unwrap();
    let failure = |inv, code, outcome| {
        serde_json::to_vec(
            &json!({"version":1,"invocation_id":inv,"error":{"code":code,"outcome":outcome}}),
        )
        .unwrap()
    };
    let valid = failure(json!(id(30)), "callback_capacity", "not_invoked");
    assert!(matches!(
        decode_reply(429, &valid, &ctx),
        Ok(CallbackReply::Failure {
            outcome: DeliveryOutcome::NotInvoked,
            ..
        })
    ));
    assert!(decode_reply(503, &valid, &ctx).is_err());
    assert!(
        decode_reply(
            502,
            &failure(json!(id(30)), "callback_failed", "not_invoked"),
            &ctx
        )
        .is_err()
    );
    assert!(matches!(
        decode_reply(
            502,
            &failure(json!(id(30)), "callback_failed", "unknown"),
            &ctx
        ),
        Ok(CallbackReply::Failure {
            outcome: DeliveryOutcome::Unknown,
            ..
        })
    ));
    assert!(matches!(
        decode_reply(
            400,
            &failure(Value::Null, "invalid_command", "not_invoked"),
            &ctx
        ),
        Ok(CallbackReply::Failure {
            outcome: DeliveryOutcome::Unknown,
            ..
        })
    ));
    for raw in [
        br#"{"version":1,"version":1,"invocation_id":"00000000-0000-0000-0000-00000000001e","result":"r"}"#.as_slice(),
        br#"{"version":1.0,"invocation_id":"00000000-0000-0000-0000-00000000001e","result":"r"}"#,
        br#"{"version":1,"invocation_id":"0000000000000000000000000000001e","result":"r"}"#,
        br#"[1,"00000000-0000-0000-0000-00000000001e","r"]"#,
    ] { assert!(decode_reply(200, raw, &ctx).is_err()); }
    assert!(matches!(
        decode_reply(200, &vec![b' '; MAX_BODY + 1], &ctx),
        Err(StorageError::LimitExceeded)
    ));
    let deep = format!("{}0{}", "[".repeat(33), "]".repeat(33));
    assert!(decode_reply(200, deep.as_bytes(), &ctx).is_err());
}

#[test]
fn commands_and_evidence_have_no_deserialization_or_clone_constructors() {
    trait AmbiguousIfDeserialize<A> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfDeserialize<()> for T {}
    impl<T: Deserialize<'static>> AmbiguousIfDeserialize<u8> for T {}
    let _ = <InventoryEvidence as AmbiguousIfDeserialize<_>>::marker;
    let _ = <InventoryRecord as AmbiguousIfDeserialize<_>>::marker;
    let _ = <ReplyContext as AmbiguousIfDeserialize<_>>::marker;
    let _ = <StartCommand as AmbiguousIfDeserialize<_>>::marker;
    let _ = <InventoryCommand as AmbiguousIfDeserialize<_>>::marker;
    trait AmbiguousIfClone<A> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfClone<()> for T {}
    impl<T: Clone> AmbiguousIfClone<u8> for T {}
    let _ = <StartCommand as AmbiguousIfClone<_>>::marker;
    let _ = <InventoryCommand as AmbiguousIfClone<_>>::marker;
    let _ = <ReplyContext as AmbiguousIfClone<_>>::marker;
    let _ = <InventoryEvidence as AmbiguousIfClone<_>>::marker;
}

#[tokio::test]
async fn start_context_associates_serialized_digest_and_target_across_claim_successor() {
    use crate::cluster::member::{Delivery, IntentState};
    let f = Fixture::new();
    let root = launched(held(&f));
    let s = store(Some(&root));
    let attached = check_attach(
        s.load_control(deadline()).await.unwrap(),
        f.ready(&f.input()),
        "Fleet_1",
        Provisioning::Managed,
    )
    .unwrap();
    let command = StartCommand::build(&root, owner(), id(21), id(30), &attached).unwrap();
    let hash = command.digest().to_owned();
    let (_, ctx) = command.into_parts();
    let mut claimed = root.clone();
    claimed.revision += 1;
    claimed.change_id = id(31);
    claimed.intents[0].state = IntentState::InProgress;
    claimed.intents[0].attempts = 1;
    claimed.intents[0].delivery = Some(Delivery {
        invocation_id: id(30),
        owner_id: owner().process_id,
        epoch: owner().epoch,
        command_sha256: hash,
    });
    assert!(ctx.observation != OwnershipObservation::new(&claimed).unwrap());
    assert_eq!(ctx.intent_id(), Some(id(21)));
    assert!(ctx.matches_delivery(&claimed, owner(), id(21)).is_ok());
    for mode in 0..5 {
        let mut other = claimed.clone();
        match mode {
            0 => other.intents[0].delivery.as_mut().unwrap().command_sha256 = "f".repeat(64),
            1 => other.intents[0].delivery.as_mut().unwrap().invocation_id = id(32),
            2 => {
                other.nodes[0].node_id = id(22);
                other.intents[0].node_id = id(22);
            }
            3 => other.intents[0].delivery.as_mut().unwrap().epoch += 1,
            _ => other.intents[0].delivery.as_mut().unwrap().owner_id = id(33),
        }
        other.validate().unwrap();
        assert!(ctx.matches_delivery(&other, owner(), id(21)).is_err());
    }
    let (_, inventory) = InventoryCommand::build(&claimed, owner(), id(35))
        .unwrap()
        .into_parts();
    assert!(
        inventory
            .matches_delivery(&claimed, owner(), id(21))
            .is_err()
    );
    assert_eq!(format!("{ctx:?}"), "ReplyContext { .. }");
}

fn stopping(root: Control) -> Control {
    let mut root = launched(root);
    root.nodes[0].resource_id = Some("provider/one".into());
    root.intents[0].resource_id = Some("provider/one".into());
    root.intents[0].state = member::IntentState::Completed;
    root.intents[0].outcome = Some(member::IntentOutcome::ResourceKnown);
    let mut stop = root.intents[0].clone();
    stop.request_id = id(22);
    stop.action = Action::Stop;
    stop.state = member::IntentState::Pending;
    stop.outcome = None;
    stop.template_fingerprint = None;
    root.intents.push(stop);
    Control::decode(&root.encode().unwrap()).unwrap()
}

#[test]
fn stop_exact_bytes_original_hash_semantics_and_strict_fields() {
    let f = Fixture::new();
    let root = stopping(held(&f));
    let (command, reads) = crate::cluster::startup_material::files::observe_reads(|| {
        StopCommand::build(&root, owner(), id(22), id(30))
    });
    let command = command.unwrap();
    assert!(reads.is_empty());
    // Independently written A04 envelope; digest computed with Python hashlib.
    let literal = br#"{"version":1,"invocation_id":"00000000-0000-0000-0000-00000000001e","cluster_id":"12345678-1234-4234-8234-123456789abc","owner_id":"00000000-0000-0000-0000-00000000000a","epoch":2,"intent_id":"00000000-0000-0000-0000-000000000016","action":"stop","target":{"node_id":"00000000-0000-0000-0000-000000000014","request_id":"00000000-0000-0000-0000-000000000015","incarnation_id":null},"payload":{"resource_id":"provider/one"}}"#;
    assert_eq!(command.bytes(), literal);
    assert_eq!(
        command.digest(),
        "398c6ff510c49b18da83f72430c18af24a71997103dd42ccdc6fd312b0f8116a"
    );
    assert_eq!(
        validate_stop_bytes(literal, &root).unwrap(),
        command.digest()
    );
    let value: Value = serde_json::from_slice(literal).unwrap();
    let spaced = serde_json::to_vec_pretty(&value).unwrap();
    assert_ne!(
        validate_stop_bytes(&spaced, &root).unwrap(),
        command.digest()
    );
    for (ptr, replacement) in [
        ("/target/node_id", json!(id(99))),
        ("/target/request_id", json!(id(99))),
        ("/target/incarnation_id", json!(id(99))),
        ("/payload/resource_id", json!("other")),
        ("/owner_id", json!(id(99))),
        ("/action", json!("start")),
        ("/intent_id", json!(id(21))),
    ] {
        let mut bad = value.clone();
        *bad.pointer_mut(ptr).unwrap() = replacement;
        assert!(
            validate_stop_bytes(&serde_json::to_vec(&bad).unwrap(), &root).is_err(),
            "{ptr}"
        );
    }
    let text = std::str::from_utf8(literal).unwrap();
    for (needle, replacement) in [
        ("\"version\":1", "\"version\":1,\"version\":1"),
        ("\"target\":{", "\"target\":{\"extra\":0,"),
        ("\"node_id\":", "\"extra\":0,\"node_id\":"),
        (
            "\"resource_id\":\"provider/one\"",
            "\"resource_id\":\"provider/one\",\"resource_id\":\"provider/one\"",
        ),
        ("\"resource_id\":", "\"extra\":0,\"resource_id\":"),
    ] {
        let bad = text.replacen(needle, replacement, 1);
        assert!(validate_stop_bytes(bad.as_bytes(), &root).is_err());
    }
    assert_eq!(format!("{command:?}"), "StopCommand { .. }");
    let (_, ctx) = command.into_parts();
    assert!(matches!(
        decode_reply(200, &response(Value::Null), &ctx),
        Ok(CallbackReply::Stopped)
    ));
    assert!(decode_reply(200, &response(json!("provider/one")), &ctx).is_err());
}

#[test]
fn stop_context_matches_exact_node_intent_and_delivery_across_successor() {
    let f = Fixture::new();
    let root = stopping(held(&f));
    let command = StopCommand::build(&root, owner(), id(22), id(30)).unwrap();
    let hash = command.digest().to_owned();
    let (_, ctx) = command.into_parts();
    let mut claimed = root.clone();
    claimed.revision += 1;
    claimed.change_id = id(31);
    claimed.intents[1].state = member::IntentState::InProgress;
    claimed.intents[1].attempts = 1;
    claimed.intents[1].delivery = Some(member::Delivery {
        invocation_id: id(30),
        owner_id: owner().process_id,
        epoch: owner().epoch,
        command_sha256: hash,
    });
    assert!(ctx.observation != OwnershipObservation::new(&claimed).unwrap());
    assert!(ctx.matches_delivery(&claimed, owner(), id(22)).is_ok());
    for mode in 0..8 {
        let mut other = claimed.clone();
        match mode {
            0 => {
                other.nodes[0].resource_id = Some("other".into());
                for i in &mut other.intents {
                    i.resource_id = Some("other".into());
                }
            }
            1 => {
                other.nodes[0].registration = member::Registration::Registered;
                other.nodes[0].incarnation = Some(id(50));
                other.nodes[0].endpoint = Some("http://worker:8000/".into());
                other.nodes[0].session = Some(member::Session {
                    id: id(51),
                    verifier: "a".repeat(64),
                });
                other.intents[1].incarnation_id = Some(id(50));
            }
            2 => other.intents[1].delivery.as_mut().unwrap().command_sha256 = "f".repeat(64),
            3 => other.intents[1].delivery.as_mut().unwrap().invocation_id = id(32),
            4 => {
                other.nodes[0].node_id = id(23);
                for i in &mut other.intents {
                    i.node_id = id(23);
                }
            }
            5 => {
                other.nodes[0].launch_id = Some(id(19));
                other.intents[0].request_id = id(19);
                for i in &mut other.intents {
                    i.launch_id = id(19);
                }
            }
            6 => other.intents[1].delivery.as_mut().unwrap().owner_id = id(33),
            _ => other.intents[1].delivery.as_mut().unwrap().epoch += 1,
        }
        other
            .validate()
            .unwrap_or_else(|e| panic!("mode {mode}: {e:?}"));
        if mode == 1 {
            let registered = StopCommand::build(&other, owner(), id(22), id(30)).unwrap();
            assert!(validate_stop_bytes(registered.bytes(), &other).is_ok());
            let registered_hash = registered.digest().to_owned();
            let (_, registered_ctx) = registered.into_parts();
            // Original digest does not belong to this new incarnation.
            assert!(
                registered_ctx
                    .matches_delivery(&other, owner(), id(22))
                    .is_err()
            );
            let mut exact = other.clone();
            exact.intents[1].delivery.as_mut().unwrap().command_sha256 = registered_hash;
            assert!(
                registered_ctx
                    .matches_delivery(&exact, owner(), id(22))
                    .is_ok()
            );
        }
        assert!(
            ctx.matches_delivery(&other, owner(), id(22)).is_err(),
            "mode {mode}"
        );
    }
    assert!(ctx.matches_delivery(&claimed, owner(), id(21)).is_err());
    assert!(
        ctx.matches_delivery(
            &claimed,
            Owner {
                process_id: id(99),
                epoch: 2
            },
            id(22)
        )
        .is_err()
    );
    let (_, inventory) = InventoryCommand::build(&claimed, owner(), id(35))
        .unwrap()
        .into_parts();
    assert!(
        inventory
            .matches_delivery(&claimed, owner(), id(22))
            .is_err()
    );
    assert_eq!(format!("{ctx:?}"), "ReplyContext { .. }");
}

#[test]
fn delivery_records_have_no_serialization_constructor() {
    trait AmbiguousIfSerialize<A> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfSerialize<()> for T {}
    impl<T: Serialize> AmbiguousIfSerialize<u8> for T {}
    trait AmbiguousIfDeserialize<A> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfDeserialize<()> for T {}
    impl<T: Deserialize<'static>> AmbiguousIfDeserialize<u8> for T {}
    trait AmbiguousIfClone<A> {
        fn marker() {}
    }
    impl<T: ?Sized> AmbiguousIfClone<()> for T {}
    impl<T: Clone> AmbiguousIfClone<u8> for T {}
    let _ = <StopCommand as AmbiguousIfClone<_>>::marker;
    let _ = <StopCommand as AmbiguousIfDeserialize<_>>::marker;
    let _ = <StopCommand as AmbiguousIfSerialize<_>>::marker;
    let _ = <StartCommand as AmbiguousIfSerialize<_>>::marker;
    let _ = <ReplyContext as AmbiguousIfSerialize<_>>::marker;
}

#[tokio::test]
async fn a02_fresh_authorization_requires_exact_retained_claim_and_owner() {
    let f = Fixture::new();
    let root = launched(held(&f));
    let s = store(Some(&root));
    let attached = check_attach(
        s.load_control(deadline()).await.unwrap(),
        f.ready(&f.input()),
        "Fleet_1",
        Provisioning::Managed,
    )
    .unwrap();
    let command = StartCommand::build(&root, owner(), id(21), id(30), &attached).unwrap();
    assert!(authorize_command(command.bytes(), &root, &attached).is_err());
    let mut claimed = root.clone();
    claimed.intents[0].state = member::IntentState::InProgress;
    claimed.intents[0].attempts = 1;
    claimed.intents[0].delivery = Some(member::Delivery {
        invocation_id: id(30),
        owner_id: owner().process_id,
        epoch: owner().epoch,
        command_sha256: command.digest().into(),
    });
    assert!(authorize_command(command.bytes(), &claimed, &attached).is_ok());
    let mut superseded = claimed.clone();
    superseded.controller.epoch += 1;
    assert!(authorize_command(command.bytes(), &superseded, &attached).is_err());
    // Even equivalent JSON must retain the exact bytes committed in the claim.
    let value: Value = serde_json::from_slice(command.bytes()).unwrap();
    assert!(
        authorize_command(
            &serde_json::to_vec_pretty(&value).unwrap(),
            &claimed,
            &attached
        )
        .is_err()
    );
    claimed.intents[0].state = member::IntentState::Reconciling;
    assert!(authorize_command(command.bytes(), &claimed, &attached).is_err());
}
