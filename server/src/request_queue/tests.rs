use super::*;

mod scheduler_properties;

fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn limits() -> QueueLimits {
    QueueLimits {
        max_pending_requests: 1000,
        max_queue_bytes_per_replica: 8 * 1024 * 1024,
        max_retained_result_bytes_per_worker: 1024 * 1024,
        result_retention_ms: 100,
    }
}
fn queue(members: &[u128]) -> RequestQueue {
    RequestQueue::new(
        id(1),
        1,
        1,
        limits(),
        members.iter().map(|n| id(*n)).collect(),
    )
    .unwrap()
}
fn body(n: u128) -> Vec<u8> {
    format!(
        r#"{{"request_id":"{}","operation":"table.list","payload":{{}}}}"#,
        id(n)
    )
    .into_bytes()
}
fn replicate(q: &mut RequestQueue, request: Uuid) {
    for member in q.members.clone() {
        q.acknowledge(request, member).unwrap();
    }
}
fn success(request: Uuid) -> Vec<u8> {
    format!(r#"{{"request_id":"{request}","data":[]}}"#).into_bytes()
}
#[test]
fn q01_thousand_accepted_identities_terminate_once() {
    let mut q = queue(&[2, 3]);
    for n in 10..1010 {
        q.propose(body(n), 0).unwrap();
        replicate(&mut q, id(n));
    }
    assert_eq!(q.pending(), 1000);
    for n in 10..1010 {
        q.start(id(n), Some(b"{}".to_vec())).unwrap();
        replicate(&mut q, id(n));
        q.finish(id(n), 200, success(id(n)), 1).unwrap();
        replicate(&mut q, id(n));
    }
    assert_eq!(q.pending(), 0);
    for n in 10..1010 {
        assert_eq!(
            q.lookup(id(1), id(n), 1).unwrap().state,
            RequestState::Succeeded
        );
        assert_eq!(
            q.finish(id(n), 200, success(id(n)), 2),
            Err(QueueError::Conflict)
        );
    }
    assert_eq!(q.snapshot().records.len(), 1000);
}
#[test]
fn q02_count_and_bytes_are_independent_and_no_partial_install() {
    let mut cap = limits();
    cap.max_pending_requests = 1;
    let mut q = RequestQueue::new(id(1), 1, 1, cap, [id(2)].into()).unwrap();
    q.propose(body(10), 0).unwrap();
    assert_eq!(q.propose(body(11), 0), Err(QueueError::Overloaded));
    assert!(q.record(id(10)).is_some());
    cap = limits();
    cap.max_queue_bytes_per_replica = 1200;
    let mut q = RequestQueue::new(id(1), 1, 1, cap, [id(2)].into()).unwrap();
    q.propose(body(10), 0).unwrap();
    assert_eq!(q.propose(body(11), 0), Err(QueueError::Overloaded));
    assert_eq!(q.pending(), 1);
}
#[test]
fn q03_replication_precedes_acceptance_and_pin_precedes_execution() {
    let mut q = queue(&[2, 3]);
    let r = q.propose(body(10), 0).unwrap();
    let mut survivor = queue(&[2, 3]);
    survivor.install(r).unwrap();
    q.acknowledge(id(10), id(2)).unwrap();
    assert!(!q.ready(id(10)));
    assert_eq!(q.lookup(id(1), id(10), 0), Err(QueueError::Unavailable));
    assert_eq!(q.start(id(10), Some(vec![])), Err(QueueError::Unavailable));
    replicate(&mut survivor, id(10));
    assert!(survivor.ready(id(10)));
    let pin = survivor.start(id(10), Some(b"captured".to_vec())).unwrap();
    assert!(!survivor.ready(id(10)));
    q.install(pin).unwrap();
    replicate(&mut q, id(10));
    assert!(q.ready(id(10)));
    assert_eq!(
        q.record(id(10)).unwrap().pinned_catalog.as_deref(),
        Some(b"captured".as_slice())
    );
}
#[test]
fn q04_q05_disconnect_ownership_dedup_and_conflict() {
    let mut q = queue(&[2]);
    let original = q.propose(body(10), 0).unwrap();
    replicate(&mut q, id(10));
    assert_eq!(q.propose(body(10), 1).unwrap().sequence, original.sequence);
    let mut changed = body(10);
    changed.push(b' ');
    assert_eq!(q.propose(changed, 1), Err(QueueError::Conflict));
    let mut new_api_view = queue(&[2]);
    new_api_view.install_snapshot(q.snapshot()).unwrap();
    assert_eq!(
        new_api_view.lookup(id(1), id(10), 2).unwrap().state,
        RequestState::Queued
    );
}
#[test]
fn q06_expiration_keeps_identity_and_never_reexecutes() {
    let mut q = queue(&[2]);
    q.propose(body(10), 0).unwrap();
    replicate(&mut q, id(10));
    q.start(id(10), Some(vec![])).unwrap();
    replicate(&mut q, id(10));
    q.finish(id(10), 200, success(id(10)), 1).unwrap();
    assert!(q.lookup(id(1), id(10), 100).is_ok());
    assert_eq!(q.lookup(id(1), id(10), 101), Err(QueueError::NotFound));
    q.expire(101);
    assert_eq!(q.charged_bytes().1, 0);
    assert!(q.propose(body(10), 102).unwrap().result_expired);
    assert_eq!(q.start(id(10), Some(vec![])), Err(QueueError::Unavailable));
}
#[test]
fn r01_r02_r03_survivor_takeover_and_join_preserve_every_record() {
    let mut q = queue(&[2, 3]);
    q.propose(body(10), 0).unwrap();
    replicate(&mut q, id(10));
    q.start(id(10), Some(b"pin".to_vec())).unwrap();
    replicate(&mut q, id(10));
    let mut survivor = queue(&[2, 3]);
    survivor.install_snapshot(q.snapshot()).unwrap();
    survivor.takeover(2, 2, [id(3)].into()).unwrap();
    assert_eq!(
        survivor.record(id(10)).unwrap().state,
        RequestState::Reconciling
    );
    assert!(!survivor.ready(id(10)));
    replicate(&mut survivor, id(10));
    survivor.takeover(2, 3, [id(3), id(4)].into()).unwrap();
    let mut joiner = RequestQueue::new(id(1), 2, 3, limits(), [id(3), id(4)].into()).unwrap();
    joiner.install_snapshot(survivor.snapshot()).unwrap();
    assert_eq!(
        joiner.record(id(10)).unwrap().pinned_catalog.as_deref(),
        Some(b"pin".as_slice())
    );
}
/// OQ-6: a join published while the queue is at its byte limit still
/// installs on every holder. The larger ready set's ACK UUIDs may exceed the
/// limit; admission then refuses until retirement frees bytes.
#[test]
fn r03_oq6_published_join_installs_at_the_byte_limit() {
    let mut q = queue(&[2]);
    for n in 10..13 {
        q.propose(body(n), 0).unwrap();
        replicate(&mut q, id(n));
    }
    q.limits.max_queue_bytes_per_replica = q.charged_bytes().0;
    q.takeover(2, 2, [id(2), id(3)].into())
        .expect("a published view installs at the byte limit");
    assert_eq!(q.members.len(), 2);
    assert_eq!(q.propose(body(20), 0), Err(QueueError::Overloaded));
    for n in 10..13 {
        assert!(q.record(id(n)).is_some());
    }
}
#[test]
fn r04_cold_run_abandons_old_memory_without_replaying() {
    let mut old = queue(&[2]);
    old.propose(body(10), 0).unwrap();
    replicate(&mut old, id(10));
    let cold = RequestQueue::new(id(9), 1, 2, limits(), [id(3)].into()).unwrap();
    assert_eq!(cold.pending(), 0);
    assert_eq!(cold.lookup(id(1), id(10), 10), Err(QueueError::Abandoned));
}
#[test]
fn r05_stale_record_and_term_cannot_replace_surviving_copy() {
    let mut q = queue(&[2]);
    let old = q.propose(body(10), 0).unwrap();
    replicate(&mut q, id(10));
    q.takeover(2, 2, [id(2)].into()).unwrap();
    assert_eq!(q.install(old), Err(QueueError::Stale));
    assert_eq!(q.takeover(1, 3, [id(2)].into()), Err(QueueError::Stale));
}
#[test]
fn q03_stale_ack_cannot_prove_new_pinned_record_installed() {
    let mut q = queue(&[2]);
    q.propose(body(10), 0).unwrap();
    replicate(&mut q, id(10));
    let previous = q.record(id(10)).unwrap().version;
    let started = q.start(id(10), Some(vec![])).unwrap();
    assert!(started.version > previous);
    assert_eq!(
        q.acknowledge_version(id(10), previous, id(2)),
        Err(QueueError::Stale)
    );
    assert!(!q.ready(id(10)));
}
#[test]
fn q06_result_cap_settles_explicit_unavailability_without_losing_identity() {
    let mut cap = limits();
    cap.max_retained_result_bytes_per_worker = 1;
    let mut q = RequestQueue::new(id(1), 1, 1, cap, [id(2)].into()).unwrap();
    q.propose(body(10), 0).unwrap();
    replicate(&mut q, id(10));
    q.start(id(10), Some(vec![])).unwrap();
    replicate(&mut q, id(10));
    let terminal = q.finish(id(10), 200, success(id(10)), 1).unwrap();
    assert_eq!(terminal.state, RequestState::Succeeded);
    assert!(terminal.result_expired);
    assert_eq!(q.pending(), 0);
    assert_eq!(q.lookup(id(1), id(10), 1), Err(QueueError::NotFound));
    assert!(q.propose(body(10), 2).unwrap().state.terminal());
}
#[test]
fn r03_join_capacity_failure_is_atomic() {
    let mut source = queue(&[2]);
    for n in 10..13 {
        source.propose(body(n), 0).unwrap();
        replicate(&mut source, id(n));
    }
    let mut cap = limits();
    cap.max_pending_requests = 1;
    let mut joiner = RequestQueue::new(id(1), 1, 1, cap, [id(2)].into()).unwrap();
    assert_eq!(
        joiner.install_snapshot(source.snapshot()),
        Err(QueueError::Overloaded)
    );
    assert_eq!(joiner.pending(), 0);
}
#[test]
fn q06_terminal_state_cannot_be_rewritten_by_newer_record() {
    let mut q = queue(&[2]);
    q.propose(body(10), 0).unwrap();
    replicate(&mut q, id(10));
    q.start(id(10), Some(vec![])).unwrap();
    replicate(&mut q, id(10));
    let mut record = q.finish(id(10), 200, success(id(10)), 1).unwrap();
    record.version += 1;
    record.response_body =
        Some(format!(r#"{{"request_id":"{}","data":[1]}}"#, id(10)).into_bytes());
    assert_eq!(q.install(record), Err(QueueError::Conflict));
}

#[test]
fn r03_paged_copy_reports_completion_for_empty_and_final_pages() {
    let mut q = queue(&[2]);
    let (empty, done) = q.snapshot_page(None);
    assert!(done);
    assert!(empty.records.is_empty());
    let first = q.propose(body(40), 0).unwrap();
    let (one, done) = q.snapshot_page(None);
    assert!(done);
    assert_eq!(one.records[0].sequence, first.sequence);
    let second = q.propose(body(30), 0).unwrap();
    let third = q.propose(body(20), 0).unwrap();
    let mut copied = Vec::new();
    let mut after = None;
    loop {
        let (page, done) = q.snapshot_page(after);
        assert!(page.records.len() <= 1);
        for record in page.records {
            after = Some(record.sequence);
            copied.push(record.request_id);
        }
        if done {
            break;
        }
    }
    assert_eq!(
        copied,
        vec![first.request_id, second.request_id, third.request_id]
    );
    let (end, done) = q.snapshot_page(Some(third.sequence));
    assert!(done);
    assert!(end.records.is_empty());
}

fn customer(bucket: &str, secret: &str, expiry: Option<&str>) -> RequestContext {
    serde_json::from_value(serde_json::json!({"version":1,"bucket":bucket,"region":"eu-west-1","prefix":"db/","credentials":{"access_key_id":"gateway-key","secret_access_key":secret,"session_token":null,"expires_at":expiry}})).unwrap()
}
#[test]
fn sl01_q05_storage_identity_and_rotation_preserve_original_snapshot() {
    let mut q = queue(&[2, 3]);
    let raw = body(77);
    let a = q
        .propose_with_context(raw.clone(), Some(customer("database-a", "first", None)), 0)
        .unwrap();
    let b = q
        .propose_with_context(raw.clone(), Some(customer("database-b", "second", None)), 0)
        .unwrap();
    assert_ne!(a.identity(), b.identity());
    assert_eq!(q.pending(), 2);
    let replay = q
        .propose_with_context(
            raw.clone(),
            Some(customer("database-a", "rotated", None)),
            0,
        )
        .unwrap();
    assert_eq!(a, replay);
    assert_eq!(a.body, raw);
    assert!(q.record(id(77)).is_none());
    let changed = String::from_utf8(body(77))
        .unwrap()
        .replace("{}", "{ }")
        .into_bytes();
    assert_eq!(
        q.propose_with_context(changed, Some(customer("database-a", "rotated", None)), 0),
        Err(QueueError::Conflict)
    );
    for record in [&a, &b] {
        for member in [id(2), id(3)] {
            q.acknowledge(record.identity(), member).unwrap();
        }
        q.start(record.identity(), Some(b"{}".to_vec())).unwrap();
        for member in [id(2), id(3)] {
            q.acknowledge(record.identity(), member).unwrap();
        }
        q.finish(record.identity(), 200, success(id(77)), 1)
            .unwrap();
        assert_eq!(
            q.lookup(id(1), record.identity(), 1).unwrap().state,
            RequestState::Succeeded
        );
    }
    assert_eq!(q.pending(), 0);
}
#[test]
fn sl03_q03_r03_complete_context_recovers_after_expiry_without_renewal() {
    let mut q = queue(&[2, 3]);
    let a = q
        .propose_with_context(
            body(77),
            Some(customer(
                "database-a",
                "original",
                Some("2026-11-16T20:00:00Z"),
            )),
            0,
        )
        .unwrap();
    q.acknowledge(a.identity(), id(2)).unwrap();
    assert!(!q.ready(a.identity()));
    let wire = serde_json::to_vec(&q.snapshot()).unwrap();
    let mut survivor = queue(&[2, 3]);
    survivor
        .install_snapshot(serde_json::from_slice(&wire).unwrap())
        .unwrap();
    assert_eq!(survivor.record(a.identity()).unwrap().context, a.context);
    assert!(!survivor.ready(a.identity()));
    survivor.acknowledge(a.identity(), id(3)).unwrap();
    assert!(survivor.ready(a.identity()));
    // Recovery retains expired credentials. Admission with that expiry fails.
    assert_eq!(
        survivor.propose_with_context(body(78), a.context.clone(), u64::MAX / 2),
        Err(QueueError::Invalid)
    );
    let snapshot = survivor.takeover(2, 2, [id(3)].into()).unwrap();
    let recovered = &snapshot.records[0];
    assert_eq!(recovered.context, a.context);
    assert_eq!(recovered.digest, a.digest);
    assert_eq!(recovered.body, a.body);
    assert!(recovered.accepted);
}
#[test]
fn sl02_context_replacement_and_forgery_are_rejected_on_replica() {
    let mut q = queue(&[2]);
    let a = q
        .propose_with_context(body(77), Some(customer("database-a", "original", None)), 0)
        .unwrap();
    let mut forged = a.clone();
    forged.context = Some(customer("database-a", "replacement", None));
    forged.version += 1;
    assert_eq!(q.install(forged), Err(QueueError::Conflict));
    let mut value = serde_json::to_value(&a).unwrap();
    value["context"]["credentials"]["secret_access_key"] = serde_json::json!("");
    assert_eq!(
        q.install(serde_json::from_value(value).unwrap()),
        Err(QueueError::Invalid)
    );
    assert_eq!(q.record(a.identity()).unwrap(), &a);
    let debug = format!("{a:?}");
    assert!(!debug.contains("original"));
    assert!(!debug.contains("gateway-key"));
}
#[test]
fn sl04_q02_context_bytes_and_stable_scope_count_share_global_queue_budget() {
    let mut q = queue(&[2]);
    q.limits.max_pending_requests = 4;
    let a = q
        .propose_with_context(body(77), Some(customer("database-a", "first", None)), 0)
        .unwrap();
    assert!(q.charged_bytes().0 >= a.body.len() + a.context_bytes().unwrap() + 1024);
    assert_eq!(
        q.propose_with_context(body(78), a.context.clone(), 0),
        Err(QueueError::Overloaded)
    );
    // New request IDs with rotated credentials AND expiry cannot bypass the
    // target's quota. The accepted immutable snapshot digest still differs.
    let rotated = customer("database-a", "rotated-secret", Some("2026-11-16T20:00:00Z"));
    assert_ne!(a.binding_digests().unwrap().1, {
        let mut other = a.clone();
        other.context = Some(rotated.clone());
        other.binding_digests().unwrap().1
    });
    assert_eq!(
        q.propose_with_context(body(82), Some(rotated), 0),
        Err(QueueError::Overloaded)
    );
    let mut new_keys = serde_json::to_value(customer("database-a", "new-secret", None)).unwrap();
    new_keys["credentials"]["access_key_id"] = serde_json::json!("new-access");
    new_keys["credentials"]["session_token"] = serde_json::json!("new-session");
    assert_eq!(
        q.propose_with_context(body(83), Some(serde_json::from_value(new_keys).unwrap()), 0),
        Err(QueueError::Overloaded)
    );
    assert_eq!(q.pending(), 1);
    // Another database can progress while this stable target scope is full.
    q.propose_with_context(body(78), Some(customer("database-b", "second", None)), 0)
        .unwrap();
    q.propose(body(79), 0).unwrap();
    q.propose(body(80), 0).unwrap();
    assert_eq!(q.propose(body(81), 0), Err(QueueError::Overloaded));
    assert_eq!(q.pending(), 4);
    let before = q.charged_bytes();
    q.limits.max_queue_bytes_per_replica = before.0;
    assert_eq!(
        q.propose_with_context(body(82), Some(customer("database-c", "third", None)), 0),
        Err(QueueError::Overloaded)
    );
    assert_eq!(q.charged_bytes(), before);
}
