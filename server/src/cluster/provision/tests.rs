use super::*;
use crate::cluster::{
    configuration::{Template, Timing},
    member::*,
    state::*,
};
use serde_json::{Value, json};
fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn fixture() -> Value {
    let template = json!({"api_urls":["http://api/"],"listen":"worker:0","storage":{"driver":"http","options":{"base_url":"http://store/","namespace":"meshdb/","token_file":"/token","ca_file":null}},"tls":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"});
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
#[test]
fn pending_launch_remains_charged() {
    let mut c = root();
    // Construct through the real member codec independently of allocation.
    let n = json!({"node_id":id(30),"launch_id":id(40),"resource_id":null,"ownership":"managed","state":"starting","incarnation":null,"retired_incarnations":[],"endpoint":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"registration":"unregistered","session":null,"drain":null});
    let i = json!({"request_id":id(40),"node_id":id(30),"launch_id":id(40),"incarnation_id":null,"resource_id":null,"action":"start","state":"pending","attempts":0,"created_at_ms":10,"deadline_ms":100,"retry_at_ms":10,"error_code":null,"template_fingerprint":c.config.template_fingerprint,"delivery":null,"outcome":null});
    c.nodes
        .push(Node::decode(&serde_json::to_vec(&n).unwrap()).unwrap());
    c.intents
        .push(Intent::decode(&serde_json::to_vec(&i).unwrap()).unwrap());
    c.validate().unwrap();
    assert_eq!(managed_capacity(&c), Ok(1));
}
#[test]
fn initial_fields_sorted_and_unrelated_preserved() {
    let mut c = allocated();
    c.admin_seq = 8;
    c.admin_head = Some(id(800));
    c.scheduler.failures = 4;
    c.scheduler.cooldown_until_ms = 999;
    c.scheduler.low_since_ms = Some(4);
    let saved = c.clone();
    let n = allocate_start(&c, owner(), id(20), id(25), 20, id(51))
        .unwrap()
        .into_parts()
        .2;
    let node = &n.nodes[0];
    let start = &n.intents[0];
    assert_eq!(
        node,
        &Node {
            resources: Some(c.template.worker_resources()),
            node_id: id(20),
            launch_id: Some(id(25)),
            resource_id: None,
            ownership: Ownership::Managed,
            state: NodeState::Starting,
            incarnation: None,
            retired_incarnations: vec![],
            endpoint: None,
            operation_slots: crate::node::queue_runtime::coordination_capacity(&c.template).unwrap()
                as u64,
            partition_slots: crate::node::queue_runtime::job_coordination_capacity(&c.template)
                .unwrap() as u64,
            queue_size: c.template.queue_size,
            registration: Registration::Unregistered,
            session: None,
            drain: None
        }
    );
    assert_eq!(
        start,
        &Intent {
            request_id: id(25),
            node_id: id(20),
            launch_id: id(25),
            incarnation_id: None,
            resource_id: None,
            action: Action::Start,
            state: IntentState::Pending,
            attempts: 0,
            created_at_ms: 20,
            deadline_ms: 20 + c.timing.readiness_ms,
            retry_at_ms: 20,
            error_code: None,
            template_fingerprint: Some(c.config.template_fingerprint.clone()),
            delivery: None,
            outcome: None
        }
    );
    let mut restored = n.clone();
    restored.nodes.remove(0);
    restored.intents.remove(0);
    restored.revision = c.revision;
    restored.change_id = c.change_id;
    assert_eq!(restored, saved);
    assert_eq!(c, saved);
    assert_eq!(n.capacity().unwrap().credits, 2);
}
fn known() -> Control {
    let mut c = allocated();
    let n = &mut c.nodes[0];
    n.resource_id = Some("resource".into());
    n.incarnation = Some(id(60));
    n.endpoint = Some("http://worker:8000".into());
    n.registration = Registration::Verified;
    n.session = Some(Session {
        id: id(61),
        verifier: "a".repeat(64),
    });
    n.state = NodeState::Ready;
    c.intents[0].state = IntentState::Completed;
    c.intents[0].resource_id = Some("resource".into());
    c.intents[0].outcome = Some(IntentOutcome::ResourceKnown);
    c.validate().unwrap();
    c
}
#[test]
fn capacity_retains_ambiguous_and_phase_only_records() {
    for phase in [
        NodeState::Starting,
        NodeState::Ready,
        NodeState::Lost,
        NodeState::Draining,
        NodeState::Stopped,
    ] {
        let mut c = known();
        c.nodes[0].state = phase;
        c.validate().unwrap();
        assert_eq!(managed_capacity(&c), Ok(1));
    }
    for phase in [
        IntentState::Pending,
        IntentState::InProgress,
        IntentState::Reconciling,
    ] {
        let mut c = allocated();
        c.intents[0].state = phase;
        if phase != IntentState::Pending {
            c.intents[0].attempts = 1;
            c.intents[0].delivery = Some(Delivery {
                invocation_id: id(70),
                owner_id: id(90),
                epoch: 7,
                command_sha256: "a".repeat(64),
            });
        }
        c.intents[0].deadline_ms = 0;
        c.validate().unwrap();
        assert_eq!(managed_capacity(&c), Ok(1));
    }
    for outcome in [IntentOutcome::NotInvoked, IntentOutcome::Terminated] {
        let mut c = allocated();
        c.intents[0].state = IntentState::Failed;
        c.intents[0].outcome = Some(outcome);
        c.validate().unwrap();
        assert_eq!(managed_capacity(&c), Ok(0));
    }
    for (phase, outcome, expected) in [
        (IntentState::Pending, None, 1),
        (IntentState::Failed, Some(IntentOutcome::NotInvoked), 1),
        (IntentState::Completed, Some(IntentOutcome::Terminated), 0),
        (IntentState::Failed, Some(IntentOutcome::Terminated), 0),
    ] {
        let mut c = known();
        let mut stop = c.intents[0].clone();
        stop.request_id = id(80);
        stop.action = Action::Stop;
        stop.incarnation_id = c.nodes[0].incarnation;
        stop.template_fingerprint = None;
        stop.state = phase;
        stop.outcome = outcome;
        c.intents.push(stop);
        c.validate().unwrap();
        assert_eq!(managed_capacity(&c), Ok(expected));
    }
}
#[test]
fn rejection_and_checked_exhaustion() {
    let c = root();
    for mode in 0..9 {
        let mut c = c.clone();
        let mut o = owner();
        let mut now = 10;
        let mut expected = StorageError::Conflict;
        match mode {
            0 => {
                c.policy.desired = 0;
                c.policy.min_nodes = 0;
                c.policy.max_nodes = 0;
            }
            1 => {
                c = allocated();
                c.policy.desired = 1;
                c.policy.min_nodes = 1;
                c.policy.max_nodes = 1;
            }
            2 => c.scheduler.backoff_until_ms = 11,
            3 => {
                c.provisioning = Provisioning::External;
                c.config.adapter_contract = None;
            }
            4 => o.epoch += 1,
            5 => {
                c.controller.lease_state = LeaseState::Released;
                c.controller.owner_id = None;
                c.controller.lease_until_ms = None;
            }
            6 => {
                c.revision = u64::MAX;
                expected = StorageError::LimitExceeded;
            }
            7 => {
                now = u64::MAX;
                expected = StorageError::LimitExceeded;
            }
            _ => {
                c.version = 9;
                expected = StorageError::InvalidCatalog;
            }
        }
        if mode != 8 {
            c.validate().unwrap();
        }
        let saved = c.clone();
        assert!(matches!(allocate_start(&c,o,id(100),id(101),now,id(102)),Err(e) if e==expected));
        assert_eq!(c, saved);
    }
    assert!(matches!(
        allocate_start(&c, owner(), id(100), id(100), 10, id(102)),
        Err(StorageError::Conflict)
    ));
    assert!(matches!(
        allocate_start(&c, owner(), id(100), id(101), 10, c.change_id),
        Err(StorageError::Conflict)
    ));
}
#[test]
fn identities_never_reused_in_any_retained_role() {
    let mut c = known();
    c.nodes[0].retired_incarnations = vec![id(62)];
    c.nodes[0].drain = Some(Drain {
        id: id(63),
        requested_at_ms: 1,
        ack_seq: None,
        confirmed_idle: false,
    });
    c.intents[0].delivery = Some(Delivery {
        invocation_id: id(64),
        owner_id: id(65),
        epoch: 1,
        command_sha256: "a".repeat(64),
    });
    c.admin_seq = 1;
    c.admin_head = Some(id(66));
    let mut stop = c.intents[0].clone();
    stop.request_id = id(80);
    stop.action = Action::Stop;
    stop.incarnation_id = c.nodes[0].incarnation;
    stop.template_fingerprint = None;
    stop.state = IntentState::Pending;
    stop.outcome = None;
    c.intents.push(stop);
    c.validate().unwrap();
    for retained in [80, 1, 50, 30, 40, 60, 61, 62, 63, 64, 65, 66, 90] {
        for is_node in [true, false] {
            let (n, l) = if is_node {
                (id(retained), id(101))
            } else {
                (id(100), id(retained))
            };
            assert!(matches!(
                allocate_start(&c, owner(), n, l, 10, id(102)),
                Err(StorageError::Conflict)
            ));
        }
    }
}
#[test]
fn full_augmented_admission_refuses_new_pair() {
    let mut c = root();
    let first = allocated();
    // Retained terminal launches consume metadata even though desired has room.
    for k in 0..1536u128 {
        let mut n = first.nodes[0].clone();
        let mut i = first.intents[0].clone();
        n.node_id = id(1000 + 2 * k);
        n.launch_id = Some(id(1001 + 2 * k));
        i.node_id = n.node_id;
        i.request_id = id(1001 + 2 * k);
        i.launch_id = i.request_id;
        c.nodes.push(n);
        c.intents.push(i);
    }
    c.validate().unwrap();
    assert_eq!(c.capacity().unwrap().credits, 1536);
    // Terminal witnesses leave 1023 charged launches and 4095 augmented slots.
    for i in &mut c.intents[..513] {
        i.state = IntentState::Failed;
        i.outcome = Some(IntentOutcome::NotInvoked);
    }
    c.policy.desired = 1024;
    c.policy.min_nodes = 1024;
    c.policy.max_nodes = 1024;
    c.check_new_admission().unwrap();
    assert_eq!(managed_capacity(&c), Ok(1023));
    let saved = c.clone();
    assert!(matches!(
        allocate_start(&c, owner(), id(5000), id(5001), 10, id(5002)),
        Err(StorageError::LimitExceeded)
    ));
    assert_eq!(c, saved);
}

#[test]
fn a02_demand_respects_age_bounds_cooldown_and_idle_stabilization() {
    let mut c = root();
    c.policy.mode = PolicyMode::Autoscale;
    c.policy.min_nodes = 1;
    c.policy.max_nodes = 4;
    c.policy.desired = 2;
    let delay = c.timing.stabilization_ms;
    assert!(
        demand_candidate(&c, owner(), 3, 2, delay - 1, 100_000, id(100))
            .unwrap()
            .is_none()
    );
    let grown = demand_candidate(&c, owner(), 3, 2, delay, 100_000, id(101))
        .unwrap()
        .unwrap()
        .into_parts()
        .2;
    assert_eq!(grown.policy.desired, 3);
    assert_eq!(grown.policy.revision, c.policy.revision + 1);
    assert_eq!(
        grown.scheduler.cooldown_until_ms,
        100_000 + c.timing.cooldown_ms
    );
    assert!(
        demand_candidate(&grown, owner(), 4, 3, delay, 100_001, id(102))
            .unwrap()
            .is_none()
    );
    let anchored = demand_candidate(&grown, owner(), 0, 0, 0, 200_000, id(103))
        .unwrap()
        .unwrap()
        .into_parts()
        .2;
    assert_eq!(anchored.policy.desired, 3);
    assert_eq!(anchored.scheduler.low_since_ms, Some(200_000));
    assert!(
        demand_candidate(&anchored, owner(), 0, 0, 0, 200_000 + delay - 1, id(104))
            .unwrap()
            .is_none()
    );
    let shrunk = demand_candidate(&anchored, owner(), 0, 0, 0, 200_000 + delay, id(105))
        .unwrap()
        .unwrap()
        .into_parts()
        .2;
    assert_eq!(shrunk.policy.desired, 2);
    assert_eq!(shrunk.scheduler.low_since_ms, None);
    let reset = demand_candidate(&anchored, owner(), 1, 1, 0, 200_001, id(106))
        .unwrap()
        .unwrap()
        .into_parts()
        .2;
    assert_eq!(reset.scheduler.low_since_ms, None);
    assert_eq!(reset.policy.desired, 3);
    c.policy.desired = 4;
    assert!(
        demand_candidate(&c, owner(), 10, 4, delay, 100_000, id(107))
            .unwrap()
            .is_none()
    );
    c.policy.desired = 1;
    assert!(
        demand_candidate(&c, owner(), 0, 0, 0, 100_000, id(108))
            .unwrap()
            .is_none()
    );
    c.scheduler.backoff_until_ms = 200_000;
    assert!(
        demand_candidate(&c, owner(), 10, 1, delay, 100_000, id(109))
            .unwrap()
            .is_none()
    );
}

#[test]
fn c01_c07_pending_managed_capacity_uses_effective_resources_and_job_frontier() {
    let mut c = root();
    c.template.resources = Some(crate::config::WorkerResources {
        max_threads: 4,
        ..Default::default()
    });
    c.template.partition_slots = 2;
    c.config.template_fingerprint = c.template.fingerprint().unwrap();
    let candidate = allocate_start(&c, owner(), id(30), id(40), 10, id(50))
        .unwrap()
        .into_parts()
        .2;
    candidate.validate().unwrap();
    let node = &candidate.nodes[0];
    assert_eq!(node.resources.unwrap().max_threads, 4);
    assert!(node.operation_slots >= 4);
    assert!(node.partition_slots >= 10);
    assert_ne!(node.partition_slots, c.template.partition_slots);
}
