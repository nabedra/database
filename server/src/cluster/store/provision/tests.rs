use super::*;
use crate::{
    cluster::{
        controller::{Owner, OwnershipChange, ownership_candidate},
        provision::{allocate_start, managed_capacity},
    },
    storage::{Download, Namespace},
    test_storage::{Fake, ReadGate},
};
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio::time::Duration;
const KEY: &str = "test/cluster/control.json";
fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
fn owner() -> Owner {
    Owner {
        process_id: id(90),
        epoch: 7,
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
fn root() -> Control {
    let mut v = super::super::tests::root();
    v["policy"] = json!({"revision":9,"mode":"fixed","desired":3,"min_nodes":3,"max_nodes":3});
    v["controller"] = json!({"owner_id":id(90),"epoch":7,"lease_state":"held","lease_until_ms":1});
    Control::decode(&serde_json::to_vec(&v).unwrap()).unwrap()
}
fn candidate(c: &Control, n: u128) -> ProvisionCandidate {
    allocate_start(c, owner(), id(n), id(n + 1), 10, id(n + 2)).unwrap()
}
#[derive(Default)]
struct Backend {
    fake: Arc<Fake>,
    conditions: Mutex<Vec<String>>,
    before: Mutex<Option<Arc<ReadGate>>>,
    after: Mutex<Option<Arc<ReadGate>>>,
}
async fn hold(g: Arc<ReadGate>) {
    g.entered.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), g.release.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}
impl ObjectStore for Backend {
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
            panic!("provision must Match")
        };
        self.conditions.lock().unwrap().push(token.to_owned());
        let gate = self.before.lock().unwrap().take();
        if let Some(g) = gate {
            hold(g).await;
        }
        let result = self.fake.put(key, bytes, condition).await;
        let gate = self.after.lock().unwrap().take();
        if let Some(g) = gate {
            hold(g).await;
        }
        result
    }
}
fn store(c: &Control) -> Storage<Backend> {
    let b = Backend::default();
    b.fake
        .state
        .lock()
        .unwrap()
        .objects
        .insert(KEY.into(), (c.encode().unwrap(), "captured".into()));
    Storage::new(b, Namespace::new("test").unwrap())
}
fn counts(s: &Storage<Backend>) -> (usize, usize) {
    let f = s.backend().fake.state.lock().unwrap();
    (f.puts, f.gets)
}
async fn entered(g: &ReadGate) -> bool {
    match tokio::time::timeout(Duration::from_secs(5), g.entered.acquire()).await {
        Ok(Ok(p)) => {
            p.forget();
            true
        }
        _ => false,
    }
}
async fn finish<T>(mut t: tokio::task::JoinHandle<T>) -> T {
    match tokio::time::timeout(Duration::from_secs(5), &mut t).await {
        Ok(r) => r.unwrap(),
        Err(_) => {
            t.abort();
            let _ = t.await;
            panic!("watchdog");
        }
    }
}

#[tokio::test]
async fn preparation_binds_complete_observation_and_owner() {
    let c = root();
    let s = store(&c);
    let base = s.load_control(deadline()).await.unwrap();
    for mode in 0..4 {
        let mut other = base.clone();
        match mode {
            0 => other.root.scheduler.failures += 1,
            1 => other.root.controller.epoch += 1,
            2 => other.root.controller.owner_id = Some(id(91)),
            _ => {
                other.root.controller.lease_state = LeaseState::Released;
                other.root.controller.owner_id = None;
                other.root.controller.lease_until_ms = None;
            }
        }
        other.root.validate().unwrap();
        assert!(matches!(
            PreparedProvision::prepare(other, candidate(&c, 10)),
            Err(StorageError::Conflict)
        ));
    }
    let p = PreparedProvision::prepare(base, candidate(&c, 10)).unwrap();
    assert_eq!(format!("{p:?}"), "PreparedProvision");
    assert_eq!(counts(&s), (0, 1));
}
#[tokio::test]
async fn same_token_allocations_have_one_winner() {
    let c = root();
    let s = Arc::new(store(&c));
    let base = s.load_control(deadline()).await.unwrap();
    let a = PreparedProvision::prepare(base.clone(), candidate(&c, 10)).unwrap();
    let b = PreparedProvision::prepare(base, candidate(&c, 20)).unwrap();
    let expected = b.candidate.clone();
    let g = Arc::new(ReadGate::default());
    *s.backend().before.lock().unwrap() = Some(g.clone());
    let t = {
        let s = s.clone();
        tokio::spawn(async move { a.submit(&s, deadline()).await })
    };
    let reached = entered(&g).await;
    let winner = if reached {
        Some(b.submit(&s, deadline()).await)
    } else {
        None
    };
    g.release.add_permits(1);
    let loser = finish(t).await;
    assert!(reached);
    assert!(matches!(
        winner,
        Some(Ok(OwnershipOutcome::Acknowledged(_)))
    ));
    assert!(matches!(
        loser,
        Ok(OwnershipOutcome::Conflict { current: Some(_) })
    ));
    assert_eq!(counts(&s), (2, 2));
    assert_eq!(
        *s.backend().conditions.lock().unwrap(),
        vec!["captured", "captured"]
    );
    let f = s.backend().fake.state.lock().unwrap();
    assert_eq!(Control::decode(&f.objects[KEY].0).unwrap(), expected);
    assert_ne!(f.objects[KEY].1, "captured");
}
#[tokio::test]
async fn applied_lost_reply_confirms_only_exact_whole_candidate() {
    for later in [false, true] {
        let c = root();
        let s = Arc::new(store(&c));
        let base = s.load_control(deadline()).await.unwrap();
        let p = PreparedProvision::prepare(base, candidate(&c, 10)).unwrap();
        let expected = p.candidate.clone();
        let mut changed = expected.clone();
        changed.revision += 1;
        changed.change_id = id(70);
        changed.scheduler.failures += 1;
        let later_bytes = changed.encode().unwrap();
        s.backend().fake.state.lock().unwrap().lose_put_response = true;
        let g = Arc::new(ReadGate::default());
        *s.backend().after.lock().unwrap() = Some(g.clone());
        let t = {
            let s = s.clone();
            tokio::spawn(async move { p.submit(&s, deadline()).await })
        };
        let reached = entered(&g).await;
        if reached && later {
            s.backend()
                .fake
                .state
                .lock()
                .unwrap()
                .objects
                .insert(KEY.into(), (later_bytes, "later".into()));
        }
        g.release.add_permits(1);
        let result = finish(t).await;
        assert!(reached);
        let result = result.unwrap();
        if later {
            let OwnershipOutcome::Unknown { current: Some(v) } = result else {
                panic!("expected unknown")
            };
            assert_eq!(v.root(), &changed);
        } else {
            let OwnershipOutcome::ObservedExact(v) = result else {
                panic!("expected exact")
            };
            assert_eq!(v.root(), &expected);
        }
        assert_eq!(counts(&s), (1, 2));
        assert_eq!(*s.backend().conditions.lock().unwrap(), vec!["captured"]);
        let f = s.backend().fake.state.lock().unwrap();
        assert_ne!(f.objects[KEY].1, "captured");
    }
}
#[tokio::test]
async fn successful_takeover_fences_old_allocation() {
    let c = root();
    let s = Arc::new(store(&c));
    let base = s.load_control(deadline()).await.unwrap();
    let p = PreparedProvision::prepare(base, candidate(&c, 10)).unwrap();
    let takeover = ownership_candidate(
        &c,
        OwnershipChange::Acquire { process_id: id(91) },
        10,
        id(80),
    )
    .unwrap();
    let bytes = takeover.encode().unwrap();
    let g = Arc::new(ReadGate::default());
    *s.backend().before.lock().unwrap() = Some(g.clone());
    let t = {
        let s = s.clone();
        tokio::spawn(async move { p.submit(&s, deadline()).await })
    };
    let reached = entered(&g).await;
    let winner = if reached {
        Some(
            s.backend()
                .fake
                .put(KEY, bytes, WriteCondition::Match("captured"))
                .await,
        )
    } else {
        None
    };
    g.release.add_permits(1);
    let result = finish(t).await;
    assert!(reached);
    assert!(matches!(winner, Some(Ok(_))));
    let OwnershipOutcome::Conflict { current: Some(v) } = result.unwrap() else {
        panic!("old allocation won")
    };
    assert_eq!(v.root(), &takeover);
    assert_eq!(counts(&s), (2, 2));
    assert_eq!(*s.backend().conditions.lock().unwrap(), vec!["captured"]);
    let f = s.backend().fake.state.lock().unwrap();
    assert_eq!(Control::decode(&f.objects[KEY].0).unwrap(), takeover);
    assert_ne!(f.objects[KEY].1, "captured");
}

#[tokio::test]
async fn unreadable_lost_allocation_reply_keeps_capacity_reserved_after_reload() {
    let mut c = root();
    c.policy.desired = 1;
    c.policy.min_nodes = 1;
    c.policy.max_nodes = 1;
    let s = store(&c);
    let base = s.load_control(deadline()).await.unwrap();
    let pending = PreparedProvision::prepare(base, candidate(&c, 10)).unwrap();
    let expected = pending.candidate.clone();
    {
        let mut faults = s.backend().fake.state.lock().unwrap();
        faults.lose_put_response = true;
        faults.read_failure = true;
    }
    assert!(matches!(
        pending.submit(&s, deadline()).await.unwrap(),
        OwnershipOutcome::Unknown { current: None }
    ));
    assert_eq!(counts(&s), (1, 2));

    // A new observer has no in-memory knowledge of the possible allocation.
    let restarted = Storage::new(
        Backend {
            fake: s.backend().fake.clone(),
            ..Backend::default()
        },
        Namespace::new("test").unwrap(),
    );
    {
        let mut faults = restarted.backend().fake.state.lock().unwrap();
        faults.lose_put_response = false;
        faults.read_failure = false;
    }
    let loaded = restarted.load_control(deadline()).await.unwrap();
    assert_eq!(loaded.root(), &expected);
    assert_eq!(managed_capacity(loaded.root()), Ok(1));
    assert_eq!(loaded.root().capacity().unwrap().credits, 1);
    assert!(matches!(
        allocate_start(loaded.root(), owner(), id(20), id(21), 20, id(22)),
        Err(StorageError::Conflict)
    ));
    // Raising the goal permits a new launch, but cannot reuse the uncertain one.
    let mut enlarged = loaded.root().clone();
    enlarged.policy.desired = 2;
    enlarged.policy.min_nodes = 2;
    enlarged.policy.max_nodes = 2;
    for (node, launch) in [(10, 21), (20, 11)] {
        assert!(matches!(
            allocate_start(&enlarged, owner(), id(node), id(launch), 20, id(22)),
            Err(StorageError::Conflict)
        ));
    }
    let successor = allocate_start(&enlarged, owner(), id(20), id(21), 20, id(22))
        .unwrap()
        .into_parts()
        .2;
    assert_eq!(managed_capacity(&successor), Ok(2));
    assert_eq!(&successor.nodes[0], &expected.nodes[0]);
    assert_eq!(&successor.intents[0], &expected.intents[0]);
    assert_eq!(counts(&restarted), (1, 3));
    assert!(restarted.backend().conditions.lock().unwrap().is_empty());
}
