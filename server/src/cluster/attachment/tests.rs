use super::*;
use crate::cluster::{
    configuration::Timing, startup_material::composition::prepare_api, state::Control,
};
use crate::storage::{Namespace, Storage};
use crate::test_storage as support;
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    time::{Duration, Instant},
};
use support::Fake;
const KEY: &str = "test/cluster/control.json";
pub(in crate::cluster) struct Fixture(pub(in crate::cluster) PathBuf);
impl Fixture {
    pub(in crate::cluster) fn new() -> Self {
        let f = Self(std::env::temp_dir().join(format!("nabedra-attach-{}", uuid::Uuid::new_v4())));
        fs::create_dir(&f.0).unwrap();
        f.write("auth", serde_json::to_vec(&json!({"admin_token":"A".repeat(43),"data_token":"D".repeat(43),"enrollment_token":"E".repeat(43)})).unwrap());
        f.write("token", b"owned-token\n");
        f
    }
    fn write(&self, name: &str, bytes: impl AsRef<[u8]>) {
        let path = self.0.join(name);
        fs::write(&path, bytes).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
    }
    pub(in crate::cluster) fn input(&self) -> Value {
        json!({"version":1,"cluster_id":"12345678-1234-4234-8234-123456789abc","bind":"127.0.0.1:8000","advertise_url":"http://api.invalid:8000","api_urls":["http://api.invalid:8000"],"auth_file":"auth","tls":null,"storage":{"driver":"http","options":{"base_url":"http://store.invalid/objects/","namespace":"owned/","token_file":"token","ca_file":null}},"worker":{"listen":"worker:0","operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"}})
    }
    pub(in crate::cluster) fn ready(&self, value: &Value) -> ApiReady {
        self.write("api.json", serde_json::to_vec(value).unwrap());
        prepare_api(
            &self.0.join("api.json"),
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap()
        .connect()
        .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
pub(in crate::cluster) fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(5)
}
pub(in crate::cluster) fn candidate(local: &ApiReady, managed: bool) -> Control {
    let template = &local.prepared.input.value.template;
    Control::decode(&serde_json::to_vec(&json!({"version":1,"cluster_id":local.prepared.input.value.cluster_id,"name":"Fleet_1","config":{"generation":2,"storage_fingerprint":local.backend.storage.as_str(),"auth_fingerprint":local.authentication.as_str(),"template_fingerprint":template.fingerprint().unwrap(),"lifecycle_fingerprint":local.lifecycle.as_str(),"worker_semantic_fingerprint":local.backend.worker.as_str(),"adapter_contract":if managed {Some("meshdb.callbacks.v1")} else {None}},"provisioning":if managed {"managed"} else {"external"},"template":template,"timing":Timing::default(),"revision":1,"change_id":uuid::Uuid::new_v4(),"policy":{"revision":1,"mode":"fixed","desired":0,"min_nodes":0,"max_nodes":0},"controller":{"owner_id":null,"epoch":0,"lease_state":"released","lease_until_ms":null},"scheduler":{"cooldown_until_ms":0,"backoff_until_ms":0,"failures":0,"low_since_ms":null},"admin_seq":0,"admin_head":null,"nodes":[],"intents":[]})).unwrap()).unwrap()
}
pub(in crate::cluster) fn store(root: Option<&Control>) -> Storage<Fake> {
    let f = Fake::default();
    if let Some(root) = root {
        f.state
            .lock()
            .unwrap()
            .objects
            .insert(KEY.into(), (root.encode().unwrap(), "private-token".into()));
    }
    Storage::new(f, Namespace::new("test").unwrap())
}
#[tokio::test]
async fn preserves_loaded_noninitial_root_with_relocated_material() {
    let a = Fixture::new();
    let b = Fixture::new();
    let local = a.ready(&a.input());
    let mut root = candidate(&local, true);
    root.revision = 8;
    root.policy.revision = 4;
    root.policy.desired = 2;
    root.policy.min_nodes = 2;
    root.policy.max_nodes = 2;
    root.admin_seq = 2;
    root.admin_head = Some(uuid::Uuid::new_v4());
    root.scheduler.failures = 7;
    root.scheduler.backoff_until_ms = 800;
    root.controller.owner_id = Some(uuid::Uuid::new_v4());
    root.controller.epoch = 3;
    root.controller.lease_state = crate::cluster::state::LeaseState::Held;
    root.controller.lease_until_ms = Some(900);
    let mut value = serde_json::to_value(&root).unwrap();
    let node_id = uuid::Uuid::from_u128(100);
    let launch_id = uuid::Uuid::from_u128(101);
    value["nodes"] = json!([{"node_id":node_id,"launch_id":launch_id,"resource_id":"resource/one","ownership":"managed","state":"starting","incarnation":null,"retired_incarnations":[],"endpoint":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"registration":"unregistered","session":null,"drain":null}]);
    value["intents"] = json!([{"request_id":launch_id,"node_id":node_id,"launch_id":launch_id,"incarnation_id":null,"resource_id":"resource/one","action":"start","state":"completed","attempts":1,"created_at_ms":0,"deadline_ms":0,"retry_at_ms":0,"error_code":null,"template_fingerprint":root.config.template_fingerprint,"delivery":null,"outcome":"resource_known"}]);
    let root = Control::decode(&serde_json::to_vec(&value).unwrap()).unwrap();
    let s = store(Some(&root));
    let loaded = s.load_control(deadline()).await.unwrap();
    let mut input = b.input();
    input["bind"] = "localhost:9000".into();
    input["advertise_url"] = "http://other.invalid:9000".into();
    input["storage"]["options"]["namespace"] = "owned".into();
    input["storage"]["options"]["base_url"] = "http://store.invalid/objects".into();
    let other = b.ready(&input);
    a.write("auth", b"replaced");
    fs::remove_file(a.0.join("token")).unwrap();
    let attached = check_attach(loaded.clone(), other, "Fleet_1", Provisioning::Managed).unwrap();
    assert_eq!(attached.loaded, loaded);
    assert_eq!(attached.loaded.root(), &root);
    assert_eq!(
        attached.local.backend.worker.as_str(),
        local.backend.worker.as_str()
    );
    assert_eq!(s.backend().state.lock().unwrap().puts, 0);
}

#[tokio::test]
async fn incompatible_create_winner_is_never_reset_or_recreated() {
    let f = Fixture::new();
    let local = f.ready(&f.input());
    let initial = candidate(&local, true);
    let mut winner = initial.clone();
    winner.name = "Other".into();
    winner.change_id = uuid::Uuid::new_v4();
    let bytes = winner.encode().unwrap();
    let s = store(None);
    s.backend().state.lock().unwrap().race = Some(bytes.clone());
    let loaded = control_result(s.initialize_control(&initial, deadline()).await).unwrap();
    let captured = loaded.clone();
    assert_eq!(
        check_attach(loaded, local, "Fleet_1", Provisioning::Managed)
            .unwrap_err()
            .to_string(),
        "incompatible_cluster"
    );
    assert_eq!(s.load_control(deadline()).await.unwrap(), captured);
    let state = s.backend().state.lock().unwrap();
    assert_eq!(state.puts, 1);
    assert_eq!(state.objects[KEY], (bytes, "opaque-race".into()));
}
#[tokio::test]
async fn real_initialization_existing_and_race_use_one_comparison() {
    let f = Fixture::new();
    for race in [false, true] {
        let local = f.ready(&f.input());
        let initial = candidate(&local, true);
        let s = store(None);
        let mut winner = initial.clone();
        winner.change_id = uuid::Uuid::new_v4();
        winner.policy.desired = 3;
        winner.policy.min_nodes = 3;
        winner.policy.max_nodes = 3;
        if race {
            s.backend().state.lock().unwrap().race = Some(winner.encode().unwrap());
        }
        let loaded = control_result(s.initialize_control(&initial, deadline()).await).unwrap();
        let attached =
            check_attach(loaded.clone(), local, "Fleet_1", Provisioning::Managed).unwrap();
        assert_eq!(attached.loaded, loaded);
        assert_eq!(
            attached.loaded.root(),
            if race { &winner } else { &initial }
        );
        let mut new_initial = initial.clone();
        new_initial.policy.desired = 9;
        new_initial.policy.min_nodes = 9;
        new_initial.policy.max_nodes = 9;
        let existing =
            control_result(s.initialize_control(&new_initial, deadline()).await).unwrap();
        let reattached = check_attach(
            existing.clone(),
            f.ready(&f.input()),
            "Fleet_1",
            Provisioning::Managed,
        )
        .unwrap();
        assert_eq!(reattached.loaded, attached.loaded);
        assert_eq!(
            check_attach(
                existing,
                f.ready(&f.input()),
                "Fleet_1",
                Provisioning::External
            )
            .unwrap_err()
            .to_string(),
            "incompatible_cluster"
        );
        let state = s.backend().state.lock().unwrap();
        assert_eq!(state.puts, 1);
        assert_eq!(
            state.objects[KEY].0,
            attached.loaded.root().encode().unwrap()
        );
    }
    let local = f.ready(&f.input());
    let external = candidate(&local, false);
    let s = store(None);
    let loaded = control_result(s.initialize_control(&external, deadline()).await).unwrap();
    let attached = check_attach(loaded, local, "Fleet_1", Provisioning::External).unwrap();
    assert_eq!(attached.loaded.root(), &external);
}
#[tokio::test]
async fn real_decode_failures_and_storage_failures_have_fixed_categories() {
    let f = Fixture::new();
    let initial = candidate(&f.ready(&f.input()), true);
    for missing in [false, true] {
        let mut value = serde_json::to_value(&initial).unwrap();
        value["config"]["generation"] = 1.into();
        if missing {
            value["config"]
                .as_object_mut()
                .unwrap()
                .remove("worker_semantic_fingerprint");
        }
        let s = store(None);
        s.backend().state.lock().unwrap().objects.insert(
            KEY.into(),
            (serde_json::to_vec(&value).unwrap(), "old-token".into()),
        );
        for result in [
            s.load_control(deadline()).await,
            s.initialize_control(&initial, deadline()).await,
        ] {
            let error = control_result(result).unwrap_err();
            assert_eq!(error.to_string(), "incompatible_cluster");
            assert!(std::error::Error::source(&error).is_none());
        }
        assert_eq!(s.backend().state.lock().unwrap().puts, 0);
    }
    let s = store(Some(&initial));
    s.backend().state.lock().unwrap().read_failure = true;
    assert_eq!(
        control_result(s.load_control(deadline()).await)
            .unwrap_err()
            .to_string(),
        "storage_unavailable"
    );
    assert_eq!(
        control_result(
            s.initialize_control(&initial, tokio::time::Instant::now())
                .await
        )
        .unwrap_err()
        .to_string(),
        "storage_unavailable"
    );
}
#[tokio::test]
async fn independently_rejects_local_and_stored_semantic_changes() {
    let f = Fixture::new();
    let root = candidate(&f.ready(&f.input()), true);
    let s = store(Some(&root));
    let loaded = s.load_control(deadline()).await.unwrap();
    for (pointer, value) in [
        ("/cluster_id", json!(uuid::Uuid::new_v4())),
        ("/api_urls", json!(["http://different.invalid:8000"])),
        ("/worker/listen", json!("worker:8002")),
        ("/worker/operation_slots", json!(5)),
        ("/worker/partition_slots", json!(5)),
        ("/worker/queue_size", json!(65)),
        ("/worker/partition_size", json!("32 MiB")),
        ("/storage/options/namespace", json!("different")),
        ("/storage/options/base_url", json!("http://else.invalid/")),
    ] {
        let mut input = f.input();
        *input.pointer_mut(pointer).unwrap() = value;
        assert_eq!(
            check_attach(
                loaded.clone(),
                f.ready(&input),
                "Fleet_1",
                Provisioning::Managed
            )
            .unwrap_err()
            .to_string(),
            "incompatible_cluster",
            "{pointer}"
        );
    }
    let mut input = f.input();
    let timing = serde_json::to_value(Timing::default()).unwrap();
    let key = timing.as_object().unwrap().keys().next().unwrap();
    input["lifecycle"] = json!({key:timing[key].as_u64().unwrap()+1});
    assert!(
        check_attach(
            loaded.clone(),
            f.ready(&input),
            "Fleet_1",
            Provisioning::Managed
        )
        .is_err()
    );
    f.write("token", b"different-token");
    assert!(
        check_attach(
            loaded.clone(),
            f.ready(&f.input()),
            "Fleet_1",
            Provisioning::Managed
        )
        .is_err()
    );
    f.write("token", b"owned-token");
    f.write("auth",serde_json::to_vec(&json!({"admin_token":"B".repeat(43),"data_token":"D".repeat(43),"enrollment_token":"E".repeat(43)})).unwrap());
    assert!(
        check_attach(
            loaded.clone(),
            f.ready(&f.input()),
            "Fleet_1",
            Provisioning::Managed
        )
        .is_err()
    );
    assert_eq!(s.backend().state.lock().unwrap().puts, 0);
    for kind in 0..8 {
        let clean = Fixture::new();
        let mut changed = candidate(&clean.ready(&clean.input()), true);
        match kind {
            0 => changed.config.lifecycle_fingerprint = "0".repeat(64),
            1 => changed.config.worker_semantic_fingerprint = "0".repeat(64),
            2 => {
                changed.template.queue_size += 1;
                changed.config.template_fingerprint = changed.template.fingerprint().unwrap();
            }
            3 => changed.timing.lease_ms += 1,
            4 => changed.config.template_fingerprint = "0".repeat(64),
            5 | 6 => {
                let crate::cluster::configuration::Storage::Http(options) =
                    &mut changed.template.storage
                else {
                    unreachable!()
                };
                if kind == 5 {
                    options.namespace = "other".into();
                } else {
                    options.base_url = "http://other.invalid/".into();
                }
                changed.config.template_fingerprint = changed.template.fingerprint().unwrap();
            }
            _ => {
                changed.template.tls = Some(crate::cluster::configuration::Tls {
                    cert_file: "/foreign/cert".into(),
                    key_file: "/foreign/key".into(),
                    ca_file: "/foreign/ca".into(),
                });
                changed.config.template_fingerprint = changed.template.fingerprint().unwrap();
            }
        }
        if kind != 4 {
            changed.validate().unwrap();
        }
        let s = store(None);
        // Deliberate exact-hash corruption must be encoded as raw bytes because
        // the real codec itself already refuses it.
        s.backend().state.lock().unwrap().objects.insert(
            KEY.into(),
            (serde_json::to_vec(&changed).unwrap(), "captured".into()),
        );
        let result = control_result(s.load_control(deadline()).await).and_then(|base| {
            check_attach(
                base,
                clean.ready(&clean.input()),
                "Fleet_1",
                Provisioning::Managed,
            )
        });
        assert_eq!(
            result.unwrap_err().to_string(),
            "incompatible_cluster",
            "kind {kind}"
        );
        assert_eq!(s.backend().state.lock().unwrap().puts, 0);
    }
}
#[tokio::test]
async fn foreign_hints_never_read_and_tls_uses_retained_material() {
    use crate::cluster::startup_material::files::{observe_reads, tests::create_tls_fixture};
    let f = Fixture::new();
    create_tls_fixture(&f.0);
    let g = Fixture::new();
    fs::create_dir(g.0.join("tls")).unwrap();
    for name in ["server.crt", "server.key", "ca.crt"] {
        fs::copy(f.0.join("tls").join(name), g.0.join("tls").join(name)).unwrap();
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(
            g.0.join("tls/server.key"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
    }
    let mut input = f.input();
    input["tls"] =
        json!({"cert_file":"tls/server.crt","key_file":"tls/server.key","ca_file":"tls/ca.crt"});
    input["api_urls"] = json!(["https://api.invalid:8000"]);
    input["advertise_url"] = "https://api.invalid:8000".into();
    input["storage"]["options"]["ca_file"] = "tls/ca.crt".into();
    let local = f.ready(&input);
    let mut root = candidate(&local, true);
    let foreign = f.0.join("never-exists-foreign");
    if let crate::cluster::configuration::Storage::Http(options) = &mut root.template.storage {
        options.token_file = foreign.join("token").to_str().unwrap().into();
        options.ca_file = Some(foreign.join("ca").to_str().unwrap().into());
    }
    let tls = root.template.tls.as_mut().unwrap();
    tls.cert_file = foreign.join("cert").to_str().unwrap().into();
    tls.key_file = foreign.join("key").to_str().unwrap().into();
    tls.ca_file = foreign.join("ca").to_str().unwrap().into();
    root.config.template_fingerprint = root.template.fingerprint().unwrap();
    let s = store(Some(&root));
    let loaded = s.load_control(deadline()).await.unwrap();
    // PEM whitespace changes exact bytes but retains certificate/root identities.
    let pem = fs::read(g.0.join("tls/ca.crt")).unwrap();
    let mut padded = b"\n".to_vec();
    padded.extend_from_slice(&pem);
    padded.extend_from_slice(&pem);
    g.write("tls/ca.crt", padded);
    let other = g.ready(&input);
    let changed = Fixture::new();
    create_tls_fixture(&changed.0);
    // Each preparation still has valid matching key/leaf material. Change only
    // one independently bound identity, with all other inputs held fixed.
    for kind in 0..3 {
        let mut altered = input.clone();
        let ca = changed.0.join("tls/ca.crt").to_str().unwrap().to_owned();
        match kind {
            0 => altered["storage"]["options"]["ca_file"] = ca.into(),
            1 => altered["tls"]["ca_file"] = ca.into(),
            _ => {
                altered["tls"]["cert_file"] =
                    changed.0.join("tls/server.crt").to_str().unwrap().into();
                altered["tls"]["key_file"] =
                    changed.0.join("tls/server.key").to_str().unwrap().into();
            }
        }
        let result = check_attach(
            loaded.clone(),
            g.ready(&altered),
            "Fleet_1",
            Provisioning::Managed,
        );
        assert_eq!(
            result.unwrap_err().to_string(),
            "incompatible_cluster",
            "TLS case {kind}"
        );
    }
    for name in [
        "tls/server.crt",
        "tls/server.key",
        "tls/ca.crt",
        "token",
        "auth",
    ] {
        fs::remove_file(f.0.join(name)).unwrap();
        fs::remove_file(g.0.join(name)).unwrap();
    }
    let (attached, attempts) =
        observe_reads(|| check_attach(loaded.clone(), other, "Fleet_1", Provisioning::Managed));
    let attached = attached.unwrap();
    assert!(attempts.is_empty());
    assert_eq!(attached.loaded, loaded);
    assert_eq!(attached.loaded.root().template, root.template);
    // Prove the observation seam sees a real failed read attempt too.
    let (_, attempts) = observe_reads(|| {
        prepare_api(
            &foreign.join("api.json"),
            Instant::now() + Duration::from_secs(5),
        )
    });
    assert_eq!(attempts, vec![foreign.join("api.json")]);
    assert_eq!(s.backend().state.lock().unwrap().puts, 0);
}

#[tokio::test]
async fn callback_enrollment_retains_material_and_checks_immutable_root() {
    let f = Fixture::new();
    let local = f.ready(&f.input());
    let root = candidate(&local, true);
    let s = store(Some(&root));
    let attached = check_attach(
        s.load_control(deadline()).await.unwrap(),
        local,
        "Fleet_1",
        Provisioning::Managed,
    )
    .unwrap();
    fs::remove_file(f.0.join("auth")).unwrap();
    assert!(attached.callback_enrollment(&root).is_ok());
    let mut progress = root.clone();
    progress.revision += 1;
    progress.scheduler.failures += 1;
    progress.policy.revision += 1;
    progress.policy.desired = 1;
    progress.policy.min_nodes = 1;
    progress.policy.max_nodes = 1;
    progress.controller.owner_id = Some(uuid::Uuid::from_u128(77));
    progress.controller.epoch = 2;
    progress.controller.lease_state = crate::cluster::state::LeaseState::Held;
    progress.controller.lease_until_ms = Some(1000);
    assert!(attached.callback_enrollment(&progress).is_ok());
    for kind in 0..7 {
        let mut changed = root.clone();
        match kind {
            0 => changed.cluster_id = uuid::Uuid::new_v4(),
            1 => changed.name = "Other".into(),
            2 => changed.config.auth_fingerprint = "0".repeat(64),
            3 => changed.template.listen = "other:1".into(),
            4 => changed.timing.lease_ms += 1,
            5 => changed.provisioning = Provisioning::External,
            _ => changed.revision = 0,
        }
        assert_eq!(
            attached.callback_enrollment(&changed).unwrap_err(),
            StartupError::IncompatibleCluster
        );
    }
}
