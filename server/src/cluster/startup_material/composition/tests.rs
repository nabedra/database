use super::super::files::tests::create_tls_fixture;
use super::*;
use std::{fs, time::Duration};
struct Fixture(std::path::PathBuf);
impl Fixture {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("meshdb-composition-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        Self(path)
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
    fn input(&self, bootstrap: bool, https: bool, ca: bool, tls: bool) -> Vec<u8> {
        let storage = serde_json::json!({"driver":"http", "options":{"base_url":if https {"https://STORE.invalid:443/objects/"} else {"http://STORE.invalid:80/objects/"},"namespace":"owned/path/","token_file":"$literal ~ token","ca_file":if ca {Some("tls/ca.crt")} else {None}}});
        let tls = if tls {
            serde_json::json!({"cert_file":"tls/server.crt","key_file":"tls/server.key","ca_file":"tls/ca.crt"})
        } else {
            serde_json::Value::Null
        };
        let scheme = if tls.is_null() { "http" } else { "https" };
        let mut value = serde_json::json!({"version":1,"cluster_id":"12345678-1234-4234-8234-123456789abc","api_urls":[format!("{scheme}://api.invalid:8000")],"storage":storage,"tls":tls});
        let worker = serde_json::json!({"listen":"127.0.0.1:8001","operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"});
        if bootstrap {
            for (key, v) in worker.as_object().unwrap() {
                value[key] = v.clone();
            }
            value["node_id"] = "12345678-1234-4234-8234-123456789abd".into();
            value["request_id"] = serde_json::Value::Null;
            value["advertise_url"] = format!("{scheme}://worker.invalid:8001").into();
            value["enrollment_token"] = "E".repeat(43).into();
        } else {
            value["worker"] = worker;
            value["bind"] = "127.0.0.1:8000".into();
            value["advertise_url"] = format!("{scheme}://api.invalid:8000").into();
            value["auth_file"] = "auth.json".into();
        }
        let bytes = serde_json::to_vec(&value).unwrap();
        self.write("input.json", &bytes);
        self.write("auth.json", serde_json::to_vec(&serde_json::json!({"admin_token":"A".repeat(43),"data_token":"D".repeat(43),"enrollment_token":"E".repeat(43)})).unwrap());
        self.write("$literal ~ token", b"HTTP-SECRET-SENTINEL\n");
        bytes
    }
    fn reader(&self) -> Reader {
        Reader::new(&self.0.join("input.json"), deadline()).unwrap()
    }
    fn remove_originals(&self) {
        for name in [
            "input.json",
            "auth.json",
            "$literal ~ token",
            "tls/ca.crt",
            "tls/server.crt",
            "tls/server.key",
        ] {
            let _ = fs::remove_file(self.0.join(name));
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}
fn assert_tls(tls: &TlsMaterial) {
    tls.certified_key.keys_match().unwrap();
    assert!(!tls.ca.roots.is_empty());
    assert!(
        tls.certified_key
            .key
            .choose_scheme(&[
                rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
                rustls::SignatureScheme::RSA_PSS_SHA256
            ])
            .is_some()
    );
}
fn assert_redacted(value: &impl fmt::Debug, path: &Path) {
    let debug = format!("{value:?}");
    for sentinel in [
        "HTTP-SECRET-SENTINEL",
        &"A".repeat(43),
        &"E".repeat(43),
        "PRIVATE KEY",
        "CERTIFICATE",
        path.to_str().unwrap(),
    ] {
        assert!(!debug.contains(sentinel));
    }
}
#[test]
fn api_retains_local_material_for_actual_http_construction() {
    let f = Fixture::new();
    create_tls_fixture(&f.0);
    let bytes = f.input(false, true, true, true);
    let prepared = prepare_api(&f.0.join("input.json"), deadline()).unwrap();
    assert!(prepared.input.bytes.as_ref() == bytes);
    assert_eq!(
        prepared.input.value.template,
        crate::cluster::startup_config::decode_api(&bytes)
            .unwrap()
            .template
    );
    assert!(prepared.auth.bytes.as_ref() == fs::read(f.0.join("auth.json")).unwrap());
    assert!(prepared.input.value.auth_file == "auth.json");
    let exact = prepared.input.value.template.fingerprint().unwrap();
    let ca = prepared.backend.ca.as_ref().unwrap().pem.clone();
    let auth = identity::authentication(prepared.input.value.cluster_id, &prepared.auth).unwrap();
    let lifecycle = identity::lifecycle(&prepared.input.value.lifecycle).unwrap();
    let storage = identity::http(
        &prepared.backend.base,
        &prepared.backend.namespace,
        &prepared.backend.token,
        prepared.backend.ca.as_ref(),
    )
    .unwrap();
    let worker = identity::worker(
        &prepared.input.value.template,
        &storage,
        prepared.tls.as_ref(),
    )
    .unwrap();
    assert_redacted(&prepared, &f.0);
    f.remove_originals();
    f.write("$literal ~ token", b"REPLACED");
    f.write("auth.json", b"broken");
    let ready = prepared.connect().unwrap();
    assert_redacted(&ready, &f.0);
    assert_eq!(ready.authentication, auth);
    assert_eq!(ready.lifecycle, lifecycle);
    assert_eq!(ready.backend.storage, storage);
    assert_eq!(ready.backend.worker, worker);
    assert!(ready.prepared.input.value.template.fingerprint().unwrap() == exact);
    assert_tls(ready.prepared.tls.as_ref().unwrap());
    crate::storage::http::startup_tests::assert_material(
        &ready.backend.client,
        "https://STORE.invalid:443/objects",
        "owned/path/",
        "HTTP-SECRET-SENTINEL",
        Some(&ca),
    );
}
#[test]
fn bootstrap_explicit_and_selected_temporary_roots_are_equivalent() {
    let f = Fixture::new();
    create_tls_fixture(&f.0);
    let pem = fs::read(f.0.join("tls/ca.crt")).unwrap();
    f.input(true, true, true, true);
    let explicit = prepare_bootstrap(&f.0.join("input.json"), deadline()).unwrap();
    let bytes = f.input(true, true, false, true);
    f.write("system.pem", &pem);
    let mut reader = f.reader();
    reader.system_candidates = Some(vec![f.0.join("absent"), f.0.join("system.pem")]);
    let selected = bootstrap(&mut reader).unwrap();
    assert!(selected.input.bytes.as_ref() == bytes);
    assert_eq!(
        selected.input.value.template,
        crate::cluster::startup_config::decode_bootstrap(&bytes)
            .unwrap()
            .template
    );
    assert!(selected.input.value.enrollment_token == "E".repeat(43));
    assert!(selected.input.value.template.tls.as_ref().unwrap().key_file == "tls/server.key");
    assert_redacted(&selected, &f.0);
    f.remove_originals();
    fs::remove_file(f.0.join("system.pem")).unwrap();
    let a = explicit.connect().unwrap();
    let b = selected.connect().unwrap();
    assert_eq!(a.backend.storage, b.backend.storage);
    assert_eq!(a.backend.worker, b.backend.worker);
    assert!(
        a.prepared.input.value.template.fingerprint().unwrap()
            != b.prepared.input.value.template.fingerprint().unwrap()
    );
    for ready in [&a, &b] {
        assert_tls(ready.prepared.tls.as_ref().unwrap());
        crate::storage::http::startup_tests::assert_material(
            &ready.backend.client,
            "https://STORE.invalid:443/objects",
            "owned/path/",
            "HTTP-SECRET-SENTINEL",
            Some(&pem),
        );
        assert_redacted(ready, &f.0);
    }
}
#[test]
fn plain_http_retains_supplied_trust_and_null_only_when_unsupplied() {
    let f = Fixture::new();
    create_tls_fixture(&f.0);
    f.input(true, false, true, false);
    let supplied = prepare_bootstrap(&f.0.join("input.json"), deadline()).unwrap();
    let pem = supplied.backend.ca.as_ref().unwrap().pem.clone();
    f.input(true, false, false, false);
    let null = prepare_bootstrap(&f.0.join("input.json"), deadline()).unwrap();
    f.remove_originals();
    let a = supplied.connect().unwrap();
    let b = null.connect().unwrap();
    assert_ne!(a.backend.storage, b.backend.storage);
    crate::storage::http::startup_tests::assert_material(
        &a.backend.client,
        "http://STORE.invalid:80/objects",
        "owned/path/",
        "HTTP-SECRET-SENTINEL",
        Some(&pem),
    );
    crate::storage::http::startup_tests::assert_material(
        &b.backend.client,
        "http://STORE.invalid:80/objects",
        "owned/path/",
        "HTTP-SECRET-SENTINEL",
        None,
    );
}
#[test]
fn local_failures_are_fixed_invalid_config_without_sources() {
    let f = Fixture::new();
    f.input(false, false, false, false);
    assert_eq!(
        prepare_api(&f.0.join("input.json"), Instant::now()).unwrap_err(),
        StartupError::InvalidConfig
    );
    assert_eq!(
        prepare_api(Path::new("relative"), deadline()).unwrap_err(),
        StartupError::InvalidConfig
    );
    f.write("$literal ~ token", vec![b'x'; 4097]);
    let error = prepare_api(&f.0.join("input.json"), deadline()).unwrap_err();
    assert_eq!(error.to_string(), "invalid_config");
    assert!(std::error::Error::source(&error).is_none());
    assert_eq!(format!("{error:?}"), "InvalidConfig");
}

#[test]
fn constructor_rejection_and_unimplemented_s3_are_sanitized() {
    let f = Fixture::new();
    f.input(true, false, false, false);
    let mut prepared = prepare_bootstrap(&f.0.join("input.json"), deadline()).unwrap();
    // Corrupt only the private retained declaration to exercise constructor mapping.
    prepared.backend.base = "http://SENTINEL.invalid/bad%path".into();
    let error = prepared.connect().unwrap_err();
    assert_eq!(error, StartupError::InvalidConfig);
    assert_eq!(error.to_string(), "invalid_config");
    assert!(std::error::Error::source(&error).is_none());
    let mut reader = f.reader();
    let mut template = reader.bootstrap().unwrap().value.template;
    template.storage = Storage::S3(crate::cluster::configuration::S3Options {
        bucket: "synthetic-bucket".into(),
        namespace: "owned".into(),
    });
    assert_eq!(
        material(&mut reader, &template).unwrap_err(),
        StartupError::InvalidConfig
    );
    template.storage = Storage::Http(crate::cluster::configuration::HttpOptions {
        base_url: "http://store.invalid/".into(),
        namespace: "../invalid".into(),
        token_file: "$literal ~ token".into(),
        ca_file: None,
    });
    assert_eq!(
        material(&mut reader, &template).unwrap_err(),
        StartupError::InvalidConfig
    );
}
#[cfg(unix)]
#[test]
fn protected_bootstrap_uses_original_symlink_directory_for_hints() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let f = Fixture::new();
    let target = Fixture::new();
    let bytes = f.input(true, false, false, false);
    target.write("remote.json", &bytes);
    fs::remove_file(f.0.join("input.json")).unwrap();
    symlink(target.0.join("remote.json"), f.0.join("input.json")).unwrap();
    let prepared = prepare_bootstrap(&f.0.join("input.json"), deadline()).unwrap();
    assert!(prepared.input.bytes.as_ref() == bytes);
    fs::set_permissions(
        target.0.join("remote.json"),
        fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    assert_eq!(
        prepare_bootstrap(&f.0.join("input.json"), deadline()).unwrap_err(),
        StartupError::InvalidConfig
    );
    f.remove_originals();
    fs::remove_file(target.0.join("remote.json")).unwrap();
    let ready = prepared.connect().unwrap();
    crate::storage::http::startup_tests::assert_material(
        &ready.backend.client,
        "http://STORE.invalid:80/objects",
        "owned/path/",
        "HTTP-SECRET-SENTINEL",
        None,
    );
}
#[test]
fn selected_bad_roots_and_supplied_http_ca_fail_locally() {
    let f = Fixture::new();
    f.input(true, true, false, false);
    f.write("bad.pem", b"CA-SENTINEL");
    let mut reader = f.reader();
    reader.system_candidates = Some(vec![f.0.join("bad.pem"), f.0.join("never-selected.pem")]);
    assert_eq!(
        bootstrap(&mut reader).unwrap_err(),
        StartupError::InvalidConfig
    );
    f.input(true, false, true, false);
    fs::create_dir(f.0.join("tls")).unwrap();
    f.write("tls/ca.crt", b"CA-SENTINEL");
    assert_eq!(
        prepare_bootstrap(&f.0.join("input.json"), deadline()).unwrap_err(),
        StartupError::InvalidConfig
    );
}
