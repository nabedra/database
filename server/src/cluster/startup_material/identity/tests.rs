use super::*;
#[test]
fn secret_fixed_vector() {
    assert_eq!(
        secret(Role::Admin, b"abc").unwrap().0,
        "7b453f0cb92fd6a3831da14d64a10217fcbf15b679e476071afbad53ad8308a2"
    );
}

fn wire(value: &impl serde::Serialize, expected: &str, label: &str, digest: &str) {
    assert_eq!(bounded(value, MAX_BYTES).unwrap(), expected.as_bytes());
    assert_eq!(hash(label, value).unwrap().as_str(), digest);
}
#[test]
fn fixed_canonical_vectors() {
    wire(
        &Secret {
            role: "admin",
            sha256: hex(&Sha256::digest(b"abc")),
        },
        r#"{"role":"admin","sha256":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}"#,
        SECRET,
        "7b453f0cb92fd6a3831da14d64a10217fcbf15b679e476071afbad53ad8308a2",
    );
    wire(
        &Certificates {
            certificates: vec![Hex(&[0, 255]), Hex(&[1])],
        },
        r#"{"certificates":["00ff","01"]}"#,
        ROOTS,
        "b0a711e387a7bca75e1a708b31ac76ee4821e248a01bc28aa539ee41ed55c097",
    );
    wire(
        &Certificates {
            certificates: vec![Hex(&[1]), Hex(&[0, 255])],
        },
        r#"{"certificates":["01","00ff"]}"#,
        CHAIN,
        "36abd02b3cda00aa28e3a0a4a2380e00b083a0549994b40f8fe555719e29a6fb",
    );
    wire(
        &Http {
            base_url: "http://Store:80",
            ca: None,
            driver: "http",
            namespace: "n/",
            token: "token",
        },
        r#"{"base_url":"http://Store:80","ca":null,"driver":"http","namespace":"n/","token":"token"}"#,
        STORAGE,
        "a11bc8a475e4448ccc75d205a3b11918831bcab53b366dcd9442cd0ab5a31141",
    );
    wire(
        &S3 {
            bucket: "bucket",
            driver: "s3",
            endpoint_policy: "aws-s3-standard.v1",
            namespace: "n/",
            region: "eu-west-1",
        },
        r#"{"bucket":"bucket","driver":"s3","endpoint_policy":"aws-s3-standard.v1","namespace":"n/","region":"eu-west-1"}"#,
        STORAGE,
        "735390624a387e7a9f949790896120c609c8777eacc4848f9de24ce0c7b1ed14",
    );
    wire(
        &AuthWire {
            admin: "a",
            cluster_id: "00000000-0000-0000-0000-000000000001",
            data: "d",
            enrollment: "e",
        },
        r#"{"admin":"a","cluster_id":"00000000-0000-0000-0000-000000000001","data":"d","enrollment":"e"}"#,
        AUTH,
        "a71002203ea6ba27e421942df88d9aca6e933a9dd9a0329938e1d40439d87793",
    );
    wire(
        &Timing::default(),
        r#"{"cooldown_ms":30000,"drain_ms":300000,"expiry_ms":15000,"heartbeat_ms":5000,"lease_ms":15000,"max_attempts":8,"readiness_ms":300000,"renew_ms":5000,"retry_initial_ms":1000,"retry_max_ms":30000,"stabilization_ms":60000}"#,
        LIFECYCLE,
        "90edaa74a67d2683a493238a8f844411341b0159e1f5f7fdb0ac3687ff4152f4",
    );
    wire(
        &Worker {
            mode: Default::default(),
            execution: Default::default(),
            resources: None,
            api_urls: &["http://a".into(), "http://b".into()],
            listen: "127.0.0.1:0",
            operation_slots: 2,
            partition_bytes: 1048576,
            partition_slots: 3,
            queue_size: 4,
            storage_fingerprint: "s",
            tls: None,
        },
        r#"{"api_urls":["http://a","http://b"],"listen":"127.0.0.1:0","operation_slots":2,"partition_bytes":1048576,"partition_slots":3,"queue_size":4,"storage_fingerprint":"s","tls":null}"#,
        WORKER,
        "f26070afc64d9f73a2f831b2760c0fbd6a5588b408ee1ed92e8409037d8c6096",
    );
    wire(
        &Worker {
            mode: Default::default(),
            execution: Default::default(),
            resources: None,
            api_urls: &["http://a".into(), "http://b".into()],
            listen: "127.0.0.1:0",
            operation_slots: 2,
            partition_bytes: 1048576,
            partition_slots: 3,
            queue_size: 4,
            storage_fingerprint: "s",
            tls: Some(TlsWire {
                ca: "r",
                certificate_chain: "c",
            }),
        },
        r#"{"api_urls":["http://a","http://b"],"listen":"127.0.0.1:0","operation_slots":2,"partition_bytes":1048576,"partition_slots":3,"queue_size":4,"storage_fingerprint":"s","tls":{"ca":"r","certificate_chain":"c"}}"#,
        WORKER,
        "278f788c9b2ee17a2fd435976a94d70021900bbc32a65265f76e07fe0176f077",
    );
}
#[test]
fn bounded_identity_includes_json_syntax() {
    let exact = "x".repeat(MAX_BYTES - 2);
    assert_eq!(bounded(&exact, MAX_BYTES).unwrap().len(), MAX_BYTES);
    assert!(hash("meshdb.config-secret.v1", &exact).is_ok());
    assert_eq!(
        hash("meshdb.config-secret.v1", &(exact.clone() + "x")),
        Err(ConfigurationError::LimitExceeded)
    );
    assert_eq!(
        bounded(&(exact + "x"), MAX_BYTES),
        Err(ConfigurationError::LimitExceeded)
    );
}

fn template() -> Template {
    Template {
        mode: Default::default(),
        execution: Default::default(),
        resources: None,
        api_urls: vec!["http://a".into(), "http://b".into()],
        listen: "127.0.0.1:0".into(),
        operation_slots: 2,
        partition_size: "1 MiB".into(),
        partition_slots: 3,
        queue_size: 4,
        storage: super::super::super::configuration::Storage::Http(
            super::super::super::configuration::HttpOptions {
                base_url: "http://store".into(),
                ca_file: None,
                namespace: "n".into(),
                token_file: "token".into(),
            },
        ),
        tls: None,
    }
}
#[test]
fn worker_semantics_and_storage_normalization() {
    let token = HttpToken {
        bytes: std::sync::Arc::from(&b"abc\n"[..]),
        value: "abc".into(),
    };
    let ns = Namespace::new("n///").unwrap();
    let storage = http("http://Store:80///", &ns, &token, None).unwrap();
    assert_eq!(
        storage,
        http(
            "http://Store:80",
            &Namespace::new("n").unwrap(),
            &token,
            None
        )
        .unwrap()
    );
    for url in ["http://store:80", "http://Store"] {
        assert_ne!(storage, http(url, &ns, &token, None).unwrap());
    }
    assert_ne!(
        s3("bucket", &ns, "eu-west-1").unwrap(),
        s3("bucket", &ns, "eu-west-2").unwrap()
    );
    let original = template();
    let expected = worker(&original, &storage, None).unwrap();
    let mut equivalent = original.clone();
    equivalent.partition_size = "1048576 B".into();
    if let super::super::super::configuration::Storage::Http(o) = &mut equivalent.storage {
        o.token_file = "relocated".into();
    }
    assert_eq!(expected, worker(&equivalent, &storage, None).unwrap());
    assert_ne!(
        original.fingerprint().unwrap(),
        equivalent.fingerprint().unwrap()
    );
    for index in 0..6 {
        let mut changed = original.clone();
        match index {
            0 => changed.api_urls.reverse(),
            1 => changed.listen = "127.0.0.1:1".into(),
            2 => changed.operation_slots += 1,
            3 => changed.partition_slots += 1,
            4 => changed.queue_size += 1,
            _ => changed.partition_size = "2 MiB".into(),
        }
        assert_ne!(expected, worker(&changed, &storage, None).unwrap());
    }
    let secrets: Vec<_> = [Role::Admin, Role::Data, Role::Enrollment, Role::Http]
        .into_iter()
        .map(|r| secret(r, b"abc").unwrap())
        .collect();
    for (i, a) in secrets.iter().enumerate() {
        for b in &secrets[i + 1..] {
            assert_ne!(a, b);
        }
    }
    assert_ne!(
        secret(Role::Admin, b"abc").unwrap(),
        secret(Role::Admin, b"abc ").unwrap()
    );
    assert_eq!(format!("{storage:?}"), "Fingerprint { .. }");
}

struct Temp(std::path::PathBuf);
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn reader(dir: &std::path::Path) -> super::super::files::Reader {
    super::super::files::Reader::new(
        &dir.join("input"),
        std::time::Instant::now() + std::time::Duration::from_secs(30),
    )
    .unwrap()
}
fn private_write(path: &std::path::Path, bytes: &[u8]) {
    std::fs::write(path, bytes).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
}
#[test]
fn loaded_material_equivalence_and_differences() {
    use std::fs;
    let dir = Temp(std::env::temp_dir().join(format!("nabedra-identity-{}", uuid::Uuid::new_v4())));
    fs::create_dir(&dir.0).unwrap();
    super::super::files::tests::create_tls_fixture(&dir.0);
    let hints = super::super::super::configuration::Tls {
        ca_file: "tls/ca.crt".into(),
        cert_file: "tls/server.crt".into(),
        key_file: "tls/server.key".into(),
    };
    let tls = reader(&dir.0).tls(&hints).unwrap();
    let ca = fs::read(dir.0.join("tls/ca.crt")).unwrap();
    let leaf = fs::read(dir.0.join("tls/server.crt")).unwrap();
    let mut bundle = b"\n \n".to_vec();
    bundle.extend(&ca);
    bundle.extend(&leaf);
    bundle.extend(&ca);
    fs::write(dir.0.join("roots-a"), &bundle).unwrap();
    let mut reverse = leaf.clone();
    reverse.extend(&ca);
    fs::write(dir.0.join("roots-b"), reverse).unwrap();
    let a = reader(&dir.0)
        .http_ca(false, Some("roots-a"))
        .unwrap()
        .unwrap();
    let b = reader(&dir.0)
        .http_ca(false, Some("roots-b"))
        .unwrap()
        .unwrap();
    assert_eq!(roots(&a).unwrap(), roots(&b).unwrap());
    assert_ne!(roots(&a).unwrap(), roots(&tls.ca).unwrap());
    // Decode the same private key into DER without spawning another fixture process.
    use rustls::pki_types::{PrivateKeyDer, pem::PemObject};
    let key = PrivateKeyDer::from_pem_slice(&tls.key_bytes).unwrap();
    private_write(&dir.0.join("key.der"), key.secret_der());
    fs::write(dir.0.join("chain-copy"), &tls.chain_pem).unwrap();
    fs::write(dir.0.join("ca-copy"), &ca).unwrap();
    let relocated = super::super::super::configuration::Tls {
        ca_file: "ca-copy".into(),
        cert_file: "chain-copy".into(),
        key_file: "key.der".into(),
    };
    let mut same = reader(&dir.0).tls(&relocated).unwrap();
    let mut original_template = template();
    original_template.tls = Some(hints.clone());
    let mut relocated_template = original_template.clone();
    relocated_template.tls = Some(relocated.clone());
    let bound_storage = Fingerprint("s".into());
    assert_eq!(
        worker(&original_template, &bound_storage, Some(&tls)).unwrap(),
        worker(&relocated_template, &bound_storage, Some(&same)).unwrap()
    );
    assert_ne!(
        original_template.fingerprint().unwrap(),
        relocated_template.fingerprint().unwrap()
    );
    let t = template();
    let storage = Fingerprint("s".into());
    assert_eq!(
        worker(&t, &storage, Some(&tls)).unwrap(),
        worker(&t, &storage, Some(&same)).unwrap()
    );
    assert_ne!(
        worker(&t, &storage, None).unwrap(),
        worker(&t, &storage, Some(&tls)).unwrap()
    );
    let mut chain = leaf.clone();
    chain.extend(&ca);
    fs::write(dir.0.join("chain-copy"), &chain).unwrap();
    let longer = reader(&dir.0).tls(&relocated).unwrap();
    assert_ne!(
        worker(&t, &storage, Some(&tls)).unwrap(),
        worker(&t, &storage, Some(&longer)).unwrap()
    );
    // The loader certifies matching keys; ordered chain hashing preserves every certificate.
    let mut reversed = longer.certified_key.cert.clone();
    reversed.reverse();
    assert_ne!(
        certificates(CHAIN, &longer.certified_key.cert, false).unwrap(),
        certificates(CHAIN, &reversed, false).unwrap()
    );
    assert_ne!(
        certificates(CHAIN, &tls.certified_key.cert, false).unwrap(),
        certificates(CHAIN, &tls.ca.der, false).unwrap()
    );
    private_write(&dir.0.join("token"), b"abc\n");
    let token = reader(&dir.0).http_token("token").unwrap();
    assert_eq!(
        secret(Role::Http, token.value.as_bytes()).unwrap(),
        secret(Role::Http, b"abc").unwrap()
    );
    private_write(&dir.0.join("auth"), br#"{"admin_token":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","data_token":"ddddddddddddddddddddddddddddddddddddddddddd","enrollment_token":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}"#);
    let auth = reader(&dir.0).auth("auth").unwrap();
    let id = uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    assert_ne!(
        authentication(id, &auth).unwrap(),
        authentication(uuid::Uuid::nil(), &auth).unwrap()
    );
    let mut changed = reader(&dir.0).auth("auth").unwrap();
    changed.data_token.push('x');
    assert_ne!(
        authentication(id, &auth).unwrap(),
        authentication(id, &changed).unwrap()
    );
    let mut timing = Timing::default();
    timing.cooldown_ms += 1;
    assert_ne!(
        lifecycle(&Timing::default()).unwrap(),
        lifecycle(&timing).unwrap()
    );
    let mut changed_token = reader(&dir.0).http_token("token").unwrap();
    changed_token.value.push('x');
    let ns = Namespace::new("n").unwrap();
    assert_ne!(
        http("http://store", &ns, &token, None).unwrap(),
        http("http://store", &ns, &changed_token, None).unwrap()
    );
    same.ca = reader(&dir.0)
        .http_ca(false, Some("roots-a"))
        .unwrap()
        .unwrap();
    assert_ne!(
        worker(&t, &storage, Some(&tls)).unwrap(),
        worker(&t, &storage, Some(&same)).unwrap()
    );
    let ca_storage = http(
        "http://store",
        &Namespace::new("n").unwrap(),
        &token,
        Some(&tls.ca),
    )
    .unwrap();
    assert_ne!(
        ca_storage,
        http(
            "http://store",
            &Namespace::new("n").unwrap(),
            &token,
            Some(&a)
        )
        .unwrap()
    );
}

#[test]
fn concrete_function_vectors() {
    assert_eq!(
        secret(Role::Admin, b"abc").unwrap().as_str(),
        "7b453f0cb92fd6a3831da14d64a10217fcbf15b679e476071afbad53ad8308a2"
    );
    assert_eq!(
        secret(Role::Data, b"abc").unwrap().as_str(),
        "fb9eb02340714bbe8ce25cae6ed2e33a39f902d75b68b02c319a51d22211b496"
    );
    assert_eq!(
        secret(Role::Enrollment, b"abc").unwrap().as_str(),
        "8bf3678a9a9f3318c0de2f734e0f28f62e20cc72f4cb11a1c3038e64e759d3d9"
    );
    assert_eq!(
        secret(Role::Http, b"abc").unwrap().as_str(),
        "0519ade72254377e3c822f08cfda0e2de391ee8769ed7cb795e22fc991e2805d"
    );
    let ns = Namespace::new("n").unwrap();
    let token = HttpToken {
        bytes: std::sync::Arc::from(&b"abc\n"[..]),
        value: "abc".into(),
    };
    assert_eq!(
        http("http://Store:80///", &ns, &token, None)
            .unwrap()
            .as_str(),
        "18854f1cf7afd4788dc5d86442e2cb211f101309046be8a3807cf45de4bea582"
    );
    assert_eq!(
        s3("bucket", &ns, "eu-west-1").unwrap().as_str(),
        "735390624a387e7a9f949790896120c609c8777eacc4848f9de24ce0c7b1ed14"
    );
    assert_eq!(
        worker(&template(), &Fingerprint("s".into()), None)
            .unwrap()
            .as_str(),
        "f26070afc64d9f73a2f831b2760c0fbd6a5588b408ee1ed92e8409037d8c6096"
    );
    let auth = Auth {
        bytes: std::sync::Arc::from([]),
        admin_token: "abc".into(),
        data_token: "def".into(),
        enrollment_token: "ghi".into(),
    };
    assert_eq!(
        authentication(uuid::Uuid::from_u128(1), &auth)
            .unwrap()
            .as_str(),
        "a192e210d842bf77d21219cc4e18b051ec0a59011625662eae0797d76ac439e1"
    );
}

#[test]
fn local_api_fields_are_excluded_from_worker_identity() {
    let dir =
        Temp(std::env::temp_dir().join(format!("nabedra-identity-api-{}", uuid::Uuid::new_v4())));
    std::fs::create_dir(&dir.0).unwrap();
    let original = r#"{"version":1,"cluster_id":"00000000-0000-0000-0000-000000000001","bind":"127.0.0.1:0","advertise_url":"http://api:80","api_urls":["http://a","http://b"],"storage":{"driver":"http","options":{"base_url":"http://store","namespace":"n","token_file":"token","ca_file":null}},"auth_file":"auth","tls":null,"worker":{"listen":"127.0.0.1:0","operation_slots":2,"partition_slots":3,"queue_size":4,"partition_size":"1 MiB"}}"#;
    std::fs::write(dir.0.join("input"), original).unwrap();
    let a = reader(&dir.0).api().unwrap();
    let changed = original
        .replace("\"bind\":\"127.0.0.1:0\"", "\"bind\":\"127.0.0.1:1234\"")
        .replace("http://api:80", "http://other:81")
        .replace("\"auth_file\":\"auth\"", "\"auth_file\":\"different\"");
    std::fs::write(dir.0.join("input"), changed).unwrap();
    let b = reader(&dir.0).api().unwrap();
    assert_ne!(a.value.bind, b.value.bind);
    assert_ne!(a.value.advertise_url, b.value.advertise_url);
    assert_ne!(a.value.auth_file, b.value.auth_file);
    let storage = Fingerprint("s".into());
    assert_eq!(
        worker(&a.value.template, &storage, None).unwrap(),
        worker(&b.value.template, &storage, None).unwrap()
    );
}
