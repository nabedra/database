#![allow(clippy::disallowed_methods)] // integration tests drive real processes and real time; the phase 7 seam covers server/src
use nabedra_database_server::cluster::configuration::{
    ConfigurationError, MAX_TEMPLATE_BYTES, MAX_TIMING_BYTES, Storage, Template, Timing,
};
use serde_json::{Value, json};

fn fixture() -> Value {
    json!({"api_urls":["http://API.example:8080/"],"listen":"[::1]:0",
        "storage":{"driver":"http","options":{"base_url":"http://store:0/prefix/",
        "namespace":"nabedra/","token_file":"/outgoing/token","ca_file":null}},
        "tls":null,"operation_slots":4,"partition_slots":4,"queue_size":64,
        "partition_size":"64 MiB"})
}
fn decode(value: &Value) -> Result<Template, ConfigurationError> {
    Template::decode(&serde_json::to_vec(value).unwrap())
}
fn template() -> Template {
    decode(&fixture()).unwrap()
}

#[test]
fn round_trip_and_canonical_fingerprint_preserve_complete_outgoing_template() {
    let original = template();
    assert_eq!(
        Template::decode(&original.encode().unwrap()).unwrap(),
        original
    );
    let reordered = br#"{ "tls":null,"storage":{"options":{"token_file":"/outgoing/token","namespace":"nabedra/","ca_file":null,"base_url":"http://store:0/prefix/"},"driver":"http"},"queue_size":64,"partition_slots":4,"partition_size":"64 MiB","operation_slots":4,"listen":"[::1]:0","api_urls":["http://API.example:8080/"] }"#;
    assert_eq!(
        Template::decode(reordered).unwrap().fingerprint().unwrap(),
        original.fingerprint().unwrap()
    );
    let canonical = serde_json::to_vec(&fixture()).unwrap();
    assert_eq!(original.encode().unwrap(), canonical);
    let digest = original.fingerprint().unwrap();
    assert_eq!(
        digest,
        "334e289caddcdde856c9d7e7942f59638ae71a9a96591aa817f16cc7abf26156"
    );
    assert_eq!(digest.len(), 64);
    assert!(
        digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    );
    for (field, value) in [
        ("queue_size", json!(65)),
        ("partition_size", json!("32 MiB")),
        ("listen", json!("worker:0")),
        ("api_urls", json!(["https://api/"])),
    ] {
        let mut changed = fixture();
        changed[field] = value;
        assert_ne!(decode(&changed).unwrap().fingerprint().unwrap(), digest);
    }
    let mut changed = fixture();
    changed["storage"]["options"]["token_file"] = json!("/other/token");
    assert_ne!(decode(&changed).unwrap().fingerprint().unwrap(), digest);
}

#[test]
fn strict_objects_required_nulls_duplicates_and_integer_tokens() {
    for input in [b"[]".as_slice(), b"null", b"{}"] {
        assert!(Template::decode(input).is_err());
    }
    for key in ["tls", "api_urls", "partition_size"] {
        let mut v = fixture();
        v.as_object_mut().unwrap().remove(key);
        assert!(decode(&v).is_err());
    }
    let mut v = fixture();
    v["storage"]["options"]
        .as_object_mut()
        .unwrap()
        .remove("ca_file");
    assert!(decode(&v).is_err());
    for pointer in ["", "/storage", "/storage/options"] {
        let mut v = fixture();
        v.pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("unknown".into(), json!(null));
        assert!(decode(&v).is_err());
    }
    for (needle, replacement) in [
        ("\"tls\":null", "\"tls\":null,\"tls\":null"),
        (
            "\"driver\":\"http\"",
            "\"driver\":\"http\",\"driver\":\"http\"",
        ),
        ("\"ca_file\":null", "\"ca_file\":null,\"ca_file\":null"),
        ("\"operation_slots\":4", "\"operation_slots\":4.0"),
        ("\"operation_slots\":4", "\"operation_slots\":4e0"),
        ("\"operation_slots\":4", "\"operation_slots\":true"),
        (
            "\"operation_slots\":4",
            "\"operation_slots\":18446744073709551616",
        ),
    ] {
        let raw = fixture().to_string().replace(needle, replacement);
        assert!(Template::decode(raw.as_bytes()).is_err(), "{replacement}");
    }
    for pointer in ["/storage", "/storage/options"] {
        let mut v = fixture();
        *v.pointer_mut(pointer).unwrap() = json!([]);
        assert!(decode(&v).is_err());
    }
    let mut v = fixture();
    v["storage"]["driver"] = json!({"http":null});
    assert!(decode(&v).is_err());
    v["storage"]["driver"] = json!("unknown");
    assert!(decode(&v).is_err());
}

#[test]
fn advertised_and_bind_authorities_have_distinct_port_and_path_rules() {
    for url in [
        "http://host:0/",
        "http://host:/",
        "http://host:65536/",
        "http://host:abc/",
        "http://user@host/",
        "http://host/path",
        "http://host/?q",
        "http://host/#x",
        "http://[bad]/",
        "http:///",
        "ftp://host/",
        "http://höst/",
    ] {
        let mut v = fixture();
        v["api_urls"] = json!([url]);
        assert!(decode(&v).is_err(), "{url}");
    }
    for bind in [
        "localhost:0",
        "127.0.0.1:65535",
        "0.0.0.0:0",
        "255.255.255.255:65535",
        "[::]:8000",
    ] {
        let mut v = fixture();
        v["listen"] = json!(bind);
        assert!(decode(&v).is_ok(), "{bind}");
    }
    for bind in [
        "host",
        "host:",
        "host:65536",
        "::1:8",
        "[bad]:8",
        "user@host:8",
        "host/path:8",
        "host:1x",
        "host :8",
        "999.999.999.999:0",
        "256.0.0.1:80",
        "127.0.0:0",
    ] {
        let mut v = fixture();
        v["listen"] = json!(bind);
        assert!(decode(&v).is_err(), "{bind}");
        let mut constructed = template();
        constructed.listen = bind.into();
        assert!(constructed.validate().is_err(), "{bind}");
        assert!(constructed.encode().is_err(), "{bind}");
    }
    for url in [
        "http://store:/prefix",
        "http://store:65536/",
        "http://store/%61",
        "http://store/a//b",
        "http://store/a/../b",
        "http://u@store/",
        "http://store/?q",
    ] {
        let mut v = fixture();
        v["storage"]["options"]["base_url"] = json!(url);
        assert!(decode(&v).is_err(), "{url}");
    }
}

#[test]
fn constructed_candidates_enforce_paths_capacities_and_encoded_limits() {
    for (field, value) in [
        ("api_urls", json!([])),
        ("api_urls", json!(vec!["http://api/"; 17])),
        ("operation_slots", json!(0)),
        ("partition_slots", json!(257)),
        ("queue_size", json!(4097)),
        ("partition_size", json!("65 MiB")),
        ("partition_size", json!("64")),
        ("listen", json!("a".repeat(257))),
    ] {
        let mut v = fixture();
        v[field] = value;
        assert!(decode(&v).is_err());
    }
    for (field, value) in [
        ("namespace", "../bad".into()),
        ("namespace", "a".repeat(1025)),
        ("token_file", String::new()),
        ("ca_file", String::new()),
        ("token_file", "a".repeat(2049)),
    ] {
        let mut v = fixture();
        v["storage"]["options"][field] = json!(value);
        assert!(decode(&v).is_err());
    }
    let mut v = fixture();
    v["tls"] = json!({"cert_file":"/absent/cert","key_file":"/absent/key","ca_file":"/absent/ca"});
    assert!(decode(&v).is_ok());
    for tls in [
        json!([]),
        json!({"cert_file":"x","key_file":"y"}),
        json!({"cert_file":"","key_file":"y","ca_file":"z"}),
        json!({"cert_file":"x","key_file":"y","ca_file":null}),
        json!({"cert_file":"x","key_file":"y","ca_file":"z","extra":0}),
    ] {
        v["tls"] = tls;
        assert!(decode(&v).is_err());
    }
    let mut t = template();
    t.operation_slots = 0;
    assert!(t.validate().is_err());
    assert!(t.encode().is_err());
    assert!(t.fingerprint().is_err());
    let mut t = template();
    if let Storage::Http(ref mut options) = t.storage {
        options.token_file.clear();
    }
    assert!(t.encode().is_err());
    let mut t = template();
    t.api_urls = vec![format!("http://{}/", "a".repeat(2039)); 16];
    // Escaped outgoing path strings can exceed the cap while each string remains valid.
    if let Storage::Http(ref mut o) = t.storage {
        o.token_file = "\u{1}".repeat(2048);
        o.ca_file = Some("\u{2}".repeat(2048));
    }
    t.tls = Some(nabedra_database_server::cluster::configuration::Tls {
        cert_file: "\u{3}".repeat(2048),
        key_file: "\u{4}".repeat(2048),
        ca_file: "\u{5}".repeat(2048),
    });
    assert_eq!(t.encode(), Err(ConfigurationError::LimitExceeded));
    assert_eq!(
        Template::decode(&vec![b' '; MAX_TEMPLATE_BYTES + 1]),
        Err(ConfigurationError::LimitExceeded)
    );
    let nested = format!("{}0{}", "[".repeat(33), "]".repeat(33));
    assert!(Template::decode(nested.as_bytes()).is_err());
}

#[test]
fn full_timing_defaults_and_relations_are_checked_on_decode_and_encode() {
    let t = Timing::default();
    let expected = json!({"heartbeat_ms":5000,"expiry_ms":15000,"readiness_ms":300000,"drain_ms":300000,"lease_ms":15000,"renew_ms":5000,"stabilization_ms":60000,"cooldown_ms":30000,"retry_initial_ms":1000,"retry_max_ms":30000,"max_attempts":8});
    assert_eq!(
        serde_json::from_slice::<Value>(&t.encode().unwrap()).unwrap(),
        expected
    );
    assert_eq!(Timing::decode(&t.encode().unwrap()).unwrap(), t);
    for key in expected.as_object().unwrap().keys() {
        let mut v = expected.clone();
        v[key] = json!(0);
        assert!(Timing::decode(v.to_string().as_bytes()).is_err());
        let mut v = expected.clone();
        v.as_object_mut().unwrap().remove(key);
        assert!(Timing::decode(v.to_string().as_bytes()).is_err());
    }
    for raw in [
        expected.to_string().replace("5000", "5e3"),
        expected.to_string().replace(
            "\"max_attempts\":8",
            "\"max_attempts\":8,\"max_attempts\":8",
        ),
        expected
            .to_string()
            .replace("\"max_attempts\":8", "\"max_attempts\":8,\"extra\":1"),
    ] {
        assert!(Timing::decode(raw.as_bytes()).is_err());
    }
    for (lease, renew, valid) in [
        (10, 4, true),
        (10, 5, false),
        (11, 5, true),
        (u64::MAX, u64::MAX, false),
        (u64::MAX, 1, true),
    ] {
        let mut t = t.clone();
        t.lease_ms = lease;
        t.renew_ms = renew;
        assert_eq!(t.validate().is_ok(), valid);
    }
    let mut bad = t.clone();
    bad.heartbeat_ms = bad.expiry_ms;
    assert!(bad.encode().is_err());
    let mut bad = t;
    bad.retry_initial_ms = bad.retry_max_ms + 1;
    assert!(bad.encode().is_err());
    assert!(Timing::decode(b"[]").is_err());
    assert_eq!(
        Timing::decode(&vec![b' '; MAX_TIMING_BYTES + 1]),
        Err(ConfigurationError::LimitExceeded)
    );
}

#[test]
fn s3_requires_feature_and_exact_bucket_options() {
    let mut v = fixture();
    v["storage"] =
        json!({"driver":"s3","options":{"bucket":"nabedra-test","namespace":"nabedra/"}});
    #[cfg(not(feature = "s3"))]
    assert_eq!(decode(&v), Err(ConfigurationError::UnsupportedStorage));
    #[cfg(feature = "s3")]
    {
        assert!(decode(&v).is_ok());
        for bucket in ["ab", "Bad-Bucket", "127.0.0.1", "bad..bucket"] {
            v["storage"]["options"]["bucket"] = json!(bucket);
            assert!(decode(&v).is_err());
        }
    }
}

#[test]
fn outgoing_paths_are_preserved_and_diagnostics_are_static() {
    let mut v = fixture();
    v["tls"] =
        json!({"cert_file":"/target/证书","key_file":"relative/key","ca_file":"relative/ca"});
    v["storage"]["options"]["ca_file"] = json!("/target/store-ca");
    let t = decode(&v).unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&t.encode().unwrap()).unwrap(),
        v
    );
    for debug in [
        format!("{t:?}"),
        format!("{:?}", t.storage),
        format!("{:?}", t.tls),
    ] {
        assert!(!debug.contains("target"));
        assert!(!debug.contains("relative"));
        assert!(!debug.contains("store"));
    }
    let mut raw = v.to_string();
    raw = raw.replace(
        "\"key_file\":\"relative/key\"",
        "\"key_file\":\"relative/key\",\"key_file\":\"secret\"",
    );
    let err = Template::decode(raw.as_bytes()).unwrap_err();
    assert_eq!(err.to_string(), "invalid control configuration");
    assert_eq!(format!("{err:?}"), "InvalidRecord");
    let mut t = t;
    t.partition_size = "1 B".to_owned() + &" ".repeat(62);
    assert!(t.encode().is_err());
    t.partition_size = "1 B".into();
    t.api_urls = vec![format!("http://{}/", "a".repeat(2041))];
    assert!(t.encode().is_err());
}
