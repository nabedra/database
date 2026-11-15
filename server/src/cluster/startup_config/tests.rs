use super::super::configuration::MAX_TEMPLATE_BYTES;
use super::*;

const ID: &str = "abcdef01-2345-4678-9abc-def012345678";
const SECRET: &str = "SENTINEL_SECRET_abcdefghijklmnopqrstuvwxyz0123456789";
const PATH: &str = "SENTINEL_PATH/$HOME/../~/秘密";
fn storage() -> String {
    format!(
        r#"{{"driver":"http","options":{{"base_url":"http://store:80/objects","namespace":"test","token_file":"{PATH}","ca_file":null}}}}"#
    )
}
fn worker() -> &'static str {
    r#"{"listen":"0.0.0.0:0","operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"}"#
}
fn api() -> String {
    format!(
        r#"{{"version":1,"cluster_id":"{ID}","bind":"localhost:0","advertise_url":"http://api:80/","api_urls":["http://api:80"],"storage":{},"auth_file":"{PATH}","tls":null,"worker":{}}}"#,
        storage(),
        worker()
    )
}
fn bootstrap() -> String {
    format!(
        r#"{{"version":1,"cluster_id":"{ID}","node_id":"{ID}","request_id":null,"api_urls":["http://api:80"],"listen":"[::]:0","advertise_url":"http://worker:81","storage":{},"enrollment_token":"{SECRET}","tls":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"}}"#,
        storage()
    )
}
fn lifecycle(s: &str) -> String {
    let mut input = api();
    input.pop();
    format!("{input},\"lifecycle\":{s}}}")
}
fn bad_api(s: &str) {
    assert!(
        decode_api(s.as_bytes()).is_err(),
        "unexpected API acceptance"
    );
}
fn bad_bootstrap(s: &str) {
    assert!(
        decode_bootstrap(s.as_bytes()).is_err(),
        "unexpected bootstrap acceptance"
    );
}
#[test]
fn valid_records_and_lifecycle_expansion() {
    let a = decode_api(api().as_bytes()).unwrap();
    assert_eq!(a.cluster_id.to_string(), ID);
    assert_eq!(a.bind, "localhost:0");
    assert_eq!(a.advertise_url, "http://api:80/");
    assert_eq!(a.auth_file, PATH);
    assert_eq!(a.template.listen, "0.0.0.0:0");
    assert_eq!(a.lifecycle, Timing::default());
    assert_eq!(
        decode_api(lifecycle("{}").as_bytes()).unwrap().lifecycle,
        Timing::default()
    );
    let t = decode_api(lifecycle(r#"{"heartbeat_ms":1000,"max_attempts":3}"#).as_bytes())
        .unwrap()
        .lifecycle;
    assert_eq!(
        t,
        Timing {
            heartbeat_ms: 1000,
            max_attempts: 3,
            ..Timing::default()
        }
    );
    for patch in [
        "null",
        "[]",
        r#"{"heartbeat_ms":15000}"#,
        r#"{"renew_ms":7500}"#,
        r#"{"retry_initial_ms":30001}"#,
        r#"{"drain_ms":0}"#,
        r#"{"drain_ms":null}"#,
        r#"{"drain_ms":1,"drain_ms":2}"#,
        r#"{"unknown":1}"#,
    ] {
        bad_api(&lifecycle(patch));
    }
    let b = decode_bootstrap(bootstrap().as_bytes()).unwrap();
    assert_eq!(b.cluster_id.to_string(), ID);
    assert_eq!(b.node_id.to_string(), ID);
    assert_eq!(b.request_id, None);
    assert_eq!(b.advertise_url, "http://worker:81");
    assert_eq!(b.enrollment_token, SECRET);
    assert_eq!(b.template.listen, "[::]:0");
    let managed = bootstrap().replace("\"request_id\":null", &format!("\"request_id\":\"{ID}\""));
    assert_eq!(
        decode_bootstrap(managed.as_bytes())
            .unwrap()
            .request_id
            .unwrap()
            .to_string(),
        ID
    );
}
#[test]
fn strict_original_objects_and_integer_identities() {
    for (from, to) in [
        ("\"version\":1", "\"version\":1,\"version\":1"),
        ("\"version\":1", "\"version\":1,\"control\":{}"),
        ("\"tls\":null,", ""),
        ("\"listen\":\"0.0.0.0:0\",", ""),
        ("\"queue_size\":64", "\"queue_size\":64,\"queue_size\":64"),
        ("\"queue_size\":64", "\"queue_size\":64,\"extra\":1"),
        ("\"ca_file\":null", "\"ca_file\":null,\"ca_file\":null"),
        ("\"ca_file\":null", "\"ca_file\":null,\"secret\":1"),
        (",\"ca_file\":null", ""),
        ("\"options\":{", "\"options\":{\"namespace\":\"test\","),
    ] {
        bad_api(&api().replace(from, to));
    }
    for n in ["true", "-1", "1.0", "1e0", "18446744073709551616", "0", "2"] {
        bad_api(&api().replace("\"version\":1", &format!("\"version\":{n}")));
    }
    for n in ["true", "-1", "4.0", "4e0", "18446744073709551616"] {
        bad_api(&api().replace("\"operation_slots\":4", &format!("\"operation_slots\":{n}")));
        bad_api(&lifecycle(&format!("{{\"max_attempts\":{n}}}")));
        bad_bootstrap(&bootstrap().replace("\"queue_size\":64", &format!("\"queue_size\":{n}")));
    }
    for id in [ID.to_uppercase(), ID.replace('-', ""), "invalid".to_owned()] {
        bad_api(&api().replace(ID, &id));
        bad_bootstrap(&bootstrap().replace(ID, &id));
    }
    for s in [
        "[]".to_owned(),
        format!("{} {{}}", api()),
        api().replace(worker(), "[]"),
    ] {
        bad_api(&s);
    }
    for (from, to) in [
        ("\"request_id\":null,", ""),
        (
            "\"request_id\":null",
            "\"request_id\":null,\"request_id\":null",
        ),
        ("\"tls\":null,", ""),
        (
            "\"advertise_url\":\"http://worker:81\"",
            "\"advertise_url\":null",
        ),
        ("\"version\":1", "\"version\":1,\"auth_file\":\"x\""),
    ] {
        bad_bootstrap(&bootstrap().replace(from, to));
    }
    assert!(decode_api(&[0xff]).is_err());
    assert!(decode_bootstrap(&[0xff]).is_err());
}
#[test]
fn local_url_tls_capacity_feature_and_path_checks() {
    for url in [
        "http://api:0",
        "http://u@api:80",
        "http://api:80/path",
        "http://api:80?x",
        "http://api:80/#x",
        "https://api:80",
        "http://é:80",
    ] {
        bad_api(&api().replace("http://api:80", url));
    }
    for (from, to) in [
        ("localhost:0", "999.999.999.999:0"),
        ("\"operation_slots\":4", "\"operation_slots\":0"),
        ("\"partition_slots\":4", "\"partition_slots\":257"),
        ("\"queue_size\":64", "\"queue_size\":4097"),
        ("64 MiB", "65 MiB"),
        (PATH, "bad\\u0000path"),
        (PATH, ""),
    ] {
        bad_api(&api().replace(from, to));
    }
    bad_api(&api().replace(PATH, &"é".repeat(1025)));
    let tls = r#"{"cert_file":"cert","key_file":"key","ca_file":"ca"}"#;
    let https = api()
        .replace("\"tls\":null", &format!("\"tls\":{tls}"))
        .replace("http://api", "https://api");
    assert!(decode_api(https.as_bytes()).is_ok());
    bad_api(&https.replace("https://api:80/", "http://api:80/"));
    bad_api(&https.replace("\"key_file\":\"key\"", "\"key_file\":\"bad\\u0000key\""));
    bad_api(&api().replace("\"driver\":\"http\"", "\"driver\":\"unknown\""));
    #[cfg(not(feature = "s3"))]
    assert_eq!(
        decode_api(
            api()
                .replace(
                    &storage(),
                    r#"{"driver":"s3","options":{"bucket":"bucket","namespace":"test"}}"#
                )
                .as_bytes()
        )
        .unwrap_err(),
        ConfigurationError::UnsupportedStorage
    );
    for token in [
        "x".to_owned(),
        "x".repeat(4097),
        " ".repeat(43),
        "é".repeat(43),
        "x\\n".repeat(43),
    ] {
        bad_bootstrap(&bootstrap().replace(SECRET, &token));
    }
    for len in [43, 4096] {
        assert!(decode_bootstrap(bootstrap().replace(SECRET, &"x".repeat(len)).as_bytes()).is_ok());
    }
}
#[test]
fn raw_byte_depth_template_bounds_and_sanitized_diagnostics() {
    for (input, api_mode) in [(api(), true), (bootstrap(), false)] {
        let mut padded = input.as_bytes().to_vec();
        padded.resize(128 * 1024, b' ');
        if api_mode {
            assert!(decode_api(&padded).is_ok());
        } else {
            assert!(decode_bootstrap(&padded).is_ok());
        }
        padded.push(b' ');
        let error = if api_mode {
            decode_api(&padded).unwrap_err()
        } else {
            decode_bootstrap(&padded).unwrap_err()
        };
        assert_eq!(error, ConfigurationError::LimitExceeded);
    }
    // Unknown RawValue options must be checked for depth before serde dispatch.
    let deep = format!("{}0{}", "[".repeat(33), "]".repeat(33));
    bad_api(&api().replace("\"namespace\":\"test\"", &format!("\"namespace\":{deep}")));
    for depth in [32, 33] {
        let nested = format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
        assert_eq!(
            configuration::input(nested.as_bytes(), MAX_INPUT_BYTES).is_ok(),
            depth == 32
        );
    }
    let mut a = decode_api(api().as_bytes()).unwrap();
    // A valid individual path can expand substantially when serialized.
    let urls = vec!["https://".to_owned() + &"a".repeat(2030); 16];
    a.template.api_urls = urls;
    if let super::super::configuration::Storage::Http(o) = &mut a.template.storage {
        o.token_file = "\u{1}".repeat(2048);
        o.ca_file = Some("\u{1}".repeat(2048));
    }
    a.template.tls = Some(super::super::configuration::Tls {
        ca_file: "\u{1}".repeat(2048),
        cert_file: "x".repeat(2048),
        key_file: String::new(),
    });
    // Tune the escaped key hint so the canonical Template is exactly 64 KiB.
    // Changing this one string by one ASCII byte then crosses that bound.
    if let Storage::Http(o) = &mut a.template.storage {
        o.token_file = "x".repeat(2048);
    }
    let remaining = MAX_TEMPLATE_BYTES - serde_json::to_vec(&a.template).unwrap().len();
    let key = "\u{1}".repeat(remaining / 6) + &"x".repeat(remaining % 6);
    assert!(key.len() < 2048);
    a.template.tls.as_mut().unwrap().key_file = key;
    assert_eq!(
        serde_json::to_vec(&a.template).unwrap().len(),
        MAX_TEMPLATE_BYTES
    );
    // Insert into the actual decoder input without normalizing input objects.
    let at_limit = api()
        .replace(
            "[\"http://api:80\"]",
            &serde_json::to_string(&a.template.api_urls).unwrap(),
        )
        .replace(
            &storage(),
            &serde_json::to_string(&a.template.storage).unwrap(),
        )
        .replace(
            "\"tls\":null",
            &format!(
                "\"tls\":{}",
                serde_json::to_string(&a.template.tls).unwrap()
            ),
        )
        .replace(
            "\"advertise_url\":\"http://api:80/\"",
            "\"advertise_url\":\"https://api:80\"",
        );
    assert!(decode_api(at_limit.as_bytes()).is_ok());
    let oversized = at_limit.replace("\"key_file\":\"", "\"key_file\":\"x");
    assert_eq!(
        decode_api(oversized.as_bytes()).unwrap_err(),
        ConfigurationError::LimitExceeded
    );
    for debug in [
        format!("{:?}", decode_api(api().as_bytes()).unwrap()),
        format!("{:?}", decode_bootstrap(bootstrap().as_bytes()).unwrap()),
        format!(
            "{:?}",
            decode_bootstrap(
                bootstrap()
                    .replace("\"version\":1", "\"version\":2")
                    .as_bytes()
            )
            .unwrap_err()
        ),
        decode_bootstrap(
            bootstrap()
                .replace("\"version\":1", "\"version\":2")
                .as_bytes(),
        )
        .unwrap_err()
        .to_string(),
        decode_api(api().replace("\"version\":1", "\"version\":2").as_bytes())
            .unwrap_err()
            .to_string(),
    ] {
        assert!(!debug.contains(SECRET));
        assert!(!debug.contains(PATH));
        assert!(!debug.contains(ID));
    }
}
