use super::*;
use nabedra_rt::{ids, time};
use std::{fs, time::Duration};
struct TempDir(std::path::PathBuf);
impl TempDir {
    fn new() -> std::io::Result<Self> {
        let path = std::env::temp_dir().join(format!("nabedra-material-{}", ids::uuid_v4()));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn reader(dir: &Path) -> Reader {
    Reader::new(
        &dir.join("input.json"),
        time::now_std() + Duration::from_secs(30),
    )
    .unwrap()
}
#[test]
fn literal_paths_bounded_reads_and_deadlines() {
    let dir = TempDir::new().unwrap();
    let mut r = reader(dir.path());
    fs::write(dir.path().join("$literal ~ space"), b"abcd").unwrap();
    assert_eq!(&*r.read("$literal ~ space", 4, false).unwrap(), b"abcd");
    assert!(
        reader(dir.path())
            .read("$literal ~ space", 3, false)
            .is_err()
    );
    let mut expired = Reader::new(&dir.path().join("input"), time::now_std()).unwrap();
    assert!(expired.read("$literal ~ space", 4, false).is_err());
}
#[cfg(unix)]
#[test]
fn protected_targets_and_preopen_swap() {
    use std::os::unix::{
        ffi::OsStrExt,
        fs::{PermissionsExt, symlink},
    };
    let dir = TempDir::new().unwrap();
    let file = dir.path().join("secret");
    fs::write(&file, b"token").unwrap();
    fs::set_permissions(&file, fs::Permissions::from_mode(0o400)).unwrap();
    symlink(&file, dir.path().join("alias")).unwrap();
    assert_eq!(
        &*reader(dir.path()).read("alias", 10, true).unwrap(),
        b"token"
    );
    fs::set_permissions(&file, fs::Permissions::from_mode(0o640)).unwrap();
    assert!(reader(dir.path()).read("secret", 10, true).is_err());
    assert!(reader(dir.path()).read(".", 10, false).is_err());
    assert!(reader(dir.path()).read("/dev/null", 10, false).is_err());
    let fifo = dir.path().join("fifo");
    let c = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
    // SAFETY: c is a live NUL-terminated owned path to this test's FIFO.
    assert_eq!(unsafe { libc::mkfifo(c.as_ptr(), 0o600) }, 0);
    assert!(reader(dir.path()).read("fifo", 10, false).is_err());
    let mut r = reader(dir.path());
    assert!(
        r.read_with("secret", 10, false, || {
            fs::remove_file(&file).unwrap();
            fs::rename(&fifo, &file).unwrap();
        })
        .is_err()
    );
    let other = TempDir::new().unwrap();
    fs::write(other.path().join("config"), b"{}").unwrap();
    symlink(other.path().join("config"), dir.path().join("input")).unwrap();
    let r = Reader::new(
        &dir.path().join("input"),
        time::now_std() + Duration::from_secs(5),
    )
    .unwrap();
    assert_eq!(r.base, dir.path());
    let grow = dir.path().join("grow");
    fs::write(&grow, b"a").unwrap();
    assert_eq!(
        reader(dir.path())
            .read_with("grow", 4, false, || {
                fs::write(&grow, b"abcde").unwrap();
            })
            .unwrap_err(),
        LIMIT
    );
    let mut r = Reader::new(
        &dir.path().join("input"),
        time::now_std() + Duration::from_millis(5),
    )
    .unwrap();
    assert_eq!(
        r.read_with("grow", 10, false, || time::thread_sleep(
            Duration::from_millis(15)
        ))
        .unwrap_err(),
        LIMIT
    );
}
#[test]
fn strict_auth_token_and_redaction() {
    let dir = TempDir::new().unwrap();
    let a = "A".repeat(43);
    let b = "B".repeat(43);
    let c = "C".repeat(43);
    let valid = format!(r#"{{"admin_token":"{a}","data_token":"{b}","enrollment_token":"{c}"}}"#);
    let path = dir.path().join("auth");
    fs::write(&path, &valid).unwrap();
    private(&path);
    let auth = reader(dir.path()).auth("auth").unwrap();
    assert_eq!(auth.admin_token, a);
    assert_eq!(auth.data_token, b);
    assert_eq!(auth.enrollment_token, c);
    assert!(!format!("{auth:?}").contains(&a));
    for bad in [
        valid.replace(&b, &a),
        valid.replace(&a, "short"),
        valid.replace(&a, &("A".repeat(4097))),
        valid.replace(&a, &" ".repeat(43)),
        valid.replace('}', ",\"extra\":0}"),
        valid.replace('}', &format!(",\"admin_token\":\"{a}\"}}")),
        format!("{valid}null"),
        "[]".into(),
        format!("{}0{}", "[".repeat(33), "]".repeat(33)),
    ] {
        fs::write(&path, bad).unwrap();
        let err = reader(dir.path()).auth("auth").unwrap_err();
        assert!(!format!("{err:?} {err}").contains(&a));
    }
    fs::write(&path, b"\xff").unwrap();
    assert!(reader(dir.path()).auth("auth").is_err());
    for good in [b"exact!".as_slice(), b"exact!\n"] {
        fs::write(&path, good).unwrap();
        let token = reader(dir.path()).http_token("auth").unwrap();
        assert_eq!(token.value, "exact!");
        assert!(!format!("{token:?}").contains("exact!"));
    }
    fs::write(&path, "T".repeat(TOKEN)).unwrap();
    assert_eq!(
        reader(dir.path()).http_token("auth").unwrap().value.len(),
        TOKEN
    );
    fs::write(&path, "T".repeat(TOKEN + 1)).unwrap();
    assert_eq!(reader(dir.path()).http_token("auth").unwrap_err(), LIMIT);
    let max_auth = valid
        .replace(&a, &"A".repeat(4096))
        .replace(&b, &"B".repeat(4096))
        .replace(&c, &"C".repeat(4096));
    fs::write(&path, max_auth).unwrap();
    assert_eq!(
        reader(dir.path()).auth("auth").unwrap().admin_token.len(),
        4096
    );
    for bad in [
        b"".as_slice(),
        b"\n",
        b"x\n\n",
        b"x\r\n",
        b" x",
        b"x ",
        b"x\ty",
        b"\xff",
    ] {
        fs::write(&path, bad).unwrap();
        assert!(reader(dir.path()).http_token("auth").is_err());
    }
}
fn private(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }
    #[cfg(not(unix))]
    let _ = path;
}
#[test]
fn aggregate_and_each_file_cap() {
    let dir = TempDir::new().unwrap();
    for cap in [4096, 16 * 1024, 128 * 1024, 256 * 1024, 1024 * 1024] {
        let file = dir.path().join(format!("cap{cap}"));
        fs::write(&file, vec![b'x'; cap]).unwrap();
        assert_eq!(
            reader(dir.path())
                .read(file.to_str().unwrap(), cap, false)
                .unwrap()
                .len(),
            cap
        );
        fs::write(&file, vec![b'x'; cap + 1]).unwrap();
        assert_eq!(
            reader(dir.path())
                .read(file.to_str().unwrap(), cap, false)
                .unwrap_err(),
            ConfigurationError::LimitExceeded
        );
    }
    let mut r = reader(dir.path());
    for n in 0..8 {
        fs::write(dir.path().join(format!("part{n}")), vec![b'x'; 1024 * 1024]).unwrap();
        r.read(&format!("part{n}"), 1024 * 1024, false).unwrap();
    }
    fs::write(dir.path().join("extra"), b"x").unwrap();
    assert_eq!(
        r.read("extra", 1, false).unwrap_err(),
        ConfigurationError::LimitExceeded
    );
}
fn run_child(mut command: std::process::Command) {
    use std::process::Stdio;
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let status = wait_child(&mut child, time::now_std() + Duration::from_secs(90))
        .expect("synthetic fixture preparation failed or timed out");
    assert!(status.success(), "synthetic fixture preparation failed");
}
fn wait_child(
    child: &mut std::process::Child,
    deadline: Instant,
) -> std::io::Result<std::process::ExitStatus> {
    let result = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Ok(status),
            Err(error) => break Err(error),
            Ok(None) => (),
        }
        if time::now_std() >= deadline {
            break Err(std::io::ErrorKind::TimedOut.into());
        }
        time::thread_sleep(Duration::from_millis(20));
    };
    if result.is_err() {
        // The handle is still owned and has not been reaped on this path.
        let _ = child.kill();
        child.wait()?;
    }
    result
}
pub(crate) fn create_tls_fixture(directory: &Path) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let plan = directory.join("openssl-commands.json");
    let mut command = std::process::Command::new("python3");
    // Reuse the fixture's config and command definition, but spawn no OpenSSL
    // grandchild. Rust owns every actual OpenSSL process and its timeout/reap.
    command.current_dir(root).arg("-c").arg(
        "import json, sys\nfrom pathlib import Path\nfrom scripts import test_tls_material as fixture\ncommands = []\nfixture._run = lambda executable, *args: commands.append([executable, *args])\nfixture.create_test_tls(Path(sys.argv[1]))\nPath(sys.argv[2]).write_text(json.dumps(commands), encoding='utf-8')\n",
    ).arg(directory.join("tls")).arg(&plan);
    run_child(command);
    let bytes = fs::read(plan).unwrap();
    assert!(bytes.len() < 16 * 1024);
    let commands: Vec<Vec<String>> = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(commands.len(), 3);
    for args in commands {
        let (executable, args) = args.split_first().unwrap();
        let mut command = std::process::Command::new(executable);
        command.args(args);
        run_child(command);
    }
}
#[test]
fn fixture_timeout_reaps_its_direct_child() {
    let mut child = std::process::Command::new("python3")
        .args(["-c", "import time; time.sleep(300)"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let result = wait_child(&mut child, time::now_std());
    let reaped = child.try_wait().unwrap().is_some();
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
    assert!(reaped);
}
#[test]
fn tls_real_material_retention_mismatch_and_strict_pem() {
    let dir = TempDir::new().unwrap();
    create_tls_fixture(dir.path());
    let hints = Tls {
        ca_file: "tls/ca.crt".into(),
        cert_file: "tls/server.crt".into(),
        key_file: "tls/server.key".into(),
    };
    let mut r = reader(dir.path());
    let tls = r.tls(&hints).unwrap();
    tls.certified_key.keys_match().unwrap();
    assert!(!tls.ca.roots.is_empty());
    assert_eq!(tls.ca.der.len(), 1);
    assert!(!format!("{tls:?} {r:?} {:?}", tls.ca).contains("CERTIFICATE"));
    let pem = fs::read(dir.path().join("tls/ca.crt")).unwrap();
    fs::write(dir.path().join("tls/ca.crt"), b"broken").unwrap();
    assert_eq!(&*r.tls(&hints).unwrap().ca.pem, &pem);
    assert!(reader(dir.path()).tls(&hints).is_err());
    fs::write(dir.path().join("tls/ca.crt"), &pem).unwrap();
    let bad_hints = Tls {
        key_file: "tls/ca.key".into(),
        ..hints.clone()
    };
    assert!(reader(dir.path()).tls(&bad_hints).is_err());
    let original_key = fs::read(dir.path().join("tls/server.key")).unwrap();
    // OpenSSL encodings of the same owned key, with bounded child ownership.
    for (name, args) in [
        (
            "pkcs8",
            vec!["pkcs8", "-topk8", "-nocrypt", "-outform", "PEM"],
        ),
        ("der", vec!["pkey", "-outform", "DER"]),
    ] {
        let dest = dir.path().join(name);
        let mut command = std::process::Command::new("openssl");
        command
            .args(args)
            .arg("-in")
            .arg(dir.path().join("tls/server.key"))
            .arg("-out")
            .arg(&dest);
        run_child(command);
        private(&dest);
        let h = Tls {
            key_file: name.into(),
            ..hints.clone()
        };
        let equivalent = reader(dir.path()).tls(&h).unwrap();
        assert_eq!(
            tls.certified_key.key.public_key(),
            equivalent.certified_key.key.public_key()
        );
    }
    let key = PrivateKeyDer::from_pem_slice(&fs::read(dir.path().join("pkcs8")).unwrap()).unwrap();
    let der = key.secret_der();
    let raw_path = dir.path().join("trailing.der");
    let pem_path = dir.path().join("trailing.pem");
    fs::write(&raw_path, der).unwrap();
    private(&raw_path);
    let raw_hints = Tls {
        key_file: "trailing.der".into(),
        ..hints.clone()
    };
    assert!(reader(dir.path()).tls(&raw_hints).is_ok());
    let mut rejected = Vec::new();
    for bad in [[der, &[0]].concat(), [der, der].concat()] {
        fs::write(&raw_path, &bad).unwrap();
        rejected.push(reader(dir.path()).tls(&raw_hints).is_err());
        // Base64-wrap the exact bad bytes; no DER parser may normalize them.
        let encoded = dir.path().join("trailing.base64");
        let mut command = std::process::Command::new("openssl");
        command
            .args(["base64", "-in"])
            .arg(&raw_path)
            .arg("-out")
            .arg(&encoded);
        run_child(command);
        fs::write(
            &pem_path,
            format!(
                "-----BEGIN PRIVATE KEY-----\n{}-----END PRIVATE KEY-----\n",
                fs::read_to_string(encoded).unwrap()
            ),
        )
        .unwrap();
        private(&pem_path);
        assert!(
            PrivateKeyDer::from_pem_slice(&fs::read(&pem_path).unwrap())
                .unwrap()
                .secret_der()
                == bad.as_slice(),
            "PEM wrapping changed the synthetic DER bytes"
        );
        let pem_hints = Tls {
            key_file: "trailing.pem".into(),
            ..hints.clone()
        };
        rejected.push(reader(dir.path()).tls(&pem_hints).is_err());
    }
    assert_eq!(rejected, vec![true; 4], "raw/PEM trailing DER acceptance");
    for bad in [
        b"broken".to_vec(),
        [pem.clone(), b"junk".to_vec()].concat(),
        [pem.clone(), b"\n-----BEGIN CERTIFICATE-----\nAAAA".to_vec()].concat(),
        [
            pem.clone(),
            b"\n-----BEGIN UNKNOWN-----\nAAAA\n-----END UNKNOWN-----".to_vec(),
        ]
        .concat(),
    ] {
        fs::write(dir.path().join("tls/ca.crt"), bad).unwrap();
        assert!(reader(dir.path()).tls(&hints).is_err());
    }
    fs::write(dir.path().join("tls/ca.crt"), &pem).unwrap();
    for bad in [
        [original_key.clone(), original_key.clone()].concat(),
        [original_key.clone(), b"junk".to_vec()].concat(),
        original_key[..original_key.len() / 2].to_vec(),
    ] {
        fs::write(dir.path().join("tls/server.key"), bad).unwrap();
        assert!(reader(dir.path()).tls(&hints).is_err());
    }
    fs::write(dir.path().join("tls/server.key"), original_key).unwrap();
    let huge_der = format!(
        "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----\n",
        "AAAA".repeat(21846)
    );
    fs::write(dir.path().join("tls/server.crt"), huge_der).unwrap();
    assert_eq!(reader(dir.path()).tls(&hints).unwrap_err(), LIMIT);
    // Restore the retained synthetic leaf before chain-count boundaries.
    fs::write(dir.path().join("tls/server.crt"), &*tls.chain_pem).unwrap();
    let cert = fs::read(dir.path().join("tls/server.crt")).unwrap();
    fs::write(dir.path().join("tls/server.crt"), cert.repeat(64)).unwrap();
    assert_eq!(
        reader(dir.path())
            .tls(&hints)
            .unwrap()
            .certified_key
            .cert
            .len(),
        64
    );
    fs::write(dir.path().join("tls/server.crt"), cert.repeat(65)).unwrap();
    assert_eq!(reader(dir.path()).tls(&hints).unwrap_err(), LIMIT);
    fs::write(dir.path().join("tls/server.crt"), &cert).unwrap();
    let r = reader(dir.path());
    // Repeated valid DER roots exercise count before dedup, with the real reader cap.
    // The synthetic RSA CA is larger than 1KiB, so test the 1024 count separately
    // through the same parser, while its on-disk byte cap is tested above.
    assert_eq!(
        r.parse_ca(Arc::from(pem.repeat(1024))).unwrap().der.len(),
        1
    );
    assert_eq!(r.parse_ca(Arc::from(pem.repeat(1025))).unwrap_err(), LIMIT);
    let mut r = reader(dir.path());
    assert!(r.http_ca_candidates(false, None, &[]).unwrap().is_none());
    assert!(
        r.http_ca_candidates(false, Some("tls/ca.crt"), &[])
            .unwrap()
            .is_some()
    );
    let absent = dir.path().join("absent");
    let ca = dir.path().join("tls/ca.crt");
    assert_eq!(
        &*reader(dir.path())
            .http_ca_candidates(true, None, &[&absent, dir.path(), &ca])
            .unwrap()
            .unwrap()
            .pem,
        &pem
    );
    let empty = dir.path().join("empty");
    fs::write(&empty, b"").unwrap();
    assert!(
        reader(dir.path())
            .http_ca_candidates(true, None, &[&empty, &ca])
            .is_err()
    );
    let oversized = dir.path().join("oversized");
    fs::write(&oversized, vec![b'x'; CERTS + 1]).unwrap();
    assert_eq!(
        reader(dir.path())
            .http_ca_candidates(true, None, &[&oversized, &ca])
            .unwrap_err(),
        LIMIT
    );
    let broken = dir.path().join("broken");
    fs::write(&broken, b"bad").unwrap();
    assert!(
        reader(dir.path())
            .http_ca_candidates(true, None, &[&broken, &ca])
            .is_err()
    );
}
#[test]
fn original_input_decoders_and_retained_bytes() {
    let dir = TempDir::new().unwrap();
    let id = "abcdef01-2345-4678-9abc-def012345678";
    let storage = r#"{"driver":"http","options":{"base_url":"http://store:80/objects","namespace":"test","token_file":"$literal ~ space","ca_file":null}}"#;
    let api = format!(
        r#"{{"version":1,"cluster_id":"{id}","bind":"localhost:0","advertise_url":"http://api:80/","api_urls":["http://api:80"],"storage":{storage},"auth_file":"auth","tls":null,"worker":{{"listen":"localhost:0","operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"}}}}"#
    );
    let file = dir.path().join("input.json");
    fs::write(&file, &api).unwrap();
    let mut r = reader(dir.path());
    let loaded = r.api().unwrap();
    assert_eq!(&*loaded.bytes, api.as_bytes());
    assert_eq!(loaded.value.auth_file, "auth");
    fs::write(&file, "broken").unwrap();
    assert_eq!(&*r.api().unwrap().bytes, api.as_bytes());
    assert!(reader(dir.path()).api().is_err());
    let bootstrap = format!(
        r#"{{"version":1,"cluster_id":"{id}","node_id":"{id}","request_id":null,"api_urls":["http://api:80"],"listen":"localhost:0","advertise_url":"http://worker:81","storage":{storage},"enrollment_token":"{}","tls":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"64 MiB"}}"#,
        "E".repeat(43)
    );
    fs::write(&file, &bootstrap).unwrap();
    private(&file);
    let loaded = reader(dir.path()).bootstrap().unwrap();
    assert_eq!(&*loaded.bytes, bootstrap.as_bytes());
    assert_eq!(loaded.value.enrollment_token, "E".repeat(43));
    assert!(!format!("{loaded:?}").contains(&"E".repeat(43)));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(reader(dir.path()).bootstrap().is_err());
    }
    let mut padded = api.into_bytes();
    padded.resize(MAX_INPUT_BYTES, b' ');
    fs::write(&file, &padded).unwrap();
    assert!(reader(dir.path()).api().is_ok());
    padded.push(b' ');
    fs::write(&file, &padded).unwrap();
    assert_eq!(reader(dir.path()).api().unwrap_err(), LIMIT);
}
