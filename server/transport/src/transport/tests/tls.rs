//! Test-only CA and enrolled worker certificates with bounded child cleanup.
#![allow(clippy::disallowed_methods)] // test support, also #[path]-included by unit tests; the phase 7 seam covers product code
use crate::tls::TlsMaterial;
use std::{
    fs,
    path::Path,
    process::{Command, Stdio},
    time::Duration,
};

fn openssl(directory: &Path, args: &[&str]) {
    use std::io::Read;
    let diagnostic = directory.join("openssl.stderr");
    let mut child = Command::new("openssl")
        .args(args)
        .current_dir(directory)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(fs::File::create(&diagnostic).unwrap()))
        .spawn()
        .unwrap();
    let until = std::time::Instant::now() + Duration::from_secs(15);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if std::time::Instant::now() >= until {
            let _ = child.kill();
            child.wait().unwrap();
            panic!("openssl fixture watchdog");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let mut output = String::new();
    fs::File::open(diagnostic)
        .unwrap()
        .take(8192)
        .read_to_string(&mut output)
        .unwrap();
    assert!(status.success(), "openssl: {output}");
}

pub fn certificates(directory: &Path, count: u64) -> Vec<TlsMaterial> {
    openssl(
        directory,
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=nabedra-test-ca",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
        ],
    );
    fs::write(directory.join("extensions"),"basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=DNS:localhost\n").unwrap();
    let ca = fs::read(directory.join("ca.pem")).unwrap();
    (1..=count)
        .map(|id| {
            let key = format!("node{id}.key");
            let csr = format!("node{id}.csr");
            let certificate = format!("node{id}.pem");
            openssl(
                directory,
                &[
                    "req",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-subj",
                    "/CN=localhost",
                    "-keyout",
                    &key,
                    "-out",
                    &csr,
                ],
            );
            openssl(
                directory,
                &[
                    "x509",
                    "-req",
                    "-in",
                    &csr,
                    "-CA",
                    "ca.pem",
                    "-CAkey",
                    "ca.key",
                    "-CAcreateserial",
                    "-out",
                    &certificate,
                    "-days",
                    "1",
                    "-extfile",
                    "extensions",
                ],
            );
            TlsMaterial::from_pem(
                &fs::read(directory.join(certificate)).unwrap(),
                &fs::read(directory.join(key)).unwrap(),
                &ca,
            )
            .unwrap()
        })
        .collect()
}
