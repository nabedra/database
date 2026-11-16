use std::process::Command;

fn binary(args: &[&str], configured: bool) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_nabedra-db"));
    command.env_clear().args(args);
    if configured {
        command
            .env("NABEDRA_S3_BUCKET", "synthetic-bucket")
            .env("NABEDRA_TOKEN", "synthetic-private-token");
    }
    command.output().unwrap()
}

#[test]
fn installed_command_identity_and_version() {
    let output = binary(&["--version"], false);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap().trim(),
        concat!("nabedra-db ", env!("CARGO_PKG_VERSION"))
    );
    assert!(output.stderr.is_empty());
    let output = binary(&["--help"], false);
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(help.contains("Usage: nabedra-db"));
    assert!(!help.contains("nabedra-database-server"));
}

#[test]
fn parser_and_invalid_startup_exit_without_aws() {
    for args in [
        vec!["--help"],
        vec!["serve", "--help"],
        vec!["--version"],
        vec!["serve", "--version"],
    ] {
        let output = binary(&args, false);
        assert!(output.status.success());
        assert!(String::from_utf8_lossy(&output.stdout).contains("nabedra-db"));
    }
    for (args, configured) in [
        (vec!["serve"], false),
        (vec!["serve", "--workers", "0"], true),
        (
            vec![
                "serve",
                "--workers",
                "18446744073709551615",
                "--queue-size",
                "1",
            ],
            true,
        ),
        (vec!["serve", "--tls-cert", "/synthetic/private-path"], true),
    ] {
        let output = binary(&args, configured);
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains("configuration invalid"), "{error}");
        assert!(!error.contains("private"));
    }
    let path = std::env::temp_dir().join(format!("nabedra-malformed-{}.pem", std::process::id()));
    std::fs::write(&path, "not a certificate or private key").unwrap();
    let output = binary(
        &[
            "serve",
            "--workers",
            "3",
            "--partition-size",
            "7 KiB",
            "--tls-cert",
            path.to_str().unwrap(),
            "--tls-key",
            path.to_str().unwrap(),
        ],
        true,
    );
    std::fs::remove_file(&path).unwrap();
    assert!(!output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stderr).trim(),
        "nabedra-db: TLS configuration invalid"
    );
}
