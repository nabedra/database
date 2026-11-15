use clap::{Parser, error::ErrorKind};
use meshdb_server::{
    cli::{Cli, Command},
    config::{Config, ServeOptions},
};

#[test]
fn partition_size_spellings_allow_default_and_explicit_workers() {
    for flag in ["--partition-size", "--partition_size"] {
        for worker_args in [vec![], vec!["--workers", "7"]] {
            let mut args = vec!["meshdb-server", "serve", flag, "64MiB"];
            let expected_workers = if worker_args.is_empty() { 4 } else { 7 };
            args.extend(worker_args);
            let parsed = Cli::try_parse_from(args);
            assert!(parsed.is_ok(), "{flag}: {parsed:?}");
            let Command::Serve(options) = parsed.unwrap().command else {
                panic!("expected serve")
            };
            assert_eq!(options.partition_size.bytes(), 67_108_864);
            assert_eq!(options.workers, expected_workers);
        }
    }
}

#[test]
fn partition_size_cli_rejects_invalid_values_and_shows_alias() {
    for flag in ["--partition-size", "--partition_size"] {
        for value in [
            "64",
            "0 B",
            "1.0 MB",
            "65 MiB",
            "67108865B",
            "1 GB",
            "1 B garbage",
        ] {
            let error = Cli::try_parse_from(["meshdb-server", "serve", flag, value]).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ValueValidation, "{flag} {value}");
            assert!(error.to_string().contains("67108864 B (64 MiB)"));
        }
    }
    let help = Cli::try_parse_from(["meshdb-server", "serve", "--help"])
        .unwrap_err()
        .to_string();
    assert!(help.contains("--partition-size"));
    assert!(help.contains("--partition_size"));
}

#[test]
fn documented_commands_parse_into_existing_options() {
    for args in [
        vec!["meshdb-server", "serve"],
        vec![
            "meshdb-server",
            "serve",
            "--host",
            "127.0.0.1",
            "--port",
            "8000",
            "--workers",
            "4",
            "--queue-size",
            "64",
        ],
    ] {
        let Command::Serve(options) = Cli::try_parse_from(args).unwrap().command else {
            panic!("expected serve")
        };
        let defaults = ServeOptions::default();
        assert_eq!(options.host, defaults.host);
        assert_eq!(options.port, defaults.port);
        assert_eq!(options.workers, defaults.workers);
        assert_eq!(options.queue_size, defaults.queue_size);
        assert_eq!(
            options.partition_size.bytes(),
            meshdb_server::partition_size::MAX_PARTITION_BYTES
        );
        assert_eq!(options.tls_cert, defaults.tls_cert);
        assert_eq!(options.tls_key, defaults.tls_key);
    }
    let Command::Serve(options) = Cli::try_parse_from([
        "meshdb-server",
        "serve",
        "--host",
        "0.0.0.0",
        "--port",
        "8443",
        "--workers",
        "4",
        "--queue-size",
        "64",
        "--tls-cert",
        "cert.pem",
        "--tls-key",
        "key.pem",
    ])
    .unwrap()
    .command
    else {
        panic!("expected serve")
    };
    assert_eq!(options.host, "0.0.0.0".parse::<std::net::IpAddr>().unwrap());
    assert_eq!(
        (options.port, options.workers, options.queue_size),
        (8443, 4, 64)
    );
    assert_eq!(
        options.tls_cert.as_deref(),
        Some(std::path::Path::new("cert.pem"))
    );
    assert_eq!(
        options.tls_key.as_deref(),
        Some(std::path::Path::new("key.pem"))
    );

    // Parsing performs no TLS I/O and leaves validation to Config.
    let Command::Serve(options) = Cli::try_parse_from([
        "meshdb-server",
        "serve",
        "--queue-size",
        "0",
        "--workers",
        "0",
        "--tls-cert",
        "missing-cert.pem",
    ])
    .unwrap()
    .command
    else {
        panic!("expected serve")
    };
    assert_eq!(options.queue_size, 0);
    assert!(options.tls_key.is_none());
    let error =
        Config::from_lookup(options, |_| panic!("workers checked before environment")).unwrap_err();
    assert_eq!(error.to_string(), "workers must be at least 1");
}

#[test]
fn command_shape_typed_inputs_and_clap_controls() {
    assert!(Cli::try_parse_from(["meshdb-server"]).is_err());
    assert!(Cli::try_parse_from(["meshdb-server", "status"]).is_err());
    assert!(Cli::try_parse_from(["meshdb-server", "--host", "127.0.0.1"]).is_err());
    for (flag, value) in [
        ("--host", "localhost"),
        ("--port", "65536"),
        ("--workers", "-1"),
        ("--queue-size", "-1"),
        ("--queue-size", "18446744073709551616"),
        ("--token", "value"),
        ("--aws-region", "value"),
    ] {
        assert!(
            Cli::try_parse_from(["meshdb-server", "serve", flag, value]).is_err(),
            "{flag} {value}"
        );
    }
    for (args, kind) in [
        (vec!["meshdb-server", "--help"], ErrorKind::DisplayHelp),
        (
            vec!["meshdb-server", "serve", "--help"],
            ErrorKind::DisplayHelp,
        ),
        (
            vec!["meshdb-server", "--version"],
            ErrorKind::DisplayVersion,
        ),
    ] {
        assert_eq!(Cli::try_parse_from(args).unwrap_err().kind(), kind);
    }
}
