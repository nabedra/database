use clap::{Parser, error::ErrorKind};
use meshdb_server::{
    cli::{Cli, Command},
    config::{Config, ServeOptions},
};

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
        let Command::Serve(options) = Cli::try_parse_from(args).unwrap().command;
        let defaults = ServeOptions::default();
        assert_eq!(options.host, defaults.host);
        assert_eq!(options.port, defaults.port);
        assert_eq!(options.workers, defaults.workers);
        assert_eq!(options.queue_size, defaults.queue_size);
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
    .command;
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
    .command;
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
