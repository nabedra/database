use nabedra_database_server::config::{self, Config, ServeOptions};
use std::{env::VarError, path::PathBuf};

fn configured(
    options: ServeOptions,
    overrides: &[(&str, Option<&str>)],
) -> Result<Config, config::ConfigError> {
    Config::from_lookup(options, |name| {
        assert!(matches!(
            name,
            "NABEDRA_S3_BUCKET" | "NABEDRA_TOKEN" | "NABEDRA_S3_PREFIX"
        ));
        let value = overrides
            .iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| *value)
            .unwrap_or(match name {
                "NABEDRA_S3_BUCKET" => Some("test-bucket"),
                "NABEDRA_TOKEN" => Some("private-bearer-value"),
                _ => None,
            });
        value.map(str::to_owned).ok_or(VarError::NotPresent)
    })
}

#[test]
fn defaults_and_explicit_options_use_storage_namespace() {
    let config = configured(ServeOptions::default(), &[]).unwrap();
    assert_eq!(
        config.options().host,
        "127.0.0.1".parse::<std::net::IpAddr>().unwrap()
    );
    assert_eq!(
        (
            config.options().port,
            config.options().workers,
            config.options().queue_size
        ),
        (8000, 4, 64)
    );
    assert!(config.options().tls_cert.is_none() && config.options().tls_key.is_none());
    assert_eq!(config.namespace().as_str(), "nabedra-v0/");
    assert_eq!(config.bucket(), "test-bucket");
    assert_eq!(config.token(), "private-bearer-value");
    assert_eq!(config.admission_limit(), 68);
    assert_eq!(
        config.options().partition_size.bytes(),
        nabedra_database_server::partition_size::MAX_PARTITION_BYTES
    );

    let options = ServeOptions {
        host: "::1".parse().unwrap(),
        port: 9000,
        workers: 8,
        queue_size: 12,
        partition_size: "61 MiB".parse().unwrap(),
        ..ServeOptions::default()
    };
    let config = configured(options, &[("NABEDRA_S3_PREFIX", Some("team/data///"))]).unwrap();
    assert_eq!(config.namespace().as_str(), "team/data/");
    assert_eq!(config.options().partition_size.bytes(), 63_963_136);
    assert_eq!(
        config.options().host,
        "::1".parse::<std::net::IpAddr>().unwrap()
    );
    assert_eq!(
        (config.options().port, config.admission_limit()),
        (9000, 20)
    );
}

#[test]
fn required_environment_and_prefix_errors_are_sanitized_and_debug_redacts_token() {
    for name in ["NABEDRA_S3_BUCKET", "NABEDRA_TOKEN"] {
        for value in [None, Some(""), Some(" \t\n")] {
            let error = configured(ServeOptions::default(), &[(name, value)]).unwrap_err();
            assert!(error.to_string().contains(name));
            assert!(!format!("{error:?} {error}").contains("private-bearer-value"));
        }
    }
    for prefix in ["", "../secret-prefix-value", "team//data"] {
        let error = configured(
            ServeOptions::default(),
            &[("NABEDRA_S3_PREFIX", Some(prefix))],
        )
        .unwrap_err();
        assert_eq!(error.to_string(), "NABEDRA_S3_PREFIX is invalid");
        assert!(!format!("{error:?}").contains("secret-prefix-value"));
    }
    let error = Config::from_lookup(ServeOptions::default(), |_| {
        Err(VarError::NotUnicode("private-input-value".into()))
    })
    .unwrap_err();
    assert!(!format!("{error:?} {error}").contains("private-input-value"));
    let debug = format!("{:?}", configured(ServeOptions::default(), &[]).unwrap());
    assert!(!debug.contains("private-bearer-value"));
}

#[test]
fn worker_and_admission_bounds_allow_zero_queue() {
    for (workers, queue_size) in [
        (0, 64),
        (257, 0),
        (usize::MAX, 1),
        (1, usize::MAX),
        (tokio::sync::Semaphore::MAX_PERMITS, 1),
    ] {
        assert!(
            configured(
                ServeOptions {
                    workers,
                    queue_size,
                    ..ServeOptions::default()
                },
                &[]
            )
            .is_err()
        );
    }
    for workers in [1, 256] {
        let config = configured(
            ServeOptions {
                workers,
                queue_size: 0,
                ..ServeOptions::default()
            },
            &[],
        )
        .unwrap();
        assert_eq!(config.admission_limit(), workers);
    }
}

struct TlsFiles(PathBuf);
impl TlsFiles {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("nabedra-config-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        Self(dir)
    }
}
impl Drop for TlsFiles {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn tls_requires_readable_regular_file_pair() {
    let files = TlsFiles::new();
    let cert = files.0.join("private-cert-path");
    let key = files.0.join("private-key-path");
    std::fs::write(&cert, "PEM parsing belongs to R10").unwrap();
    std::fs::write(&key, "PEM parsing belongs to R10").unwrap();
    let pair = ServeOptions {
        tls_cert: Some(cert.clone()),
        tls_key: Some(key.clone()),
        ..ServeOptions::default()
    };
    assert!(configured(pair.clone(), &[]).is_ok());
    for options in [
        ServeOptions {
            tls_key: None,
            ..pair.clone()
        },
        ServeOptions {
            tls_cert: None,
            ..pair.clone()
        },
        ServeOptions {
            tls_cert: Some(files.0.clone()),
            ..pair.clone()
        },
        ServeOptions {
            tls_key: Some(files.0.join("missing-private-path")),
            ..pair.clone()
        },
    ] {
        let error = configured(options, &[]).unwrap_err();
        let message = format!("{error:?} {error}");
        assert!(
            !message.contains("private-")
                && !message.contains(&files.0.to_string_lossy().to_string())
        );
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o000)).unwrap();
        // Privileged test users may still open mode-000 files.
        if std::fs::File::open(&key).is_err() {
            assert!(configured(pair, &[]).is_err());
        }
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
}
