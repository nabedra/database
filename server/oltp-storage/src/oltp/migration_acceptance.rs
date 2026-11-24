//! Complete offline migration scenario uses two independent production binaries.
#[test]
#[ignore = "run through scripts/acceptance_oltp.py"]
fn acceptance_o20() {
    let Some(olap) = std::env::var_os("NABEDRA_OLAP_BINARY") else {
        eprintln!(
            "NABEDRA_OLTP_ACCEPTANCE {{\"scenario\":\"O20\",\"status\":\"not-run\",\"reason\":\"missing independent OLAP exporter binary\"}}"
        );
        return;
    };
    let Some(oltp) = std::env::var_os("NABEDRA_OLTP_BINARY") else {
        eprintln!(
            "NABEDRA_OLTP_ACCEPTANCE {{\"scenario\":\"O20\",\"status\":\"not-run\",\"reason\":\"missing independent OLTP importer binary\"}}"
        );
        return;
    };
    let status = std::process::Command::new(
        std::env::var_os("NABEDRA_ACCEPTANCE_PYTHON").unwrap_or_else(|| "python3.11".into()),
    )
    .arg(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../tests/oltp_migration_acceptance.py"),
    )
    .arg("--olap-binary")
    .arg(olap)
    .arg("--oltp-binary")
    .arg(oltp)
    .env("PYTHONDONTWRITEBYTECODE", "1")
    .status()
    .expect("launch independent engine migration scenario");
    assert!(status.success(), "offline engine migration scenario failed");
}

#[test]
#[ignore = "run through scripts/acceptance_oltp.py"]
fn acceptance_o24() {
    let Some(config) = std::env::var_os("NABEDRA_OLTP_BENCHMARK_CONFIG") else {
        eprintln!(
            "NABEDRA_OLTP_ACCEPTANCE {{\"scenario\":\"O24\",\"status\":\"not-run\",\"reason\":\"missing matched quota/volume endpoint benchmark configuration and cold-reset capabilities\"}}"
        );
        return;
    };
    let output = std::env::var_os("NABEDRA_OLTP_ACCEPTANCE_OUTPUT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("nabedra-oltp-acceptance"))
        .join("postgres-o24.json");
    let status = std::process::Command::new(
        std::env::var_os("NABEDRA_ACCEPTANCE_PYTHON").unwrap_or_else(|| "python3.11".into()),
    )
    .arg(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../tests/oltp_postgres_benchmark.py"))
    .arg("--config")
    .arg(config)
    .arg("--output")
    .arg(output)
    .env("PYTHONDONTWRITEBYTECODE", "1")
    .status()
    .expect("launch matched PostgreSQL workload");
    assert!(status.success(), "matched PostgreSQL workload failed");
}
