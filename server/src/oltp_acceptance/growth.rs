use std::{path::PathBuf, process::Command};

pub(crate) use crate::oltp::test_hooks::growth::data_raft_isolated;

pub(crate) fn snapshot_test_config(mut config: openraft::Config) -> openraft::Config {
    if std::env::var_os("NABEDRA_OLTP_TEST_SHORT_DATA_SNAPSHOTS").is_some() {
        config.snapshot_policy = openraft::SnapshotPolicy::LogsSinceLast(16);
        config.max_in_snapshot_log_to_keep = 4;
        config.purge_batch_size = 1;
    }
    config
}

#[test]
#[ignore = "registered worker process for the growth scenarios"]
fn registered_worker_child() {
    let _ = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .with_max_level(tracing::Level::WARN)
        .try_init();
    let path = std::env::var_os("NABEDRA_OLTP_TEST_REGISTERED_CONFIG")
        .expect("registered worker configuration path");
    crate::oltp_runtime::standalone_network_runtime()
        .unwrap()
        .block_on(crate::oltp_runtime::registered_process::run(path.into()))
        .unwrap();
}

fn run(scenario: &str) {
    let Some(binary) = std::env::var_os("NABEDRA_OLTP_BINARY") else {
        eprintln!(
            "NABEDRA_OLTP_ACCEPTANCE {{\"scenario\":\"{scenario}\",\"status\":\"not-run\",\"reason\":\"missing OLTP server executable\"}}"
        );
        return;
    };
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/oltp_growth_acceptance.py");
    let status = Command::new(
        std::env::var_os("NABEDRA_ACCEPTANCE_PYTHON").unwrap_or_else(|| "python3.11".into()),
    )
    .arg(script)
    .args(["--scenario", scenario])
    .arg("--binary")
    .arg(binary)
    .arg("--test-executable")
    .arg(std::env::current_exe().unwrap())
    .env("PYTHONDONTWRITEBYTECODE", "1")
    .status()
    .expect("launch RF3 growth acceptance");
    assert!(status.success(), "{scenario} RF3 assertions failed");
}

#[test]
#[ignore = "requires the Linux OLTP binary and tmpfs mounts"]
fn o21_capacity_deficit() {
    let binary = std::env::var_os("NABEDRA_BIN").expect("OLTP server executable");
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/oltp_growth_acceptance.py");
    let status = Command::new(
        std::env::var_os("NABEDRA_ACCEPTANCE_PYTHON").unwrap_or_else(|| "python3".into()),
    )
    .arg(script)
    .args(["--scenario", "O21", "--capacity-only", "--binary"])
    .arg(binary)
    .arg("--test-executable")
    .arg(std::env::current_exe().unwrap())
    .env("PYTHONDONTWRITEBYTECODE", "1")
    .status()
    .expect("launch O21 capacity regression");
    assert!(status.success(), "O21 capacity assertions failed");
}

#[test]
#[ignore = "run through scripts/acceptance_oltp.py"]
fn acceptance_o16() {
    run("O16");
}

#[test]
#[ignore = "run through scripts/acceptance_oltp.py"]
fn acceptance_o21() {
    run("O21");
}

#[test]
#[ignore = "run through scripts/acceptance_oltp.py"]
fn acceptance_o26() {
    run("O26");
}

#[test]
#[ignore = "run through scripts/acceptance_oltp.py"]
fn acceptance_o28() {
    run("O28");
}
