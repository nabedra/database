//! Essential production acceptance entry points. Component regressions remain
//! in their owning modules. Run these explicitly through the central runner.
use std::{ffi::OsString, path::PathBuf, process::Command};

#[path = "oltp_acceptance/acc6.rs"]
mod acc6;
#[path = "oltp_acceptance/o07.rs"]
mod o07;
#[path = "oltp_acceptance/o10.rs"]
mod o10;

fn transaction_scenario(scenario: &str) {
    process_scenario(scenario, "oltp_transaction_acceptance.py");
}

fn process_binary() -> Option<OsString> {
    std::env::var_os("NABEDRA_OLTP_BINARY").or_else(|| std::env::var_os("NABEDRA_BIN"))
}

fn process_driver(driver: &str) -> Command {
    let python = std::env::var_os("NABEDRA_ACCEPTANCE_PYTHON").unwrap_or_else(|| {
        if std::env::var_os("NABEDRA_OLTP_BINARY").is_some() {
            "python3.11".into()
        } else {
            // The shared-build repeat job provides NABEDRA_BIN and runner Python.
            "python3".into()
        }
    });
    let mut command = Command::new(python);
    command
        .arg(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests")
                .join(driver),
        )
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .env(
            "TOKIO_WORKER_THREADS",
            std::env::var_os("TOKIO_WORKER_THREADS").unwrap_or_else(|| "2".into()),
        );
    command
}

fn process_scenario(scenario: &str, driver: &str) {
    let Some(binary) = process_binary() else {
        let marker = serde_json::json!({
            "scenario": scenario, "status": "not-run", "reason": "missing actual OLTP server executable",
        });
        eprintln!("NABEDRA_OLTP_ACCEPTANCE {marker}");
        return;
    };
    let status = process_driver(driver)
        .args(["--scenario", scenario])
        .arg("--binary")
        .arg(binary)
        .arg("--test-executable")
        .arg(std::env::current_exe().unwrap())
        .status()
        .expect("launch RF3 acceptance driver");
    assert!(status.success(), "{scenario} RF3 process assertions failed");
}

#[test]
#[ignore = "run through scripts/acceptance_oltp.py"]
fn acceptance_o01() {
    let mut binaries = Vec::new();
    for name in ["NABEDRA_OLAP_BINARY", "NABEDRA_OLTP_BINARY"] {
        let Some(value) = std::env::var_os(name) else {
            eprintln!(
                "NABEDRA_OLTP_ACCEPTANCE {{\"scenario\":\"O01\",\"status\":\"not-run\",\"reason\":\"missing {name}\"}}"
            );
            return;
        };
        let path = PathBuf::from(value);
        assert!(path.is_file(), "configured acceptance binary is absent");
        binaries.push(path);
    }
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let status = Command::new(
        std::env::var_os("NABEDRA_ACCEPTANCE_PYTHON").unwrap_or_else(|| "python3.11".into()),
    )
    .arg(manifest.join("tests/oltp_compatibility.py"))
    .arg("--olap-binary")
    .arg(&binaries[0])
    .arg("--oltp-binary")
    .arg(&binaries[1])
    .env("PYTHONDONTWRITEBYTECODE", "1")
    .status()
    .expect("launch unchanged public ORM program");
    assert!(
        status.success(),
        "public ORM parity failed on actual servers"
    );
}

/// Scratch probe for ACC3-ready-fence (never shipped).
#[cfg(target_os = "linux")]
#[test]
#[ignore = "real RF3 processes and loopback mounts, run through the CI tests input"]
fn probe_acc3_ready_fence_survives_a_timestamp_outage() {
    let status = process_driver("probe_ready_fence.py")
        .arg("--binary")
        .arg(process_binary().expect("shared-build OLTP binary"))
        .arg("--test-executable")
        .arg(std::env::current_exe().unwrap())
        .env(
            "PYTHONPATH",
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../orm/python/src"),
        )
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .status();
    assert!(
        status.expect("launch ready-fence probe").success(),
        "the rolling join did not survive the Timestamp outage"
    );
}

#[test]
#[ignore = "run through scripts/acceptance_oltp.py"]
fn acceptance_o06() {
    crate::oltp_replication::manager::acceptance_o06_storage();
    process_scenario("O06", "oltp_hardening_acceptance.py");
}

#[test]
#[ignore = "run through scripts/acceptance_oltp.py"]
fn acceptance_o08() {
    process_scenario("O08", "oltp_hardening_acceptance.py");
}

#[test]
#[ignore = "run through scripts/acceptance_oltp.py"]
fn acceptance_o09() {
    process_scenario("O09", "oltp_hardening_acceptance.py");
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires an OLTP server and Linux network namespaces with passwordless sudo"]
fn o06_o18_packet_drop_failover() {
    let output = std::env::temp_dir().join(format!("nabedra-blackhole-{}", nabedra_rt::ids::uuid_v4()));
    let status = process_driver("oltp_blackhole.py")
        .arg("--binary")
        .arg(process_binary().expect("shared-build OLTP binary"))
        .arg("--test-executable")
        .arg(std::env::current_exe().unwrap())
        .arg("--output")
        .arg(&output)
        .env(
            "PYTHONPATH",
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../orm/python/src"),
        )
        .status();
    let _ = std::fs::remove_dir_all(output);
    assert!(
        status.expect("launch packet-drop fixture").success(),
        "O06/O18 packet-drop assertions failed"
    );
}

#[test]
#[ignore = "run through scripts/acceptance_oltp.py"]
fn acceptance_o11() {
    crate::oltp_replication::hosted::acceptance_o11_storage();
    process_scenario("O11", "oltp_hardening_acceptance.py");
}

#[tokio::test]
#[ignore = "run through scripts/acceptance_oltp.py"]
async fn acceptance_o29() {
    nabedra_rt::time::timeout(
        std::time::Duration::from_secs(30),
        crate::oltp_runtime::recovery_tests::pool_recovery::o29_pool_recovery(),
    )
    .await
    .expect("O29 pool recovery watchdog");
    process_scenario("O29", "oltp_o29_acceptance.py");
}

pub(crate) fn clause(scenario: &str, assertion: &str, status: &str, detail: &str) {
    let marker = serde_json::json!({
        "scenario": scenario, "assertion": assertion, "status": status, "detail": detail,
    });
    println!("NABEDRA_OLTP_ASSERT {marker}");
}
