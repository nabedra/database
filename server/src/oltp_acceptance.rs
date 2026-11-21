//! Essential production acceptance entry points. Component regressions remain
//! in their owning modules. Run these explicitly through the central runner.
use std::{path::PathBuf, process::Command};

#[path = "oltp_acceptance/o07.rs"]
mod o07;
#[path = "oltp_acceptance/o10.rs"]
mod o10;
#[path = "oltp_acceptance/growth.rs"]
mod growth;

fn transaction_scenario(scenario: &str) {
    let Some(binary) = std::env::var_os("NABEDRA_OLTP_BINARY") else {
        let marker = serde_json::json!({
            "scenario": scenario, "status": "not-run", "reason": "missing actual OLTP server executable",
        });
        eprintln!("NABEDRA_OLTP_ACCEPTANCE {marker}");
        return;
    };
    let script =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/oltp_transaction_acceptance.py");
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
    .expect("launch RF3 transaction acceptance");
    assert!(
        status.success(),
        "{scenario} RF3 transaction assertions failed"
    );
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
