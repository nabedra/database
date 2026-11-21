use std::{path::PathBuf, process::Command};

fn run(scenario: &str) {
    let Some(binary) = std::env::var_os("NABEDRA_OLTP_BINARY") else {
        let marker = serde_json::json!({
            "scenario": scenario, "status": "not-run",
            "reason": "missing the installed OLTP server executable",
        });
        eprintln!("NABEDRA_OLTP_ACCEPTANCE {marker}");
        return;
    };
    let status = Command::new(
        std::env::var_os("NABEDRA_ACCEPTANCE_PYTHON").unwrap_or_else(|| "python3.11".into()),
    )
    .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/oltp_acc6_acceptance.py"))
    .arg(scenario)
    .arg(binary)
    .arg(std::env::current_exe().unwrap())
    .env("PYTHONDONTWRITEBYTECODE", "1")
    .status()
    .expect("launch ACC6 process assertions");
    assert!(status.success(), "{scenario} process assertions failed");
}

#[test]
#[ignore = "run through scripts/acceptance_oltp.py"]
fn acceptance_o02() {
    run("O02");
}

#[test]
#[ignore = "run through scripts/acceptance_oltp.py"]
fn acceptance_o03() {
    run("O03");
}

#[test]
#[ignore = "run through scripts/acceptance_oltp.py"]
fn acceptance_o04() {
    run("O04");
}

#[test]
#[ignore = "run through scripts/acceptance_oltp.py"]
fn acceptance_o05() {
    run("O05");
}

#[test]
#[ignore = "run through scripts/acceptance_oltp.py"]
fn acceptance_o22() {
    run("O22");
}

#[test]
#[ignore = "run through scripts/acceptance_oltp.py"]
fn acceptance_o23() {
    run("O23");
}
