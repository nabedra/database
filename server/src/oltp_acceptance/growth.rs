use std::{path::PathBuf, process::Command};

fn run(scenario: &str) {
    let binary = (scenario == "O21")
        .then(|| std::env::var_os("NABEDRA_OLTP_GROWTH_BINARY"))
        .flatten()
        .or_else(|| std::env::var_os("NABEDRA_OLTP_BINARY"));
    let Some(binary) = binary else {
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
