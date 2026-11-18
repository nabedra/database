//! Essential production acceptance entry points. Component regressions remain
//! in their owning modules. Run these explicitly through the central runner.
use std::{path::PathBuf, process::Command};

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
