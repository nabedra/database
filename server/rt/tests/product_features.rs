//! No product build of the server enables the simulation (scenario O02, NS26).
//!
//! For each engine feature set the server ships, this reads the resolved normal and build
//! dependency graph with `cargo tree --edges normal,build --format {p}|{f}` (the
//! per-package form of `cargo tree -e features`) and asserts that `nabedra-rt` is in it
//! without its `sim` feature, and that tokio is in it without `test-util`, which only
//! the simulation enables. `scripts/check_engine_features.py` can assert the same on the
//! graphs it already reads for O02.

use std::path::Path;
use std::process::Command;

/// The engine selections `scripts/check_engine_features.py` checks for O02.
const PRODUCT_BUILDS: &[(&str, &[&str])] = &[
    ("default", &[]),
    ("olap", &["--no-default-features", "--features", "olap"]),
    (
        "olap-s3-image",
        &["--no-default-features", "--features", "olap,s3"],
    ),
    ("oltp", &["--no-default-features", "--features", "oltp"]),
    (
        "oltp-s3",
        &["--no-default-features", "--features", "oltp-backup-s3"],
    ),
    ("cache", &["--no-default-features", "--features", "cache"]),
];

fn features_of<'a>(tree: &'a str, package: &str) -> Vec<Vec<&'a str>> {
    tree.lines()
        // A repeated package is printed once in full and then marked `(*)`.
        .map(|line| line.trim_end().trim_end_matches("(*)").trim_end())
        .filter_map(|line| line.split_once('|'))
        .filter(|(name, _)| name.split_whitespace().next() == Some(package))
        .map(|(_, features)| features.split(',').filter(|f| !f.is_empty()).collect())
        .collect()
}

fn tree(selection: &[&str]) -> String {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("../Cargo.toml");
    let output = Command::new(env!("CARGO"))
        .args(["tree", "--locked", "--manifest-path"])
        .arg(&manifest)
        .args(["-p", "nabedra-database"])
        .args(selection)
        .args([
            "--edges",
            "normal,build",
            "--prefix",
            "none",
            "--format",
            "{p}|{f}",
        ])
        .output()
        .expect("run cargo tree");
    assert!(
        output.status.success(),
        "cargo tree {selection:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("cargo tree prints UTF-8")
}

/// Returns the violations of one graph; empty when it is a product graph.
fn violations(name: &str, tree: &str) -> Vec<String> {
    let mut found = Vec::new();
    let runtime = features_of(tree, "nabedra-rt");
    if runtime.is_empty() {
        found.push(format!("{name}: nabedra-rt is not in the graph"));
    }
    if runtime.iter().any(|features| features.contains(&"sim")) {
        found.push(format!("{name}: nabedra-rt has the sim feature"));
    }
    let tokio = features_of(tree, "tokio");
    if tokio.is_empty() {
        found.push(format!("{name}: tokio is not in the graph"));
    }
    if tokio.iter().any(|features| features.contains(&"test-util")) {
        found.push(format!("{name}: tokio has test-util"));
    }
    found
}

#[test]
fn product_builds_do_not_enable_the_simulation() {
    let mut found = Vec::new();
    for (name, selection) in PRODUCT_BUILDS {
        found.extend(violations(name, &tree(selection)));
    }
    assert!(found.is_empty(), "{found:#?}");
}

#[test]
fn a_simulation_build_is_detected() {
    // The check is not vacuous: the server's sim feature shows up in the same graph.
    let found = violations(
        "sim",
        &tree(&["--no-default-features", "--features", "oltp,sim"]),
    );
    assert!(
        found.contains(&"sim: nabedra-rt has the sim feature".to_owned()),
        "{found:#?}"
    );
    assert!(
        found.contains(&"sim: tokio has test-util".to_owned()),
        "{found:#?}"
    );
}
