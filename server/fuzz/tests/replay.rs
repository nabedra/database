//! Stable replay of every corpus seed and regression input.
//!
//! Each target returns its decoder verdict, such as `accept`, `reject` or
//! `Err(Syntax)`. `corpus/<target>.expected` and `regressions/<target>.expected`
//! hold one `<path>\t<verdict>` line per input, so a decoder that starts
//! accepting a known-bad input, or rejecting a valid one, fails here even when
//! it does not panic. After an intended decoder change, rewrite the files with
//! `NABEDRA_FUZZ_BLESS=1 cargo test` and review the diff.

use std::{
    collections::BTreeMap,
    env, fs,
    io::ErrorKind,
    path::{Path, PathBuf},
};

#[test]
fn replay_corpora_and_regressions() {
    let mut failures = Vec::new();
    #[cfg(feature = "cache")]
    {
        replay("cache_resp", nabedra_fuzz::fuzz_cache_resp, &mut failures);
        replay(
            "cache_command",
            nabedra_fuzz::fuzz_cache_command,
            &mut failures,
        );
    }
    #[cfg(feature = "events")]
    {
        replay(
            "events_publish",
            nabedra_fuzz::fuzz_events_publish,
            &mut failures,
        );
        replay("events_poll", nabedra_fuzz::fuzz_events_poll, &mut failures);
        replay("events_ack", nabedra_fuzz::fuzz_events_ack, &mut failures);
        replay(
            "events_lease",
            nabedra_fuzz::fuzz_events_lease,
            &mut failures,
        );
    }
    #[cfg(feature = "olap")]
    {
        replay(
            "olap_operation",
            nabedra_fuzz::fuzz_olap_operation,
            &mut failures,
        );
        replay(
            "olap_predicate",
            nabedra_fuzz::fuzz_olap_predicate,
            &mut failures,
        );
        replay(
            "olap_catalog",
            nabedra_fuzz::fuzz_olap_catalog,
            &mut failures,
        );
    }
    assert!(
        failures.is_empty(),
        "{} replay mismatches (rerun with NABEDRA_FUZZ_BLESS=1 only for intended decoder changes):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

fn replay(target: &str, fuzz: fn(&[u8]) -> String, failures: &mut Vec<String>) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let corpus = root.join("corpus");
    let seeds = check_dir(&corpus, target, fuzz, failures);
    assert!(seeds > 0, "empty corpus for {target}");

    let regressions = root.join("regressions");
    match fs::metadata(regressions.join(target)) {
        Ok(_) => {
            check_dir(&regressions, target, fuzz, failures);
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => panic!("{}: {error}", regressions.display()),
    }
}

/// Replays `<base>/<target>/**` against `<base>/<target>.expected` and returns
/// the number of inputs.
fn check_dir(
    base: &Path,
    target: &str,
    fuzz: fn(&[u8]) -> String,
    failures: &mut Vec<String>,
) -> usize {
    let dir = base.join(target);
    let mut paths = Vec::new();
    collect(&dir, &mut paths);
    paths.sort_unstable();

    let mut actual = BTreeMap::new();
    for path in &paths {
        let data = fs::read(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        let name = path
            .strip_prefix(&dir)
            .unwrap()
            .to_str()
            .expect("UTF-8 replay path")
            .replace('\\', "/");
        let verdict = fuzz(&data);
        assert!(
            !verdict.contains(['\t', '\n']),
            "{target}/{name}: verdict {verdict:?} must fit on one line"
        );
        actual.insert(name, verdict);
    }

    let expected_path = base.join(format!("{target}.expected"));
    if env::var_os("NABEDRA_FUZZ_BLESS").is_some() {
        let text: String = actual
            .iter()
            .map(|(name, verdict)| format!("{name}\t{verdict}\n"))
            .collect();
        fs::write(&expected_path, text)
            .unwrap_or_else(|error| panic!("{}: {error}", expected_path.display()));
        return paths.len();
    }

    let text = fs::read_to_string(&expected_path)
        .unwrap_or_else(|error| panic!("{}: {error}", expected_path.display()));
    let mut expected = BTreeMap::new();
    for line in text.lines() {
        let (name, verdict) = line
            .split_once('\t')
            .unwrap_or_else(|| panic!("{}: malformed line {line:?}", expected_path.display()));
        expected.insert(name.to_owned(), verdict.to_owned());
    }
    for (name, verdict) in &actual {
        match expected.remove(name) {
            Some(want) if &want == verdict => {}
            Some(want) => failures.push(format!(
                "{target}/{name}: expected {want:?}, decoder returned {verdict:?}"
            )),
            None => failures.push(format!(
                "{target}/{name}: no expectation in {}",
                expected_path.display()
            )),
        }
    }
    for name in expected.keys() {
        failures.push(format!(
            "{target}/{name}: listed in {} but the file is missing",
            expected_path.display()
        ));
    }
    paths.len()
}

fn collect(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap_or_else(|error| panic!("{}: {error}", dir.display())) {
        let path = entry.expect("read replay directory entry").path();
        let metadata = fs::symlink_metadata(&path)
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        if metadata.is_dir() {
            collect(&path, out);
        } else {
            assert!(
                metadata.is_file(),
                "expected a regular file at {}",
                path.display()
            );
            out.push(path);
        }
    }
}
