#![allow(clippy::disallowed_methods)] // test support, also #[path]-included by unit tests; the phase 7 seam covers product code
use std::{
    fs,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

pub(super) fn root() -> PathBuf {
    let root = PathBuf::from(
        std::env::var_os("NABEDRA_DURABILITY_ROOT")
            .expect("run this fixture through oltp_native_durability.py"),
    );
    assert_eq!(root.canonicalize().unwrap(), root);
    root
}

pub(super) fn publish(root: &Path, name: &str, contents: impl AsRef<[u8]>) {
    let temporary = root.join(format!("{name}.new"));
    fs::write(&temporary, contents).unwrap();
    fs::rename(temporary, root.join(name)).unwrap();
}

pub(super) fn arm(root: &Path, mode: &str) {
    let mut logs = fs::read_dir(root.join("pool"))
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .and_then(|name| name.strip_suffix(".log"))
                .is_some_and(|number| {
                    !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit())
                })
        });
    let log = logs.next().expect("fresh pool must have an active WAL");
    assert!(logs.next().is_none(), "expected a single owned WAL");
    assert!(log.file_type().unwrap().is_file());
    let metadata = log.metadata().unwrap();
    publish(
        root,
        "arm",
        format!(
            "{} {mode} {} {} {}\n",
            log.file_name().to_str().unwrap(),
            metadata.len(),
            metadata.dev(),
            metadata.ino()
        ),
    );
}
