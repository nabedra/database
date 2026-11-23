#![allow(clippy::disallowed_methods)] // integration tests drive real processes and real time; the phase 7 seam covers server/src
use nabedra_database_server::node::spool::{Spool, SpoolError, SpoolLimits};
use std::os::unix::fs::PermissionsExt;
use std::{
    fs,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::{Arc, Barrier},
};

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("nabedra-spool-test-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn root(&self) -> PathBuf {
        fs::read_dir(&self.0)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path()
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn limits(bytes: u64, files: u64) -> SpoolLimits {
    SpoolLimits::new(bytes, files).unwrap()
}

#[test]
fn budgets_and_failed_writers_recover_only_on_drop() {
    let temp = Temp::new();
    let spool = Spool::new(&temp.0, limits(8, 2)).unwrap();
    let op = spool.operation(limits(6, 1));
    let mut writer = op.writer(4).unwrap();
    writer.append(b"abcd").unwrap();
    assert_eq!(op.writer(4).unwrap_err(), SpoolError::Capacity);
    assert_eq!(writer.append(b"e"), Err(SpoolError::Capacity));
    assert_eq!(writer.append(b""), Err(SpoolError::Poisoned));
    assert_eq!(writer.finish().unwrap_err(), SpoolError::Poisoned);
    let mut writer = op.writer(6).unwrap();
    for _ in 0..3 {
        writer.append(b"ab").unwrap();
    }
    let payload = writer.finish().unwrap();
    let other = spool.operation(limits(8, 2));
    let mut pending = other.writer(8).unwrap();
    assert_eq!(pending.append(b"abc"), Err(SpoolError::Capacity));
    drop(pending);
    drop(payload);
    let mut pending = other.writer(8).unwrap();
    pending.append(b"12345678").unwrap();
    assert_eq!(pending.finish().unwrap().len(), 8);
    assert!(fs::read_dir(temp.root()).unwrap().next().is_none());
    // Operation limits can be smaller than process limits, independently.
    let mut writer = op.writer(8).unwrap();
    assert_eq!(writer.append(b"1234567"), Err(SpoolError::Capacity));
    drop(writer);
    drop(op.writer(8).unwrap());
}

#[test]
fn concurrent_operations_share_bytes_and_file_slots() {
    for (bytes, files, successful) in [(6, 4, 2), (100, 1, 1)] {
        let temp = Temp::new();
        let spool = Arc::new(Spool::new(&temp.0, limits(bytes, files)).unwrap());
        let start = Arc::new(Barrier::new(5));
        let retained = Arc::new(Barrier::new(5));
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let spool = spool.clone();
                let start = start.clone();
                let retained = retained.clone();
                std::thread::spawn(move || {
                    let op = spool.operation(limits(100, 4));
                    start.wait();
                    let payload = op.writer(3).and_then(|mut w| {
                        w.append(b"abc")?;
                        w.finish()
                    });
                    retained.wait();
                    payload.is_ok()
                })
            })
            .collect();
        start.wait();
        retained.wait();
        assert_eq!(
            threads
                .into_iter()
                .filter_map(|t| t.join().unwrap().then_some(()))
                .count(),
            successful
        );
        assert!(spool.operation(limits(100, 4)).writer(3).is_ok());
    }
}

#[test]
fn immutable_readers_are_bounded_independent_and_retain_ownership() {
    let temp = Temp::new();
    let spool = Spool::new(&temp.0, limits(6, 1)).unwrap();
    let root = temp.root();
    assert_eq!(
        fs::metadata(&root).unwrap().permissions().mode() & 0o777,
        0o700
    );
    let op = spool.operation(limits(6, 1));
    let mut writer = op.writer(6).unwrap();
    writer.append(b"abcdef").unwrap();
    let file = fs::read_dir(&root).unwrap().next().unwrap().unwrap().path();
    assert_eq!(
        fs::metadata(file).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let payload = writer.finish().unwrap();
    assert_eq!(payload.len(), 6);
    let mut a = payload.open().unwrap();
    let mut b = payload.open().unwrap();
    assert_eq!(a.seek(SeekFrom::End(-2)).unwrap(), 4);
    let mut buf = [0; 10];
    assert_eq!(a.read(&mut buf).unwrap(), 2);
    assert_eq!(&buf[..2], b"ef");
    assert_eq!(a.read(&mut buf).unwrap(), 0);
    assert!(a.seek(SeekFrom::Start(7)).is_err());
    assert!(a.seek(SeekFrom::Current(i64::MIN)).is_err());
    assert_eq!(b.read(&mut buf[..2]).unwrap(), 2);
    assert_eq!(&buf[..2], b"ab");
    drop(payload);
    assert_eq!(op.writer(1).unwrap_err(), SpoolError::Capacity);
    drop(a);
    assert_eq!(op.writer(1).unwrap_err(), SpoolError::Capacity);
    drop(b);
    assert!(op.writer(1).is_ok());
    let payload = op.writer(1).unwrap().finish().unwrap();
    assert!(payload.is_empty());
    drop(payload);
    let mut writer = op.writer(6).unwrap();
    writer.append(b"abcdef").unwrap();
    let payload = writer.finish().unwrap();
    let mut reader = payload.open().unwrap();
    drop(payload);
    drop(op);
    drop(spool);
    assert!(root.exists());
    let mut retained = String::new();
    reader.read_to_string(&mut retained).unwrap();
    assert_eq!(retained, "abcdef");
    drop(reader);
    assert!(!root.exists());
}

#[test]
fn sanitized_failures_creation_rollback_and_absent_file_cleanup() {
    let temp = Temp::new();
    assert_eq!(
        SpoolLimits::new(0, 1).unwrap_err(),
        SpoolError::InvalidConfiguration
    );
    assert_eq!(
        SpoolLimits::new(1, 0).unwrap_err(),
        SpoolError::InvalidConfiguration
    );
    let error = Spool::new(temp.0.join("secret-missing-parent"), limits(1, 1)).unwrap_err();
    assert_eq!(error, SpoolError::Io);
    assert!(!format!("{error:?} {error}").contains("secret"));
    let spool = Spool::new(&temp.0, limits(u64::MAX, u64::MAX)).unwrap();
    assert_eq!(format!("{spool:?}"), "Spool");
    drop(spool);
    let spool = Spool::new(&temp.0, limits(8, 1)).unwrap();
    let op = spool.operation(limits(u64::MAX, u64::MAX));
    assert_eq!(op.writer(0).unwrap_err(), SpoolError::InvalidConfiguration);
    let root = temp.root();
    fs::remove_dir(&root).unwrap();
    assert_eq!(op.writer(1).unwrap_err(), SpoolError::Io);
    fs::create_dir(&root).unwrap();
    let mut writer = op.writer(8).unwrap();
    writer.append(b"12345678").unwrap();
    let payload = writer.finish().unwrap();
    let file = fs::read_dir(&root).unwrap().next().unwrap().unwrap().path();
    fs::remove_file(file).unwrap();
    assert_eq!(payload.open().unwrap_err(), SpoolError::Io);
    drop(payload);
    let mut writer = op.writer(8).unwrap();
    writer.append(b"12345678").unwrap();
}

#[test]
fn failed_deletion_keeps_quota_charged() {
    let temp = Temp::new();
    let spool = Spool::new(&temp.0, limits(1, 1)).unwrap();
    let op = spool.operation(limits(1, 1));
    let mut writer = op.writer(1).unwrap();
    writer.append(b"x").unwrap();
    let payload = writer.finish().unwrap();
    let file = fs::read_dir(temp.root())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    // Narrow fault injection confined to this test's private owned directory.
    fs::remove_file(&file).unwrap();
    fs::create_dir(&file).unwrap();
    drop(payload);
    assert_eq!(op.writer(1).unwrap_err(), SpoolError::Capacity);
}
