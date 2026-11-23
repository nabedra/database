//! `fs::os` on the host. [`scenario`] is also run on a simulated volume
//! (`sim::volumes::tests`), and the two transcripts must be equal: the simulated
//! branch is trusted only where it matches `std::fs`.

use std::fmt::Debug;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::{
    File, OpenOptions, canonicalize, create_dir, euid, metadata, read, read_dir, read_to_string,
    remove_dir, remove_dir_all, remove_file, rename, rename_noreplace, set_mode, symlink_metadata,
    sync_dir, try_exists, write,
};

fn show<T: Debug>(result: io::Result<T>) -> String {
    match result {
        Ok(value) => format!("ok {value:?}"),
        Err(error) => format!("err {:?}", error.kind()),
    }
}

/// Runs the same sequence of `fs::os` calls under `root` (an existing, empty directory)
/// and returns what each one did, with `root` removed from every path.
pub(crate) fn scenario(root: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut log = |label: &str, line: String| out.push(format!("{label}: {line}"));
    let d = root.join("d");

    log("mkdir", show(create_dir(&d)));
    log("mkdir again", show(create_dir(&d)));
    log("mkdir orphan", show(create_dir(root.join("x/y"))));
    log("write", show(write(d.join("a"), b"hello")));
    log("read_to_string", show(read_to_string(d.join("a"))));
    log("read missing", show(read(d.join("missing"))));

    let b = d.join("b");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o640)
        .open(&b);
    log(
        "create_new",
        show(
            file.as_ref()
                .map(|_| ())
                .map_err(|e| io::Error::from(e.kind())),
        ),
    );
    let file = file.unwrap();
    log("write_all", show((&file).write_all(b"abc")));
    log("seek", show((&file).seek(SeekFrom::Start(1))));
    let mut two = [0u8; 2];
    log("read", show((&file).read_exact(&mut two).map(|()| two)));
    let meta = file.metadata().unwrap();
    log(
        "fstat",
        format!(
            "len {} file {} dir {} mode {:o} owner {}",
            meta.len(),
            meta.is_file(),
            meta.is_dir(),
            meta.mode() & 0o7777,
            meta.uid() == euid()
        ),
    );
    log(
        "create_new again",
        show(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&b)
                .map(|_| ()),
        ),
    );
    let appender = OpenOptions::new().append(true).open(&b).unwrap();
    log("append", show((&appender).write_all(b"de")));
    log("after append", show(read(&b)));
    log(
        "truncate",
        show(
            OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&b)
                .and_then(|f| f.metadata())
                .map(|m| m.len()),
        ),
    );
    log("read past end", show(file.read_exact_at(&mut two, 5)));
    log("write_all_at", show(file.write_all_at(b"z", 10)));
    log("len", show(metadata(&b).map(|m| m.len())));
    log("set_len", show(file.set_len(4).and_then(|()| read(&b))));
    log("no access", show(OpenOptions::new().open(&b).map(|_| ())));
    log(
        "create read-only",
        show(
            OpenOptions::new()
                .read(true)
                .create(true)
                .open(d.join("n"))
                .map(|_| ()),
        ),
    );

    let directory = File::open(&d).unwrap();
    log(
        "dir fstat",
        show(directory.metadata().map(|m| (m.is_dir(), m.is_file()))),
    );
    log("dir sync", show(directory.sync_all()));
    log("dir lock", show(directory.try_lock_exclusive()));
    let other = File::open(&d).unwrap();
    log("dir lock held", show(other.try_lock_exclusive()));
    directory.unlock();
    log("dir lock freed", show(other.try_lock_exclusive()));
    other.unlock();
    log(
        "O_DIRECTORY file",
        show(
            OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
                .open(&b)
                .map(|_| ()),
        ),
    );
    log(
        "O_DIRECTORY dir",
        show(
            OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
                .open(&d)
                .map(|_| ()),
        ),
    );
    log(
        "dir for write",
        show(OpenOptions::new().write(true).open(&d).map(|_| ())),
    );

    let mut names: Vec<(String, bool)> = read_dir(&d)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            assert_eq!(entry.path(), d.join(entry.file_name()));
            (
                entry.file_name().into_string().unwrap(),
                entry.metadata().unwrap().is_file() && entry.file_type().unwrap().is_file(),
            )
        })
        .collect();
    names.sort();
    log("read_dir", format!("{names:?}"));
    log(
        "read_dir missing",
        show(read_dir(root.join("none")).map(|_| ())),
    );

    log("rename", show(rename(d.join("a"), d.join("c"))));
    log("rename missing", show(rename(d.join("a"), d.join("c"))));
    log(
        "noreplace onto file",
        show(rename_noreplace(d.join("c"), &b)),
    );
    log(
        "noreplace",
        show(rename_noreplace(d.join("c"), d.join("e"))),
    );
    log("set_mode", show(set_mode(d.join("e"), 0o600)));
    let e = metadata(d.join("e")).unwrap();
    let dir = metadata(&d).unwrap();
    log(
        "stat",
        format!(
            "mode {:o} nlink {} same dev {} distinct ino {} symlink {}",
            e.mode() & 0o7777,
            e.nlink(),
            e.dev() == dir.dev(),
            e.ino() != dir.ino() && e.ino() != metadata(&b).unwrap().ino(),
            symlink_metadata(d.join("e")).unwrap().is_symlink()
        ),
    );
    log(
        "file type",
        format!(
            "{:?}",
            (
                e.file_type().is_file(),
                e.file_type().is_dir(),
                e.file_type().is_block_device()
            )
        ),
    );
    log("exists", show(try_exists(d.join("e"))));
    log("exists missing", show(try_exists(d.join("a"))));
    log("rmdir non-empty", show(remove_dir(&d)));
    log("unlink", show(remove_file(d.join("e"))));
    log("unlink missing", show(remove_file(d.join("e"))));
    create_dir(root.join("d2")).unwrap();
    log(
        "canonicalize",
        show(
            canonicalize(root.join("d2/../d2"))
                .and_then(|path| Ok(path == canonicalize(root.join("d2"))?)),
        ),
    );
    log("rmdir", show(remove_dir(root.join("d2"))));
    log("sync_dir", show(sync_dir(root)));
    let space = file.statvfs().unwrap();
    log(
        "statvfs",
        format!(
            "{}",
            space.block_size > 0 && space.available <= space.blocks && space.blocks > 0
        ),
    );
    log(
        "create truncates",
        show(File::create(&b).and_then(|f| f.metadata()).map(|m| m.len())),
    );
    drop((file, appender, directory, other));
    log("rm -r", show(remove_dir_all(&d)));
    log("gone", show(try_exists(&d)));
    let root_text = root.display().to_string();
    out.into_iter()
        .map(|line| line.replace(&root_text, "<root>"))
        .collect()
}

/// A fresh, empty directory on the host.
pub(crate) fn host_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("nabedra-rt-os-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::canonicalize(dir).unwrap()
}

#[test]
fn the_scenario_runs_on_the_host() {
    let root = host_dir("scenario");
    let transcript = scenario(&root);
    std::fs::remove_dir_all(&root).unwrap();
    // A few lines pin the host behaviour the simulated volume must match.
    for expected in [
        "mkdir again: err AlreadyExists",
        "mkdir orphan: err NotFound",
        "read_to_string: ok \"hello\"",
        "read: ok [98, 99]",
        "fstat: len 3 file true dir false mode 640 owner true",
        "create_new again: err AlreadyExists",
        "after append: ok [97, 98, 99, 100, 101]",
        "truncate: ok 0",
        "dir lock: ok true",
        "dir lock held: ok false",
        "dir lock freed: ok true",
        "O_DIRECTORY file: err NotADirectory",
        "dir for write: err IsADirectory",
        "noreplace onto file: err AlreadyExists",
        "rmdir non-empty: err DirectoryNotEmpty",
        "gone: ok false",
    ] {
        assert!(
            transcript.iter().any(|line| line == expected),
            "missing {expected:?} in {transcript:#?}"
        );
    }
}

#[test]
fn fd_path_names_the_open_file() {
    let root = host_dir("fd-path");
    let file = File::create(root.join("f")).unwrap();
    let path = file.fd_path();
    #[cfg(target_os = "linux")]
    assert_eq!(std::fs::read_link(&path).unwrap(), root.join("f"));
    let _ = path;
    std::fs::remove_dir_all(&root).unwrap();
}

#[cfg(not(feature = "sim"))]
#[test]
fn product_wrappers_are_the_std_types() {
    use std::mem::size_of;
    assert_eq!(size_of::<File>(), size_of::<std::fs::File>());
    assert_eq!(size_of::<OpenOptions>(), size_of::<std::fs::OpenOptions>());
    assert_eq!(size_of::<super::Metadata>(), size_of::<std::fs::Metadata>());
    assert_eq!(size_of::<super::FileType>(), size_of::<std::fs::FileType>());
    assert_eq!(size_of::<super::ReadDir>(), size_of::<std::fs::ReadDir>());
    assert_eq!(size_of::<super::DirEntry>(), size_of::<std::fs::DirEntry>());
}
