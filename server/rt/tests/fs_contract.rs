//! The filesystem contract: the same cases against `RealFs` (in a temporary directory)
//! and against `FaultFs`. A fake is trusted only for behaviour this suite confirms on the
//! real backend, so a case that passes on `FaultFs` and fails on `RealFs` means the fake
//! is wrong (scenarios NS04, NS05 and NS15 rely on it; guarantee LSM-G01).
//!
//! The directory and metadata cases compare `FaultFs`'s own methods (`create_dir`,
//! `remove_dir`, `remove_dir_all`, `set_mode`, `stat`) with what `std::fs` does on the
//! real filesystem, because that is what `nabedra_rt::fs::os` forwards to in product.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};

use nabedra_rt::fs::{FaultFs, Fs, FsKind, FsOp, FsStat, OpenMode, RealFs};

trait Backend {
    fn fs(&self) -> Arc<dyn Fs>;
    /// An existing, empty directory for the case.
    fn root(&self) -> &Path;
    fn create_dir(&self, path: &Path) -> io::Result<()>;
    fn remove_dir(&self, path: &Path) -> io::Result<()>;
    fn remove_dir_all(&self, path: &Path) -> io::Result<()>;
    fn set_mode(&self, path: &Path, mode: u32) -> io::Result<()>;
    fn stat(&self, path: &Path) -> io::Result<FsStat>;
    /// The owner of files this backend creates.
    fn uid(&self) -> u32;
}

struct Fake {
    fs: FaultFs,
    root: PathBuf,
}

impl Fake {
    fn new() -> Fake {
        let fs = FaultFs::with_device(11, 1 << 20);
        let root = PathBuf::from("/contract");
        fs.create_dir_all_durable(&root);
        Fake { fs, root }
    }
}

impl Backend for Fake {
    fn fs(&self) -> Arc<dyn Fs> {
        Arc::new(self.fs.clone())
    }

    fn root(&self) -> &Path {
        &self.root
    }

    fn create_dir(&self, path: &Path) -> io::Result<()> {
        self.fs.create_dir(path)
    }

    fn remove_dir(&self, path: &Path) -> io::Result<()> {
        self.fs.remove_dir(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        self.fs.remove_dir_all(path)
    }

    fn set_mode(&self, path: &Path, mode: u32) -> io::Result<()> {
        self.fs.set_mode(path, mode)
    }

    fn stat(&self, path: &Path) -> io::Result<FsStat> {
        self.fs.stat(path)
    }

    fn uid(&self) -> u32 {
        FaultFs::UID
    }
}

#[cfg(unix)]
struct Real {
    root: PathBuf,
}

#[cfg(unix)]
impl Real {
    fn new(case: &str) -> Real {
        let root = std::env::temp_dir().join(format!(
            "nabedra-rt-fs-contract-{case}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        Real { root }
    }
}

#[cfg(unix)]
impl Drop for Real {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[cfg(unix)]
impl Backend for Real {
    fn fs(&self) -> Arc<dyn Fs> {
        Arc::new(RealFs)
    }

    fn root(&self) -> &Path {
        &self.root
    }

    fn create_dir(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir(path)
    }

    fn remove_dir(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_dir(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_dir_all(path)
    }

    fn set_mode(&self, path: &Path, mode: u32) -> io::Result<()> {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
    }

    fn stat(&self, path: &Path) -> io::Result<FsStat> {
        use std::os::unix::fs::MetadataExt;
        let meta = std::fs::symlink_metadata(path)?;
        let kind = if meta.is_dir() {
            FsKind::Dir
        } else {
            assert!(meta.is_file(), "the contract creates no other kind");
            FsKind::File
        };
        Ok(FsStat {
            kind,
            len: meta.len(),
            dev: meta.dev(),
            ino: meta.ino(),
            nlink: meta.nlink(),
            mode: meta.mode() & 0o7777,
            uid: meta.uid(),
        })
    }

    fn uid(&self) -> u32 {
        // SAFETY: geteuid has no preconditions.
        unsafe { libc::geteuid() }
    }
}

/// Runs `case` on both backends as two tests.
macro_rules! contract {
    ($($case:ident),* $(,)?) => {
        mod fault_fs {
            $(
                #[test]
                fn $case() {
                    super::$case(&super::Fake::new());
                }
            )*
        }

        #[cfg(unix)]
        mod real_fs {
            $(
                #[test]
                fn $case() {
                    super::$case(&super::Real::new(stringify!($case)));
                }
            )*
        }
    };
}

contract!(
    create_new_and_open_errors,
    create_new_has_one_winner,
    rename_replaces_an_existing_file,
    listing_follows_create_and_remove,
    lock_is_exclusive_until_dropped,
    lock_follows_the_file_through_a_directory_rename,
    set_len_and_len_agree,
    positional_io_at_and_beyond_the_end,
    create_dir_and_remove_dir_errors,
    remove_dir_all_removes_a_tree,
    stat_kinds_modes_and_identity,
    rename_moves_a_directory_and_its_tree,
);

fn kind<T>(result: io::Result<T>) -> io::ErrorKind {
    match result {
        Ok(_) => panic!("expected an error"),
        Err(error) => error.kind(),
    }
}

fn write(fs: &dyn Fs, path: &Path, bytes: &[u8]) {
    let file = fs.open(path, OpenMode::CreateNew).unwrap();
    file.write_all_at(bytes, 0).unwrap();
}

fn read(fs: &dyn Fs, path: &Path) -> Vec<u8> {
    let file = fs.open(path, OpenMode::Read).unwrap();
    let mut bytes = vec![0; file.len().unwrap() as usize];
    file.read_exact_at(&mut bytes, 0).unwrap();
    bytes
}

fn sorted(mut paths: Vec<PathBuf>) -> Vec<PathBuf> {
    paths.sort();
    paths
}

fn create_new_and_open_errors(b: &dyn Backend) {
    let fs = b.fs();
    let path = b.root().join("a");
    let file = fs.open(&path, OpenMode::CreateNew).unwrap();
    assert_eq!(
        kind(fs.open(&path, OpenMode::CreateNew)),
        io::ErrorKind::AlreadyExists
    );
    assert_eq!(
        kind(fs.open(&b.root().join("missing"), OpenMode::Read)),
        io::ErrorKind::NotFound
    );
    assert_eq!(
        kind(fs.open(&b.root().join("missing"), OpenMode::ReadWrite)),
        io::ErrorKind::NotFound
    );
    assert_eq!(
        kind(fs.open(&b.root().join("nodir/a"), OpenMode::CreateNew)),
        io::ErrorKind::NotFound
    );
    // A read-only handle cannot write.
    let reader = fs.open(&path, OpenMode::Read).unwrap();
    assert!(reader.write_all_at(b"x", 0).is_err());
    // Both handles see the same file.
    file.write_all_at(b"shared", 0).unwrap();
    let mut buf = [0u8; 6];
    reader.read_exact_at(&mut buf, 0).unwrap();
    assert_eq!(&buf, b"shared");
    assert!(fs.exists(&path).unwrap());
    assert!(fs.exists(b.root()).unwrap());
    assert!(!fs.exists(&b.root().join("missing")).unwrap());
}

fn create_new_has_one_winner(b: &dyn Backend) {
    let fs = b.fs();
    let path = b.root().join("raced");
    let arrive = Barrier::new(2);
    let (first, second) = std::thread::scope(|scope| {
        let create = |payload: u8| {
            arrive.wait();
            fs.open(&path, OpenMode::CreateNew)
                .and_then(|file| file.write_all_at(&[payload], 0))
        };
        let first = scope.spawn(move || create(1));
        let second = scope.spawn(move || create(2));
        (first.join().unwrap(), second.join().unwrap())
    });
    let winner = match (first, second) {
        (Ok(()), Err(error)) if error.kind() == io::ErrorKind::AlreadyExists => 1,
        (Err(error), Ok(())) if error.kind() == io::ErrorKind::AlreadyExists => 2,
        results => panic!("expected one creator and one AlreadyExists: {results:?}"),
    };
    assert_eq!(read(&*fs, &path), [winner]);
}

fn rename_replaces_an_existing_file(b: &dyn Backend) {
    let fs = b.fs();
    let (from, to) = (b.root().join("from"), b.root().join("to"));
    write(&*fs, &from, b"new");
    write(&*fs, &to, b"old contents");
    let ino = b.stat(&from).unwrap().ino;
    fs.rename(&from, &to).unwrap();
    assert_eq!(read(&*fs, &to), b"new");
    assert!(!fs.exists(&from).unwrap());
    assert_eq!(b.stat(&to).unwrap().ino, ino, "a rename keeps the inode");
    assert_eq!(kind(fs.rename(&from, &to)), io::ErrorKind::NotFound);
    // A file cannot replace a directory.
    let dir = b.root().join("dir");
    b.create_dir(&dir).unwrap();
    assert_eq!(kind(fs.rename(&to, &dir)), io::ErrorKind::IsADirectory);
    assert!(fs.exists(&to).unwrap());
}

fn listing_follows_create_and_remove(b: &dyn Backend) {
    let fs = b.fs();
    let root = b.root();
    assert_eq!(fs.list(root).unwrap(), Vec::<PathBuf>::new());
    write(&*fs, &root.join("a"), b"a");
    write(&*fs, &root.join("b"), b"b");
    fs.create_dir_all(&root.join("sub/deeper")).unwrap();
    assert_eq!(
        sorted(fs.list(root).unwrap()),
        vec![root.join("a"), root.join("b"), root.join("sub")]
    );
    fs.remove(&root.join("a")).unwrap();
    assert_eq!(kind(fs.remove(&root.join("a"))), io::ErrorKind::NotFound);
    assert_eq!(
        sorted(fs.list(root).unwrap()),
        vec![root.join("b"), root.join("sub")]
    );
    assert_eq!(
        fs.list(&root.join("sub")).unwrap(),
        vec![root.join("sub/deeper")]
    );
    assert_eq!(kind(fs.list(&root.join("none"))), io::ErrorKind::NotFound);
    // create_dir_all on an existing directory succeeds.
    fs.create_dir_all(&root.join("sub")).unwrap();
    fs.sync_dir(root).unwrap();
}

fn lock_is_exclusive_until_dropped(b: &dyn Backend) {
    let fs = b.fs();
    let path = b.root().join("LOCK");
    let lock = fs.lock(&path).unwrap();
    assert_eq!(kind(fs.lock(&path)), io::ErrorKind::WouldBlock);
    let other = fs.lock(&b.root().join("OTHER")).unwrap();
    drop(lock);
    let again = fs.lock(&path).unwrap();
    drop((again, other));
    assert!(fs.exists(&path).unwrap(), "the lock file stays");
}

fn lock_follows_the_file_through_a_directory_rename(b: &dyn Backend) {
    let fs = b.fs();
    let from = b.root().join("staging");
    let to = b.root().join("active");
    b.create_dir(&from).unwrap();
    let original = fs.lock(&from.join("LOCK")).unwrap();
    fs.rename(&from, &to).unwrap();
    let path = to.join("LOCK");
    assert_eq!(kind(fs.lock(&path)), io::ErrorKind::WouldBlock);
    drop(original);
    let held = fs.lock(&path).unwrap();

    fs.remove(&path).unwrap();
    let replacement = fs.lock(&path).unwrap();
    drop(held);
    assert_eq!(kind(fs.lock(&path)), io::ErrorKind::WouldBlock);
    drop(replacement);
    assert!(fs.lock(&path).is_ok());
    assert_eq!(kind(fs.lock(&to)), io::ErrorKind::IsADirectory);
}

fn set_len_and_len_agree(b: &dyn Backend) {
    let fs = b.fs();
    let file = fs.open(&b.root().join("f"), OpenMode::CreateNew).unwrap();
    assert_eq!(file.len().unwrap(), 0);
    file.write_all_at(&[7u8; 100], 0).unwrap();
    file.set_len(10).unwrap();
    assert_eq!(file.len().unwrap(), 10);
    file.set_len(50).unwrap();
    assert_eq!(file.len().unwrap(), 50);
    let mut tail = [9u8; 40];
    file.read_exact_at(&mut tail, 10).unwrap();
    assert_eq!(tail, [0u8; 40], "bytes past a truncation read as zeros");
    file.preallocate(4096).unwrap();
    assert!(file.len().unwrap() >= 4096);
    file.sync_data().unwrap();
    file.sync_all().unwrap();
}

fn positional_io_at_and_beyond_the_end(b: &dyn Backend) {
    let fs = b.fs();
    let file = fs.open(&b.root().join("f"), OpenMode::CreateNew).unwrap();
    // A write past the end extends the file with zeros.
    file.write_all_at(b"end", 100).unwrap();
    assert_eq!(file.len().unwrap(), 103);
    let mut head = [1u8; 100];
    file.read_exact_at(&mut head, 0).unwrap();
    assert_eq!(head, [0u8; 100]);
    let mut buf = [0u8; 3];
    file.read_exact_at(&mut buf, 100).unwrap();
    assert_eq!(&buf, b"end");
    // Reads that cross or start past the end fail.
    assert_eq!(
        kind(file.read_exact_at(&mut buf, 101)),
        io::ErrorKind::UnexpectedEof
    );
    assert_eq!(
        kind(file.read_exact_at(&mut buf, 500)),
        io::ErrorKind::UnexpectedEof
    );
    // An empty read at the end succeeds.
    file.read_exact_at(&mut [], 103).unwrap();
    // Overwrites keep the length.
    file.write_all_at(b"E", 100).unwrap();
    assert_eq!(file.len().unwrap(), 103);
}

fn create_dir_and_remove_dir_errors(b: &dyn Backend) {
    let fs = b.fs();
    let dir = b.root().join("d");
    b.create_dir(&dir).unwrap();
    assert_eq!(kind(b.create_dir(&dir)), io::ErrorKind::AlreadyExists);
    assert_eq!(
        kind(b.create_dir(&b.root().join("missing/d"))),
        io::ErrorKind::NotFound
    );
    write(&*fs, &dir.join("f"), b"x");
    assert_eq!(
        kind(b.create_dir(&dir.join("f"))),
        io::ErrorKind::AlreadyExists
    );
    assert_eq!(kind(b.remove_dir(&dir)), io::ErrorKind::DirectoryNotEmpty);
    assert_eq!(
        kind(b.remove_dir(&dir.join("f"))),
        io::ErrorKind::NotADirectory
    );
    assert_eq!(
        kind(b.remove_dir(&b.root().join("none"))),
        io::ErrorKind::NotFound
    );
    fs.remove(&dir.join("f")).unwrap();
    b.remove_dir(&dir).unwrap();
    assert!(!fs.exists(&dir).unwrap());
}

fn remove_dir_all_removes_a_tree(b: &dyn Backend) {
    let fs = b.fs();
    let tree = b.root().join("t");
    fs.create_dir_all(&tree.join("a/b")).unwrap();
    write(&*fs, &tree.join("x"), b"x");
    write(&*fs, &tree.join("a/b/y"), b"y");
    b.remove_dir_all(&tree).unwrap();
    assert!(!fs.exists(&tree).unwrap());
    assert_eq!(fs.list(b.root()).unwrap(), Vec::<PathBuf>::new());
    assert_eq!(kind(b.remove_dir_all(&tree)), io::ErrorKind::NotFound);
}

fn stat_kinds_modes_and_identity(b: &dyn Backend) {
    let fs = b.fs();
    let (file, other, dir) = (b.root().join("f"), b.root().join("g"), b.root().join("d"));
    write(&*fs, &file, b"12345");
    write(&*fs, &other, b"");
    b.create_dir(&dir).unwrap();
    let f = b.stat(&file).unwrap();
    let g = b.stat(&other).unwrap();
    let d = b.stat(&dir).unwrap();
    assert_eq!(
        (f.kind, f.len, f.nlink, f.uid),
        (FsKind::File, 5, 1, b.uid())
    );
    assert_eq!((d.kind, d.uid), (FsKind::Dir, b.uid()));
    assert_eq!(b.stat(b.root()).unwrap().kind, FsKind::Dir);
    // One device, distinct inodes.
    assert_eq!(f.dev, d.dev);
    assert_eq!(f.dev, b.stat(b.root()).unwrap().dev);
    assert!(f.ino != g.ino && f.ino != d.ino && g.ino != d.ino);
    b.set_mode(&file, 0o640).unwrap();
    b.set_mode(&dir, 0o750).unwrap();
    assert_eq!(b.stat(&file).unwrap().mode, 0o640);
    assert_eq!(b.stat(&dir).unwrap().mode, 0o750);
    b.set_mode(&file, 0o600).unwrap();
    assert_eq!(b.stat(&file).unwrap().mode, 0o600);
    assert_eq!(
        kind(b.stat(&b.root().join("none"))),
        io::ErrorKind::NotFound
    );
    assert_eq!(
        kind(b.set_mode(&b.root().join("none"), 0o600)),
        io::ErrorKind::NotFound
    );
}

fn rename_moves_a_directory_and_its_tree(b: &dyn Backend) {
    let fs = b.fs();
    let (staging, active) = (b.root().join("staging"), b.root().join("active"));
    fs.create_dir_all(&staging.join("inner")).unwrap();
    write(&*fs, &staging.join("inner/f"), b"payload");
    let ino = b.stat(&staging).unwrap().ino;
    let full = b.root().join("full");
    b.create_dir(&full).unwrap();
    write(&*fs, &full.join("x"), b"x");
    assert_eq!(
        kind(fs.rename(&staging, &full)),
        io::ErrorKind::DirectoryNotEmpty
    );
    assert_eq!(
        kind(fs.rename(&staging, &staging.join("inner/into"))),
        io::ErrorKind::InvalidInput
    );
    fs.rename(&staging, &active).unwrap();
    assert!(!fs.exists(&staging).unwrap());
    assert_eq!(read(&*fs, &active.join("inner/f")), b"payload");
    assert_eq!(b.stat(&active).unwrap().ino, ino);
    // Onto an empty directory.
    let empty = b.root().join("empty");
    b.create_dir(&empty).unwrap();
    fs.rename(&active, &empty).unwrap();
    assert_eq!(read(&*fs, &empty.join("inner/f")), b"payload");
    fs.sync_dir(b.root()).unwrap();
}

/// NS15 on the fake only: the call log names each sync kind and path, which the engine
/// tests read to check that journal segments get `sync_data` and runs, manifests and
/// directories get `sync_all` and `sync_dir`.
#[test]
fn fault_fs_call_log_names_each_sync_kind_and_path() {
    let b = Fake::new();
    let fs = b.fs();
    let journal = b.root().join("journal");
    fs.create_dir_all(&journal).unwrap();
    fs.sync_dir(b.root()).unwrap();
    let segment = fs
        .open(&journal.join("1.jnl"), OpenMode::CreateNew)
        .unwrap();
    b.fs.clear_calls();
    segment.write_all_at(&[1; 100], 0).unwrap();
    segment.sync_data().unwrap();
    let run = fs
        .open(&b.root().join("1.run"), OpenMode::CreateNew)
        .unwrap();
    run.sync_all().unwrap();
    fs.sync_dir(b.root()).unwrap();
    let syncs: Vec<(FsOp, PathBuf)> =
        b.fs.calls()
            .into_iter()
            .filter(|c| matches!(c.op, FsOp::SyncData | FsOp::SyncAll | FsOp::SyncDir))
            .map(|c| (c.op, c.path))
            .collect();
    assert_eq!(
        syncs,
        vec![
            (FsOp::SyncData, journal.join("1.jnl")),
            (FsOp::SyncAll, b.root().join("1.run")),
            (FsOp::SyncDir, b.root().to_path_buf()),
        ]
    );
}
