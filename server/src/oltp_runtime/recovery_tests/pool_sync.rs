//! Private process fixture: persist the selected native journal write, then
//! return EIO from its sync before the journal can publish it to the live view.
use nabedra_kv::fs::{Fs, FsFile, FsLock, OpenMode};
use std::{
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use uuid::Uuid;

fn directory() -> Option<PathBuf> {
    let path = PathBuf::from(std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR")?);
    path.join("enable-post-sync-fault")
        .is_file()
        .then_some(path)
}

pub(crate) fn wrap(fs: Arc<dyn Fs>) -> Arc<dyn Fs> {
    match directory() {
        Some(directory) => Arc::new(FaultSync { fs, directory }),
        None => fs,
    }
}

pub(crate) fn status_probe() -> Option<Uuid> {
    std::fs::read_to_string(directory()?.join("post-sync-status"))
        .ok()?
        .trim()
        .parse()
        .ok()
}

pub(crate) fn observe_create_plan(request: Uuid, table: Uuid) {
    let Some(directory) = directory() else {
        return;
    };
    if std::fs::read_to_string(directory.join("post-sync-status"))
        .ok()
        .is_some_and(|selected| selected.trim() == request.to_string())
    {
        let mut trace = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(directory.join(format!("create-plan-{request}")))
            .unwrap();
        writeln!(trace, "{table}").unwrap();
    }
}

struct FaultSync {
    fs: Arc<dyn Fs>,
    directory: PathBuf,
}

impl Fs for FaultSync {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn FsFile>> {
        let file = self.fs.open(path, mode)?;
        match path.extension().and_then(|ext| ext.to_str()) {
            // A preallocated .free segment can become live without reopening.
            Some("jnl" | "free") => Ok(Arc::new(JournalFile {
                file,
                directory: self.directory.clone(),
                selected: Mutex::new(None),
            })),
            _ => Ok(file),
        }
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.fs.rename(from, to)
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        self.fs.remove(path)
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        self.fs.exists(path)
    }

    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        self.fs.create_dir_all(dir)
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        self.fs.list(dir)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        self.fs.sync_dir(dir)
    }

    fn lock(&self, path: &Path) -> io::Result<FsLock> {
        self.fs.lock(path)
    }
}

struct JournalFile {
    file: Arc<dyn FsFile>,
    directory: PathBuf,
    selected: Mutex<Option<String>>,
}

impl FsFile for JournalFile {
    fn write_all_at(&self, bytes: &[u8], offset: u64) -> io::Result<()> {
        self.file.write_all_at(bytes, offset)?;
        let arm = self.directory.join("post-sync-arm");
        if let Ok(selection) = std::fs::read_to_string(&arm) {
            let (operation, request) = selection.trim().split_once(' ').unwrap();
            assert!(matches!(operation, "prepare_page" | "resolve"));
            assert!(request.parse::<Uuid>().is_ok());
            let start = format!("\"{operation}\":{{");
            let identity = format!("\"request_id\":\"{request}\"");
            // The selector matches the command tag followed by the request ID
            // in the same encoded Raft batch. The fixture releases just this
            // create at the targeted boundary.
            if bytes
                .windows(start.len())
                .position(|part| part == start.as_bytes())
                .is_some_and(|start| {
                    bytes[start..]
                        .windows(identity.len())
                        .any(|part| part == identity.as_bytes())
                })
            {
                std::fs::remove_file(arm).unwrap();
                *self.selected.lock().unwrap() = Some(selection);
            }
        }
        Ok(())
    }

    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        self.file.read_exact_at(bytes, offset)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.file.sync_data()?;
        if let Some(selection) = self.selected.lock().unwrap().take() {
            let marker = self.directory.join("post-sync-eio");
            let pending = marker.with_extension("tmp");
            std::fs::write(&pending, selection).unwrap();
            std::fs::rename(pending, marker).unwrap();
            return Err(io::Error::from_raw_os_error(libc::EIO));
        }
        Ok(())
    }

    fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn preallocate(&self, len: u64) -> io::Result<()> {
        self.file.preallocate(len)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }

    fn len(&self) -> io::Result<u64> {
        self.file.len()
    }

    fn advise_dont_need(&self, offset: u64, len: u64) {
        self.file.advise_dont_need(offset, len)
    }
}
