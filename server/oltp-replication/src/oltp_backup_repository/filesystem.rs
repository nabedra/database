use super::*;
use crate::oltp::pool::ExecutionClass;
use nabedra_rt::fs::os::{self as fs, File, OpenOptions};
use std::{
    io::{Read, Write},
    path::Path,
};

#[doc(hidden)]
pub struct FilesystemRepository {
    root: Arc<File>,
    shared: Arc<WorkerSharedResources>,
}
impl FilesystemRepository {
    #[doc(hidden)]
    pub fn open(
        root: &Path,
        shared: Arc<WorkerSharedResources>,
    ) -> Result<Arc<Self>, ProtocolError> {
        #[cfg(unix)]
        {
            if !root.is_absolute()
                || root.as_os_str().len() > 4096
                || fs::canonicalize(root).map_err(|_| invalid())? != root
            {
                return Err(invalid());
            }
            let root = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(root)
                .map_err(|_| invalid())?;
            let metadata = root.metadata().map_err(|_| invalid())?;
            if metadata.uid() != fs::euid() || metadata.mode() & 0o077 != 0 {
                return Err(invalid());
            }
            Ok(Arc::new(Self {
                root: Arc::new(root),
                shared,
            }))
        }
        #[cfg(not(unix))]
        {
            let _ = (root, shared);
            Err(invalid())
        }
    }
}
impl private::Sealed for FilesystemRepository {}
impl Repository for FilesystemRepository {
    fn shared(&self) -> &Arc<WorkerSharedResources> {
        &self.shared
    }
    fn put<'a>(
        &'a self,
        key: ArchiveKey,
        name: String,
        body: ChargedBytes,
    ) -> RepositoryFuture<'a, Digest> {
        Box::pin(async move {
            if !key.valid() || body.is_empty() || body.len() > stored_limit(&name) {
                return Err(invalid());
            }
            let root = self.root.clone();
            self.shared
                .execute(ExecutionClass::Query, 0, 0, move || {
                    #[cfg(unix)]
                    {
                        put_native(&root, key, &name, body)
                    }
                    #[cfg(not(unix))]
                    {
                        let _ = (root, key, name, body);
                        Err(invalid())
                    }
                })
                .await
        })
    }
    fn get<'a>(
        &'a self,
        key: ArchiveKey,
        name: String,
        limit: usize,
    ) -> RepositoryFuture<'a, ChargedBytes> {
        Box::pin(async move {
            if !key.valid() || limit == 0 || limit > stored_limit(&name) {
                return Err(invalid());
            }
            let root = self.root.clone();
            let shared = self.shared.clone();
            self.shared
                .execute(ExecutionClass::Query, 0, 0, move || {
                    #[cfg(unix)]
                    {
                        read_native(&root, key, &name, limit, &shared)
                    }
                    #[cfg(not(unix))]
                    {
                        let _ = (root, key, name, limit, shared);
                        Err(invalid())
                    }
                })
                .await
        })
    }
}
#[cfg(unix)]
fn directory_at(parent: &File, name: &str, create: bool) -> Result<File, ProtocolError> {
    if name.contains(char::from(0)) {
        return Err(invalid());
    }
    if create {
        match parent.create_dir_at(name, 0o700) {
            Ok(()) => parent.sync_all().map_err(|_| unavailable())?,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) => return Err(unavailable()),
        }
    }
    let directory = parent
        .open_at(
            name,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0,
        )
        .map_err(|_| unavailable())?;
    let metadata = directory.metadata().map_err(|_| unavailable())?;
    if metadata.uid() != fs::euid() || metadata.mode() & 0o077 != 0 {
        return Err(corrupt());
    }
    Ok(directory)
}
#[cfg(unix)]
fn archive_directory(root: &File, key: ArchiveKey, create: bool) -> Result<File, ProtocolError> {
    let cluster = directory_at(root, &key.cluster_id.to_string(), create)?;
    directory_at(&cluster, &key.archive_id.to_string(), create)
}
#[cfg(unix)]
fn file_at(directory: &File, name: &str, flags: i32) -> Result<File, ProtocolError> {
    if name.contains(char::from(0)) {
        return Err(invalid());
    }
    let file = directory
        .open_at(
            name,
            flags | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
            0o600,
        )
        .map_err(|_| unavailable())?;
    let metadata = file.metadata().map_err(|_| unavailable())?;
    if !metadata.is_file()
        || metadata.uid() != fs::euid()
        || metadata.mode() & 0o077 != 0
    {
        return Err(corrupt());
    }
    Ok(file)
}
#[cfg(unix)]
fn read_native(
    root: &File,
    key: ArchiveKey,
    name: &str,
    limit: usize,
    shared: &Arc<WorkerSharedResources>,
) -> Result<ChargedBytes, ProtocolError> {
    let directory = archive_directory(root, key, false)?;
    let mut file = file_at(&directory, name, libc::O_RDONLY)?;
    let len = usize::try_from(file.metadata().map_err(|_| unavailable())?.len())
        .map_err(|_| corrupt())?;
    if len == 0 || len > limit {
        return Err(corrupt());
    }
    let mut buffer = shared.allocate_bytes(ByteClass::Data, len)?;
    file.read_exact(buffer.as_mut_slice())
        .map_err(|_| unavailable())?;
    let mut extra = [0];
    if file.read(&mut extra).map_err(|_| unavailable())? != 0 {
        return Err(corrupt());
    }
    Ok(buffer.freeze())
}
#[cfg(unix)]
fn put_native(
    root: &File,
    key: ArchiveKey,
    name: &str,
    body: ChargedBytes,
) -> Result<Digest, ProtocolError> {
    let directory = archive_directory(root, key, true)?;
    let expected = hash(body.as_ref());
    let temporary = format!(".upload-{}", nabedra_rt::ids::uuid_v4());
    let mut file = file_at(
        &directory,
        &temporary,
        libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
    )?;
    // This closure owns both native execution and the charged source bytes.
    // Cancelling its awaiter cannot release them while fsync is still live.
    let written = (|| {
        file.write_all(body.as_ref()).map_err(|_| unavailable())?;
        file.set_mode(0o400).map_err(|_| unavailable())?;
        file.sync_all().map_err(|_| unavailable())?;
        if name.contains(char::from(0)) {
            return Err(invalid());
        }
        if let Err(error) = directory.hard_link_at(&temporary, name) {
            if error.kind() != std::io::ErrorKind::AlreadyExists {
                return Err(unavailable());
            }
            // Existing objects and lost acknowledgements reconcile by actual
            // length and streamed digest. Metadata/HEAD alone is insufficient.
            let mut existing = file_at(&directory, name, libc::O_RDONLY)?;
            // The encryption layer may reconcile a ciphertext conflict as an
            // exact plaintext retry. Confirm the existing inode was synced.
            existing.sync_all().map_err(|_| unavailable())?;
            if existing.metadata().map_err(|_| unavailable())?.len() != body.len() as u64 {
                return Err(conflict());
            }
            let mut digest = Sha256::new();
            let mut buffer = [0; 64 << 10];
            let mut bounded = (&mut existing).take(body.len() as u64 + 1);
            let mut total = 0;
            loop {
                let count = bounded.read(&mut buffer).map_err(|_| unavailable())?;
                if count == 0 {
                    break;
                }
                total += count;
                digest.update(&buffer[..count]);
            }
            if total != body.len() || Digest::from_bytes(digest.finalize().into()) != expected {
                return Err(conflict());
            }
        }
        Ok(expected)
    })();
    directory
        .remove_file_at(&temporary)
        .map_err(|_| unavailable())?;
    directory.sync_all().map_err(|_| unavailable())?;
    written
}
