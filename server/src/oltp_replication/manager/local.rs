//! Explicit standalone development bootstrap. A real process-lifetime flock
//! excludes retained local owners. The generated private CA is restricted to
//! this worker's loopback transport; it does not authorize a remote deployment.
pub(crate) use super::super::authority::control::LocalOperatorPermit;
mod registered;
mod restore;
use super::super::{
    TlsMaterial,
    multiplex::{PeerDirectory, TransportConfig, TransportTrust, WorkerTransport},
    types::{WorkerEnrollment, WorkerIdentity},
};
use super::*;
use crate::oltp::pool::{OltpPool, PoolIdentity, SharedResourceConfig};
pub(crate) use registered::{
    GroupPlacement, RegisteredConfig, RegisteredSystemGroups, RegisteredWorker, SystemPlacement,
};
pub(crate) use restore::RegisteredRestoreConfig;
use nabedra_resources::config::WorkerResources;
use nabedra_rt::fs::os::{File, OpenOptions, self as fs};
use serde::{Deserialize, Serialize};
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};
use tokio::net::TcpListener;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LocalTopology {
    pub format: u32,
    pub cluster_id: Uuid,
    pub worker_id: Uuid,
    pub pool_id: Uuid,
    pub database: Text<128>,
    pub control: Uuid,
    pub timestamps: Uuid,
    pub status: Uuid,
    pub catalog: Uuid,
    pub node_id: u64,
    pub enrollment_revision: u64,
    pub directory_dev: u64,
    pub directory_inode: u64,
    pub lock_dev: u64,
    pub lock_inode: u64,
}
impl LocalTopology {
    pub(crate) fn group(&self, kind: GroupKind) -> Result<GroupIdentity, ProtocolError> {
        let group_id = match kind {
            GroupKind::Control => self.control,
            GroupKind::Timestamps => self.timestamps,
            GroupKind::Status { bucket: 0 } => self.status,
            GroupKind::Catalog => self.catalog,
            _ => return Err(invalid()),
        };
        Ok(GroupIdentity {
            cluster_id: self.cluster_id,
            database: self.database.clone(),
            group_id,
            kind,
            group_format_version: GROUP_FORMAT_VERSION,
        })
    }
    pub(crate) fn member(&self) -> ReplicaMember {
        ReplicaMember {
            node_id: self.node_id,
            worker_id: self.worker_id,
            pool_id: self.pool_id,
        }
    }
}
struct DirectoryOwner {
    _lock: File,
    path: PathBuf,
}
pub(crate) struct LocalWorker {
    pub manager: Arc<HostedGroupManager>,
    pub shared: Arc<WorkerSharedResources>,
    pub pool: OltpPool,
    pub topology: LocalTopology,
    pub enrollment: WorkerEnrollment,
    pub operator: LocalOperatorPermit,
    pub transport: Arc<WorkerTransport>,
    pub listener: TcpListener,
    pub directory: Arc<crate::oltp_replication::multiplex::AppliedPeerDirectory>,
    ownership: Arc<DirectoryOwner>,
}
impl LocalWorker {
    pub(crate) async fn open(
        path: &Path,
        database: &str,
        limits: WorkerResources,
        config: SharedResourceConfig,
    ) -> Result<Self, ProtocolError> {
        let database = Text::try_from(database).map_err(|_| invalid())?;
        let ownership = Arc::new(lock_directory(path)?);
        if fs::symlink_metadata(ownership.path.join("RESTORE_IN_PROGRESS.json")).is_ok()
            && !fs::symlink_metadata(ownership.path.join("RESTORED_GENESIS.json"))
                .is_ok_and(|metadata| metadata.is_file() && !metadata.file_type().is_symlink())
        {
            return Err(invalid());
        }
        let topology = topology(&ownership, &database)?;
        let tls_dir = ownership.path.join("internal-tls");
        fs::create_dir_all(&tls_dir).map_err(storage)?;
        private_directory(&tls_dir)?;
        let incarnation = nabedra_rt::ids::uuid_v4();
        let process_tls = tls_dir.join(incarnation.to_string());
        fs::create_dir(&process_tls).map_err(storage)?;
        private_directory(&process_tls)?;
        let tls = development_tls(&process_tls)?;
        let shared = WorkerSharedResources::new(topology.node_id, limits, config)?;
        let pool = OltpPool::open_development(
            &ownership.path.join("pool"),
            PoolIdentity {
                engine: "oltp".into(),
                pool_format_version: crate::oltp::pool::POOL_FORMAT_VERSION,
                cluster_id: topology.cluster_id,
                worker_id: topology.worker_id,
                pool_id: topology.pool_id,
            },
            shared.clone(),
        )?;
        let listener = TcpListener::bind("127.0.0.1:0").await.map_err(storage)?;
        let enrollment = WorkerEnrollment {
            worker_id: topology.worker_id,
            incarnation,
            certificate_sha256: Digest::parse(tls.certificate_sha256()).map_err(|_| invalid())?,
            endpoint: Text::new(listener.local_addr().map_err(storage)?.to_string())
                .map_err(|_| invalid())?,
            server_name: Text::try_from("localhost").map_err(|_| invalid())?,
            enrollment_revision: topology.enrollment_revision,
        };
        let registry = Arc::new(EndpointRegistry::new(shared.clone()));
        let directory = crate::oltp_replication::multiplex::AppliedPeerDirectory::from_bootstrap(
            topology.group(GroupKind::Control)?,
            vec![enrollment.clone()],
            shared.clone(),
        )?;
        let transport = Arc::new(WorkerTransport::new(
            &enrollment,
            &tls,
            shared.clone(),
            directory.clone(),
            registry.clone(),
            TransportConfig::default(),
        )?);
        let manager = Arc::new(HostedGroupManager {
            shared: shared.clone(),
            client: transport.client(),
            registry,
            routes: Arc::new(Mutex::new(BTreeMap::new())),
            ownership: Some(ownership.clone()),
            starts: tokio::sync::Mutex::new(()),
            damage_sink: Arc::new(Mutex::new(None)),
            health: Arc::default(),
            features: Arc::default(),
        });
        let operator = LocalOperatorPermit::from_local_bootstrap(
            WorkerIdentity::from(&enrollment),
            shared.clone(),
            ownership.clone(),
        )?;
        Ok(Self {
            manager,
            shared,
            pool,
            topology,
            enrollment,
            operator,
            transport,
            listener,
            directory,
            ownership,
        })
    }
    /// Positive local exclusion evidence: the old process cannot hold this same
    /// directory's lifetime lock. It says nothing about a remote/provider owner.
    pub(crate) fn owns_directory(&self) -> bool {
        Arc::strong_count(&self.ownership) > 0
    }
}
fn topology(owner: &DirectoryOwner, database: &Text<128>) -> Result<LocalTopology, ProtocolError> {
    let path = &owner.path;
    let (directory_dev, directory_inode, lock_dev, lock_inode) = directory_identity(owner)?;
    let file = path.join("NABEDRA_TYPED_WORKER.json");
    match bounded_read(&file, 64 << 10) {
        Ok(bytes) => {
            let mut t: LocalTopology = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
            if (t.directory_dev, t.directory_inode, t.lock_dev, t.lock_inode)
                != (directory_dev, directory_inode, lock_dev, lock_inode)
                // The pool's store exists: `pool/kv/CURRENT` (format 2). A format 1
                // `pool/CURRENT` (previous engine) passes here so the pool open refuses it with
                // a clear unsupported-format error (NS22).
                || !(fs::metadata(path.join("pool/kv/CURRENT")).is_ok_and(|meta| meta.is_file())
                    || fs::metadata(path.join("pool/CURRENT")).is_ok_and(|meta| meta.is_file()))
                || t.format != 1
                || t.database != *database
                || t.node_id == 0
                || [
                    t.cluster_id,
                    t.worker_id,
                    t.pool_id,
                    t.control,
                    t.timestamps,
                    t.status,
                    t.catalog,
                ]
                .iter()
                .any(Uuid::is_nil)
            {
                return Err(invalid());
            }
            t.enrollment_revision = t.enrollment_revision.checked_add(1).ok_or_else(invalid)?;
            replace_topology(path, &t)?;
            Ok(t)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // A legacy RF1 directory must be migrated explicitly, never opened
            // as an unrelated empty typed database alongside committed data.
            if fs::try_exists(path.join("NABEDRA_OLTP.json")).unwrap_or(false)
                || fs::try_exists(path.join("CURRENT")).unwrap_or(false)
            {
                return Err(ProtocolError::new(
                    ErrorCode::ValidationError,
                    "legacy OLTP directory requires explicit migration to typed groups",
                    Outcome::NotApplied,
                ));
            }
            let t = LocalTopology {
                format: 1,
                cluster_id: nabedra_rt::ids::uuid_v4(),
                worker_id: nabedra_rt::ids::uuid_v4(),
                pool_id: nabedra_rt::ids::uuid_v4(),
                database: database.clone(),
                control: nabedra_rt::ids::uuid_v4(),
                timestamps: nabedra_rt::ids::uuid_v4(),
                status: nabedra_rt::ids::uuid_v4(),
                catalog: nabedra_rt::ids::uuid_v4(),
                node_id: 1,
                enrollment_revision: 1,
                directory_dev,
                directory_inode,
                lock_dev,
                lock_inode,
            };
            let bytes = serde_json::to_vec(&t).map_err(|_| invalid())?;
            let mut output = private_file(&file, true).map_err(storage)?;
            output.write_all(&bytes).map_err(storage)?;
            output.sync_all().map_err(storage)?;
            fs::sync_dir(path).map_err(storage)?;
            Ok(t)
        }
        Err(error) => Err(storage(error)),
    }
}
#[cfg(unix)]
fn lock_directory(path: &Path) -> Result<DirectoryOwner, ProtocolError> {
    fs::create_dir_all(path).map_err(storage)?;
    let meta = fs::symlink_metadata(path).map_err(storage)?;
    if !meta.is_dir()
        || meta.file_type().is_symlink()
        || meta.uid() != fs::euid()
        || meta.mode() & 0o022 != 0
    {
        return Err(invalid());
    }
    let lock = private_file(&path.join("worker.lock"), false).map_err(storage)?;
    if !lock.metadata().map_err(storage)?.is_file() {
        return Err(invalid());
    }
    if !lock.try_lock_exclusive().unwrap_or(false) {
        return Err(ProtocolError::new(
            ErrorCode::Conflict,
            "another local OLTP process owns this data directory",
            Outcome::NotApplied,
        ));
    }
    Ok(DirectoryOwner {
        _lock: lock,
        path: path.to_path_buf(),
    })
}
#[cfg(not(unix))]
fn lock_directory(_: &Path) -> Result<DirectoryOwner, ProtocolError> {
    Err(invalid())
}
fn private_file(path: &Path, new: bool) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    if new {
        options.create_new(true);
    } else {
        options.create(true);
    }
    #[cfg(unix)]
    {
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    options.open(path)
}
fn bounded_read(path: &Path, max: usize) -> std::io::Result<Vec<u8>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > max as u64 {
        return Err(std::io::Error::other("invalid bounded startup file"));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(max as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > max {
        return Err(std::io::Error::other("startup file grew beyond its bound"));
    }
    Ok(bytes)
}
fn private_directory(path: &Path) -> Result<(), ProtocolError> {
    #[cfg(unix)]
    {
        fs::set_mode(path, 0o700).map_err(storage)?;
    }
    Ok(())
}
fn development_tls(path: &Path) -> Result<TlsMaterial, ProtocolError> {
    openssl(
        path,
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
            "-days",
            "3650",
            "-subj",
            "/CN=Nabedra local development CA",
        ],
    )?;
    openssl(
        path,
        &[
            "req",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "worker.key",
            "-out",
            "worker.csr",
            "-subj",
            "/CN=localhost",
        ],
    )?;
    let mut ext = private_file(&path.join("worker.ext"), true).map_err(storage)?;
    ext.write_all(
        b"subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\n",
    )
    .map_err(storage)?;
    drop(ext);
    openssl(
        path,
        &[
            "x509",
            "-req",
            "-in",
            "worker.csr",
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-out",
            "worker.pem",
            "-days",
            "3650",
            "-extfile",
            "worker.ext",
        ],
    )?;
    TlsMaterial::from_pem(
        &bounded_read(&path.join("worker.pem"), 64 << 10).map_err(storage)?,
        &bounded_read(&path.join("worker.key"), 64 << 10).map_err(storage)?,
        &bounded_read(&path.join("ca.pem"), 64 << 10).map_err(storage)?,
    )
    .map_err(storage)
}
fn openssl(path: &Path, args: &[&str]) -> Result<(), ProtocolError> {
    // The host child needs a real descriptor for stderr.
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let log = options.open(path.join("openssl.log")).map_err(storage)?;
    log.set_len(0).map_err(storage)?;
    let mut child = Command::new("openssl")
        .args(args)
        .current_dir(path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
        .spawn()
        .map_err(storage)?;
    let until = nabedra_rt::time::now_std() + Duration::from_secs(15);
    loop {
        if let Some(status) = child.try_wait().map_err(storage)? {
            return if status.success() {
                Ok(())
            } else {
                Err(ProtocolError::new(
                    ErrorCode::StorageUnavailable,
                    "internal development TLS generation failed",
                    Outcome::NotApplied,
                ))
            };
        }
        if nabedra_rt::time::now_std() >= until {
            let _ = child.kill();
            let _ = child.wait();
            return Err(ProtocolError::new(
                ErrorCode::StorageUnavailable,
                "internal development TLS generation timed out",
                Outcome::NotApplied,
            ));
        }
        nabedra_rt::time::thread_sleep(Duration::from_millis(10));
    }
}
fn storage(error: std::io::Error) -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        format!("local worker storage: {error}"),
        Outcome::Unknown,
    )
}

#[cfg(unix)]
fn directory_identity(owner: &DirectoryOwner) -> Result<(u64, u64, u64, u64), ProtocolError> {
    let directory = fs::metadata(&owner.path).map_err(storage)?;
    let lock = owner._lock.metadata().map_err(storage)?;
    Ok((directory.dev(), directory.ino(), lock.dev(), lock.ino()))
}
#[cfg(not(unix))]
fn directory_identity(_: &DirectoryOwner) -> Result<(u64, u64, u64, u64), ProtocolError> {
    Err(invalid())
}
fn replace_topology(path: &Path, topology: &LocalTopology) -> Result<(), ProtocolError> {
    let next = path.join(format!("typed-worker-{}.next", nabedra_rt::ids::uuid_v4()));
    let mut file = private_file(&next, true).map_err(storage)?;
    let bytes = serde_json::to_vec(topology).map_err(|_| invalid())?;
    file.write_all(&bytes).map_err(storage)?;
    file.sync_all().map_err(storage)?;
    drop(file);
    fs::rename(&next, path.join("NABEDRA_TYPED_WORKER.json")).map_err(storage)?;
    fs::sync_dir(path).map_err(storage)
}
