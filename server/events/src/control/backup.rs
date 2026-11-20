mod transport;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use tempfile::{NamedTempFile, TempDir};
use uuid::Uuid;

use super::{BackupLayout, Config, METADATA, RESTORE_REBIND, parse_partition_id};
use crate::raft::SnapshotCut;

pub(super) use transport::router;

// These limits match the snapshot format's cap and the authenticated peer response budget.
const CHUNK_BYTES: usize = 128 * 1024;
const MAX_IMAGE_BYTES: u64 = 64 * 1024 * 1024 * 1024;
const MAX_MANIFEST_BYTES: usize = 8 * 1024 * 1024;
const MANIFEST: &str = "manifest.json";
const RESTORE_IN_PROGRESS: &str = "restore-in-progress";

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Bundle {
    manifest: Manifest,
    sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    layout: BackupLayout,
    cuts: BTreeMap<String, SnapshotCut>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Restore {
    cluster_id: Uuid,
    broker_id: u64,
    manifest_sha256: String,
}

pub async fn run_backup(config: &Config, destination: &Path) -> Result<()> {
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let staging = TempDir::new_in(parent).context("stage backup in destination filesystem")?;
    let peers = transport::connect(config)?;
    let bootstrap = config.bootstrap.broker_map(config.allow_plaintext)?;
    let layout = transport::layout(
        &peers,
        bootstrap.values().map(|node| node.peer_url.as_str()),
    )
    .await?;
    ensure!(
        layout.cluster_id == config.cluster_id,
        "backup source cluster differs from config"
    );
    validate_layout(&layout)?;

    let mut cuts = BTreeMap::new();
    for (id, voters) in &layout.groups {
        let cut = transport::download(
            &peers,
            id,
            voters,
            layout.cluster_id,
            &image_path(staging.path(), id),
        )
        .await
        .with_context(|| format!("export committed snapshot for group {id}"))?;
        cuts.insert(id.clone(), cut);
    }

    let checked = transport::layout(
        &peers,
        layout.roster.values().map(|node| node.peer_url.as_str()),
    )
    .await?;
    ensure!(
        checked == layout,
        "topology changed during backup; retry the export"
    );
    let manifest = Manifest {
        version: 1,
        layout,
        cuts,
    };
    let sha256 = checksum(&serde_json::to_vec(&manifest)?);
    let bundle = Bundle { manifest, sha256 };
    let manifest = serde_json::to_vec(&bundle)?;
    ensure!(
        manifest.len() <= MAX_MANIFEST_BYTES,
        "backup manifest exceeds size limit"
    );
    write_marker(staging.path().join(MANIFEST), &manifest)?;
    publish(staging, destination, parent)?;
    Ok(())
}

pub async fn run_restore(config: &Config, backup: &Path) -> Result<()> {
    let bundle = read_bounded::<Bundle>(&backup.join(MANIFEST), MAX_MANIFEST_BYTES)?;
    let manifest = &bundle.manifest;
    ensure!(
        manifest.version == 1,
        "unsupported Events backup manifest version"
    );
    ensure!(
        checksum(&serde_json::to_vec(manifest)?) == bundle.sha256,
        "backup manifest checksum mismatch"
    );
    validate_layout(&manifest.layout)?;
    ensure!(
        manifest.cuts.keys().eq(manifest.layout.groups.keys()),
        "snapshot cuts differ from committed layout"
    );
    ensure!(
        manifest.layout.cluster_id != config.cluster_id,
        "restore requires a fresh cluster ID"
    );
    let roster = config.bootstrap.broker_map(config.allow_plaintext)?;
    ensure!(
        roster.iter().map(|(id, node)| (id, &node.zone)).eq(manifest
            .layout
            .roster
            .iter()
            .map(|(id, node)| (id, &node.zone))),
        "restore config must preserve the source broker IDs and zones"
    );
    let metadata = manifest
        .layout
        .groups
        .get(METADATA)
        .context("missing metadata placement")?;
    ensure!(
        config.bootstrap.metadata_voters.iter().eq(metadata.keys()),
        "restore config must preserve the source metadata placement"
    );

    let mut local = BTreeSet::new();
    for (id, cut) in &manifest.cuts {
        validate_cut(id, cut, manifest.layout.cluster_id)?;
        if manifest.layout.groups[id].contains_key(&config.broker_id) {
            let path = image_path(backup, id);
            let expected = cut.clone();
            tokio::task::spawn_blocking(move || verify_image(&path, &expected)).await??;
            local.insert(id.clone());
        }
    }

    let wanted = Restore {
        cluster_id: config.cluster_id,
        broker_id: config.broker_id,
        manifest_sha256: bundle.sha256,
    };
    // The store's exclusive `kv/LOCK` also serializes this marker protocol with
    // broker startup.
    let broker = config.open_broker()?;
    let pending = config.data_dir.join(RESTORE_IN_PROGRESS);
    let complete = config.data_dir.join(RESTORE_REBIND);
    let pending_marker = read_marker(&pending)?;
    if let Some(previous) = &pending_marker {
        ensure!(
            previous == &wanted,
            "another backup is being restored on this volume"
        );
    }
    if let Some(previous) = read_marker(&complete)? {
        ensure!(
            previous == wanted,
            "volume already restored from a different backup"
        );
        if pending_marker.is_some() {
            finish_restore(&pending, &config.data_dir)?;
        }
        return Ok(());
    }

    // Quarantined groups count: a volume that holds only damaged groups is not fresh,
    // and a damaged copy of a backup group must not be restored over (RV-EV L5).
    let stored = broker.hosted_group_ids()?;
    if pending_marker.is_none() {
        ensure!(stored.is_empty(), "restore requires a fresh broker volume");
        write_marker(pending.clone(), &serde_json::to_vec(&wanted)?)?;
    } else {
        ensure!(
            stored.iter().all(|id| local.contains(id)),
            "restore volume contains groups outside its backup placement"
        );
    }

    for id in local {
        let expected = &manifest.cuts[&id];
        let cut = match broker.seeded_cut(&id)? {
            Some(cut) => cut,
            None => broker.seed_group(&id, &image_path(backup, &id)).await?,
        };
        ensure!(
            &cut == expected,
            "restored snapshot cut differs for group {id}"
        );
    }
    write_marker(complete, &serde_json::to_vec(&wanted)?)?;
    finish_restore(&pending, &config.data_dir)
}

fn validate_layout(layout: &BackupLayout) -> Result<()> {
    ensure!(!layout.cluster_id.is_nil(), "invalid backup cluster ID");
    ensure!(
        layout.groups.contains_key(METADATA),
        "missing metadata placement"
    );
    for (id, voters) in &layout.groups {
        ensure!(
            id == METADATA || parse_partition_id(id).is_some(),
            "invalid group in backup layout"
        );
        ensure!(
            matches!(voters.len(), 1 | 3),
            "invalid voter count for group {id}"
        );
        ensure!(
            voters.iter().all(|(id, url)| layout
                .roster
                .get(id)
                .is_some_and(|node| { node.id == *id && node.peer_url == *url })),
            "unknown voter in backup layout for group {id}"
        );
    }
    Ok(())
}

fn validate_cut(id: &str, cut: &SnapshotCut, cluster: Uuid) -> Result<()> {
    ensure!(
        cut.group == id
            && cut.cluster_id == cluster
            && cut.applied_index.is_some()
            && (33..=MAX_IMAGE_BYTES).contains(&cut.bytes)
            && cut.sha256.len() == 64
            && cut
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "invalid committed snapshot cut for group {id}"
    );
    Ok(())
}

const EXPORT_SPOOL: &str = "backup-export";

pub(super) fn cleanup_spool(data_dir: &Path) -> std::io::Result<()> {
    // This directory belongs exclusively to the running broker's backup service.
    match fs::remove_dir_all(data_dir.join(EXPORT_SPOOL)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn image_path(root: &Path, group: &str) -> PathBuf {
    root.join(format!("{group}.snapshot"))
}

fn checksum(data: &[u8]) -> String {
    format!("{:x}", Sha256::digest(data))
}

fn verify_image(path: &Path, cut: &SnapshotCut) -> Result<()> {
    let result = (|| -> Result<()> {
        let file = File::open(path)?;
        ensure!(
            file.metadata()?.len() == cut.bytes,
            "snapshot length differs from manifest"
        );
        let mut input = BufReader::with_capacity(CHUNK_BYTES, file);
        let mut hash = Sha256::new();
        let mut bytes = vec![0; CHUNK_BYTES];
        let mut remaining = cut.bytes - 32;
        while remaining != 0 {
            let take = remaining.min(CHUNK_BYTES as u64) as usize;
            input.read_exact(&mut bytes[..take])?;
            hash.update(&bytes[..take]);
            remaining -= take as u64;
        }
        let sha256 = hash.finalize();
        let mut trailer = [0; 32];
        input.read_exact(&mut trailer)?;
        ensure!(
            input.read(&mut [0])? == 0
                && sha256[..] == trailer
                && format!("{sha256:x}") == cut.sha256,
            "snapshot checksum mismatch"
        );
        Ok(())
    })();
    result.with_context(|| format!("verify backup image for group {}", cut.group))
}

fn read_bounded<T: DeserializeOwned>(path: &Path, max_bytes: usize) -> Result<T> {
    parse_bounded(
        File::open(path).with_context(|| format!("open {}", path.display()))?,
        max_bytes,
    )
}

fn parse_bounded<T: DeserializeOwned>(file: File, max_bytes: usize) -> Result<T> {
    let mut contents = Vec::new();
    file.take(max_bytes as u64 + 1).read_to_end(&mut contents)?;
    ensure!(
        contents.len() <= max_bytes,
        "backup metadata exceeds size limit"
    );
    Ok(serde_json::from_slice(&contents)?)
}

fn read_marker(path: &Path) -> Result<Option<Restore>> {
    match File::open(path) {
        Ok(file) => parse_bounded(file, 4096).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn write_marker(path: PathBuf, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().context("marker directory is missing")?;
    let mut file = NamedTempFile::new_in(parent)?;
    file.write_all(bytes)?;
    file.as_file().sync_all()?;
    file.persist_noclobber(&path).map_err(|error| error.error)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn finish_restore(marker: &Path, data_dir: &Path) -> Result<()> {
    fs::remove_file(marker)?;
    File::open(data_dir)?.sync_all()?;
    Ok(())
}

fn publish(staging: TempDir, destination: &Path, parent: &Path) -> Result<()> {
    // Reserve the destination atomically. rename replaces only the empty directory we created.
    fs::create_dir(destination).context("backup output must be a new directory")?;
    if let Err(error) = fs::rename(staging.path(), destination) {
        let _ = fs::remove_dir(destination);
        return Err(error).context("publish completed backup directory");
    }
    File::open(parent)?.sync_all()?;
    Ok(())
}
