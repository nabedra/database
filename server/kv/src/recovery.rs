//! Store-level recovery steps (task NS5, plan section 3): format and lock checks (R4),
//! `CURRENT` and the manifest (R2, R3, L3), and cleanup of leftovers (L4). The journal
//! replay and the run checks are driven from [`crate::Store::open`].

use std::io::ErrorKind;
use std::path::Path;

use crate::error::{Error, FileId, Result};
use crate::fs::{Fs, FsLock};
use crate::mac::JournalKey;
use crate::manifest::{
    self, CURRENT_FILE, CURRENT_TMP, FORMAT_CONTENT, FORMAT_CONTENT_V2, FORMAT_FILE, FORMAT_TMP,
    JOURNAL_DIR, JOURNAL_KEY_FILE, JOURNAL_KEY_TMP, LOCK_FILE, ManifestState, RUNS_DIR,
};
use crate::run::parse_run_file_name;

pub(crate) struct Prepared {
    pub lock: FsLock,
    pub state: ManifestState,
    pub manifest_torn_tail: bool,
    /// The journal key of a format 2 store; `None` for format 1.
    pub key: Option<JournalKey>,
}

/// True when `dir` holds a format 1 store (its `FORMAT` says so), the case
/// [`crate::JournalIntegrity::Authenticated`] upgrades.
pub(crate) fn is_format1_store(fs: &dyn Fs, dir: &Path) -> Result<bool> {
    let format = dir.join(FORMAT_FILE);
    if !fs.exists(&format)? {
        return Ok(false);
    }
    Ok(manifest::read_file(fs, &format)? == FORMAT_CONTENT)
}

/// The key of a format 2 store. Without it no authenticated record can be read, so a
/// missing or damaged key refuses the store (R3).
fn read_journal_key(fs: &dyn Fs, dir: &Path) -> Result<JournalKey> {
    let path = dir.join(JOURNAL_KEY_FILE);
    if !fs.exists(&path)? {
        return Err(Error::Corruption {
            file: FileId::Other,
            offset: 0,
            what: "FORMAT names format 2 but JOURNAL_KEY is missing",
        });
    }
    JournalKey::decode_file(&manifest::read_file(fs, &path)?)
}

fn names(fs: &dyn Fs, dir: &Path) -> Result<Vec<String>> {
    Ok(fs
        .list(dir)?
        .iter()
        .filter_map(|p| p.file_name()?.to_str().map(str::to_owned))
        .collect())
}

fn unsupported(dir: &Path, what: String) -> Error {
    Error::UnsupportedFormat {
        found: format!("{}: {what}", dir.display()),
    }
}

/// R4 checks, the lock, creation of a new store, and the manifest state.
///
/// A directory without `FORMAT` must be empty apart from the leftovers of an interrupted
/// creation (`LOCK`, `FORMAT.tmp`); anything else (another engine's store, for instance) is
/// refused before anything in it is touched. A new store is created in format 2 when
/// `create_auth`: its key is durable before `FORMAT` names format 2.
pub(crate) fn prepare(fs: &dyn Fs, dir: &Path, create_auth: bool) -> Result<Prepared> {
    if !fs.exists(dir)? {
        fs.create_dir_all(dir)?;
        if let Some(parent) = dir.parent()
            && !parent.as_os_str().is_empty()
        {
            fs.sync_dir(parent)?;
        }
    }
    let entries = names(fs, dir)?;
    let mut key = None;
    let has_format = entries.iter().any(|n| n == FORMAT_FILE);
    if has_format {
        let content = manifest::read_file(fs, &dir.join(FORMAT_FILE))?;
        if content == FORMAT_CONTENT_V2 {
            key = Some(read_journal_key(fs, dir)?);
        } else if content != FORMAT_CONTENT {
            let shown = String::from_utf8_lossy(&content[..content.len().min(64)]).into_owned();
            return Err(unsupported(dir, format!("FORMAT holds {shown:?}")));
        }
    } else {
        // Leftovers of an interrupted creation: the lock, temp files, and a key that no
        // FORMAT names yet.
        let mut foreign: Vec<&String> = entries
            .iter()
            .filter(|n| {
                *n != LOCK_FILE
                    && *n != FORMAT_TMP
                    && *n != JOURNAL_KEY_FILE
                    && *n != JOURNAL_KEY_TMP
            })
            .collect();
        if !foreign.is_empty() {
            foreign.sort();
            foreign.truncate(6);
            return Err(unsupported(
                dir,
                format!("not a nabedra-kv store (no FORMAT; found {foreign:?})"),
            ));
        }
    }
    let lock = fs.lock(&dir.join(LOCK_FILE))?;
    if key.is_some() {
        // A kill between the rename of FORMAT (or JOURNAL_KEY) and its directory sync
        // leaves a name the OS shows but a power loss would undo. Make both durable
        // before this open writes an authenticated segment, which a store that loses its
        // FORMAT 2 could not read (JournalIntegrity.tla, mutant no_open_dir_sync).
        fs.sync_dir(dir)?;
    }
    if !has_format {
        if create_auth {
            let new_key = JournalKey::generate()?;
            manifest::write_journal_key(fs, dir, &new_key)?;
            manifest::write_format(fs, dir, FORMAT_CONTENT_V2)?;
            key = Some(new_key);
        } else {
            manifest::write_format(fs, dir, FORMAT_CONTENT)?;
        }
    }
    let mut created = false;
    for sub in [JOURNAL_DIR, RUNS_DIR] {
        let path = dir.join(sub);
        if !fs.exists(&path)? {
            fs.create_dir_all(&path)?;
            created = true;
        }
    }
    if created {
        fs.sync_dir(dir)?;
    }
    let (state, manifest_torn_tail) = if fs.exists(&dir.join(CURRENT_FILE))? {
        let number = manifest::read_current(fs, dir)?;
        let (state, report) = manifest::read_manifest(fs, dir, number)?;
        (state, report.torn_tail)
    } else {
        // Only an interrupted creation leaves no CURRENT: CURRENT is durable before any
        // run or journal segment exists, and later only replaced by an atomic rename.
        // A manifest left by that creation holds the empty state and is removed.
        let leftovers = !names(fs, &dir.join(RUNS_DIR))?.is_empty()
            || !names(fs, &dir.join(JOURNAL_DIR))?.is_empty();
        if leftovers {
            return Err(Error::Corruption {
                file: FileId::Current,
                offset: 0,
                what: "CURRENT is missing",
            });
        }
        // A new store: its first manifest and CURRENT exist before any journal segment.
        for name in names(fs, dir)? {
            if manifest::parse_manifest_name(&name).is_some() {
                fs.remove(&dir.join(name))?;
            }
        }
        let mut state = ManifestState::new();
        let number = state.next_file;
        state.next_file += 1;
        manifest::ManifestWriter::create(fs, dir, number, &state)?;
        manifest::set_current(fs, dir, number)?;
        (state, false)
    };
    Ok(Prepared {
        lock,
        state,
        manifest_torn_tail,
        key,
    })
}

/// L4: deletes stale temp files, manifests other than `current`, and run files the state
/// does not reference. Returns how many files it removed.
pub(crate) fn cleanup(fs: &dyn Fs, dir: &Path, current: u64, state: &ManifestState) -> Result<u64> {
    let mut removed = 0u64;
    let mut remove = |path: &Path| -> Result<()> {
        match fs.remove(path) {
            Ok(()) => {
                removed += 1;
                Ok(())
            }
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    };
    for name in names(fs, dir)? {
        let stale_manifest = manifest::parse_manifest_name(&name).is_some_and(|n| n != current);
        if stale_manifest || name == CURRENT_TMP || name == FORMAT_TMP || name == JOURNAL_KEY_TMP {
            remove(&dir.join(&name))?;
        }
    }
    let runs_dir = dir.join(RUNS_DIR);
    for name in names(fs, &runs_dir)? {
        if let Some(id) = parse_run_file_name(&name)
            && !state.runs.contains_key(&id)
            && !state.quarantined_runs.contains_key(&id)
        {
            remove(&runs_dir.join(&name))?;
        }
    }
    Ok(removed)
}
