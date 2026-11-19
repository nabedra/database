//! Store-level recovery steps (task NS5, plan section 3): format and lock checks (R4),
//! `CURRENT` and the manifest (R2, R3, L3), and cleanup of leftovers (L4). The journal
//! replay and the run checks are driven from [`crate::Store::open`].

use std::io::ErrorKind;
use std::path::Path;

use crate::error::{Error, FileId, Result};
use crate::fs::{Fs, FsLock};
use crate::manifest::{
    self, CURRENT_FILE, CURRENT_TMP, FORMAT_CONTENT, FORMAT_FILE, FORMAT_TMP, JOURNAL_DIR,
    LOCK_FILE, ManifestState, RUNS_DIR,
};
use crate::run::parse_run_file_name;

pub(crate) struct Prepared {
    pub lock: FsLock,
    pub state: ManifestState,
    pub manifest_torn_tail: bool,
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
/// creation (`LOCK`, `FORMAT.tmp`); anything else (a RocksDB store, for instance) is
/// refused before anything in it is touched.
pub(crate) fn prepare(fs: &dyn Fs, dir: &Path, _scope_of: fn(&[u8]) -> usize) -> Result<Prepared> {
    if !fs.exists(dir)? {
        fs.create_dir_all(dir)?;
        if let Some(parent) = dir.parent()
            && !parent.as_os_str().is_empty()
        {
            fs.sync_dir(parent)?;
        }
    }
    let entries = names(fs, dir)?;
    let has_format = entries.iter().any(|n| n == FORMAT_FILE);
    if has_format {
        let content = manifest::read_file(fs, &dir.join(FORMAT_FILE))?;
        if content != FORMAT_CONTENT {
            let shown = String::from_utf8_lossy(&content[..content.len().min(64)]).into_owned();
            return Err(unsupported(dir, format!("FORMAT holds {shown:?}")));
        }
    } else {
        let mut foreign: Vec<&String> = entries
            .iter()
            .filter(|n| *n != LOCK_FILE && *n != FORMAT_TMP)
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
    if !has_format {
        manifest::write_format(fs, dir)?;
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
        if stale_manifest || name == CURRENT_TMP || name == FORMAT_TMP {
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
