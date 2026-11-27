//! `FORMAT`, `CURRENT` and `MANIFEST-<n>` (task NS5, plan sections 2 and 3).
//!
//! # Files
//!
//! - `FORMAT` holds `nabedra-kv 1\n` (format 1: journal records in the CRC frame) or
//!   `nabedra-kv 2\n` (format 2: journal segments created from then on use the
//!   authenticated frame, LSM-1). It is written by a temp file, a sync, a rename and a
//!   directory sync, once at creation and once more by the upgrade from format 1 to 2.
//!   Anything else, or a non-empty directory without it, is [`Error::UnsupportedFormat`]:
//!   a release that knows only format 1 refuses a format 2 store this way, before it
//!   changes any file.
//! - `JOURNAL_KEY` (format 2 only) holds the 128-bit journal key with a CRC
//!   ([`crate::mac`]). It is durable before `FORMAT` names format 2 and never rewritten
//!   after that.
//! - `CURRENT` names the live manifest: `MANIFEST-<n> <crc>\n`, where `<crc>` is the masked
//!   CRC32C of `MANIFEST-<n>` in hex. It is replaced by writing `CURRENT.tmp`, syncing it,
//!   renaming it over `CURRENT` and syncing the directory. Any other content is R3.
//! - `MANIFEST-<n>` is a sequence of records, each `masked_crc u32 ‖ len u32 ‖ edit`, where
//!   the CRC covers `len ‖ edit`. The first record of a manifest is a full snapshot of the
//!   state; later records are [`VersionEdit`]s. A store writes a new manifest at every
//!   open and whenever the current one grows past [`ROLL_BYTES`] or a write to it failed.
//! - Every record starts at a multiple of [`RECORD_ALIGN`] (4 KiB); the gap after a record
//!   is never written. An append therefore never rewrites a device unit that holds a
//!   synced record, so a power loss that destroys a whole 4 KiB unit (zeros or garbage)
//!   can only hit the unsynced final record.
//!
//! # Torn tails (L3) and damage (R3)
//!
//! The store appends one edit and syncs it before it appends the next or relies on it, so
//! at most the final record can be unsynced. A record that fails its checks is therefore
//! a torn final record (L3, dropped) unless a valid record can be found anywhere after it,
//! which proves it was synced and later damaged (R3, the open is refused).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;

use crate::coding::{get_length_prefixed, get_varint32, put_length_prefixed, put_varint32};
use crate::crc;
use crate::error::{Damage, DamageReason, Error, FileId, Result};
use crate::fs::{Fs, FsFile, OpenMode};
use crate::mac::JournalKey;
use crate::types::Seq;

pub const FORMAT_FILE: &str = "FORMAT";
pub const FORMAT_CONTENT: &[u8] = b"nabedra-kv 1\n";
/// `FORMAT` of a store whose journal uses authenticated frames (LSM-1).
pub const FORMAT_CONTENT_V2: &[u8] = b"nabedra-kv 2\n";
pub const JOURNAL_KEY_FILE: &str = "JOURNAL_KEY";
pub(crate) const JOURNAL_KEY_TMP: &str = "JOURNAL_KEY.tmp";
pub const CURRENT_FILE: &str = "CURRENT";
pub const LOCK_FILE: &str = "LOCK";
pub const JOURNAL_DIR: &str = "journal";
pub const RUNS_DIR: &str = "runs";
pub(crate) const FORMAT_TMP: &str = "FORMAT.tmp";
pub(crate) const CURRENT_TMP: &str = "CURRENT.tmp";

/// A manifest larger than this is replaced by a fresh snapshot at the next edit.
pub const ROLL_BYTES: u64 = 4 << 20;
/// Records start at multiples of this.
pub const RECORD_ALIGN: u64 = 4096;

fn align(pos: usize) -> usize {
    pos.div_ceil(RECORD_ALIGN as usize) * RECORD_ALIGN as usize
}

const RECORD_HEADER: usize = 8;

pub fn manifest_name(number: u64) -> String {
    format!("MANIFEST-{number:06}")
}

pub fn parse_manifest_name(name: &str) -> Option<u64> {
    let digits = name.strip_prefix("MANIFEST-")?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let number = digits.parse().ok()?;
    (manifest_name(number) == name).then_some(number)
}

/// One run as the manifest records it. The key range survives even when the file is
/// unreadable (G2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunDesc {
    pub id: u64,
    pub level: u8,
    pub file_size: u64,
    /// Smallest and largest internal keys (see [`crate::run::RunMeta`]).
    pub smallest: Bytes,
    pub largest: Bytes,
    pub smallest_seq: Seq,
    pub largest_seq: Seq,
    pub num_entries: u64,
    pub oldest_journal_segment: Option<u64>,
}

/// A change to the persistent state. Every field is optional or additive.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VersionEdit {
    /// Every record with a sequence at or below this is in runs.
    pub flushed_seq: Option<Seq>,
    /// File numbers below this are taken.
    pub next_file: Option<u64>,
    pub removed: Vec<(u8, u64)>,
    pub added: Vec<RunDesc>,
    /// Replaces the recorded set of live journal segments `(number, epoch)`.
    pub segments: Option<Vec<(u64, u64)>>,
    pub quarantined: Vec<Damage>,
    /// Scopes released by [`crate::Store::release_scope`], with the sequence of the
    /// release's range delete. Journal damage of that scope below it is history.
    pub released: Vec<(Bytes, Seq)>,
    /// Runs taken out of the version because they are damaged (G2). Their files stay on
    /// disk until their scope is released.
    pub quarantined_runs: Vec<(u64, Bytes)>,
    pub dropped_quarantined_runs: Vec<u64>,
}

mod tag {
    pub const FLUSHED_SEQ: u8 = 1;
    pub const NEXT_FILE: u8 = 2;
    pub const REMOVE_RUN: u8 = 3;
    pub const ADD_RUN: u8 = 4;
    pub const SEGMENTS: u8 = 5;
    pub const QUARANTINE: u8 = 6;
    pub const RELEASE: u8 = 7;
    pub const QUARANTINE_RUN: u8 = 8;
    pub const DROP_QUARANTINED_RUN: u8 = 9;
}

fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

struct Reader<'a> {
    data: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn u8(&mut self) -> Option<u8> {
        let b = *self.data.get(self.at)?;
        self.at += 1;
        Some(b)
    }
    fn u64(&mut self) -> Option<u64> {
        let v = crate::coding::read_u64_le(self.data, self.at)?;
        self.at += 8;
        Some(v)
    }
    fn varint(&mut self) -> Option<u32> {
        let (v, used) = get_varint32(&self.data[self.at..])?;
        self.at += used;
        Some(v)
    }
    fn bytes(&mut self) -> Option<&'a [u8]> {
        let (b, used) = get_length_prefixed(&self.data[self.at..])?;
        self.at += used;
        Some(b)
    }
    fn done(&self) -> bool {
        self.at >= self.data.len()
    }
}

fn opt(v: Option<u64>) -> u64 {
    v.unwrap_or(u64::MAX)
}

fn unopt(v: u64) -> Option<u64> {
    (v != u64::MAX).then_some(v)
}

fn encode_file(out: &mut Vec<u8>, file: FileId) {
    let (tag, n) = match file {
        FileId::Journal { segment } => (0, segment),
        FileId::Run { id } => (1, id),
        FileId::Manifest { number } => (2, number),
        FileId::Current => (3, 0),
        FileId::Lock => (4, 0),
        FileId::JournalValue { segment } => (5, segment),
        FileId::Other => (6, 0),
    };
    out.push(tag);
    put_u64(out, n);
}

fn decode_file(r: &mut Reader<'_>) -> Option<FileId> {
    let tag = r.u8()?;
    let n = r.u64()?;
    Some(match tag {
        0 => FileId::Journal { segment: n },
        1 => FileId::Run { id: n },
        2 => FileId::Manifest { number: n },
        3 => FileId::Current,
        4 => FileId::Lock,
        5 => FileId::JournalValue { segment: n },
        6 => FileId::Other,
        _ => return None,
    })
}

fn reason_byte(reason: DamageReason) -> u8 {
    match reason {
        DamageReason::JournalRecord => 0,
        DamageReason::RunFile => 1,
        DamageReason::RunBlock => 2,
        DamageReason::JournalValue => 3,
    }
}

fn reason_from(byte: u8) -> Option<DamageReason> {
    Some(match byte {
        0 => DamageReason::JournalRecord,
        1 => DamageReason::RunFile,
        2 => DamageReason::RunBlock,
        3 => DamageReason::JournalValue,
        _ => return None,
    })
}

impl VersionEdit {
    pub fn is_empty(&self) -> bool {
        *self == VersionEdit::default()
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        if let Some(seq) = self.flushed_seq {
            out.push(tag::FLUSHED_SEQ);
            put_u64(out, seq);
        }
        if let Some(next) = self.next_file {
            out.push(tag::NEXT_FILE);
            put_u64(out, next);
        }
        for &(level, id) in &self.removed {
            out.push(tag::REMOVE_RUN);
            out.push(level);
            put_u64(out, id);
        }
        for run in &self.added {
            out.push(tag::ADD_RUN);
            out.push(run.level);
            put_u64(out, run.id);
            put_u64(out, run.file_size);
            put_length_prefixed(out, &run.smallest);
            put_length_prefixed(out, &run.largest);
            put_u64(out, run.smallest_seq);
            put_u64(out, run.largest_seq);
            put_u64(out, run.num_entries);
            put_u64(out, opt(run.oldest_journal_segment));
        }
        if let Some(segments) = &self.segments {
            out.push(tag::SEGMENTS);
            put_varint32(out, segments.len() as u32);
            for &(number, epoch) in segments {
                put_u64(out, number);
                put_u64(out, epoch);
            }
        }
        for damage in &self.quarantined {
            out.push(tag::QUARANTINE);
            put_length_prefixed(out, &damage.scope);
            out.push(reason_byte(damage.reason));
            encode_file(out, damage.file);
            put_u64(out, opt(damage.first_bad_seq));
        }
        for (scope, seq) in &self.released {
            out.push(tag::RELEASE);
            put_length_prefixed(out, scope);
            put_u64(out, *seq);
        }
        for (id, scope) in &self.quarantined_runs {
            out.push(tag::QUARANTINE_RUN);
            put_u64(out, *id);
            put_length_prefixed(out, scope);
        }
        for id in &self.dropped_quarantined_runs {
            out.push(tag::DROP_QUARANTINED_RUN);
            put_u64(out, *id);
        }
    }

    pub fn decode(data: &[u8]) -> Option<VersionEdit> {
        let mut r = Reader { data, at: 0 };
        let mut edit = VersionEdit::default();
        while !r.done() {
            match r.u8()? {
                tag::FLUSHED_SEQ => edit.flushed_seq = Some(r.u64()?),
                tag::NEXT_FILE => edit.next_file = Some(r.u64()?),
                tag::REMOVE_RUN => edit.removed.push((r.u8()?, r.u64()?)),
                tag::ADD_RUN => {
                    let level = r.u8()?;
                    let id = r.u64()?;
                    let file_size = r.u64()?;
                    let smallest = Bytes::copy_from_slice(r.bytes()?);
                    let largest = Bytes::copy_from_slice(r.bytes()?);
                    crate::types::parse_internal_key(&smallest)?;
                    crate::types::parse_internal_key(&largest)?;
                    edit.added.push(RunDesc {
                        id,
                        level,
                        file_size,
                        smallest,
                        largest,
                        smallest_seq: r.u64()?,
                        largest_seq: r.u64()?,
                        num_entries: r.u64()?,
                        oldest_journal_segment: unopt(r.u64()?),
                    });
                }
                tag::SEGMENTS => {
                    let n = r.varint()? as usize;
                    let mut segments = Vec::with_capacity(n.min(4096));
                    for _ in 0..n {
                        segments.push((r.u64()?, r.u64()?));
                    }
                    edit.segments = Some(segments);
                }
                tag::QUARANTINE => {
                    let scope = Bytes::copy_from_slice(r.bytes()?);
                    let reason = reason_from(r.u8()?)?;
                    let file = decode_file(&mut r)?;
                    edit.quarantined.push(Damage {
                        scope,
                        reason,
                        file,
                        first_bad_seq: unopt(r.u64()?),
                    });
                }
                tag::RELEASE => {
                    let scope = Bytes::copy_from_slice(r.bytes()?);
                    edit.released.push((scope, r.u64()?));
                }
                tag::QUARANTINE_RUN => {
                    let id = r.u64()?;
                    edit.quarantined_runs
                        .push((id, Bytes::copy_from_slice(r.bytes()?)));
                }
                tag::DROP_QUARANTINED_RUN => edit.dropped_quarantined_runs.push(r.u64()?),
                _ => return None,
            }
        }
        Some(edit)
    }
}

/// The state a manifest describes: every edit applied in order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ManifestState {
    pub runs: BTreeMap<u64, RunDesc>,
    pub flushed_seq: Seq,
    pub next_file: u64,
    pub segments: Vec<(u64, u64)>,
    pub quarantine: Vec<Damage>,
    pub released: Vec<(Bytes, Seq)>,
    pub quarantined_runs: BTreeMap<u64, Bytes>,
}

impl ManifestState {
    pub fn new() -> ManifestState {
        ManifestState {
            next_file: 1,
            ..ManifestState::default()
        }
    }

    /// Applies `edit`. `None` when it contradicts the state (a structural error, R3).
    pub fn apply(&mut self, edit: &VersionEdit) -> Option<()> {
        if let Some(seq) = edit.flushed_seq {
            self.flushed_seq = self.flushed_seq.max(seq);
        }
        if let Some(next) = edit.next_file {
            self.next_file = self.next_file.max(next);
        }
        for &(level, id) in &edit.removed {
            let run = self.runs.remove(&id)?;
            if run.level != level {
                return None;
            }
        }
        for run in &edit.added {
            if run.id >= self.next_file || self.runs.insert(run.id, run.clone()).is_some() {
                return None;
            }
        }
        if let Some(segments) = &edit.segments {
            self.segments.clone_from(segments);
        }
        // Releases first: a release edit lifts the quarantine its scope had, but a
        // snapshot record carries the release history next to the current quarantine,
        // and a scope released once and damaged later must stay quarantined.
        for (scope, seq) in &edit.released {
            self.quarantine.retain(|d| d.scope != *scope);
            self.released.retain(|(s, _)| s != scope);
            self.released.push((scope.clone(), *seq));
        }
        for damage in &edit.quarantined {
            if !self.quarantine.iter().any(|d| d.scope == damage.scope) {
                self.quarantine.push(damage.clone());
            }
        }
        for (id, scope) in &edit.quarantined_runs {
            self.runs.remove(id);
            self.quarantined_runs.insert(*id, scope.clone());
        }
        for id in &edit.dropped_quarantined_runs {
            self.quarantined_runs.remove(id);
        }
        Some(())
    }

    /// One edit that rebuilds this state from nothing.
    pub fn snapshot(&self) -> VersionEdit {
        VersionEdit {
            flushed_seq: Some(self.flushed_seq),
            next_file: Some(self.next_file),
            added: self.runs.values().cloned().collect(),
            segments: Some(self.segments.clone()),
            quarantined: self.quarantine.clone(),
            released: self.released.clone(),
            quarantined_runs: self
                .quarantined_runs
                .iter()
                .map(|(&id, scope)| (id, scope.clone()))
                .collect(),
            ..VersionEdit::default()
        }
    }
}

fn io(error: std::io::Error) -> Error {
    Error::from(error)
}

fn corrupt(file: FileId, offset: u64, what: &'static str) -> Error {
    Error::Corruption { file, offset, what }
}

fn read_all(file: &dyn FsFile) -> Result<Vec<u8>> {
    let len = file.len().map_err(io)?;
    let mut buf = vec![0u8; len as usize];
    file.read_exact_at(&mut buf, 0).map_err(io)?;
    Ok(buf)
}

/// Reads a small file whole.
pub(crate) fn read_file(fs: &dyn Fs, path: &Path) -> Result<Vec<u8>> {
    let file = fs.open(path, OpenMode::Read).map_err(io)?;
    read_all(&*file)
}

fn encode_record(edit: &VersionEdit) -> Vec<u8> {
    let mut out = vec![0u8; RECORD_HEADER];
    edit.encode(&mut out);
    let len = (out.len() - RECORD_HEADER) as u32;
    out[4..8].copy_from_slice(&len.to_le_bytes());
    let crc = crc::mask(crc::crc32c(&out[4..]));
    out[0..4].copy_from_slice(&crc.to_le_bytes());
    out
}

/// The record starting at `at`, if it is whole and its checks pass: `(edit, end)`.
fn decode_record(data: &[u8], at: usize) -> Option<(VersionEdit, usize)> {
    let crc = crate::coding::read_u32_le(data, at)?;
    let len = crate::coding::read_u32_le(data, at + 4)? as usize;
    let end = (at + RECORD_HEADER).checked_add(len)?;
    let body = data.get(at + 4..end)?;
    if crc::unmask(crc) != crc::crc32c(body) {
        return None;
    }
    Some((VersionEdit::decode(&body[4..])?, end))
}

/// What reading a manifest found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ManifestReport {
    pub records: u64,
    /// L3: a damaged or partial final record was dropped.
    pub torn_tail: bool,
    pub tail_bytes: u64,
}

/// Replays `MANIFEST-<number>`.
pub fn read_manifest(
    fs: &dyn Fs,
    dir: &Path,
    number: u64,
) -> Result<(ManifestState, ManifestReport)> {
    let file_id = FileId::Manifest { number };
    let path = dir.join(manifest_name(number));
    let data = match fs.open(&path, OpenMode::Read) {
        Ok(file) => read_all(&*file)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(corrupt(
                FileId::Current,
                0,
                "CURRENT names a missing manifest",
            ));
        }
        Err(e) => return Err(io(e)),
    };
    let mut state = ManifestState::new();
    let mut report = ManifestReport::default();
    let mut at = 0usize;
    while at < data.len() {
        match decode_record(&data, at) {
            Some((edit, end)) => {
                if state.apply(&edit).is_none() {
                    return Err(corrupt(
                        file_id,
                        at as u64,
                        "manifest edit contradicts its state",
                    ));
                }
                report.records += 1;
                at = align(end);
            }
            None => {
                // Torn final record, unless a valid record follows (R3).
                let synced_later = (align(at + 1)..data.len())
                    .step_by(RECORD_ALIGN as usize)
                    .any(|p| decode_record(&data, p).is_some());
                if synced_later {
                    return Err(corrupt(file_id, at as u64, "damaged manifest record"));
                }
                report.torn_tail = true;
                report.tail_bytes = (data.len() - at) as u64;
                break;
            }
        }
    }
    if report.records == 0 {
        return Err(corrupt(file_id, 0, "manifest without a snapshot record"));
    }
    Ok((state, report))
}

fn current_line(number: u64) -> String {
    let name = manifest_name(number);
    let crc = crc::mask(crc::crc32c(name.as_bytes()));
    format!("{name} {crc:08x}\n")
}

/// The manifest number `CURRENT` names. Damage is R3, a read error R2.
pub fn read_current(fs: &dyn Fs, dir: &Path) -> Result<u64> {
    let data = read_file(fs, &dir.join(CURRENT_FILE))?;
    let bad = || corrupt(FileId::Current, 0, "malformed CURRENT");
    let text = std::str::from_utf8(&data).map_err(|_| bad())?;
    let line = text.strip_suffix('\n').ok_or_else(bad)?;
    let (name, _) = line.split_once(' ').ok_or_else(bad)?;
    let number = parse_manifest_name(name).ok_or_else(bad)?;
    if current_line(number) != text {
        return Err(bad());
    }
    Ok(number)
}

/// Points `CURRENT` at `MANIFEST-<number>`: temp file, sync, rename, directory sync.
pub fn set_current(fs: &dyn Fs, dir: &Path, number: u64) -> Result<()> {
    switch_current(fs, dir, number).map_err(|e| e.error())
}

/// [`set_current`], telling whether a failure may have switched `CURRENT` already.
/// `Retry`: it still names the old manifest. `Fatal`: the rename was attempted, so
/// `CURRENT` may name the new manifest now or after a restart; neither manifest may be
/// deleted before the next open decides (RV-KV2 finding 3).
pub fn switch_current(
    fs: &dyn Fs,
    dir: &Path,
    number: u64,
) -> std::result::Result<(), AppendError> {
    let before = |e: std::io::Error| AppendError::Retry(io(e));
    let tmp = dir.join(CURRENT_TMP);
    if fs.exists(&tmp).map_err(before)? {
        fs.remove(&tmp).map_err(before)?;
    }
    let file = fs.open(&tmp, OpenMode::CreateNew).map_err(before)?;
    file.write_all_at(current_line(number).as_bytes(), 0)
        .map_err(before)?;
    file.sync_all().map_err(before)?;
    drop(file);
    let after = |e: std::io::Error| AppendError::Fatal(io(e));
    fs.rename(&tmp, &dir.join(CURRENT_FILE)).map_err(after)?;
    fs.sync_dir(dir).map_err(after)
}

/// Writes `FORMAT` (`content` is [`FORMAT_CONTENT`] or [`FORMAT_CONTENT_V2`]): temp
/// file, sync, rename (over an older `FORMAT`), directory sync.
pub fn write_format(fs: &dyn Fs, dir: &Path, content: &[u8]) -> Result<()> {
    replace_file(fs, dir, FORMAT_TMP, FORMAT_FILE, content)
}

/// Writes `JOURNAL_KEY` the same way. Called only while `FORMAT` still names format 1
/// (or does not exist), so nothing depends on an earlier key file it replaces.
pub(crate) fn write_journal_key(fs: &dyn Fs, dir: &Path, key: &JournalKey) -> Result<()> {
    replace_file(
        fs,
        dir,
        JOURNAL_KEY_TMP,
        JOURNAL_KEY_FILE,
        &key.encode_file(),
    )
}

fn replace_file(fs: &dyn Fs, dir: &Path, tmp: &str, name: &str, content: &[u8]) -> Result<()> {
    let tmp = dir.join(tmp);
    if fs.exists(&tmp).map_err(io)? {
        fs.remove(&tmp).map_err(io)?;
    }
    let file = fs.open(&tmp, OpenMode::CreateNew).map_err(io)?;
    file.write_all_at(content, 0).map_err(io)?;
    file.sync_all().map_err(io)?;
    drop(file);
    fs.rename(&tmp, &dir.join(name)).map_err(io)?;
    fs.sync_dir(dir).map_err(io)
}

/// The open manifest the store appends edits to.
pub struct ManifestWriter {
    number: u64,
    path: PathBuf,
    file: Arc<dyn FsFile>,
    len: u64,
    /// A write failed: the file may end in a partial record, so the next edit goes to
    /// a fresh manifest.
    needs_roll: bool,
}

/// Why an append failed.
#[derive(Debug)]
pub enum AppendError {
    /// Nothing reached the manifest that a later open could mistake for an edit, or
    /// the next edit rolls to a new manifest. Retry later.
    Retry(Error),
    /// The outcome of the write or sync is unknown: latch `RecoveryRequired`.
    Fatal(Error),
}

impl AppendError {
    pub fn error(&self) -> Error {
        match self {
            AppendError::Retry(e) | AppendError::Fatal(e) => e.clone(),
        }
    }
}

impl ManifestWriter {
    /// Creates `MANIFEST-<number>` holding `state` as one snapshot record, syncs it and
    /// the directory. Does not switch `CURRENT`.
    pub fn create(
        fs: &dyn Fs,
        dir: &Path,
        number: u64,
        state: &ManifestState,
    ) -> Result<ManifestWriter> {
        let path = dir.join(manifest_name(number));
        let result = (|| {
            // Numbers are never reused, so an existing file is a leftover of a roll that
            // never reached CURRENT.
            if fs.exists(&path).map_err(io)? {
                fs.remove(&path).map_err(io)?;
            }
            let file = fs.open(&path, OpenMode::CreateNew).map_err(io)?;
            let record = encode_record(&state.snapshot());
            file.write_all_at(&record, 0).map_err(io)?;
            file.sync_all().map_err(io)?;
            // The new name must be durable before CURRENT can point at it.
            fs.sync_dir(dir).map_err(io)?;
            Ok((file, align(record.len()) as u64))
        })();
        match result {
            Ok((file, len)) => Ok(ManifestWriter {
                // The next record starts at the next aligned offset.
                number,
                path,
                file,
                len,
                needs_roll: false,
            }),
            Err(e) => {
                let _ = fs.remove(&path);
                Err(e)
            }
        }
    }

    pub fn number(&self) -> u64 {
        self.number
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// True when the next edit should go to a fresh manifest.
    pub fn wants_roll(&self) -> bool {
        self.needs_roll || self.len >= ROLL_BYTES
    }

    /// Appends one edit and syncs it.
    pub fn append(&mut self, edit: &VersionEdit) -> std::result::Result<(), AppendError> {
        let record = encode_record(edit);
        if let Err(e) = self.file.write_all_at(&record, self.len) {
            let e = io(e);
            self.needs_roll = true;
            return Err(match e {
                Error::NoSpace => AppendError::Retry(e),
                e => AppendError::Fatal(e),
            });
        }
        if let Err(e) = self.file.sync_all() {
            self.needs_roll = true;
            return Err(AppendError::Fatal(io(e)));
        }
        self.len = align((self.len + record.len() as u64) as usize) as u64;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::FaultFs;

    fn sample_edit() -> VersionEdit {
        VersionEdit {
            flushed_seq: Some(42),
            next_file: Some(9),
            removed: vec![(0, 3)],
            added: vec![RunDesc {
                id: 7,
                level: 1,
                file_size: 4096,
                smallest: Bytes::from(crate::types::internal_key(b"a", 1, crate::Kind::Put)),
                largest: Bytes::from(crate::types::internal_key(b"z", 2, crate::Kind::Put)),
                smallest_seq: 1,
                largest_seq: 2,
                num_entries: 2,
                oldest_journal_segment: Some(4),
            }],
            segments: Some(vec![(1, 1), (2, 3)]),
            quarantined: vec![Damage {
                scope: Bytes::from_static(b"@g"),
                reason: DamageReason::JournalRecord,
                file: FileId::Journal { segment: 2 },
                first_bad_seq: Some(17),
            }],
            released: vec![(Bytes::from_static(b"@h"), 30)],
            quarantined_runs: vec![(5, Bytes::from_static(b"@g"))],
            dropped_quarantined_runs: vec![4],
        }
    }

    /// KVfix3: a scope released once and quarantined later keeps its quarantine through
    /// a snapshot record (a fresh manifest), while a later release still lifts it.
    #[test]
    fn ns07_snapshot_keeps_the_quarantine_of_a_once_released_scope() {
        let scope = Bytes::from_static(b"@g");
        let damage = Damage {
            scope: scope.clone(),
            reason: DamageReason::RunBlock,
            file: FileId::Run { id: 3 },
            first_bad_seq: None,
        };
        let mut state = ManifestState::new();
        let release = |seq| VersionEdit {
            released: vec![(scope.clone(), seq)],
            ..VersionEdit::default()
        };
        state.apply(&release(10)).unwrap();
        state
            .apply(&VersionEdit {
                quarantined: vec![damage.clone()],
                ..VersionEdit::default()
            })
            .unwrap();
        let mut rebuilt = ManifestState::new();
        rebuilt.apply(&state.snapshot()).unwrap();
        assert_eq!(rebuilt.quarantine, vec![damage]);
        assert_eq!(rebuilt.released, vec![(scope.clone(), 10)]);
        rebuilt.apply(&release(20)).unwrap();
        assert!(rebuilt.quarantine.is_empty());
    }

    #[test]
    fn ns_unit_version_edits_round_trip() {
        let edit = sample_edit();
        let mut buf = Vec::new();
        edit.encode(&mut buf);
        assert_eq!(VersionEdit::decode(&buf), Some(edit));
        assert_eq!(VersionEdit::decode(&[0xEE]), None);
        assert_eq!(parse_manifest_name(&manifest_name(12)), Some(12));
        assert_eq!(parse_manifest_name("MANIFEST-12"), None);
    }

    #[test]
    fn ns07_manifest_torn_tail_is_dropped_and_inner_damage_refuses() {
        let fs = FaultFs::new();
        let dir = Path::new("/m");
        fs.create_dir_all_durable(dir);
        let mut state = ManifestState::new();
        state.next_file = 5;
        let mut writer = ManifestWriter::create(&fs, dir, 1, &state).unwrap();
        let e1 = VersionEdit {
            flushed_seq: Some(10),
            ..VersionEdit::default()
        };
        let e2 = VersionEdit {
            flushed_seq: Some(20),
            ..VersionEdit::default()
        };
        writer.append(&e1).unwrap();
        writer.append(&e2).unwrap();
        let path = dir.join(manifest_name(1));
        let (read, report) = read_manifest(&fs, dir, 1).unwrap();
        assert_eq!(read.flushed_seq, 20);
        assert_eq!(report.records, 3);
        let len = fs.contents(&path).unwrap().len() as u64;

        // L3: damage in the final record drops it.
        let torn = fs.kill();
        torn.flip_bit(&path, len - 3, 1).unwrap();
        let (read, report) = read_manifest(&torn, dir, 1).unwrap();
        assert!(report.torn_tail);
        assert_eq!(read.flushed_seq, 10);

        // R3: damage before the final record refuses.
        let damaged = fs.kill();
        damaged.flip_bit(&path, 10, 0).unwrap();
        assert!(matches!(
            read_manifest(&damaged, dir, 1),
            Err(Error::Corruption {
                file: FileId::Manifest { number: 1 },
                ..
            })
        ));
    }

    #[test]
    fn ns07_current_damage_is_r3() {
        let fs = FaultFs::new();
        let dir = Path::new("/c");
        fs.create_dir_all_durable(dir);
        set_current(&fs, dir, 3).unwrap();
        assert_eq!(read_current(&fs, dir).unwrap(), 3);
        for offset in 0..20 {
            let damaged = fs.kill();
            damaged
                .flip_bit(&dir.join(CURRENT_FILE), offset, 2)
                .unwrap();
            assert!(
                matches!(
                    read_current(&damaged, dir),
                    Err(Error::Corruption {
                        file: FileId::Current,
                        ..
                    })
                ),
                "offset {offset}"
            );
        }
    }
}
