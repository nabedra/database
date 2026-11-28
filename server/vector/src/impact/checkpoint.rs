//! Checkpoints of an impact index, under the rules of `index/checkpoint.rs`. One pins like a scan
//! (it holds removals back) and writes the documents of the listed segments to immutable files, in
//! frames with a checksum each. Only when every file is synced does a manifest name them, by rename.
//! Recovery reads the documents back and builds the lists again.
//!
//! ```text
//! doc-<n>   = magic ‖ frame*              frame = len u32 ‖ crc32c u32 ‖ entry*
//! MANIFEST  = magic ‖ body ‖ crc32c u32   body  = identity, columns, ranges (pos, low, bound, closed), files
//! entry     = range ‖ key ‖ at ‖ ts ‖ next at ‖ next ts ‖ terms ‖ columns ‖ image
//! ```

use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use nabedra_rt::fs::{Fs, FsFile, FsLock, OpenMode};

use super::mutant::mutant;
use super::postings::{Entry, PostingStore, Term};
use super::rows::{Gen, Point, Slot, Stored};
use super::{Analysis, Config, ImpactIndex, MAX_DOC_TERMS, PinInner, Service};
use crate::index::Damage;
use crate::text::MAX_TERM_BYTES;
use crate::types::{ColumnType, CommitTs, Error, Key, Pos, RangeId, Result, Value};

const MANIFEST: &str = "MANIFEST";
const MANIFEST_TMP: &str = "MANIFEST.tmp";
const LOCK: &str = "LOCK";
const FILE_MAGIC: &[u8; 8] = b"NVIDOC01";
const MANIFEST_MAGIC: &[u8; 8] = b"NVIMAN01";
/// A file is closed beyond this.
const FILE_BYTES: u64 = 64 << 20;
/// A frame is closed beyond this, and holds at most one more row.
const FRAME_BYTES: usize = 1 << 20;
const MAX_FRAME: usize = 32 << 20;
const MAX_MANIFEST: u64 = 64 << 20;
const COPY_BYTES: usize = 1 << 20;

/// The directory of one impact index's checkpoints.
#[derive(Clone)]
pub struct Checkpoints {
    fs: Arc<dyn Fs>,
    dir: PathBuf,
}

#[derive(Clone)]
struct Head {
    id: RangeId,
    generation: Gen,
    pos: Pos,
    low: Pos,
    bound: CommitTs,
    closed: CommitTs,
}

#[derive(Clone)]
struct FileInfo {
    number: u64,
    len: u64,
    ranges: BTreeSet<RangeId>,
}

#[derive(Clone)]
struct Manifest {
    /// The analysis and the filter columns. A checkpoint of another one is not loaded.
    identity: Vec<u8>,
    ranges: Vec<Head>,
    files: Vec<FileInfo>,
}

#[derive(Clone)]
struct OpenFile {
    file: Arc<dyn FsFile>,
    info: FileInfo,
}

/// The running checkpoint.
#[derive(Clone)]
pub(super) struct Running {
    /// Holds removals back, like a scan's.
    _pin: Arc<PinInner>,
    heads: Vec<Head>,
    todo: Vec<u64>,
    /// File numbers in the directory or written since. A new file takes one above them all.
    taken: BTreeSet<u64>,
    file: Option<OpenFile>,
    files: Vec<FileInfo>,
    /// Excludes another writer until publication and cleanup finish.
    _lease: Arc<FsLock>,
}

fn io_error(e: io::Error) -> Error {
    Error::Io(e.to_string())
}

fn damaged(why: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, why)
}

fn file_name(number: u64) -> String {
    format!("doc-{number:016x}")
}

/// A number above every one taken, or, past the last one, the lowest that is free.
fn free_number(taken: &BTreeSet<u64>) -> Option<u64> {
    match taken.last() {
        None => Some(0),
        Some(last) => last
            .checked_add(1)
            .or_else(|| (0..=u64::MAX).find(|n| !taken.contains(n))),
    }
}

fn identity(analysis: &Analysis, columns: &[ColumnType]) -> Vec<u8> {
    let mut out = analysis.identity().into_bytes();
    out.push(b':');
    out.extend(columns.iter().map(|c| match c {
        ColumnType::Bool => b'b',
        ColumnType::Int => b'i',
        ColumnType::Float => b'f',
        ColumnType::Text => b's',
        ColumnType::Tags => b't',
    }));
    out
}

impl Checkpoints {
    pub fn new(fs: Arc<dyn Fs>, dir: impl Into<PathBuf>) -> Checkpoints {
        Checkpoints {
            fs,
            dir: dir.into(),
        }
    }

    /// Persist each directory link, including links left by a failed prepare.
    fn prepare(&self) -> io::Result<()> {
        self.fs.create_dir_all(&self.dir)?;
        for parent in self.dir.ancestors().skip(1) {
            let parent = if parent.as_os_str().is_empty() {
                Path::new(".")
            } else {
                parent
            };
            self.fs.sync_dir(parent)?;
        }
        Ok(())
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    fn lease(&self) -> Result<FsLock> {
        self.prepare().map_err(io_error)?;
        self.fs.lock(&self.path(LOCK)).map_err(|e| match e.kind() {
            io::ErrorKind::WouldBlock => Error::Busy,
            _ => io_error(e),
        })
    }

    /// The numbers of the files named as [`file_name`] names them. Other names are ignored.
    fn numbers(&self) -> io::Result<Vec<u64>> {
        let names = self.fs.list(&self.dir)?;
        let number = |p: &PathBuf| {
            let hex = p.file_name()?.to_str()?.strip_prefix("doc-")?;
            let exact =
                hex.len() == 16 && hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
            exact.then(|| u64::from_str_radix(hex, 16).ok())?
        };
        Ok(names.iter().filter_map(number).collect())
    }

    fn load_manifest(&self) -> io::Result<Option<Manifest>> {
        let path = self.path(MANIFEST);
        if !self.fs.exists(&path)? {
            return Ok(None);
        }
        let file = self.fs.open(&path, OpenMode::Read)?;
        let len = file.len()?;
        if !(12..=MAX_MANIFEST).contains(&len) {
            return Err(damaged("the manifest's size is wrong"));
        }
        let mut bytes = vec![0; len as usize];
        file.read_exact_at(&mut bytes, 0)?;
        let (body, crc) = bytes.split_at(bytes.len() - 4);
        if crc32c(body).to_le_bytes() != crc {
            return Err(damaged("the manifest's checksum is wrong"));
        }
        Manifest::decode(body).map(Some)
    }

    /// Publishes a manifest. Everything it names has to be durable.
    fn install(&self, manifest: &Manifest) -> io::Result<()> {
        let tmp = self.path(MANIFEST_TMP);
        if self.fs.exists(&tmp)? {
            self.fs.remove(&tmp)?;
        }
        let mut bytes = manifest.encode();
        bytes.extend(crc32c(&bytes).to_le_bytes());
        let file = self.fs.open(&tmp, OpenMode::CreateNew)?;
        file.write_all_at(&bytes, 0)?;
        file.sync_data()?;
        self.fs.rename(&tmp, &self.path(MANIFEST))?;
        self.fs.sync_dir(&self.dir)
    }

    /// Removes files that the manifest does not name. Failing to only leaves garbage.
    fn clean(&self, manifest: &Manifest) {
        for number in self.numbers().unwrap_or_default() {
            if manifest.files.iter().all(|f| f.number != number) {
                let _ = self.fs.remove(&self.path(&file_name(number)));
            }
        }
    }

    /// Takes a range out of the installed checkpoint, so that a crash cannot bring a dropped or
    /// replaced index back.
    pub(super) fn forget(&self, id: RangeId) -> Result<()> {
        match self.load_manifest() {
            Ok(None) => Ok(()),
            Ok(Some(mut manifest)) => {
                if manifest.ranges.iter().all(|h| h.id != id) {
                    return Ok(());
                }
                manifest.ranges.retain(|h| h.id != id);
                self.install(&manifest).map_err(io_error)
            }
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                // It holds nothing that could be recovered.
                let removed = self.fs.remove(&self.path(MANIFEST));
                removed
                    .and_then(|()| self.fs.sync_dir(&self.dir))
                    .map_err(io_error)
            }
            Err(e) => Err(io_error(e)),
        }
    }

    /// Calls `f` with every frame of a file.
    fn frames(
        &self,
        info: &FileInfo,
        f: &mut dyn FnMut(&[u8]) -> io::Result<()>,
    ) -> io::Result<()> {
        let file = self
            .fs
            .open(&self.path(&file_name(info.number)), OpenMode::Read)?;
        if file.len()? != info.len {
            return Err(damaged("a file's size is wrong"));
        }
        let mut magic = [0; 8];
        file.read_exact_at(&mut magic, 0)?;
        if &magic != FILE_MAGIC {
            return Err(damaged("not a checkpoint file"));
        }
        let mut at = 8;
        while at < info.len {
            let mut head = [0; 8];
            file.read_exact_at(&mut head, at)?;
            let word =
                |i: usize| u32::from_le_bytes([head[i], head[i + 1], head[i + 2], head[i + 3]]);
            let (len, crc) = (word(0) as usize, word(4));
            let end = at.saturating_add(8 + len as u64);
            if len > MAX_FRAME || end > info.len {
                return Err(damaged("a frame's size is wrong"));
            }
            let mut payload = vec![0; len];
            file.read_exact_at(&mut payload, at + 8)?;
            if crc32c(&payload) != crc {
                return Err(damaged("a frame's checksum is wrong"));
            }
            f(&payload)?;
            at = end;
        }
        Ok(())
    }
}

/// Copies the installed checkpoint of another search replica. The manifest goes last.
pub fn copy_checkpoint(from: &Checkpoints, to: &Checkpoints) -> Result<()> {
    let manifest = from
        .load_manifest()
        .and_then(|m| m.ok_or_else(|| damaged("the peer has no checkpoint")))
        .map_err(io_error)?;
    let _lease = to.lease()?;
    let copy = |mut manifest: Manifest| -> io::Result<()> {
        let mut taken: BTreeSet<_> = to.numbers()?.into_iter().collect();
        // A file named by the installed manifest stays reserved even if it is missing.
        match to.load_manifest() {
            Ok(Some(local)) => taken.extend(local.files.into_iter().map(|file| file.number)),
            Ok(None) => {}
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {}
            Err(e) => return Err(e),
        }
        for info in &mut manifest.files {
            let source = from.fs.open(&from.path(&file_name(info.number)), OpenMode::Read)?;
            if source.len()? != info.len {
                return Err(damaged("a peer file's size is wrong"));
            }
            info.number = free_number(&taken).ok_or_else(|| damaged("no file number is free"))?;
            taken.insert(info.number);
            let target = to.fs.open(&to.path(&file_name(info.number)), OpenMode::CreateNew)?;
            let mut at = 0;
            while at < info.len {
                let mut chunk = vec![0; COPY_BYTES.min((info.len - at) as usize)];
                source.read_exact_at(&mut chunk, at)?;
                target.write_all_at(&chunk, at)?;
                at += chunk.len() as u64;
            }
            target.sync_data()?;
        }
        to.fs.sync_dir(&to.dir)?;
        to.install(&manifest)?;
        to.clean(&manifest);
        Ok(())
    };
    copy(manifest).map_err(io_error)
}

impl<S: PostingStore> ImpactIndex<S> {
    /// `BeginCheckpoint`: pins a read point and the segments. Every range that is not being installed
    /// takes part.
    pub fn begin_checkpoint(&mut self) -> Result<()> {
        let store = self
            .checkpoints
            .as_ref()
            .ok_or(Error::Internal("no checkpoint store"))?;
        if self.checkpoint.is_some() {
            return Err(Error::Busy);
        }
        let lease = store.lease()?;
        let taken = store.numbers().map_err(io_error)?.into_iter().collect();
        let held = |s: &Service| matches!(s, Service::CatchingUp | Service::Serving);
        let heads: Vec<Head> = (self.ranges.iter().filter(|(_, r)| held(&r.service)))
            .map(|(id, r)| Head {
                id: *id,
                generation: r.generation,
                pos: r.app,
                low: r.low,
                bound: r.bound,
                closed: r.closed,
            })
            .collect();
        // It needs everything that a scan at its oldest cut or later needs.
        let point = |h: &Head| {
            let p = Point {
                pos: h.low,
                cut: CommitTs::MAX,
            };
            (h.generation, p)
        };
        let pin = |id| PinInner {
            id,
            points: heads.iter().map(point).collect(),
            ranges: Vec::new(),
            segments: Vec::new(),
            failed: AtomicBool::new(false),
        };
        self.checkpoint = Some(Running {
            _pin: self.readers.begin(pin),
            todo: self.listed(),
            heads,
            taken,
            file: None,
            files: Vec::new(),
            _lease: Arc::new(lease),
        });
        Ok(())
    }

    /// `WriteFile`: writes one segment. Returns whether there is more to write. An error ends the
    /// checkpoint, and the installed one stays.
    pub fn write_file(&mut self) -> Result<bool> {
        let mut ck = self
            .checkpoint
            .take()
            .ok_or(Error::Internal("no checkpoint is running"))?;
        let Some(seg) = ck.todo.pop() else {
            self.checkpoint = Some(ck);
            return Ok(false);
        };
        self.write_segment(&mut ck, seg).map_err(io_error)?;
        let more = !ck.todo.is_empty();
        self.checkpoint = Some(ck);
        Ok(more)
    }

    fn write_segment(&self, ck: &mut Running, seg: u64) -> io::Result<()> {
        let store = self
            .checkpoints
            .as_ref()
            .ok_or_else(|| damaged("no checkpoint store"))?;
        let segment = self
            .segments
            .get(&seg)
            .ok_or_else(|| damaged("a pinned segment is gone"))?;
        let mut frame = Vec::new();
        let mut ranges = BTreeSet::new();
        for e in &segment.entries {
            let Some(head) = ck.heads.iter().find(|h| h.generation == e.generation) else {
                continue;
            };
            if self.encode_entry(head, e, &mut frame)? {
                ranges.insert(head.id);
            }
            if frame.len() >= FRAME_BYTES {
                ck.write_frame(store, &frame, &mut ranges)?;
                frame.clear();
            }
        }
        if frame.is_empty() {
            return Ok(());
        }
        ck.write_frame(store, &frame, &mut ranges)
    }

    /// Adds an entry if a cut that the checkpoint can serve sees it.
    fn encode_entry(&self, head: &Head, e: &Entry, out: &mut Vec<u8>) -> io::Result<bool> {
        let rows = &self.rows;
        let version = rows.version(e.slot, e.at);
        let (Some(version), Some(key)) = (version, rows.key(e.slot)) else {
            return Err(damaged("an entry without its row"));
        };
        let next = rows.successor(e.slot, e.at).filter(|s| s.at <= head.pos);
        if e.at > head.pos || next.is_some_and(|s| s.at <= head.low) {
            return Ok(false);
        }
        let row = version
            .row
            .as_ref()
            .ok_or_else(|| damaged("an entry without its row"))?;
        let mut o = Out(std::mem::take(out));
        o.0.extend(head.id.0.to_le_bytes());
        o.bytes(key)?;
        o.u64(e.at);
        o.u64(version.ts);
        let (next_at, next_ts) = next.map_or((0, 0), |s| (s.at, s.ts));
        o.u64(next_at);
        o.u64(next_ts);
        o.u32(u32::try_from(row.terms.len()).map_err(|_| damaged("too many terms"))?);
        match &self.analysis {
            Analysis::Text(_) => {
                // By name, as ids differ between workers and runs.
                let mut named = Vec::with_capacity(row.terms.len());
                for &(term, value) in row.terms.iter() {
                    let name = self
                        .dict
                        .name(term)
                        .ok_or_else(|| damaged("a term is gone"))?;
                    named.push((name, value));
                }
                named.sort_unstable_by(|a, b| a.0.cmp(b.0));
                for (name, value) in named {
                    o.bytes(name.as_bytes())?;
                    o.0.extend(value.to_le_bytes());
                }
            }
            Analysis::Sparse => {
                for &(term, value) in row.terms.iter() {
                    o.u32(term);
                    o.0.extend(value.to_le_bytes());
                }
            }
        }
        o.u32(u32::try_from(row.cols.len()).map_err(|_| damaged("too many columns"))?);
        for v in &row.cols {
            o.value(v)?;
        }
        o.bytes(&row.image)?;
        *out = o.0;
        Ok(true)
    }

    /// `InstallManifest`: every file is on disk, then the manifest is.
    pub fn install_manifest(&mut self) -> Result<()> {
        let mut ck = self
            .checkpoint
            .take()
            .ok_or(Error::Internal("no checkpoint is running"))?;
        if !ck.todo.is_empty() {
            self.checkpoint = Some(ck);
            return Err(Error::Busy);
        }
        let store = self
            .checkpoints
            .as_ref()
            .ok_or(Error::Internal("no checkpoint store"))?;
        for h in &mut ck.heads {
            if mutant("checkpoint_ahead_of_files") {
                h.pos = self.ranges.get(&h.id).map_or(h.pos, |r| r.app);
            }
            if mutant("checkpoint_forgets_low") {
                (h.low, h.bound) = (0, 0);
            }
        }
        let manifest = Manifest {
            identity: identity(&self.analysis, &self.cfg.columns),
            ranges: ck.heads.clone(),
            files: Vec::new(),
        };
        let install = |ck: &mut Running| -> io::Result<()> {
            ck.seal()?;
            if !mutant("manifest_before_sync") {
                store.fs.sync_dir(&store.dir)?;
            }
            let manifest = Manifest {
                files: ck.files.clone(),
                ..manifest
            };
            store.install(&manifest)?;
            store.clean(&manifest);
            Ok(())
        };
        install(&mut ck).map_err(io_error)
    }

    /// A whole checkpoint.
    pub fn checkpoint(&mut self) -> Result<()> {
        self.begin_checkpoint()?;
        while self.write_file()? {}
        self.install_manifest()
    }

    /// Starts from the installed checkpoint, or empty without one. Every recovered range resumes the
    /// tail at its position and serves after `caught_up`. A damaged file takes the ranges in it out of
    /// service.
    pub fn recover(cfg: Config, store: S, checkpoints: Checkpoints) -> Result<(Self, Vec<Damage>)> {
        let mut index = ImpactIndex::new(cfg, store, Some(checkpoints.clone()))?;
        let damage = |file: &str, ranges: Vec<RangeId>, e: io::Error| Damage {
            file: file.to_owned(),
            ranges,
            reason: e.to_string(),
        };
        let own = identity(&index.analysis, &index.cfg.columns);
        let manifest = match checkpoints.load_manifest() {
            Ok(None) => return Ok((index, Vec::new())),
            Ok(Some(m)) if m.identity == own => m,
            Ok(Some(_)) => {
                let e = damaged("the checkpoint is of another analysis or other columns");
                return Ok((index, vec![damage(MANIFEST, Vec::new(), e)]));
            }
            Err(e) => return Ok((index, vec![damage(MANIFEST, Vec::new(), e)])),
        };
        for h in &manifest.ranges {
            index.new_range(h.id, Service::CatchingUp, h.pos, h.low);
            if let Some(r) = index.ranges.get_mut(&h.id) {
                (r.bound, r.closed) = (h.bound, h.closed);
            }
        }
        let mut found = Vec::new();
        for info in &manifest.files {
            if let Err(e) = checkpoints.frames(info, &mut |frame| index.load_frame(frame)) {
                let name = file_name(info.number);
                found.push(damage(&name, info.ranges.iter().copied().collect(), e));
            }
        }
        for d in &found {
            for id in &d.ranges {
                index.fail_range(*id, &format!("{}: {}", d.file, d.reason));
            }
        }
        index.after_recovery();
        Ok((index, found))
    }

    /// The counts of the newest versions and the versions that superseded others, from the row map.
    fn after_recovery(&mut self) {
        let text = self.text();
        for r in self.ranges.values_mut() {
            let now = Point {
                pos: r.app,
                cut: CommitTs::MAX,
            };
            let mut touches = Vec::new();
            for &slot in r.keys.values() {
                let rows = &self.rows;
                touches.extend(rows.superseding(slot, r.low).map(|(at, ts)| (at, ts, slot)));
                let shown = rows
                    .versions(slot)
                    .iter()
                    .find(|v| rows.visible(slot, v.at, now));
                if let Some(row) = shown.and_then(|v| v.row.as_ref()) {
                    r.counts.add(row, text);
                }
            }
            touches.sort_unstable();
            r.touches = touches.into();
        }
    }

    fn decode_terms(
        &mut self,
        r: &mut In<'_>,
        new: &mut Vec<Term>,
    ) -> io::Result<(Vec<(Term, f32)>, f64)> {
        let n = r.u32()? as usize;
        if n > MAX_DOC_TERMS {
            return Err(damaged("too many terms"));
        }
        let mut terms = Vec::new();
        let (mut words, mut squares) = (0f64, 0f64);
        let mut last: Option<&[u8]> = None;
        for _ in 0..n {
            let term = match self.analysis {
                Analysis::Text(_) => {
                    let name = r.bytes()?;
                    let text =
                        std::str::from_utf8(name).map_err(|_| damaged("a term is not UTF-8"))?;
                    let ordered = last.is_none_or(|l| l < name);
                    if !ordered || name.is_empty() || name.len() > MAX_TERM_BYTES {
                        return Err(damaged("a term is out of order or out of bounds"));
                    }
                    last = Some(name);
                    let known = self.dict.get(text);
                    let id = self
                        .dict
                        .intern(text)
                        .map_err(|_| damaged("the dictionary is full"))?;
                    if known.is_none() {
                        new.push(id);
                    }
                    id
                }
                Analysis::Sparse => r.u32()?,
            };
            let value = f32::from_le_bytes(r.array()?);
            let text = matches!(self.analysis, Analysis::Text(_));
            let fits = if text {
                value >= 1.0 && value.fract() == 0.0 && value.is_finite()
            } else {
                value.is_finite() && value != 0.0
            };
            if !fits {
                return Err(damaged("a term's value is out of bounds"));
            }
            words += f64::from(value);
            squares += f64::from(value).powi(2);
            terms.push((term, value));
        }
        match self.analysis {
            Analysis::Text(_) => {
                terms.sort_unstable_by_key(|t| t.0);
                Ok((terms, words))
            }
            Analysis::Sparse => {
                if !terms.windows(2).all(|w| w[0].0 < w[1].0) {
                    return Err(damaged("indices are out of order"));
                }
                Ok((terms, squares.sqrt()))
            }
        }
    }

    /// Puts the rows of a frame back.
    fn load_frame(&mut self, frame: &[u8]) -> io::Result<()> {
        let mut r = In(frame);
        while !r.0.is_empty() {
            let mut new = Vec::new();
            let loaded = self.load_entry(&mut r, &mut new);
            self.dict.sweep(&new);
            loaded?;
        }
        Ok(())
    }

    fn load_entry(&mut self, r: &mut In<'_>, new: &mut Vec<Term>) -> io::Result<()> {
        let id = RangeId(u128::from_le_bytes(r.array()?));
        let key: Key = r.bytes()?.into();
        let (at, ts) = (r.u64()?, r.u64()?);
        let (next_at, next_ts) = (r.u64()?, r.u64()?);
        let (terms, norm) = self.decode_terms(r, new)?;
        let n = r.u32()? as usize;
        if n != self.cfg.columns.len() {
            return Err(damaged("a row does not fit the columns"));
        }
        let cols = (0..n).map(|_| r.value()).collect::<io::Result<Vec<_>>>()?;
        let fits = cols
            .iter()
            .zip(&self.cfg.columns)
            .all(|(v, t)| v.kind() == *t && !matches!(v, Value::Float(x) if !x.is_finite()));
        if !fits {
            return Err(damaged("a row does not fit the columns"));
        }
        let image = r.bytes()?.into();
        let stored = Stored {
            terms: terms.into(),
            norm,
            cols,
            image,
        };
        // A range that was dropped since is in the file and not in the manifest.
        let Some(range) = self.ranges.get_mut(&id) else {
            return Ok(());
        };
        let (generation, keys) = (range.generation, &mut range.keys);
        let full = || damaged("too many rows");
        let row = Arc::new(stored);
        let version = (at, ts, Some(row.clone()));
        let slot: Slot = self
            .rows
            .restore(keys, generation, &key, version)
            .ok_or_else(full)?;
        if next_at > 0 {
            self.rows
                .restore(keys, generation, &key, (next_at, next_ts, None));
        }
        if self.rows.refs(slot, at) > 0 {
            return Ok(());
        }
        self.rows.name(slot, at);
        let entry = Entry {
            slot,
            at,
            generation,
            norm: row.norm,
        };
        let head = self.head_segment();
        self.append(head, entry, &row.terms).map_err(|_| full())
    }
}

impl Running {
    fn write_frame(
        &mut self,
        store: &Checkpoints,
        rows: &[u8],
        ranges: &mut BTreeSet<RangeId>,
    ) -> io::Result<()> {
        if self.file.as_ref().is_some_and(|s| s.info.len >= FILE_BYTES) {
            self.seal()?;
        }
        let open = match &mut self.file {
            Some(s) => s,
            none => {
                let number =
                    free_number(&self.taken).ok_or_else(|| damaged("no file number is free"))?;
                self.taken.insert(number);
                let path = store.path(&file_name(number));
                let file = store.fs.open(&path, OpenMode::CreateNew)?;
                file.write_all_at(FILE_MAGIC, 0)?;
                let info = FileInfo {
                    number,
                    len: 8,
                    ranges: BTreeSet::new(),
                };
                none.insert(OpenFile { file, info })
            }
        };
        let len = u32::try_from(rows.len()).map_err(|_| damaged("a frame is too large"))?;
        if rows.len() > MAX_FRAME {
            return Err(damaged("a frame is too large"));
        }
        let mut frame = Vec::with_capacity(rows.len() + 8);
        frame.extend(len.to_le_bytes());
        frame.extend(crc32c(rows).to_le_bytes());
        frame.extend(rows);
        open.file.write_all_at(&frame, open.info.len)?;
        open.info.len = (open.info.len)
            .checked_add(frame.len() as u64)
            .ok_or_else(|| damaged("a file is too large"))?;
        open.info.ranges.append(ranges);
        Ok(())
    }

    /// Syncs the open file and closes it.
    fn seal(&mut self) -> io::Result<()> {
        if let Some(open) = self.file.take() {
            if !mutant("manifest_before_sync") {
                open.file.sync_data()?;
            }
            self.files.push(open.info);
        }
        Ok(())
    }
}

impl Manifest {
    fn encode(&self) -> Vec<u8> {
        let mut o = Out(MANIFEST_MAGIC.to_vec());
        o.u64(self.identity.len() as u64);
        o.0.extend(&self.identity);
        o.u64(self.ranges.len() as u64);
        for h in &self.ranges {
            o.0.extend(h.id.0.to_le_bytes());
            for x in [h.pos, h.low, h.bound, h.closed] {
                o.u64(x);
            }
        }
        o.u64(self.files.len() as u64);
        for f in &self.files {
            o.u64(f.number);
            o.u64(f.len);
            o.u64(f.ranges.len() as u64);
            for id in &f.ranges {
                o.0.extend(id.0.to_le_bytes());
            }
        }
        o.0
    }

    fn decode(body: &[u8]) -> io::Result<Manifest> {
        let mut r = In(body);
        if r.take(8)? != MANIFEST_MAGIC {
            return Err(damaged("not a checkpoint manifest"));
        }
        let n = usize::try_from(r.u64()?).map_err(|_| damaged("a value ends early"))?;
        let identity = r.take(n)?.to_vec();
        let mut ranges = Vec::new();
        for _ in 0..r.u64()? {
            let id = RangeId(u128::from_le_bytes(r.array()?));
            let (pos, low, bound, closed) = (r.u64()?, r.u64()?, r.u64()?, r.u64()?);
            ranges.push(Head {
                id,
                generation: 0,
                pos,
                low,
                bound,
                closed,
            });
        }
        let mut files = Vec::new();
        for _ in 0..r.u64()? {
            let (number, len) = (r.u64()?, r.u64()?);
            let mut ids = BTreeSet::new();
            for _ in 0..r.u64()? {
                ids.insert(RangeId(u128::from_le_bytes(r.array()?)));
            }
            files.push(FileInfo {
                number,
                len,
                ranges: ids,
            });
        }
        Ok(Manifest {
            identity,
            ranges,
            files,
        })
    }
}

#[derive(Default)]
struct Out(Vec<u8>);

impl Out {
    fn u32(&mut self, x: u32) {
        self.0.extend(x.to_le_bytes());
    }
    fn u64(&mut self, x: u64) {
        self.0.extend(x.to_le_bytes());
    }
    fn bytes(&mut self, b: &[u8]) -> io::Result<()> {
        self.u32(u32::try_from(b.len()).map_err(|_| damaged("a value is too large"))?);
        self.0.extend(b);
        Ok(())
    }
    fn value(&mut self, v: &Value) -> io::Result<()> {
        match v {
            Value::Bool(b) => self.0.extend([0, u8::from(*b)]),
            Value::Int(i) => {
                self.0.push(1);
                self.0.extend(i.to_le_bytes());
            }
            Value::Float(f) => {
                self.0.push(2);
                self.0.extend(f.to_le_bytes());
            }
            Value::Text(t) => {
                self.0.push(3);
                self.bytes(t.as_bytes())?;
            }
            Value::Tags(tags) => {
                self.0.push(4);
                self.u32(u32::try_from(tags.len()).map_err(|_| damaged("too many tags"))?);
                for t in tags.iter() {
                    self.bytes(t.as_bytes())?;
                }
            }
        }
        Ok(())
    }
}

/// Reads from the front. Nothing is allocated beyond what is there.
struct In<'a>(&'a [u8]);

impl<'a> In<'a> {
    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let (head, rest) = self
            .0
            .split_at_checked(n)
            .ok_or_else(|| damaged("a value ends early"))?;
        self.0 = rest;
        Ok(head)
    }
    fn array<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        self.take(N)?
            .try_into()
            .map_err(|_| damaged("a value ends early"))
    }
    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }
    fn bytes(&mut self) -> io::Result<&'a [u8]> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    fn text(&mut self) -> io::Result<Arc<str>> {
        let text =
            std::str::from_utf8(self.bytes()?).map_err(|_| damaged("a text is not UTF-8"))?;
        Ok(text.into())
    }
    fn value(&mut self) -> io::Result<Value> {
        let [kind] = self.array()?;
        Ok(match kind {
            0 => Value::Bool(self.take(1)? != [0]),
            1 => Value::Int(i64::from_le_bytes(self.array()?)),
            2 => Value::Float(f64::from_le_bytes(self.array()?)),
            3 => Value::Text(self.text()?),
            4 => {
                let n = self.u32()?;
                // Each tag takes at least its length, so a count beyond the frame is damage.
                if n as usize > self.0.len() / 4 {
                    return Err(damaged("too many tags"));
                }
                Value::Tags((0..n).map(|_| self.text()).collect::<io::Result<_>>()?)
            }
            _ => return Err(damaged("unknown value")),
        })
    }
}

/// CRC32C (Castagnoli).
fn crc32c(data: &[u8]) -> u32 {
    const TABLE: [u32; 256] = {
        let mut table = [0u32; 256];
        let mut i = 0;
        while i < 256 {
            let mut x = i as u32;
            let mut bit = 0;
            while bit < 8 {
                x = if x & 1 == 1 {
                    (x >> 1) ^ 0x82F6_3B78
                } else {
                    x >> 1
                };
                bit += 1;
            }
            table[i] = x;
            i += 1;
        }
        table
    };
    let step = |crc: u32, b: &u8| TABLE[((crc ^ u32::from(*b)) & 0xFF) as usize] ^ (crc >> 8);
    !data.iter().fold(!0, step)
}

#[cfg(test)]
impl Checkpoints {
    pub(super) fn files(&self) -> Vec<PathBuf> {
        self.fs.list(&self.dir).unwrap_or_default()
    }

    /// Installs the manifest again under another identity, as another build would have written it.
    pub(super) fn set_identity(&self, identity: &[u8]) -> io::Result<()> {
        let mut manifest = self
            .load_manifest()?
            .ok_or_else(|| damaged("no manifest"))?;
        manifest.identity = identity.to_vec();
        self.install(&manifest)
    }
}
