//! Checkpoints. One pins like a scan and writes the listed partitions to immutable files, in frames
//! with a checksum each. Only when every file is synced does a manifest name them, by rename.
//!
//! ```text
//! seg-<n>   = magic ‖ frame*              frame = len u32 ‖ crc32c u32 ‖ payload
//! MANIFEST  = magic ‖ body ‖ crc32c u32   body  = ranges (pos, low, bound, closed), files
//! ```

use std::collections::{BTreeMap, BTreeSet, btree_map};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use nabedra_rt::fs::{Fs, FsFile, OpenMode};

use super::codec::{Codec, direction, length};
use super::mutant::mutant;
use super::rows::{Gen, Point, Slot};
use super::{Append, Config, Entry, Index, PinInner, Range, Service};
use crate::types::{CommitTs, Error, Key, PartId, Pos, RangeId, Result, Row, Value};

const MANIFEST: &str = "MANIFEST";
const MANIFEST_TMP: &str = "MANIFEST.tmp";
const SEGMENT_MAGIC: &[u8; 8] = b"NVXSEG01";
const MANIFEST_MAGIC: &[u8; 8] = b"NVXMAN01";
/// A segment is closed beyond this.
const SEGMENT_BYTES: u64 = 64 << 20;
/// A frame is closed beyond this, and holds at most one more row.
const FRAME_BYTES: usize = 1 << 20;
const MAX_FRAME: usize = 16 << 20;
const MAX_SEGMENT_BYTES: u64 = SEGMENT_BYTES + MAX_FRAME as u64 + 8;
const MAX_MANIFEST: u64 = 64 << 20;
const COPY_BYTES: usize = 1 << 20;

/// The directory of one index's checkpoints.
#[derive(Clone)]
pub struct Store {
    fs: Arc<dyn Fs>,
    dir: PathBuf,
}

/// What recovery found damaged. Empty ranges: the manifest, so every range.
#[derive(Clone, PartialEq, Debug)]
pub struct Damage {
    pub file: String,
    pub ranges: Vec<RangeId>,
    pub reason: String,
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
    dim: u32,
    ranges: Vec<Head>,
    files: Vec<FileInfo>,
}

#[derive(Clone)]
struct Segment {
    file: Arc<dyn FsFile>,
    info: FileInfo,
}

struct RestoredPart {
    centroid: Arc<[f32]>,
    id: Option<PartId>,
}

/// The running checkpoint.
#[derive(Clone)]
pub(super) struct Running {
    /// Holds removals back, like a scan's.
    _pin: Arc<PinInner>,
    heads: Vec<Head>,
    todo: Vec<PartId>,
    next: u64,
    segment: Option<Segment>,
    files: Vec<FileInfo>,
}

fn io_error(e: io::Error) -> Error {
    Error::Io(e.to_string())
}

fn damaged(why: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, why)
}

fn segment_name(number: u64) -> String {
    format!("seg-{number:016x}")
}

fn segment_after(number: u64) -> io::Result<u64> {
    number
        .checked_add(1)
        .ok_or_else(|| io::Error::other("checkpoint segment numbers exhausted"))
}

impl Store {
    pub fn new(fs: Arc<dyn Fs>, dir: impl Into<PathBuf>) -> Store {
        Store {
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

    fn segments(&self) -> io::Result<Vec<u64>> {
        let names = self.fs.list(&self.dir)?;
        let number = |p: &PathBuf| {
            p.file_name()?
                .to_str()?
                .strip_prefix("seg-")
                .map(str::to_owned)
        };
        Ok(names
            .iter()
            .filter_map(|p| u64::from_str_radix(&number(p)?, 16).ok())
            .collect())
    }

    /// A file named by the installed manifest remains reserved even if it is missing.
    fn next_segment(&self) -> io::Result<u64> {
        let existing = self.segments()?.into_iter().max();
        let installed = match self.load_manifest() {
            Ok(Some(manifest)) => manifest.files.into_iter().map(|file| file.number).max(),
            Ok(None) => None,
            Err(error) if error.kind() == io::ErrorKind::InvalidData => None,
            Err(error) => return Err(error),
        };
        existing
            .into_iter()
            .chain(installed)
            .max()
            .map_or(Ok(0), segment_after)
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

    /// Removes segments that the manifest does not name. Failing to only leaves garbage.
    fn clean(&self, manifest: &Manifest) {
        for number in self.segments().unwrap_or_default() {
            if manifest.files.iter().all(|f| f.number != number) {
                let _ = self.fs.remove(&self.path(&segment_name(number)));
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

    /// Calls `f` with every frame of a segment.
    fn frames(
        &self,
        info: &FileInfo,
        f: &mut dyn FnMut(&[u8]) -> io::Result<()>,
    ) -> io::Result<()> {
        let file = self
            .fs
            .open(&self.path(&segment_name(info.number)), OpenMode::Read)?;
        if info.len > MAX_SEGMENT_BYTES || file.len()? != info.len {
            return Err(damaged("a file's size is wrong"));
        }
        let mut magic = [0; 8];
        file.read_exact_at(&mut magic, 0)?;
        if &magic != SEGMENT_MAGIC {
            return Err(damaged("not a checkpoint file"));
        }
        let mut at = 8;
        while at < info.len {
            let mut head = [0; 8];
            file.read_exact_at(&mut head, at)?;
            let word =
                |i: usize| u32::from_le_bytes([head[i], head[i + 1], head[i + 2], head[i + 3]]);
            let (len, crc) = (word(0) as usize, word(4));
            let end = at + 8 + len as u64;
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
pub fn copy_checkpoint(from: &Store, to: &Store) -> Result<()> {
    let copy = || -> io::Result<()> {
        let mut manifest = from
            .load_manifest()?
            .ok_or_else(|| damaged("the peer has no checkpoint"))?;
        to.prepare()?;
        let mut next = to.next_segment()?;
        for info in &mut manifest.files {
            let source_name = segment_name(info.number);
            let source = from.fs.open(&from.path(&source_name), OpenMode::Read)?;
            if source.len()? != info.len {
                return Err(damaged("a peer file's size is wrong"));
            }
            info.number = next;
            next = segment_after(next)?;
            let target_name = segment_name(info.number);
            let target = to.fs.open(&to.path(&target_name), OpenMode::CreateNew)?;
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
    copy().map_err(io_error)
}

impl<C: Codec> Index<C> {
    /// `BeginCheckpoint`: pins a read point and the partitions. Every range that is not being
    /// installed takes part.
    pub fn begin_checkpoint(&mut self) -> Result<()> {
        let store = self
            .store
            .as_ref()
            .ok_or(Error::Internal("no checkpoint store"))?;
        if self.checkpoint.is_some() {
            return Err(Error::Busy);
        }
        store.prepare().map_err(io_error)?;
        let next = store.next_segment().map_err(io_error)?;
        let held = |r: &Range| matches!(r.service, Service::CatchingUp | Service::Serving);
        let heads: Vec<Head> = (self.shared.ranges.iter().filter(|(_, r)| held(r)))
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
            (
                h.generation,
                Point {
                    pos: h.low,
                    cut: CommitTs::MAX,
                },
            )
        };
        let pin = |id| PinInner {
            id,
            points: heads.iter().map(point).collect(),
            ranges: Vec::new(),
            roots: Vec::new(),
            group: None,
            failed: AtomicBool::new(false),
        };
        let holds = !mutant("prefix_checkpoint_without_epoch");
        self.checkpoint = Some(Running {
            _pin: self.shared.readers.begin(pin, holds),
            todo: self.listed().into_iter().collect(),
            heads,
            next,
            segment: None,
            files: Vec::new(),
        });
        Ok(())
    }

    /// `WriteFile`: writes one partition. Returns whether there is more to write. An error ends the
    /// checkpoint, and the installed one stays.
    pub fn write_file(&mut self) -> Result<bool> {
        let mut ck = self
            .checkpoint
            .take()
            .ok_or(Error::Internal("no checkpoint is running"))?;
        let Some(part) = ck.todo.pop() else {
            self.checkpoint = Some(ck);
            return Ok(false);
        };
        self.write_part(&mut ck, part).map_err(io_error)?;
        let more = !ck.todo.is_empty();
        self.checkpoint = Some(ck);
        Ok(more)
    }

    fn write_part(&self, ck: &mut Running, part: PartId) -> io::Result<()> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| damaged("no checkpoint store"))?;
        let Some(meta) = self.shared.dir.get(&part) else {
            // The mutants that free partitions early are caught by what is then missing.
            let early = mutant("prefix_checkpoint_without_epoch") || mutant("free_without_wait");
            let gone = damaged("a pinned partition is gone");
            return if early { Ok(()) } else { Err(gone) };
        };
        let mut header = Out::default();
        header.u32(part.0);
        header.bytes(&floats(&meta.centroid))?;
        let mut frame = Vec::new();
        let mut ranges = BTreeSet::new();
        let entries = self.shards[meta.owner]
            .parts
            .get(&part)
            .into_iter()
            .flat_map(|p| p.entries());
        for e in entries {
            let Some(head) = ck.heads.iter().find(|h| h.generation == e.generation) else {
                continue;
            };
            if self.encode_entry(head, e, &mut frame)? {
                ranges.insert(head.id);
            }
            if frame.len() >= FRAME_BYTES {
                ck.write_frame(store, &header.0, &frame, &mut ranges)?;
                frame.clear();
            }
        }
        if frame.is_empty() {
            return Ok(());
        }
        ck.write_frame(store, &header.0, &frame, &mut ranges)
    }

    /// Adds an entry if a cut that the checkpoint can serve sees it.
    fn encode_entry(&self, head: &Head, e: &Entry, out: &mut Vec<u8>) -> io::Result<bool> {
        let rows = &self.shared.rows;
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
        let vec = rows.vector(e.slot, e.at).map_err(|e| damaged(&e.to_string()))?;
        o.bytes(&floats(vec.as_deref().unwrap_or(&[])))?;
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
            .store
            .as_ref()
            .ok_or(Error::Internal("no checkpoint store"))?;
        for h in &mut ck.heads {
            if mutant("checkpoint_ahead_of_files") {
                h.pos = self.shared.ranges.get(&h.id).map_or(h.pos, |r| r.app);
            }
            if mutant("checkpoint_forgets_low") {
                (h.low, h.bound) = (0, 0);
            }
        }
        let install = |ck: &mut Running| -> io::Result<()> {
            ck.seal()?;
            if !mutant("manifest_before_sync") {
                store.fs.sync_dir(&store.dir)?;
            }
            let manifest = Manifest {
                dim: self.cfg.schema.dim as u32,
                ranges: ck.heads.clone(),
                files: ck.files.clone(),
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

    /// Starts from the installed checkpoint of `store`, or empty without one. Every recovered range
    /// resumes the tail at its position and serves after `caught_up`. A damaged file takes the ranges
    /// in it out of service.
    pub fn recover(cfg: Config, codec: C, store: Store) -> Result<(Self, Vec<Damage>)> {
        let mut index = Index::new(cfg, codec, Some(store.clone()))?;
        let damage = |file: &str, ranges: Vec<RangeId>, e: io::Error| Damage {
            file: file.to_owned(),
            ranges,
            reason: e.to_string(),
        };
        let manifest = match store.load_manifest() {
            Ok(None) => return Ok((index, Vec::new())),
            Ok(Some(m)) if m.dim as usize == index.cfg.schema.dim => m,
            Ok(Some(_)) => {
                let e = damaged("the checkpoint has other dimensions");
                return Ok((index, vec![damage(MANIFEST, Vec::new(), e)]));
            }
            Err(e) => return Ok((index, vec![damage(MANIFEST, Vec::new(), e)])),
        };
        for h in &manifest.ranges {
            let generation = index.new_generation(h.id);
            let range = Range {
                generation,
                service: Service::CatchingUp,
                app: h.pos,
                low: h.low,
                delivered: h.pos,
                outstanding: BTreeMap::new(),
                bound: h.bound,
                closed: h.closed,
                closes: Default::default(),
                touches: Default::default(),
                keys: BTreeMap::new(),
            };
            index.shared.ranges.insert(h.id, range);
        }
        let mut found = Vec::new();
        for info in &manifest.files {
            let mut parts = BTreeMap::new();
            if let Err(e) = store.frames(info, &mut |frame| index.load_frame(frame, &mut parts)) {
                let name = segment_name(info.number);
                found.push(damage(&name, info.ranges.iter().copied().collect(), e));
            }
        }
        for d in &found {
            for id in &d.ranges {
                index.fail_range(*id, &format!("{}: {}", d.file, d.reason));
            }
        }
        for r in index.shared.ranges.values_mut() {
            let touch = |&slot: &Slot| {
                let rows = index.shared.rows.superseding(slot, r.low);
                rows.map(move |(at, ts)| (at, ts, slot))
            };
            let mut touches: Vec<_> = r.keys.values().flat_map(touch).collect();
            touches.sort_unstable();
            r.touches = touches.into();
        }
        index.fail_damaged();
        Ok((index, found))
    }

    /// Puts the rows of a frame back. `parts` maps the partitions of its file to new ones.
    fn load_frame(
        &mut self,
        frame: &[u8],
        parts: &mut BTreeMap<u32, RestoredPart>,
    ) -> io::Result<()> {
        let mut r = In(frame);
        let old = r.u32()?;
        let centroid = r.floats(self.cfg.schema.dim)?;
        let undirected = centroid.is_empty();
        // Codes are made against the centroid and assert unit directions, so nothing read from a
        // file may reach them unchecked: a centroid is a unit vector, as `direction` makes it.
        let unit = |v: &[f32]| (length(v) - 1.0).abs() <= 1e-4;
        if !undirected && !unit(&centroid) {
            return Err(damaged("a centroid is not a unit vector"));
        }
        let restored = match parts.entry(old) {
            btree_map::Entry::Occupied(entry) if entry.get().centroid.as_ref() != centroid => {
                return Err(damaged("a partition has conflicting centroids"));
            }
            btree_map::Entry::Occupied(entry) => entry.into_mut(),
            btree_map::Entry::Vacant(entry) => entry.insert(RestoredPart {
                centroid: centroid.into(),
                id: None,
            }),
        };
        while !r.0.is_empty() {
            let id = RangeId(u128::from_le_bytes(r.array()?));
            let key: Key = r.bytes()?.into();
            let (at, ts) = (r.u64()?, r.u64()?);
            let (next_at, next_ts) = (r.u64()?, r.u64()?);
            let vec = r.floats(self.cfg.schema.dim)?;
            let n = r.u32()?;
            let cols = (0..n).map(|_| r.value()).collect::<io::Result<Vec<_>>>()?;
            let image = r.bytes()?.into();
            let row = Row {
                vec: Some(vec.into()),
                cols,
                image,
            };
            if !self.fits(&row) {
                return Err(damaged("a row does not fit the schema"));
            }
            // A directed partition holds only vectors with a direction (decision L3).
            if !undirected && row.vec.as_deref().and_then(direction).is_none() {
                return Err(damaged("an undirected vector in a directed partition"));
            }
            let sh = &mut self.shared;
            // A range that was dropped since is in the file and not in the manifest.
            let Some(range) = sh.ranges.get_mut(&id) else {
                continue;
            };
            let (generation, keys) = (range.generation, &mut range.keys);
            let group = super::group_of(sh.partition_by, &row);
            let version = (at, ts, Some(Arc::new(row)));
            let full = || damaged("too many rows");
            let slot = sh
                .rows
                .restore(keys, generation, &key, version)
                .ok_or_else(full)?;
            sh.rows
                .store_vector(slot, at)
                .map_err(|e| damaged(&e.to_string()))?;
            if next_at > 0 {
                sh.rows
                    .restore(keys, generation, &key, (next_at, next_ts, None));
            }
            if sh.rows.refs(slot, at) > 0 {
                continue;
            }
            let part = match (restored.id, undirected) {
                (Some(part), _) => part,
                (None, false) => sh
                    .new_part(group, Arc::clone(&restored.centroid), false)
                    .ok_or_else(full)?,
                (None, true) => match sh.roots.get(&group).and_then(|r| r.undirected) {
                    Some(part) => part,
                    None => sh.new_part(group, Arc::new([]), true).ok_or_else(full)?,
                },
            };
            restored.id = Some(part);
            sh.rows.name(slot, at);
            let a = Append {
                slot,
                at,
                generation,
            };
            let id = sh.new_id();
            let (sh, shard, codec) = self.owner(part);
            shard.put(sh, codec, part, Entry::new(a, id));
        }
        Ok(())
    }
}

impl Running {
    #[cfg(test)]
    pub(super) fn is_written(&self) -> bool {
        self.todo.is_empty()
    }

    fn write_frame(
        &mut self,
        store: &Store,
        header: &[u8],
        rows: &[u8],
        ranges: &mut BTreeSet<RangeId>,
    ) -> io::Result<()> {
        if self
            .segment
            .as_ref()
            .is_some_and(|s| s.info.len >= SEGMENT_BYTES)
        {
            self.seal()?;
        }
        let segment = match &mut self.segment {
            Some(s) => s,
            none => {
                let number = self.next;
                self.next = segment_after(number)?;
                let path = store.path(&segment_name(number));
                let file = store.fs.open(&path, OpenMode::CreateNew)?;
                file.write_all_at(SEGMENT_MAGIC, 0)?;
                let info = FileInfo {
                    number,
                    len: 8,
                    ranges: BTreeSet::new(),
                };
                none.insert(Segment { file, info })
            }
        };
        let payload = [header, rows].concat();
        let len = u32::try_from(payload.len()).map_err(|_| damaged("a frame is too large"))?;
        if payload.len() > MAX_FRAME {
            return Err(damaged("a frame is too large"));
        }
        let mut frame = Vec::with_capacity(payload.len() + 8);
        frame.extend(len.to_le_bytes());
        frame.extend(crc32c(&payload).to_le_bytes());
        frame.extend(payload);
        segment.file.write_all_at(&frame, segment.info.len)?;
        segment.info.len += frame.len() as u64;
        segment.info.ranges.append(ranges);
        Ok(())
    }

    /// Syncs the open segment and closes it.
    pub(super) fn seal(&mut self) -> io::Result<()> {
        if let Some(segment) = self.segment.take() {
            if !mutant("manifest_before_sync") {
                segment.file.sync_data()?;
            }
            self.files.push(segment.info);
        }
        Ok(())
    }
}

impl Manifest {
    fn encode(&self) -> Vec<u8> {
        let mut o = Out(MANIFEST_MAGIC.to_vec());
        o.u32(self.dim);
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
        let dim = r.u32()?;
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
        Ok(Manifest { dim, ranges, files })
    }
}

fn floats(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|x| x.to_le_bytes()).collect()
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
    /// A vector of `dim` values, or an empty one.
    fn floats(&mut self, dim: usize) -> io::Result<Vec<f32>> {
        let b = self.bytes()?;
        if b.len() != dim * 4 && !b.is_empty() {
            return Err(damaged("a vector has other dimensions"));
        }
        let float = |c: &[u8]| f32::from_le_bytes([c[0], c[1], c[2], c[3]]);
        Ok(b.chunks_exact(4).map(float).collect())
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
                Value::Tags((0..n).map(|_| self.text()).collect::<io::Result<_>>()?)
            }
            _ => return Err(damaged("unknown value")),
        })
    }
}

/// CRC32C (Castagnoli).
pub(super) fn crc32c(data: &[u8]) -> u32 {
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
impl Store {
    pub(super) fn files(&self) -> Vec<PathBuf> {
        self.fs.list(&self.dir).unwrap_or_default()
    }
}
