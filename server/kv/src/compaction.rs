//! Flush and compaction output, and leveled compaction picking (task NS5).
//!
//! Both flush (memtable -> L0) and compaction (Ln -> Ln+1) stream sorted internal entries
//! through the same snapshot-aware filter into [`RunWriter`]s:
//!
//! - For each user key, versions are grouped into *stripes* by the live snapshots: the
//!   versions visible to the same set of readers. Only the newest version of each stripe
//!   is kept; the current state counts as a reader above every snapshot.
//! - A version hidden by a range tombstone of the same stripe is dropped.
//! - At the bottom (no older data below the output's key range), a point tombstone at or
//!   below the oldest snapshot is dropped, and so is every range tombstone at or below it.
//! - Range tombstones are written collapsed: one sequence per fragment and stripe.
//!
//! Output is split at `target_file_size`, only between user keys, and range tombstones
//! are clipped to each output's key range, so the runs of a level never overlap.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::fs::Fs;
use crate::iter::range_del::FragmentedRangeTombstones;
use crate::run::{RunMeta, RunWriter, RunWriterOptions, run_file_name};
use crate::source::{InternalIterator, RangeTombstone};
use crate::types::{Kind, MAX_SEQ, Seq, parse_internal_key};
use crate::version::{LevelOptions, NUM_LEVELS, RunFile, Version};

/// Everything an output pass needs besides its input.
pub(crate) struct OutputSpec<'a> {
    pub fs: &'a dyn Fs,
    pub runs_dir: &'a Path,
    pub writer: RunWriterOptions,
    /// Split outputs at about this size (`u64::MAX` for a flush).
    pub target_file_size: u64,
    pub bottommost: bool,
    /// Live snapshot sequences, ascending.
    pub snapshots: &'a [Seq],
    /// Gauge of run-writer memory (plan section 5 "Memory": pool overhead).
    pub memory: &'a AtomicUsize,
}

/// Charges the open run writer's memory to a gauge until dropped.
struct WriterCharge<'a> {
    gauge: &'a AtomicUsize,
    charged: usize,
}

impl WriterCharge<'_> {
    fn set(&mut self, bytes: usize) {
        if bytes > self.charged {
            self.gauge
                .fetch_add(bytes - self.charged, Ordering::Relaxed);
        } else {
            self.gauge
                .fetch_sub(self.charged - bytes, Ordering::Relaxed);
        }
        self.charged = bytes;
    }
}

impl Drop for WriterCharge<'_> {
    fn drop(&mut self) {
        self.gauge.fetch_sub(self.charged, Ordering::Relaxed);
    }
}

/// What an open run writer holds: its write buffer and data block, about 8 bytes per
/// entry for the bloom filter's key hashes, and one index entry (a key and a handle) per
/// data block.
fn writer_memory(writer: &RunWriter, options: &RunWriterOptions, key_len: usize) -> usize {
    let block = options.block_size.max(1);
    let blocks = writer.estimated_size() as usize / block + 1;
    options.write_buffer_bytes.min(1 << 20)
        + 2 * block
        + writer.num_entries() as usize * 8
        + blocks * (key_len + 24)
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct OutputStats {
    pub input_entries: u64,
    pub output_entries: u64,
    pub bytes_written: u64,
}

/// Decides which point entries survive.
struct Filter<'a> {
    snapshots: &'a [Seq],
    tombstones: FragmentedRangeTombstones,
    bottommost: bool,
    earliest: Seq,
    current: Vec<u8>,
    has_current: bool,
    last_stripe: usize,
}

impl<'a> Filter<'a> {
    fn new(snapshots: &'a [Seq], tombstones: &[RangeTombstone], bottommost: bool) -> Self {
        Filter {
            snapshots,
            tombstones: FragmentedRangeTombstones::new(tombstones, MAX_SEQ),
            bottommost,
            earliest: snapshots.first().copied().unwrap_or(MAX_SEQ),
            current: Vec::new(),
            has_current: false,
            last_stripe: usize::MAX,
        }
    }

    /// Stripe of a version at `seq`: the index of the oldest snapshot that sees it, or
    /// `snapshots.len()` for the current state only.
    fn stripe(&self, seq: Seq) -> usize {
        self.snapshots.partition_point(|&s| s < seq)
    }

    fn keep(&mut self, user_key: &[u8], seq: Seq, kind: Kind) -> bool {
        if !self.has_current || self.current != user_key {
            self.current.clear();
            self.current.extend_from_slice(user_key);
            self.has_current = true;
            self.last_stripe = usize::MAX;
        }
        let stripe = self.stripe(seq);
        if stripe == self.last_stripe {
            return false; // shadowed by a newer version every reader of it sees
        }
        self.last_stripe = stripe;
        let upper = self.snapshots.get(stripe).copied().unwrap_or(MAX_SEQ);
        if self.tombstones.covers(user_key, seq, upper) {
            return false; // a range tombstone of the same stripe hides it
        }
        !(kind == Kind::Delete && self.bottommost && seq <= self.earliest)
    }
}

/// The tombstones outputs carry: collapsed per stripe, and at the bottom without those
/// every reader already sees applied.
fn output_tombstones(
    tombstones: &[RangeTombstone],
    snapshots: &[Seq],
    bottommost: bool,
) -> Vec<RangeTombstone> {
    let earliest = snapshots.first().copied().unwrap_or(MAX_SEQ);
    FragmentedRangeTombstones::collapsed(tombstones, MAX_SEQ, snapshots)
        .tombstones()
        .filter(|t| !(bottommost && t.seq <= earliest))
        .collect()
}

struct Out {
    writer: RunWriter,
    /// First user key of this output (`None` for the first output: unbounded below).
    lo: Option<Bytes>,
}

/// Streams `input` (from its first entry) through the filter into new runs. `next_id`
/// allocates run ids. On error every file this call created is removed.
pub(crate) fn write_outputs(
    input: &mut dyn InternalIterator,
    tombstones: &[RangeTombstone],
    spec: &OutputSpec<'_>,
    next_id: &mut dyn FnMut() -> u64,
    stats: &mut OutputStats,
) -> Result<Vec<RunMeta>> {
    let mut created: Vec<u64> = Vec::new();
    let result = write_outputs_inner(input, tombstones, spec, next_id, stats, &mut created);
    if result.is_err() {
        for id in created {
            let _ = spec.fs.remove(&spec.runs_dir.join(run_file_name(id)));
        }
    }
    result
}

fn write_outputs_inner(
    input: &mut dyn InternalIterator,
    tombstones: &[RangeTombstone],
    spec: &OutputSpec<'_>,
    next_id: &mut dyn FnMut() -> u64,
    stats: &mut OutputStats,
    created: &mut Vec<u64>,
) -> Result<Vec<RunMeta>> {
    let mut filter = Filter::new(spec.snapshots, tombstones, spec.bottommost);
    let pending = output_tombstones(tombstones, spec.snapshots, spec.bottommost);
    let mut outputs = Vec::new();
    let mut out: Option<Out> = None;
    let mut last_user: Vec<u8> = Vec::new();
    let mut charge = WriterCharge {
        gauge: spec.memory,
        charged: 0,
    };

    let open = |lo: Option<Bytes>, created: &mut Vec<u64>, next_id: &mut dyn FnMut() -> u64| {
        let id = next_id();
        created.push(id);
        let writer = RunWriter::create(
            spec.fs,
            &spec.runs_dir.join(run_file_name(id)),
            id,
            spec.writer,
        )?;
        Ok::<_, Error>(Out { writer, lo })
    };

    input.seek_to_first();
    while input.valid() {
        let key = input.key();
        let parsed = parse_internal_key(key).ok_or(Error::Corruption {
            file: crate::error::FileId::Other,
            offset: 0,
            what: "bad internal key in compaction input",
        })?;
        stats.input_entries += 1;
        if filter.keep(parsed.user_key, parsed.seq, parsed.kind) {
            let new_user_key = parsed.user_key != last_user.as_slice();
            if let Some(o) = &out
                && new_user_key
                && o.writer.estimated_size() >= spec.target_file_size
            {
                let boundary = Bytes::copy_from_slice(parsed.user_key);
                let o = out.take().expect("checked");
                outputs.push(finish(o, Some(&boundary), &pending)?);
                out = Some(open(Some(boundary), created, next_id)?);
            }
            if out.is_none() {
                out = Some(open(None, created, next_id)?);
            }
            let o = out.as_mut().expect("opened");
            o.writer.add(key, &input.value())?;
            stats.output_entries += 1;
            if stats.output_entries % 256 == 1 {
                charge.set(writer_memory(&o.writer, &spec.writer, key.len()));
            }
            if new_user_key {
                last_user.clear();
                last_user.extend_from_slice(parsed.user_key);
            }
        }
        input.next();
    }
    input.status()?;
    if out.is_none() && !pending.is_empty() {
        out = Some(open(None, created, next_id)?);
    }
    if let Some(o) = out {
        outputs.push(finish(o, None, &pending)?);
    }
    stats.bytes_written += outputs.iter().map(|m| m.file_size).sum::<u64>();
    Ok(outputs)
}

/// Adds the tombstone pieces inside `[lo, hi)` and finishes the run.
fn finish(mut out: Out, hi: Option<&Bytes>, tombstones: &[RangeTombstone]) -> Result<RunMeta> {
    for t in tombstones {
        let start = match &out.lo {
            Some(lo) if t.start < *lo => lo.clone(),
            _ => t.start.clone(),
        };
        let end = match hi {
            Some(hi) if t.end > *hi => hi.clone(),
            _ => t.end.clone(),
        };
        if start < end {
            out.writer.add_range_tombstone(RangeTombstone {
                start,
                end,
                seq: t.seq,
            })?;
        }
    }
    if out.writer.num_entries() == 0 && tombstones.is_empty() {
        return Err(Error::InvalidArgument("empty compaction output"));
    }
    out.writer.finish()
}

/// A chosen compaction.
pub(crate) struct Pick {
    pub level: usize,
    pub output_level: usize,
    pub inputs: Vec<Arc<RunFile>>,
    /// Inputs from `output_level`.
    pub overlapping: Vec<Arc<RunFile>>,
}

impl Pick {
    pub fn all_inputs(&self) -> impl Iterator<Item = &Arc<RunFile>> {
        self.inputs.iter().chain(self.overlapping.iter())
    }

    /// A move without rewriting: one input and nothing to merge with.
    pub fn is_trivial_move(&self) -> bool {
        self.level > 0 && self.inputs.len() == 1 && self.overlapping.is_empty()
    }

    /// User-key range of every input (`hi` inclusive; exclusive ends are widened, which
    /// is conservative).
    pub fn range(&self) -> (Bytes, Bytes) {
        let lo = self
            .all_inputs()
            .map(|r| r.lo.clone())
            .min()
            .unwrap_or_default();
        let hi = self
            .all_inputs()
            .map(|r| r.hi.clone())
            .max()
            .unwrap_or_default();
        (lo, hi)
    }
}

fn combined_range(runs: &[Arc<RunFile>]) -> (Bytes, Bytes) {
    let lo = runs.iter().map(|r| r.lo.clone()).min().unwrap_or_default();
    let hi = runs.iter().map(|r| r.hi.clone()).max().unwrap_or_default();
    (lo, hi)
}

/// How urgently each level needs compaction: L0 by run count, deeper levels by bytes
/// over target. The best level with a score of at least 1, if any.
pub(crate) fn pick(version: &Version, options: &LevelOptions, cursor: &[Bytes]) -> Option<Pick> {
    let mut best: Option<(f64, usize)> = None;
    let l0 = version.l0_count() as f64 / options.l0_compaction_trigger.max(1) as f64;
    if l0 >= 1.0 {
        best = Some((l0, 0));
    }
    for level in 1..NUM_LEVELS - 1 {
        let score = version.level_bytes(level) as f64 / options.level_target(level) as f64;
        if score >= 1.0 && best.is_none_or(|(b, _)| score > b) {
            best = Some((score, level));
        }
    }
    let (_, level) = best?;
    let output_level = level + 1;
    let inputs: Vec<Arc<RunFile>> = if level == 0 {
        version.levels[0].clone()
    } else {
        // Round robin through the level by key.
        let runs = &version.levels[level];
        let after = cursor.get(level).cloned().unwrap_or_default();
        let next = runs
            .iter()
            .find(|r| r.lo > after)
            .or_else(|| runs.first())?;
        vec![next.clone()]
    };
    let (lo, hi) = combined_range(&inputs);
    let overlapping = version.overlapping(output_level, &lo, Some(&hi));
    Some(Pick {
        level,
        output_level,
        inputs,
        overlapping,
    })
}

/// True when no level below `output_level` may hold keys in `[lo, hi]`.
pub(crate) fn is_bottommost(version: &Version, output_level: usize, lo: &[u8], hi: &[u8]) -> bool {
    (output_level + 1..NUM_LEVELS).all(|level| version.overlapping(level, lo, Some(hi)).is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter_run(
        entries: &[(&[u8], Seq, Kind)],
        tombstones: &[RangeTombstone],
        snapshots: &[Seq],
        bottommost: bool,
    ) -> Vec<(Vec<u8>, Seq)> {
        let mut f = Filter::new(snapshots, tombstones, bottommost);
        entries
            .iter()
            .filter(|(k, s, kind)| f.keep(k, *s, *kind))
            .map(|(k, s, _)| (k.to_vec(), *s))
            .collect()
    }

    #[test]
    fn ns_unit_filter_keeps_one_version_per_snapshot_stripe() {
        let entries: &[(&[u8], Seq, Kind)] = &[
            (b"a", 9, Kind::Put),
            (b"a", 7, Kind::Put),
            (b"a", 5, Kind::Put),
            (b"a", 2, Kind::Put),
            (b"b", 3, Kind::Delete),
            (b"b", 1, Kind::Put),
        ];
        // No snapshots: only the newest of each key, and the bottom drops b's tombstone.
        assert_eq!(
            filter_run(entries, &[], &[], true),
            vec![(b"a".to_vec(), 9)]
        );
        assert_eq!(
            filter_run(entries, &[], &[], false),
            vec![(b"a".to_vec(), 9), (b"b".to_vec(), 3)]
        );
        // A snapshot at 5 keeps the version of a it sees. Every reader sees b deleted,
        // so at the bottom its tombstone and the put below it both go.
        assert_eq!(
            filter_run(entries, &[], &[5], true),
            vec![(b"a".to_vec(), 9), (b"a".to_vec(), 5)]
        );
        // A snapshot at 2 still sees b@1, so the tombstone must stay.
        assert_eq!(
            filter_run(entries, &[], &[2], true),
            vec![
                (b"a".to_vec(), 9),
                (b"a".to_vec(), 2),
                (b"b".to_vec(), 3),
                (b"b".to_vec(), 1)
            ]
        );
    }

    #[test]
    fn ns_unit_filter_drops_versions_a_same_stripe_tombstone_hides() {
        let t = |s: &'static [u8], e: &'static [u8], seq| RangeTombstone {
            start: Bytes::from_static(s),
            end: Bytes::from_static(e),
            seq,
        };
        let entries: &[(&[u8], Seq, Kind)] = &[(b"a", 4, Kind::Put), (b"c", 8, Kind::Put)];
        let tombstones = [t(b"a", b"d", 6)];
        assert_eq!(
            filter_run(entries, &tombstones, &[], false),
            vec![(b"c".to_vec(), 8)]
        );
        // A snapshot at 5 still sees a@4.
        assert_eq!(
            filter_run(entries, &tombstones, &[5], false),
            vec![(b"a".to_vec(), 4), (b"c".to_vec(), 8)]
        );
        assert!(output_tombstones(&tombstones, &[], true).is_empty());
        assert_eq!(output_tombstones(&tombstones, &[5], true).len(), 1);
    }
}
