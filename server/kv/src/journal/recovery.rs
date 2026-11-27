//! The recovery scan (plan section 3, "Our rule", journal cases).
//!
//! Replay walks the records of the live segments in order, starting at the newest segment
//! whose first sequence is at or below `replay_after + 1`, and hands every record with a
//! sequence above `replay_after` to the sink. Each position is one of:
//!
//! - **valid**: header CRC, payload CRC, the segment's epoch and sequence continuity hold;
//! - **end marker**: an all-zero header or a record of an older epoch (leftover data);
//! - **damaged, scope known**: the header CRC holds, the payload CRC fails;
//! - **damaged, scope unknown**: a non-zero header whose CRC fails.
//!
//! A read error is returned as `Io` (R2) and is never taken for a torn write. A valid
//! header that breaks continuity, names an unknown kind or a future epoch, or overruns
//! the segment is structural damage (R3, `Corruption`).
//!
//! At the first position P that is not valid, with `seq(P)` the sequence P would take,
//! the scan reads the rest of the journal for **evidence**: a later valid record of its
//! segment's current epoch with `durable_seq >= seq(P)`. After a damaged header it
//! resumes at the next 4 KiB boundary, where every group commit that follows a sync
//! starts. The manifest certifies sequences through `replay_after`, so a fully covered
//! damaged record can be skipped and an unknown boundary may be crossed without overwriting
//! the covered prefix. Evidence beyond that prefix classifies the remaining unflushed gap.
//! Then:
//!
//! - end marker, nothing valid after it, no evidence: **L1**, clean end;
//! - otherwise without evidence: **L2**, torn tail, the journal ends exactly before P;
//! - damaged with a group scope and evidence: **G1**, a [`Damage`] for that scope, and
//!   replay skips the record and every later record of an overlapping scope;
//! - any other case with evidence: **R1**, `Corruption`, unless the sequences that were
//!   lost are all at or below `replay_after` (their state is in runs) or there are none.
//!
//! After a clean close the last record is a `Pad` with `FLAG_CLOSE`. The next open
//! appends after it, but first writes and syncs an *open pad* (a full-block pad without
//! `FLAG_CLOSE`): a record lost later in that run then follows a non-close record and
//! takes the crash path below, so a lost first record can never be overwritten in place
//! while a later record of the same run survives to chain onto the overwrite (RV-KV1 F1).
//! After a crash the end segment is never appended to again in the same epoch, because a
//! lost write may have left valid-looking records past P that the boundary scan cannot all
//! see: the end is fenced with a `FLAG_ROLL` pad and writing continues in another segment,
//! or, when the end segment holds no valid record, the segment takes a new epoch. This
//! replaces zeroing the tail (an interface deviation from the plan's "zero the tail and
//! sync the zeros" with the same exact-prefix result).
//!
//! A non-valid position P whose sequence is exactly where the next live segment starts is
//! that segment's roll point (a roll or fence pad, torn or damaged): no sequence is lost
//! and replay continues in the next segment, never in fenced leftovers after P.
//!
//! A live name with no intact header copy (both 4 KiB header blocks destroyed) is R3,
//! unless it is the newest segment and holds no record that could continue the log
//! (no header-valid record with a sequence at or after the recovered end): then it is an
//! unused segment (L4) and is deleted.

use bytes::Bytes;

use crate::batch::WriteBatch;
use crate::crc;
use crate::error::{Damage, DamageReason, Error, FileId, Result};
use crate::fs::FsFile;
use crate::mac::JournalKey;
use crate::types::{Locator, Seq};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::format::{
    self, FLAG_CLOSE, FLAG_ROLL, HeaderCheck, RECORD_HEADER_LEN, RECORDS_START, RecordHeader,
    RecordType,
};
use super::segment::{self, FreeSeg, Headerless, LiveSeg};
use super::{JournalOptions, JournalSink, RecordKind, RecoveryReport, TailOutcome};

/// Read-ahead of the recovery reader.
const CHUNK: u64 = 1 << 20;
const MAX_HEADER_AND_SCOPE: u64 = (RECORD_HEADER_LEN + 255) as u64;

/// Where the log ended and how writing resumes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum End {
    /// No segment exists yet.
    Fresh,
    /// Clean close: append at `pos` of segment `idx`.
    Append { idx: usize, pos: u64 },
    /// Segment `idx` holds no valid record: give it a new epoch and write from its start.
    Reepoch { idx: usize },
    /// Fence segment `idx` with a roll pad at `pad_at` (none when the chain already left
    /// it) and continue in a new segment.
    Fence { idx: usize, pad_at: Option<u64> },
}

impl End {
    pub(super) fn index(&self) -> Option<usize> {
        match *self {
            End::Fresh => None,
            End::Append { idx, .. } | End::Reepoch { idx } | End::Fence { idx, .. } => Some(idx),
        }
    }
}

pub(super) struct Recovered {
    pub report: RecoveryReport,
    pub live: Vec<LiveSeg>,
    pub free: Vec<FreeSeg>,
    pub max_number: u64,
    pub end: End,
    /// Sequence the next record takes.
    pub next_seq: Seq,
    /// Highest `durable_seq` seen in a valid record.
    pub max_durable: Seq,
    /// Unused segment files (L4) to delete after validating the recovered identities.
    pub discard: Vec<PathBuf>,
}

impl Recovered {
    /// Validate the scanned on-disk identities and the plan's preserved prefix together.
    /// A fresh segment cannot satisfy the manifest, and a re-epoched one loses its old identity.
    pub(super) fn validate_segments(&self, required: &[(u64, u64)]) -> Result<()> {
        let preserved = match self.end {
            End::Reepoch { idx } => idx,
            _ => self.end.index().map_or(0, |idx| idx + 1),
        };
        for &(segment, epoch) in required {
            if !self.live[..preserved]
                .iter()
                .any(|s| s.number == segment && s.epoch == epoch)
            {
                return Err(missing_segment(segment));
            }
        }
        Ok(())
    }
}

pub(super) fn missing_segment(segment: u64) -> Error {
    corrupt(
        segment,
        0,
        "a journal segment the manifest records is missing",
    )
}

fn corrupt(segment: u64, offset: u64, what: &'static str) -> Error {
    Error::Corruption {
        file: FileId::Journal { segment },
        offset,
        what,
    }
}

/// Buffered positioned reads over one segment.
struct SegReader {
    file: Arc<dyn FsFile>,
    len: u64,
    buf: Vec<u8>,
    start: u64,
}

impl SegReader {
    fn new(seg: &LiveSeg) -> SegReader {
        SegReader::over(seg.file.clone(), seg.len)
    }

    fn over(file: Arc<dyn FsFile>, len: u64) -> SegReader {
        SegReader {
            file,
            len,
            buf: Vec::new(),
            start: 0,
        }
    }

    /// `[off, off + n)`, or `None` when it extends past the segment.
    fn get(&mut self, off: u64, n: u64) -> Result<Option<&[u8]>> {
        if off + n > self.len {
            return Ok(None);
        }
        let end = self.start + self.buf.len() as u64;
        if off < self.start || off + n > end {
            let want = n.max(CHUNK).min(self.len - off) as usize;
            self.buf.resize(want, 0);
            self.file
                .read_exact_at(&mut self.buf, off)
                .map_err(segment::io)?;
            self.start = off;
        }
        let at = (off - self.start) as usize;
        Ok(Some(&self.buf[at..at + n as usize]))
    }
}

enum At {
    /// Fewer than a record header's bytes left in the segment.
    End,
    Zero,
    Bad,
    /// The header CRC holds; the scope was copied out.
    Valid(RecordHeader),
}

/// The key to check `seg`'s record headers with: the journal key for a segment in the
/// authenticated frame, none (the CRC frame) otherwise.
fn frame_key<'a>(seg: &LiveSeg, key: Option<&'a JournalKey>) -> Option<&'a JournalKey> {
    if seg.auth { key } else { None }
}

fn header_at(
    r: &mut SegReader,
    segment: u64,
    pos: u64,
    scope: &mut Vec<u8>,
    key: Option<&JournalKey>,
) -> Result<At> {
    if pos + RECORD_HEADER_LEN as u64 > r.len {
        return Ok(At::End);
    }
    let avail = MAX_HEADER_AND_SCOPE.min(r.len - pos);
    let bytes = r.get(pos, avail)?.expect("within the segment");
    match format::check_header(bytes, key) {
        HeaderCheck::Zero => Ok(At::Zero),
        HeaderCheck::Bad => Ok(At::Bad),
        HeaderCheck::Valid(h) => {
            scope.clear();
            scope.extend_from_slice(&bytes[RECORD_HEADER_LEN..][..usize::from(h.scope_len)]);
            if pos + h.record_len() > r.len {
                return Err(corrupt(segment, pos, "journal record overruns its segment"));
            }
            Ok(At::Valid(h))
        }
    }
}

fn payload_ok(r: &mut SegReader, pos: u64, h: &RecordHeader) -> Result<bool> {
    let payload = r
        .get(pos + h.payload_offset(), u64::from(h.payload_len))?
        .expect("checked by header_at");
    Ok(format::payload_crc_ok(h, payload))
}

/// What the evidence scan found after P.
#[derive(Default)]
struct Scan {
    max_durable: Option<Seq>,
    /// First valid current-epoch record: segment index, offset, first sequence.
    first_valid: Option<(usize, u64, Seq)>,
    valid_bytes: u64,
}

/// Only a record with `durable_seq < first_seq` counts, as evidence or as a resync point:
/// the journal never writes another (the last completed sync covers only earlier records),
/// so a header with a larger `durable_seq` is not one of its records. In the CRC frame
/// that rejects one class of forged headers (LSM-1, mitigation b); in the authenticated
/// frame client bytes cannot carry a valid header at all.
fn evidence_scan(
    live: &[LiveSeg],
    idx: usize,
    from: u64,
    key: Option<&JournalKey>,
) -> Result<Scan> {
    let mut scan = Scan::default();
    let mut scope = Vec::new();
    for (i, seg) in live.iter().enumerate().skip(idx) {
        let mut r = SegReader::new(seg);
        let mut pos = if i == idx { from } else { RECORDS_START };
        let seg_key = frame_key(seg, key);
        while pos + RECORD_HEADER_LEN as u64 <= seg.len {
            if let At::Valid(h) = header_at(&mut r, seg.number, pos, &mut scope, seg_key)?
                && h.epoch == seg.epoch
                && h.durable_seq < h.first_seq
                && h.record_type().is_some()
                && payload_ok(&mut r, pos, &h)?
            {
                scan.max_durable = Some(scan.max_durable.unwrap_or(0).max(h.durable_seq));
                scan.first_valid.get_or_insert((i, pos, h.first_seq));
                scan.valid_bytes += h.record_len();
                if h.is_pad() && h.flags & FLAG_ROLL != 0 {
                    break;
                }
                pos += h.record_len();
                continue;
            }
            pos = format::align_up(pos + 1);
        }
    }
    Ok(scan)
}

/// True when a record of `scope` belongs to the quarantined group `q`. Store-wide records
/// (empty scope) are always replayed.
fn overlaps(q: &[u8], scope: &[u8]) -> bool {
    !scope.is_empty() && (q.starts_with(scope) || scope.starts_with(q))
}

enum NonValid {
    EndMarker,
    DamagedUnknown,
    Damaged(RecordHeader),
}

pub(super) fn recover(
    fs: &dyn crate::fs::Fs,
    dir: &Path,
    options: &JournalOptions,
    replay_after: Seq,
    sink: &dyn JournalSink,
) -> Result<Recovered> {
    let key = options.auth_key.as_ref();
    let mut inventory = segment::inventory(fs, dir, options.segment_bytes, key)?;
    // A header-less live name below the newest valid segment sits inside the chain: R3.
    // At most one, the newest, may turn out to be an unused segment (checked at the end).
    let newest_valid = inventory.live.last().map(|s| s.number);
    let mut headerless = std::mem::take(&mut inventory.headerless);
    if let Some(h) = headerless
        .iter()
        .find(|h| newest_valid.is_some_and(|n| h.number < n))
        .or_else(|| headerless.iter().rev().nth(1))
    {
        return Err(corrupt(h.number, 0, "journal segment header damaged"));
    }
    let candidate = headerless.pop();
    let mut discard = inventory.discard;
    let mut report = RecoveryReport {
        tail: TailOutcome::Clean,
        damage: Vec::new(),
        replayed_records: 0,
        replayed_bytes: 0,
        skipped_records: 0,
        flushed_damage: 0,
        last_seq: 0,
        tail_truncations: 0,
        tail_bytes: 0,
        recovery_ms: 0,
        voided_at_open: 0,
    };
    let live = inventory.live;
    if live.is_empty() {
        let next_seq = replay_after + 1;
        discard.extend(check_headerless(candidate, next_seq, key)?);
        return Ok(Recovered {
            report,
            live,
            free: inventory.free,
            max_number: inventory.max_number,
            end: End::Fresh,
            next_seq,
            max_durable: 0,
            discard,
        });
    }
    let start = live
        .iter()
        .rposition(|s| s.first_seq <= replay_after + 1)
        .unwrap_or(0);
    if live[start].first_seq > replay_after + 1 {
        return Err(corrupt(
            live[start].number,
            0,
            "journal starts after the replay point",
        ));
    }

    let mut idx = start;
    let mut pos = RECORDS_START;
    let mut next_seq = live[start].first_seq;
    let mut max_durable = 0;
    let mut last_close = false;
    let mut quarantined: Vec<Vec<u8>> = Vec::new();
    let mut scope = Vec::new();
    let mut reader = SegReader::new(&live[idx]);
    let mut locators: Vec<Locator> = Vec::new();

    let end = loop {
        let seg = &live[idx];
        let non_valid = match header_at(
            &mut reader,
            seg.number,
            pos,
            &mut scope,
            frame_key(seg, key),
        )? {
            At::End => {
                // Fewer than a header's bytes left: the segment is full.
                if idx + 1 < live.len() {
                    idx += 1;
                    if live[idx].first_seq != next_seq {
                        return Err(corrupt(
                            live[idx].number,
                            0,
                            "journal segments do not chain",
                        ));
                    }
                    pos = RECORDS_START;
                    reader = SegReader::new(&live[idx]);
                    continue;
                }
                break End::Fence { idx, pad_at: None };
            }
            At::Valid(h) if h.epoch == seg.epoch => {
                if h.first_seq != next_seq {
                    return Err(corrupt(seg.number, pos, "journal sequence discontinuity"));
                }
                let Some(kind) = h.record_type() else {
                    return Err(corrupt(seg.number, pos, "unknown journal record kind"));
                };
                if kind != RecordType::Pad && quarantined.iter().any(|q| overlaps(q, &scope)) {
                    report.skipped_records += 1;
                    next_seq = h.end_seq();
                    pos += h.record_len();
                    last_close = false;
                    continue;
                }
                if !payload_ok(&mut reader, pos, &h)? {
                    NonValid::Damaged(h)
                } else {
                    max_durable = max_durable.max(h.durable_seq);
                    if kind == RecordType::Pad && h.flags & FLAG_ROLL != 0 {
                        if h.count != 0 {
                            return Err(corrupt(seg.number, pos, "roll pad takes sequences"));
                        }
                        if idx + 1 < live.len() {
                            idx += 1;
                            if live[idx].first_seq != next_seq {
                                return Err(corrupt(
                                    live[idx].number,
                                    0,
                                    "journal segments do not chain",
                                ));
                            }
                            pos = RECORDS_START;
                            reader = SegReader::new(&live[idx]);
                            last_close = false;
                            continue;
                        }
                        break End::Fence { idx, pad_at: None };
                    }
                    if kind != RecordType::Pad && h.count > 0 && h.end_seq() - 1 > replay_after {
                        let payload = reader
                            .get(pos + h.payload_offset(), u64::from(h.payload_len))?
                            .expect("checked by header_at")
                            .to_vec();
                        let payload_at = pos + h.payload_offset();
                        let mut batch = WriteBatch::from_encoded(
                            payload,
                            FileId::Journal {
                                segment: seg.number,
                            },
                            payload_at,
                        )?;
                        if batch.count() != h.count {
                            return Err(corrupt(seg.number, pos, "journal record count mismatch"));
                        }
                        batch.set_scope(&scope);
                        if batch.leaves_scope() {
                            return Err(corrupt(
                                seg.number,
                                pos,
                                "journal record leaves its scope",
                            ));
                        }
                        locators.clear();
                        let data = batch.data();
                        for jv in batch.journal_values() {
                            let value = &data[jv.offset as usize..][..jv.len as usize];
                            locators.push(Locator {
                                segment: seg.number,
                                epoch: seg.epoch,
                                offset: payload_at + u64::from(jv.offset),
                                len: jv.len,
                                crc32c: crc::crc32c(value),
                            });
                        }
                        let kind = if kind == RecordType::Relocate {
                            RecordKind::Relocate
                        } else {
                            RecordKind::Batch
                        };
                        sink.apply(kind, h.first_seq, &batch, &locators)?;
                        report.replayed_records += 1;
                        report.replayed_bytes += h.record_len();
                    }
                    next_seq = h.end_seq();
                    last_close = kind == RecordType::Pad && h.flags & FLAG_CLOSE != 0;
                    pos += h.record_len();
                    continue;
                }
            }
            At::Valid(h) if h.epoch > seg.epoch => {
                return Err(corrupt(
                    seg.number,
                    pos,
                    "journal record from a future epoch",
                ));
            }
            At::Valid(_) | At::Zero => NonValid::EndMarker,
            At::Bad => NonValid::DamagedUnknown,
        };

        // P = (idx, pos) is not a valid record.
        let seq_p = next_seq;
        let covered = seq_p <= replay_after;
        if matches!(non_valid, NonValid::EndMarker) && last_close && !covered {
            // Clean close, and the next open never wrote its open pad: nothing was
            // written after the close pad.
            break End::Append { idx, pos };
        }
        if !matches!(non_valid, NonValid::Damaged(_))
            && live
                .get(idx + 1)
                .is_some_and(|next| next.first_seq == seq_p)
        {
            // The next segment starts exactly at seq(P): P is this segment's roll or fence
            // pad (lost, torn or damaged), and whatever follows P here takes no sequence
            // the log keeps (at most fenced leftovers). No sequence is lost.
            idx += 1;
            pos = RECORDS_START;
            reader = SegReader::new(&live[idx]);
            last_close = false;
            continue;
        }
        let first_unflushed = replay_after + 1;
        if let NonValid::Damaged(h) = &non_valid
            && !h.is_pad()
            && h.end_seq() <= first_unflushed
        {
            // The flush certifies every sequence in this record. Preserve the bytes
            // so any locator whose own checksum still holds remains usable.
            report.flushed_damage += 1;
            next_seq = h.end_seq();
            pos += h.record_len();
            last_close = false;
            continue;
        }
        let resume = match &non_valid {
            NonValid::Damaged(h) => pos + h.record_len(),
            _ => format::align_up(pos + 1),
        };
        let scan = evidence_scan(&live, idx, resume, key)?;
        // A validated boundary inside the flushed prefix or at its end is safe to use
        // even when no later record carries a journal sync certificate.
        let flush_covers_gap = covered
            && !matches!(non_valid, NonValid::Damaged(_))
            && scan
                .first_valid
                .is_some_and(|(_, _, first_seq)| first_seq <= first_unflushed);
        let unflushed = seq_p.max(first_unflushed);
        let fence = if covered {
            // A locator into the skipped block may still be live. Preserve the segment
            // byte for byte. The next segment records the gap as a void pad before writes.
            End::Fence { idx, pad_at: None }
        } else if pos == RECORDS_START {
            End::Reepoch { idx }
        } else {
            End::Fence {
                idx,
                pad_at: Some(pos),
            }
        };
        if !flush_covers_gap && scan.max_durable.is_none_or(|d| d < unflushed) {
            if covered {
                report.flushed_damage += 1;
            }
            if matches!(non_valid, NonValid::EndMarker) && scan.first_valid.is_none() {
                // L1.
                break fence;
            }
            // L2: everything from P on came after the last completed sync.
            // Bytes the scan saw: the damaged record (up to the resume point when its
            // length is unknown) and every valid record after it.
            let damaged = match &non_valid {
                NonValid::Damaged(h) => h.record_len(),
                NonValid::DamagedUnknown => resume - pos,
                NonValid::EndMarker => 0,
            };
            let bytes_discarded = damaged + scan.valid_bytes;
            report.tail = TailOutcome::TornTail {
                truncated_at_seq: unflushed,
                segment: seg.number,
                offset: pos,
                bytes_discarded,
            };
            report.tail_truncations = 1;
            report.tail_bytes = bytes_discarded;
            break fence;
        }

        // A completed sync or the durable flush covered P.
        last_close = false;
        match non_valid {
            NonValid::Damaged(h) => {
                let no_state = h.is_pad() || h.count == 0 || h.end_seq() - 1 <= replay_after;
                if no_state {
                    report.flushed_damage += u64::from(!h.is_pad());
                } else if !scope.is_empty() {
                    // G1.
                    report.damage.push(Damage {
                        scope: Bytes::copy_from_slice(&scope),
                        reason: DamageReason::JournalRecord,
                        file: FileId::Journal {
                            segment: seg.number,
                        },
                        first_bad_seq: Some(h.first_seq),
                    });
                    quarantined.push(scope.clone());
                } else {
                    // R1.
                    return Err(corrupt(
                        seg.number,
                        pos,
                        "synced journal record damaged (store-wide scope)",
                    ));
                }
                next_seq = h.end_seq();
                pos += h.record_len();
            }
            NonValid::DamagedUnknown | NonValid::EndMarker => {
                let (fidx, fpos, first_seq) = scan.first_valid.expect("evidence is a valid record");
                if first_seq < seq_p {
                    return Err(corrupt(seg.number, pos, "journal sequence discontinuity"));
                }
                if first_seq != seq_p && first_seq - 1 > replay_after {
                    // R1.
                    return Err(corrupt(
                        seg.number,
                        pos,
                        "synced journal record damaged (scope unknown)",
                    ));
                }
                report.flushed_damage += u64::from(first_seq != seq_p);
                if fidx != idx {
                    reader = SegReader::new(&live[fidx]);
                }
                idx = fidx;
                pos = fpos;
                next_seq = first_seq;
            }
        }
    };

    discard.extend(check_headerless(candidate, next_seq, key)?);
    Ok(Recovered {
        report,
        live,
        free: inventory.free,
        max_number: inventory.max_number,
        end,
        next_seq,
        max_durable,
        discard,
    })
}

/// The newest live name has no intact header. It is unused (L4) when no record in it has
/// a valid header and a sequence at or after `next_seq`, the sequence the log continues
/// with: nothing then chains into it, and older records in it are leftovers of an earlier
/// use. Otherwise it may hold records the log needs: R3. Its frame is unknown without a
/// header, so in a store with a journal key a record checks in either frame.
fn check_headerless(
    candidate: Option<Headerless>,
    next_seq: Seq,
    key: Option<&JournalKey>,
) -> Result<Vec<PathBuf>> {
    let Some(h) = candidate else {
        return Ok(Vec::new());
    };
    let mut r = SegReader::over(h.file.clone(), h.len);
    let mut pos = RECORDS_START;
    while pos + RECORD_HEADER_LEN as u64 <= h.len {
        let avail = MAX_HEADER_AND_SCOPE.min(h.len - pos);
        let bytes = r.get(pos, avail)?.expect("within the segment");
        let check = match format::check_header(bytes, None) {
            HeaderCheck::Valid(header) => HeaderCheck::Valid(header),
            _ if key.is_some() => format::check_header(bytes, key),
            other => other,
        };
        if let HeaderCheck::Valid(header) = check
            && pos + header.record_len() <= h.len
        {
            if header.record_type().is_some() && header.end_seq() > next_seq {
                return Err(corrupt(h.number, 0, "journal segment header damaged"));
            }
            pos += header.record_len();
            continue;
        }
        pos = format::align_up(pos + 1);
    }
    Ok(vec![h.path])
}
