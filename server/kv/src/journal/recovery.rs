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
//! starts. Then:
//!
//! - end marker, nothing valid after it, no evidence: **L1**, clean end;
//! - otherwise without evidence: **L2**, torn tail, the journal ends exactly before P;
//! - damaged with a group scope and evidence: **G1**, a [`Damage`] for that scope, and
//!   replay skips the record and every later record of an overlapping scope;
//! - any other case with evidence: **R1**, `Corruption`, unless the sequences that were
//!   lost are all at or below `replay_after` (their state is in runs) or there are none.
//!
//! After a clean close the last record is a `Pad` with `FLAG_CLOSE` and the log simply
//! continues after it. After a crash the end segment is never appended to again in the
//! same epoch, because a lost write may have left valid-looking records past P that the
//! boundary scan cannot all see: the end is fenced with a `FLAG_ROLL` pad and writing
//! continues in another segment, or, when the end segment holds no valid record, the
//! segment takes a new epoch. This replaces zeroing the tail (an interface deviation from
//! the plan's "zero the tail and sync the zeros" with the same exact-prefix result).

use bytes::Bytes;

use crate::batch::WriteBatch;
use crate::crc;
use crate::error::{Damage, DamageReason, Error, FileId, Result};
use crate::fs::FsFile;
use crate::types::{Locator, Seq};
use std::path::Path;
use std::sync::Arc;

use super::format::{
    self, BLOCK, FLAG_CLOSE, FLAG_ROLL, HeaderCheck, RECORD_HEADER_LEN, RecordHeader, RecordType,
};
use super::segment::{self, FreeSeg, LiveSeg};
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
    /// First segment of the replay range.
    pub start: usize,
    pub end: End,
    /// Sequence the next record takes.
    pub next_seq: Seq,
    /// Highest `durable_seq` seen in a valid record.
    pub max_durable: Seq,
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
        SegReader {
            file: seg.file.clone(),
            len: seg.len,
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

fn header_at(r: &mut SegReader, segment: u64, pos: u64, scope: &mut Vec<u8>) -> Result<At> {
    if pos + RECORD_HEADER_LEN as u64 > r.len {
        return Ok(At::End);
    }
    let avail = MAX_HEADER_AND_SCOPE.min(r.len - pos);
    let bytes = r.get(pos, avail)?.expect("within the segment");
    match format::check_header(bytes) {
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

fn evidence_scan(live: &[LiveSeg], idx: usize, from: u64) -> Result<Scan> {
    let mut scan = Scan::default();
    let mut scope = Vec::new();
    for (i, seg) in live.iter().enumerate().skip(idx) {
        let mut r = SegReader::new(seg);
        let mut pos = if i == idx { from } else { BLOCK };
        while pos + RECORD_HEADER_LEN as u64 <= seg.len {
            if let At::Valid(h) = header_at(&mut r, seg.number, pos, &mut scope)?
                && h.epoch == seg.epoch
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
    let inventory = segment::inventory(fs, dir, options.segment_bytes)?;
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
    };
    let live = inventory.live;
    if live.is_empty() {
        return Ok(Recovered {
            report,
            live,
            free: inventory.free,
            max_number: inventory.max_number,
            start: 0,
            end: End::Fresh,
            next_seq: replay_after + 1,
            max_durable: 0,
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
    let mut pos = BLOCK;
    let mut next_seq = live[start].first_seq;
    let mut max_durable = 0;
    let mut last_close = false;
    let mut quarantined: Vec<Vec<u8>> = Vec::new();
    let mut scope = Vec::new();
    let mut reader = SegReader::new(&live[idx]);
    let mut locators: Vec<Locator> = Vec::new();

    let end = loop {
        let seg = &live[idx];
        let non_valid = match header_at(&mut reader, seg.number, pos, &mut scope)? {
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
                    pos = BLOCK;
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
                            pos = BLOCK;
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
        if matches!(non_valid, NonValid::EndMarker) && last_close {
            // Clean close: nothing was written after the close pad.
            break End::Append { idx, pos };
        }
        let seq_p = next_seq;
        let resume = match &non_valid {
            NonValid::Damaged(h) => pos + h.record_len(),
            _ => format::align_up(pos + 1),
        };
        let scan = evidence_scan(&live, idx, resume)?;
        let fence = if pos == BLOCK {
            End::Reepoch { idx }
        } else {
            End::Fence {
                idx,
                pad_at: Some(pos),
            }
        };
        if !scan.max_durable.is_some_and(|d| d >= seq_p) {
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
                truncated_at_seq: seq_p,
                segment: seg.number,
                offset: pos,
                bytes_discarded,
            };
            report.tail_truncations = 1;
            report.tail_bytes = bytes_discarded;
            break fence;
        }

        // A completed sync covered P.
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

    Ok(Recovered {
        report,
        live,
        free: inventory.free,
        max_number: inventory.max_number,
        start,
        end,
        next_seq,
        max_durable,
    })
}
