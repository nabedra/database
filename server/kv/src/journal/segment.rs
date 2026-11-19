//! Segment files: naming, the inventory taken at open, creation, activation and retirement.
//!
//! A live segment is `<n>.jnl` whose header names segment `n`. A free segment is a file
//! kept for reuse: `<n>.free`, or a `<n>.jnl` whose header names another number (a reuse
//! interrupted between the rename and the header write). Reuse renames a free file to
//! the next segment number and writes a header with the next epoch, so records left from
//! an earlier use read as the end of the log. Retiring renames `<n>.jnl` to `<n>.free`.
//!
//! Every header rewrite writes and syncs copy A (block 0) before it writes copy B
//! (block 1), so one torn or destroyed 4 KiB unit never takes both copies.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::fs::{Fs, FsFile, OpenMode};
use crate::types::Seq;

use super::format::{BLOCK, HeaderCopy, RECORDS_START, SegmentHeader};

pub(super) const LIVE_EXT: &str = "jnl";
pub(super) const FREE_EXT: &str = "free";

/// Zero-fill chunk for new segment files.
const ZERO_CHUNK: usize = 1 << 20;

pub(super) fn path_of(dir: &Path, number: u64, ext: &str) -> PathBuf {
    dir.join(format!("{number:016}.{ext}"))
}

fn parse_name(path: &Path) -> Option<(u64, bool)> {
    let ext = path.extension()?.to_str()?;
    let number = path.file_stem()?.to_str()?.parse::<u64>().ok()?;
    match ext {
        LIVE_EXT => Some((number, true)),
        FREE_EXT => Some((number, false)),
        _ => None,
    }
}

#[derive(Clone)]
pub(super) struct LiveSeg {
    pub number: u64,
    pub epoch: u64,
    pub first_seq: Seq,
    pub file: Arc<dyn FsFile>,
    pub path: PathBuf,
    pub len: u64,
}

pub(super) struct FreeSeg {
    pub path: PathBuf,
    pub file: Arc<dyn FsFile>,
    /// Epoch of the file's last use; reuse takes `epoch + 1`.
    pub epoch: u64,
}

/// A live name (`<n>.jnl`) with no intact header copy. Recovery decides whether it is an
/// unused segment (L4) or damage (R3).
pub(super) struct Headerless {
    pub number: u64,
    pub path: PathBuf,
    pub file: Arc<dyn FsFile>,
    pub len: u64,
}

pub(super) struct Inventory {
    /// Sorted by number.
    pub live: Vec<LiveSeg>,
    pub free: Vec<FreeSeg>,
    /// Sorted by number.
    pub headerless: Vec<Headerless>,
    /// Highest number in any file name.
    pub max_number: u64,
}

pub(super) fn io(error: std::io::Error) -> Error {
    Error::from(error)
}

/// Lists and classifies the segment files of `dir` (plan section 3: L4 and R3 for headers).
pub(super) fn inventory(fs: &dyn Fs, dir: &Path, segment_bytes: u64) -> Result<Inventory> {
    let mut live = Vec::new();
    let mut free = Vec::new();
    let mut headerless = Vec::new();
    let mut max_number = 0;
    let mut removed = false;
    for path in fs.list(dir).map_err(io)? {
        let Some((number, is_live_name)) = parse_name(&path) else {
            continue;
        };
        max_number = max_number.max(number);
        let file = fs.open(&path, OpenMode::ReadWrite).map_err(io)?;
        let len = file.len().map_err(io)?;
        let mut block = vec![0u8; len.min(RECORDS_START) as usize];
        file.read_exact_at(&mut block, 0).map_err(io)?;
        let copies = SegmentHeader::decode_blocks(&block);
        for copy in copies {
            if let HeaderCopy::Unsupported(version) = copy {
                return Err(Error::UnsupportedFormat {
                    found: format!(
                        "journal segment {} has format version {version}",
                        path.display()
                    ),
                });
            }
        }
        let valid = || {
            copies.iter().filter_map(|c| match c {
                HeaderCopy::Valid(h) => Some(*h),
                _ => None,
            })
        };
        let named = valid()
            .filter(|h| is_live_name && h.segment == number)
            .max_by_key(|h| h.epoch);
        if let Some(header) = named {
            live.push(LiveSeg {
                number,
                epoch: header.epoch,
                first_seq: header.first_seq,
                file,
                path,
                len: len / BLOCK * BLOCK,
            });
        } else if let Some(header) = valid().max_by_key(|h| h.epoch) {
            if len == segment_bytes {
                free.push(FreeSeg {
                    path,
                    file,
                    epoch: header.epoch,
                });
            } else {
                // A free file of another size (an older option): drop it, new ones are created.
                drop(file);
                fs.remove(&path).map_err(io)?;
                removed = true;
            }
        } else if is_live_name {
            // No intact header copy on a live name. The protocol never produces this (a
            // live name only ever receives a valid header from a free file, and the two
            // copies are never rewritten together), so it is media damage. Recovery
            // refuses it (R3) unless it is the newest segment and holds no record.
            headerless.push(Headerless {
                number,
                path,
                file,
                len: len / BLOCK * BLOCK,
            });
        } else {
            // A free file whose creation never finished: L4.
            drop(file);
            fs.remove(&path).map_err(io)?;
            removed = true;
        }
    }
    if removed {
        fs.sync_dir(dir).map_err(io)?;
    }
    live.sort_by_key(|s| s.number);
    headerless.sort_by_key(|s| s.number);
    Ok(Inventory {
        live,
        free,
        headerless,
        max_number,
    })
}

/// Creates a new free segment file: preallocated, zero-filled when asked, with a valid
/// epoch-0 header, durable in its directory. `cancel` stops a zero fill early (the file
/// is then removed and `Io(Interrupted)` returned).
pub(super) fn create_free(
    fs: &dyn Fs,
    dir: &Path,
    number: u64,
    segment_bytes: u64,
    zero_fill: bool,
    cancel: Option<&std::sync::atomic::AtomicBool>,
) -> Result<FreeSeg> {
    let path = path_of(dir, number, FREE_EXT);
    let file = fs.open(&path, OpenMode::CreateNew).map_err(io)?;
    let result = (|| -> std::io::Result<()> {
        file.preallocate(segment_bytes)?;
        if file.len()? > segment_bytes {
            file.set_len(segment_bytes)?;
        }
        if zero_fill {
            let zeros = vec![0u8; ZERO_CHUNK];
            let mut at = RECORDS_START;
            while at < segment_bytes {
                if cancel.is_some_and(|c| c.load(std::sync::atomic::Ordering::Relaxed)) {
                    return Err(std::io::ErrorKind::Interrupted.into());
                }
                let n = ZERO_CHUNK.min((segment_bytes - at) as usize);
                file.write_all_at(&zeros[..n], at)?;
                at += n as u64;
            }
        }
        let header = SegmentHeader {
            segment: number,
            epoch: 0,
            first_seq: 0,
        };
        // A new file: nothing is synced yet, so both copies go in one write.
        file.write_all_at(&header.encode_blocks(), 0)?;
        file.sync_all()?;
        fs.sync_dir(dir)
    })();
    if let Err(error) = result {
        drop(file);
        let _ = fs.remove(&path);
        return Err(io(error));
    }
    Ok(FreeSeg {
        path,
        file,
        epoch: 0,
    })
}

/// Turns a free file into live segment `number` whose first record takes `first_seq`.
/// When this returns, the header is durable and records may be written.
pub(super) fn activate(
    fs: &dyn Fs,
    dir: &Path,
    free: FreeSeg,
    number: u64,
    first_seq: Seq,
) -> Result<LiveSeg> {
    let path = path_of(dir, number, LIVE_EXT);
    let epoch = free.epoch + 1;
    fs.rename(&free.path, &path).map_err(io)?;
    let header = SegmentHeader {
        segment: number,
        epoch,
        first_seq,
    };
    write_header(&*free.file, &header)?;
    fs.sync_dir(dir).map_err(io)?;
    let len = free.file.len().map_err(io)? / BLOCK * BLOCK;
    Ok(LiveSeg {
        number,
        epoch,
        first_seq,
        file: free.file,
        path,
        len,
    })
}

/// Gives a live segment that holds no valid record a new epoch, so anything a lost
/// write left in it reads as leftover data.
pub(super) fn reepoch(seg: &mut LiveSeg) -> Result<()> {
    let header = SegmentHeader {
        segment: seg.number,
        epoch: seg.epoch + 1,
        first_seq: seg.first_seq,
    };
    write_header(&*seg.file, &header)?;
    seg.epoch += 1;
    Ok(())
}

/// Rewrites both header copies of a file whose header blocks an earlier sync covered:
/// copy A, sync, copy B, sync. Whatever a power loss does to the block being written,
/// the other block holds a valid copy (the old one or the new one).
fn write_header(file: &dyn FsFile, header: &SegmentHeader) -> Result<()> {
    let block = header.encode_block();
    file.write_all_at(&block, 0).map_err(io)?;
    file.sync_data().map_err(io)?;
    file.write_all_at(&block, BLOCK).map_err(io)?;
    file.sync_data().map_err(io)
}

/// Moves a live segment to the free pool, or deletes it when its size differs from
/// `segment_bytes`. The caller syncs the directory.
pub(super) fn release(
    fs: &dyn Fs,
    dir: &Path,
    seg: LiveSeg,
    segment_bytes: u64,
) -> Result<Option<FreeSeg>> {
    let file_len = seg.file.len().map_err(io)?;
    if file_len != segment_bytes {
        drop(seg.file);
        fs.remove(&seg.path).map_err(io)?;
        return Ok(None);
    }
    let path = path_of(dir, seg.number, FREE_EXT);
    fs.rename(&seg.path, &path).map_err(io)?;
    Ok(Some(FreeSeg {
        path,
        file: seg.file,
        epoch: seg.epoch,
    }))
}
