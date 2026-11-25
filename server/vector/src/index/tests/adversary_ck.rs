//! Adversarial tests of checkpoints: every kind of damage at every offset, a manifest that is old,
//! from the future or forged, a peer copy over a live checkpoint, and every I/O error. The rule is:
//! never a wrong answer, always an explicit error or a correct recovery.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use nabedra_rt::fs::{Fault, FaultError, FaultFs, Fs, FsOp, OpenMode};

use super::super::checkpoint::crc32c;
use super::*;

const DIR: &str = "/data/ck";
const OTHER: RangeId = RangeId(8);

fn store(fs: &FaultFs) -> Store {
    Store::new(Arc::new(fs.clone()), DIR)
}

fn disk() -> FaultFs {
    let fs = FaultFs::new();
    fs.create_dir_all_durable(Path::new("/data"));
    fs
}

fn cfg() -> Config {
    Config {
        part_min: 2,
        part_max: 6,
        ..Config::new(schema(2, &[ColumnType::Tags], None), 2)
    }
}

fn change(range: RangeId, n: Pos) -> (u64, Option<Row>) {
    let k = (n * 5) % 8 + range.0 as u64 * 100;
    let angle = (n * 7) as f32;
    let tags = Value::Tags(Arc::new([Arc::from("t")]));
    let len = if n.is_multiple_of(9) { 0.0 } else { (n % 4 + 1) as f32 };
    let write = !n.is_multiple_of(6);
    (k, write.then(|| row(&[angle.cos() * len, angle.sin() * len], vec![tags], n)))
}

fn advance(ix: &mut Index, range: RangeId, to: Pos) {
    let after = ix.status(range).unwrap().delivered;
    let changes = (after + 1..=to).map(|n| change(range, n)).collect();
    feed(ix, range, &page(after, changes));
}

fn visible(range: RangeId, pos: Pos) -> Vec<u64> {
    let last = |k: u64| (1..=pos).rev().find(|n| change(range, *n).0 == k);
    let keys = (0..8).map(|k| k + range.0 as u64 * 100);
    let mut all: Vec<u64> = keys.filter_map(last).filter(|n| change(range, *n).1.is_some()).collect();
    all.sort_unstable();
    all
}

/// The answer at every retained cut is the feed's. Returns what is wrong, if anything.
fn wrong(ix: &mut Index, range: RangeId) -> Option<String> {
    let s = ix.status(range).unwrap();
    ix.caught_up(range, s.applied).unwrap();
    for pos in s.low..=s.applied {
        let pin = ix.pin(&[(range, ReadAt::Position(pos))], None).unwrap();
        let got = match ix.search(&pin, &query(Op::L2, &[0.3, 0.1], 100)) {
            Ok(hits) => hits,
            Err(e) => return Some(format!("range {range:?} at {pos}: {e}")),
        };
        let mut got = images(&got);
        got.sort_unstable();
        if got != visible(range, pos) {
            return Some(format!("range {range:?} at {pos}: {got:?}, the feed says {:?}", visible(range, pos)));
        }
    }
    None
}

/// An index with a checkpoint at 20 (low 12) and feed up to 30.
fn prepared(fs: &FaultFs) -> Index {
    let mut ix = serving(cfg(), ExactCodec, Some(store(fs)));
    ix.begin_load(OTHER, 0, 0).unwrap();
    ix.finish_load(OTHER).unwrap();
    ix.caught_up(OTHER, 0).unwrap();
    for range in [RANGE, OTHER] {
        advance(&mut ix, range, 20);
        ix.advance_low(range, 12).unwrap();
    }
    settle(&mut ix);
    ix.checkpoint().unwrap();
    for range in [RANGE, OTHER] {
        advance(&mut ix, range, 30);
        ix.advance_low(range, 25).unwrap();
    }
    settle(&mut ix);
    ix
}

#[derive(Debug, PartialEq)]
enum Verdict {
    /// The recovery failed or damaged something, explicitly.
    Explicit,
    /// Everything it holds is right.
    Right,
}

/// Recovers `fs` and checks that nothing is served wrong, also after the feed continues.
fn sound(fs: &FaultFs, why: &str) -> Verdict {
    let (mut ix, damage) = match Index::recover(cfg(), ExactCodec, store(fs)) {
        Ok(x) => x,
        Err(_) => return Verdict::Explicit,
    };
    for range in [RANGE, OTHER] {
        let Ok(s) = ix.status(range) else { continue };
        if s.damage.is_some() {
            assert!(!damage.is_empty(), "{why}: out of service without damage");
            continue;
        }
        if let Some(w) = wrong(&mut ix, range) {
            panic!("{why}: a wrong answer: {w}");
        }
        advance(&mut ix, range, 40);
        settle(&mut ix);
        if let Some(w) = wrong(&mut ix, range) {
            panic!("{why}: a wrong answer after the tail: {w}");
        }
    }
    if damage.is_empty() { Verdict::Right } else { Verdict::Explicit }
}

fn files(fs: &FaultFs) -> Vec<PathBuf> {
    let mut all = store(fs).files();
    all.sort();
    all
}

fn rewrite(fs: &FaultFs, path: &Path, bytes: &[u8]) {
    let f = fs.open(path, OpenMode::ReadWrite).unwrap();
    f.set_len(0).unwrap();
    f.write_all_at(bytes, 0).unwrap();
}

/// A checkpoint of two ranges, installed, on a disk image that has them.
fn installed() -> (FaultFs, Vec<PathBuf>) {
    let fs = disk();
    let mut ix = prepared(&fs);
    ix.checkpoint().unwrap();
    let all = files(&fs);
    (fs, all)
}

/// A truncated file is always damage, at every length.
#[test]
fn adv10_a_file_cut_at_any_length_is_found() {
    let (fs, all) = installed();
    for path in all {
        let bytes = fs.contents(&path).unwrap();
        let step = (bytes.len() / 500).max(1);
        for len in (0..bytes.len()).step_by(step).chain([bytes.len() - 1]) {
            let bad = fs.kill();
            rewrite(&bad, &path, &bytes[..len]);
            assert_eq!(sound(&bad, &format!("{path:?} cut to {len}")), Verdict::Explicit, "{path:?} cut to {len} of {}", bytes.len());
        }
    }
}

/// A byte that changes to any other value, and two bytes that swap, are always found.
#[test]
fn adv10_changed_and_swapped_bytes_are_found_at_every_offset() {
    let (fs, all) = installed();
    for path in all {
        let bytes = fs.contents(&path).unwrap();
        let step = (bytes.len() / 700).max(1);
        for at in (0..bytes.len()).step_by(step) {
            for value in [bytes[at] ^ 0xff, bytes[at].wrapping_add(1), 0] {
                if value == bytes[at] {
                    continue;
                }
                let mut bad = bytes.clone();
                bad[at] = value;
                let image = fs.kill();
                rewrite(&image, &path, &bad);
                assert_eq!(sound(&image, "changed"), Verdict::Explicit, "{path:?} byte {at} to {value}");
            }
            let other = (at * 7 + 13) % bytes.len();
            if bytes[at] != bytes[other] {
                let mut bad = bytes.clone();
                bad.swap(at, other);
                let image = fs.kill();
                rewrite(&image, &path, &bad);
                assert_eq!(sound(&image, "swapped"), Verdict::Explicit, "{path:?} bytes {at} and {other} swapped");
            }
        }
    }
}

/// A sector that reads back as zeros or ones, and bytes appended at the end, are found.
#[test]
fn adv10_a_wiped_sector_and_a_long_tail_are_found() {
    let (fs, all) = installed();
    for path in all {
        let bytes = fs.contents(&path).unwrap();
        for sector in (0..bytes.len()).step_by(512) {
            for fill in [0u8, 0xff] {
                let end = (sector + 512).min(bytes.len());
                if bytes[sector..end].iter().all(|b| *b == fill) {
                    continue;
                }
                let mut bad = bytes.clone();
                bad[sector..end].fill(fill);
                let image = fs.kill();
                rewrite(&image, &path, &bad);
                assert_eq!(sound(&image, "wiped"), Verdict::Explicit, "{path:?} sector {sector} filled with {fill}");
            }
        }
        for extra in [1usize, 8, 4096] {
            let mut bad = bytes.clone();
            bad.extend(std::iter::repeat_n(0u8, extra));
            let image = fs.kill();
            rewrite(&image, &path, &bad);
            assert_eq!(sound(&image, "appended"), Verdict::Explicit, "{path:?} with {extra} bytes appended");
        }
    }
}

/// Frames of a segment in another order are still a right checkpoint: frames are independent.
/// A frame that appears twice, or in another file, is not allowed to double or lose a row.
#[test]
fn adv10_frames_in_another_order_or_twice_never_give_a_wrong_answer() {
    let (fs, all) = installed();
    for path in all.iter().filter(|p| !p.ends_with("MANIFEST")) {
        let bytes = fs.contents(path).unwrap();
        let mut frames = Vec::new();
        let mut at = 8;
        while at < bytes.len() {
            let len = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
            frames.push(bytes[at..at + 8 + len].to_vec());
            at += 8 + len;
        }
        if frames.len() < 2 {
            continue;
        }
        let mut order: Vec<usize> = (0..frames.len()).collect();
        order.reverse();
        let mut reversed = bytes[..8].to_vec();
        for i in order {
            reversed.extend(&frames[i]);
        }
        let image = fs.kill();
        rewrite(&image, path, &reversed);
        // The same length: nothing tells the files apart, so the answers must be right.
        assert_eq!(sound(&image, "frames reversed"), Verdict::Right);
        // The first frame twice in place of the last: a row is in two frames.
        let mut twice = bytes[..8].to_vec();
        for i in 0..frames.len() {
            twice.extend(&frames[if i + 1 == frames.len() { 0 } else { i }]);
        }
        let image = fs.kill();
        rewrite(&image, path, &twice);
        sound(&image, "a frame twice");
    }
}

/// Files that swap their names are found or right, whatever their sizes.
#[test]
fn adv10_segments_under_each_others_names_never_give_a_wrong_answer() {
    let (fs, all) = installed();
    let segments: Vec<&PathBuf> = all.iter().filter(|p| !p.ends_with("MANIFEST")).collect();
    for a in &segments {
        for b in &segments {
            if a >= b {
                continue;
            }
            let (x, y) = (fs.contents(a).unwrap(), fs.contents(b).unwrap());
            let image = fs.kill();
            rewrite(&image, a, &y);
            rewrite(&image, b, &x);
            sound(&image, "swapped segments");
        }
    }
}

fn manifest_bytes(body: &[u8]) -> Vec<u8> {
    let mut bytes = body.to_vec();
    bytes.extend(crc32c(body).to_le_bytes());
    bytes
}

/// A manifest of another version, of the future, or naming files that are not there, is damage.
/// A valid manifest of an older checkpoint, whose files are gone, is damage too.
#[test]
fn adv10_a_manifest_of_another_time_is_never_trusted() {
    let fs = disk();
    let mut ix = prepared(&fs);
    let first = fs.contents(Path::new("/data/ck/MANIFEST")).unwrap();
    ix.checkpoint().unwrap();
    let second = fs.contents(Path::new("/data/ck/MANIFEST")).unwrap();
    assert_ne!(first, second);
    let path = Path::new("/data/ck/MANIFEST");
    // The older manifest over the newer files.
    let image = fs.kill();
    rewrite(&image, path, &first);
    assert_eq!(sound(&image, "older manifest"), Verdict::Explicit);
    // A future version: the magic says so, the checksum is right.
    for version in [b"NVXMAN02", b"NVXMAN00", b"NVXMAN99", b"nvxman01"] {
        let mut body = second[..second.len() - 4].to_vec();
        body[..8].copy_from_slice(version);
        let image = fs.kill();
        rewrite(&image, path, &manifest_bytes(&body));
        assert_eq!(sound(&image, "future manifest"), Verdict::Explicit, "{version:?}");
    }
    // A segment of a future version under a right manifest.
    for seg in files(&fs).iter().filter(|p| !p.ends_with("MANIFEST")) {
        let mut bytes = fs.contents(seg).unwrap();
        bytes[..8].copy_from_slice(b"NVXSEG02");
        let image = fs.kill();
        rewrite(&image, seg, &bytes);
        assert_eq!(sound(&image, "future segment"), Verdict::Explicit);
    }
    // Counts that promise more than the file holds.
    for at in [12usize, 116] {
        let mut body = second[..second.len() - 4].to_vec();
        body[at..at + 8].copy_from_slice(&u64::MAX.to_le_bytes());
        let image = fs.kill();
        rewrite(&image, path, &manifest_bytes(&body));
        assert_eq!(sound(&image, "huge counts"), Verdict::Explicit, "count at {at}");
    }
    // A missing manifest with its temporary file left behind: nothing is held, and nothing is wrong.
    let image = fs.kill();
    image.rename(path, Path::new("/data/ck/MANIFEST.tmp")).unwrap();
    let (ix, damage) = Index::recover(cfg(), ExactCodec, store(&image)).unwrap();
    assert!(damage.is_empty());
    assert!(ix.status(RANGE).is_err() && ix.status(OTHER).is_err());
    // An empty or tiny manifest.
    for len in [0usize, 1, 11, 12, 13] {
        let image = fs.kill();
        rewrite(&image, path, &second[..len]);
        assert_eq!(sound(&image, "tiny manifest"), Verdict::Explicit, "{len}");
    }
}

/// A copy from the peer over a checkpoint that is installed here, interrupted at every step: the
/// disk holds the old checkpoint, the new one, or says it is damaged. Never a mix that answers wrong.
#[test]
fn adv10_a_peer_copy_over_a_live_checkpoint_never_gives_a_wrong_answer() {
    let peer = disk();
    {
        let mut ix = prepared(&peer);
        for range in [RANGE, OTHER] {
            advance(&mut ix, range, 34);
            ix.advance_low(range, 28).unwrap();
        }
        settle(&mut ix);
        ix.checkpoint().unwrap();
    }
    let local = disk();
    prepared(&local).checkpoint().unwrap();
    let copy = |fs: &FaultFs| copy_checkpoint(&store(&peer), &store(fs));
    let whole = local.kill();
    copy(&whole).unwrap();
    let steps = whole.mutating_ops();
    let (mut old, mut new, mut explicit) = (0, 0, 0);
    for step in 0..=steps {
        let fs = local.kill();
        fs.crash_after(step);
        let result = copy(&fs);
        for after in [fs.crash(), fs.kill(), fs.power_loss(step), fs.power_loss(step + 99)] {
            match sound(&after, &format!("copy step {step} of {steps}, {result:?}")) {
                Verdict::Explicit => explicit += 1,
                Verdict::Right => {
                    let (ix, _) = Index::recover(cfg(), ExactCodec, store(&after)).unwrap();
                    match ix.status(RANGE).map(|s| s.applied) {
                        Ok(30) => old += 1,
                        Ok(34) => new += 1,
                        other => panic!("step {step}: a checkpoint at {other:?}"),
                    }
                }
            }
        }
    }
    println!("old {old}, new {new}, explicit {explicit}");
}

// ----- I/O errors.

const OPS: [FsOp; 11] = [
    FsOp::Open,
    FsOp::Write,
    FsOp::SyncData,
    FsOp::SyncDir,
    FsOp::Rename,
    FsOp::Remove,
    FsOp::Exists,
    FsOp::List,
    FsOp::Read,
    FsOp::Len,
    FsOp::CreateDirAll,
];

/// An error of any call, EIO or a full disk, while a checkpoint is written: the installed checkpoint
/// stays right, and the next checkpoint works once the disk frees up.
#[test]
fn adv10_every_error_of_every_call_of_a_checkpoint() {
    for error in [FaultError::Eio, FaultError::Enospc] {
        for op in OPS {
            for nth in 1..40 {
                let fs = disk();
                let mut ix = prepared(&fs);
                fs.inject(Fault::on(op).nth(nth).error(error));
                let result = ix.checkpoint();
                fs.clear_faults();
                sound(&fs.crash(), &format!("{op:?} {nth} {error:?}"));
                sound(&fs.kill(), &format!("{op:?} {nth} {error:?} kill"));
                if result.is_ok() {
                    break;
                }
                assert!(matches!(result, Err(Error::Io(_))), "{result:?}");
                if let Some(w) = wrong(&mut ix, RANGE) {
                    panic!("{op:?} {nth}: the index answers wrong after a failed checkpoint: {w}");
                }
                ix.checkpoint().unwrap_or_else(|e| panic!("{op:?} {nth} {error:?}: the next checkpoint: {e}"));
                assert_eq!(sound(&fs.crash(), "next"), Verdict::Right, "{op:?} {nth}");
            }
        }
    }
}

/// An error of any call while a checkpoint is read: explicit damage or an error, or right.
#[test]
fn adv10_every_error_of_every_call_of_a_recovery() {
    let (fs, _) = installed();
    for error in [FaultError::Eio, FaultError::Enospc] {
        for op in OPS {
            for nth in 1..30 {
                let image = fs.kill();
                image.inject(Fault::on(op).nth(nth).error(error));
                let got = Index::recover(cfg(), ExactCodec, store(&image));
                image.clear_faults();
                let Ok((mut ix, damage)) = got else { continue };
                for range in [RANGE, OTHER] {
                    let Ok(s) = ix.status(range) else { continue };
                    if s.damage.is_some() {
                        assert!(!damage.is_empty());
                        continue;
                    }
                    if let Some(w) = wrong(&mut ix, range) {
                        panic!("{op:?} {nth} {error:?}: a wrong answer: {w}");
                    }
                }
            }
        }
    }
}

/// An error of any call while a range drops or resyncs: the drop fails and the range stays, or it
/// succeeds and no crash brings the range back.
#[test]
fn adv10_every_error_of_every_call_of_a_drop_and_a_resync() {
    for error in [FaultError::Eio, FaultError::Enospc] {
        for op in OPS {
            for nth in 1..16 {
                for resync in [false, true] {
                    let fs = disk();
                    let mut ix = prepared(&fs);
                    fs.inject(Fault::on(op).nth(nth).error(error));
                    let result = if resync { ix.begin_load(RANGE, 0, 0) } else { ix.drop_range(RANGE) };
                    fs.clear_faults();
                    let why = format!("{op:?} {nth} {error:?} resync {resync}: {result:?}");
                    let gone = ix.status(RANGE).is_err() || resync && ix.status(RANGE).map(|s| s.applied) == Ok(0);
                    assert_eq!(result.is_ok(), gone, "{why}: the result and the memory disagree");
                    for after in [fs.crash(), fs.kill()] {
                        let (mut back, _) = Index::recover(cfg(), ExactCodec, store(&after)).unwrap();
                        if result.is_ok() {
                            assert!(back.status(RANGE).is_err(), "{why}: a dropped range came back");
                        }
                        if let Ok(s) = back.status(OTHER)
                            && s.damage.is_none()
                            && let Some(w) = wrong(&mut back, OTHER)
                        {
                            panic!("{why}: the other range answers wrong: {w}");
                        }
                    }
                    if result.is_err() {
                        // The range is still served by what it had.
                        if let Some(w) = wrong(&mut ix, RANGE) {
                            panic!("{why}: after a failed drop: {w}");
                        }
                    }
                }
            }
        }
    }
}
