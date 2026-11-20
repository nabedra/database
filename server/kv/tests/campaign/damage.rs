//! Bit flips in synced bytes of a cleanly closed store (NS07 classes of plan section 3).
//!
//! After a clean close the journal ends with a `Pad` whose `durable_seq` covers every
//! earlier record, so damage to any record has evidence and can never pass as a torn tail.
//! Each trial flips one bit in a crash image of that store and reopens it.
//!
//! The oracle, for every class:
//! - open either succeeds or refuses with `Corruption` (R1, R3); a flip in a group-scoped
//!   journal payload must never refuse (G1);
//! - a read that returns a value returns the oracle's value: never a wrong value, never a
//!   silently missing key;
//! - a read that returns `Corruption` is in a quarantined scope (G1, G2, G3), or the store
//!   latched `RecoveryRequired` for store-wide damage (R5);
//! - a second open keeps the quarantine; `release_scope` lifts it and leaves the scope
//!   empty, and another reopen still never returns a wrong value.

use std::collections::BTreeSet;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use nabedra_kv::fs::{FaultFs, Fs};
use nabedra_kv::{Background, Error, MemtableBudget, Store};

use crate::common::{Rng, group_key, group_prefix, scope4};
use crate::harness::{Config, dir, options_for, release_batch};
use crate::oracle::{Model, scan, show};
use crate::workload::store_key;

#[derive(Clone, Debug, Default)]
pub struct DamageCounts {
    pub trials: u64,
    /// The flip changed nothing a reader could see (a flushed record, a free segment,
    /// unused space, an obsolete file).
    pub clean: u64,
    /// Open quarantined scopes (G1, G2 at open).
    pub quarantined_at_open: u64,
    /// Reads quarantined a scope (G2 blocks, G3 values).
    pub quarantined_at_read: u64,
    /// Open refused with `Corruption` (R1, R3, G2 spanning scopes).
    pub refused: u64,
    /// Store-wide damage at run time latched `RecoveryRequired` (R5).
    pub latched: u64,
    pub releases: u64,
    /// Flips by target: journal needle (group), journal needle (store-wide), journal
    /// byte, run byte, manifest byte, `CURRENT` byte.
    pub targets: [u64; 6],
}

impl DamageCounts {
    pub fn add(&mut self, o: &DamageCounts) {
        self.trials += o.trials;
        self.clean += o.clean;
        self.quarantined_at_open += o.quarantined_at_open;
        self.quarantined_at_read += o.quarantined_at_read;
        self.refused += o.refused;
        self.latched += o.latched;
        self.releases += o.releases;
        for i in 0..6 {
            self.targets[i] += o.targets[i];
        }
    }
}

fn files(fs: &FaultFs, d: &Path) -> Vec<PathBuf> {
    let mut v = fs.list(d).unwrap_or_default();
    v.sort();
    v
}

fn find(fs: &FaultFs, paths: &[PathBuf], needle: &[u8]) -> Vec<(PathBuf, u64)> {
    let mut out = Vec::new();
    for p in paths {
        let Some(data) = fs.durable_contents(p) else {
            continue;
        };
        for (i, w) in data.windows(needle.len()).enumerate() {
            if w == needle {
                out.push((p.clone(), i as u64));
            }
        }
    }
    out
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Target {
    GroupNeedle,
    StoreNeedle,
    JournalByte,
    RunByte,
    ManifestByte,
    CurrentByte,
}

/// Picks a target and flips one bit in `img`. Returns the target, the file, the offset
/// and, for a needle, the batch scope.
fn flip(
    rng: &mut Rng,
    img: &FaultFs,
    needles: &[(Vec<u8>, Vec<u8>)],
) -> Option<(Target, PathBuf, u64, Vec<u8>)> {
    let d = dir();
    let journal: Vec<PathBuf> = files(img, &d.join("journal"))
        .into_iter()
        .filter(|p| p.extension().is_some_and(|e| e == "jnl"))
        .collect();
    let runs = files(img, &d.join("runs"));
    let top = files(img, &d);
    let manifests: Vec<PathBuf> = top
        .iter()
        .filter(|p| {
            p.file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with("MANIFEST-"))
        })
        .cloned()
        .collect();
    let roll = rng.below(100);
    let (target, path, offset, scope) = if roll < 45 && !needles.is_empty() {
        let (needle, scope) = &needles[rng.below(needles.len() as u64) as usize];
        let hits = find(img, &journal, needle);
        if hits.is_empty() {
            return None;
        }
        let (path, at) = hits[rng.below(hits.len() as u64) as usize].clone();
        let t = if scope.is_empty() {
            Target::StoreNeedle
        } else {
            Target::GroupNeedle
        };
        (t, path, at + rng.below(needle.len() as u64), scope.clone())
    } else {
        let (t, pool) = match roll {
            0..=64 => (Target::RunByte, &runs),
            65..=84 => (Target::JournalByte, &journal),
            85..=94 => (Target::ManifestByte, &manifests),
            _ => (Target::CurrentByte, &vec![d.join("CURRENT")]),
        };
        if pool.is_empty() {
            return None;
        }
        let path = pool[rng.below(pool.len() as u64) as usize].clone();
        let len = img.durable_contents(&path)?.len() as u64;
        if len == 0 {
            return None;
        }
        // Journal segments are mostly zeros past the end; aim at the used part.
        let used = if t == Target::JournalByte {
            let data = img.durable_contents(&path)?;
            data.iter()
                .rposition(|&b| b != 0)
                .map_or(len, |i| i as u64 + 1)
        } else {
            len
        };
        (t, path, rng.below(used.max(1)), Vec::new())
    };
    img.flip_bit(&path, offset, rng.below(8) as u8).ok()?;
    Some((target, path, offset, scope))
}

/// The scopes the model can hold, as `(lower, upper)` key ranges with their scope bytes
/// (empty for store-wide keys).
fn scopes(groups: u64) -> Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    let mut v = Vec::new();
    for g in 0..groups {
        let p = group_prefix(g);
        let mut e = p.clone();
        *e.last_mut().unwrap() += 1;
        v.push((p.clone(), e, p));
    }
    v.push((b"s/".to_vec(), b"s0".to_vec(), Vec::new()));
    v
}

/// The read oracle on a store that may hold damage. Returns the number of scopes that
/// reads (not open) put into quarantine.
fn check_reads(store: &Store, cfg: &Config, model: &Model, ctx: &str) -> Result<u64, String> {
    let before: BTreeSet<Bytes> = store.quarantined().into_iter().map(|d| d.scope).collect();
    let mut corrupt: Vec<Vec<u8>> = Vec::new();
    let snap = store.snapshot();
    for (lo, hi, _) in scopes(cfg.mix.groups) {
        let want: Model = model
            .range::<Vec<u8>, _>((Bound::Included(&lo), Bound::Excluded(&hi)))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for reverse in [false, true] {
            match scan(&snap, Bound::Included(&lo), Bound::Excluded(&hi), reverse) {
                Ok(got) if got == want => {}
                Ok(got) => {
                    return Err(format!(
                        "{ctx}: scan of {} returned wrong data: {}",
                        show(&lo),
                        crate::oracle::first_difference(&got, &want)
                    ));
                }
                Err(Error::Corruption { .. }) => corrupt.push(lo.clone()),
                Err(e) => return Err(format!("{ctx}: scan of {}: {e}", show(&lo))),
            }
        }
    }
    let mut keys: Vec<Vec<u8>> = model.keys().cloned().collect();
    for g in 0..cfg.mix.groups {
        for k in (0..cfg.mix.keys).step_by(7) {
            keys.push(group_key(g, k));
        }
    }
    for k in (0..cfg.mix.keys).step_by(5) {
        keys.push(store_key(k));
    }
    for key in keys {
        match snap.get(&key) {
            Ok(got) => {
                if got.as_deref() != model.get(&key).map(Vec::as_slice) {
                    return Err(format!(
                        "{ctx}: get {} returned a wrong value ({:?} want {:?})",
                        show(&key),
                        got.map(|v| show(&v[..v.len().min(20)])),
                        model.get(&key).map(|v| show(&v[..v.len().min(20)]))
                    ));
                }
            }
            Err(Error::Corruption { .. }) => corrupt.push(key),
            Err(e) => return Err(format!("{ctx}: get {}: {e}", show(&key))),
        }
    }
    let after: BTreeSet<Bytes> = store.quarantined().into_iter().map(|d| d.scope).collect();
    for key in &corrupt {
        let n = scope4(key);
        if n == 0 {
            if !store.requires_recovery() {
                return Err(format!(
                    "{ctx}: store-wide key {} read Corruption without latching (R5)",
                    show(key)
                ));
            }
        } else if !after.contains(&key[..n]) {
            return Err(format!(
                "{ctx}: {} read Corruption but its scope is not quarantined ({after:?})",
                show(key)
            ));
        }
    }
    Ok(after.difference(&before).count() as u64)
}

/// `trials` bit flips on crash images of the cleanly closed `closed`.
pub fn trials(
    cfg: &Config,
    closed: &FaultFs,
    model: &Model,
    needles: &[(Vec<u8>, Vec<u8>)],
) -> Result<DamageCounts, String> {
    let mut counts = DamageCounts::default();
    let bg = Background::new(2).unwrap();
    let mut rng = Rng(cfg.seed ^ 0xD4_4A6E);
    let mut attempts = 0;
    while counts.trials < cfg.damage_trials as u64 && attempts < cfg.damage_trials * 4 {
        attempts += 1;
        let img = closed.crash();
        let Some((target, path, offset, scope)) = flip(&mut rng, &img, needles) else {
            continue;
        };
        counts.trials += 1;
        counts.targets[target as usize] += 1;
        let ctx = format!(
            "seed {} damage trial {}: {target:?} bit flip in {} at {offset}",
            cfg.seed,
            counts.trials,
            path.display()
        );
        let budget = Arc::new(MemtableBudget::new(8 << 20));
        let opts =
            |fs: &FaultFs| options_for(cfg, Arc::new(fs.clone()), bg.clone(), budget.clone());
        let opened = match Store::open(&dir(), opts(&img)) {
            Ok(o) => o,
            Err(Error::Corruption { .. }) if target != Target::GroupNeedle => {
                counts.refused += 1;
                continue;
            }
            Err(e) => return Err(format!("{ctx}: open failed: {e} ({e:?})")),
        };
        let at_open: BTreeSet<Bytes> = opened.quarantined.iter().map(|d| d.scope.clone()).collect();
        if target == Target::GroupNeedle && at_open.iter().any(|s| s.as_ref() != scope.as_slice()) {
            return Err(format!(
                "{ctx}: G1 must quarantine only {}, got {at_open:?}",
                show(&scope)
            ));
        }
        if at_open.iter().any(|s| s.is_empty()) {
            return Err(format!(
                "{ctx}: a store-wide scope was quarantined: {at_open:?}"
            ));
        }
        let store = opened.store;
        let by_reads = check_reads(&store, cfg, model, &ctx)?;
        let latched = store.requires_recovery();
        if !at_open.is_empty() {
            counts.quarantined_at_open += 1;
        }
        if by_reads > 0 {
            counts.quarantined_at_read += 1;
        }
        if latched {
            counts.latched += 1;
        }
        if at_open.is_empty() && by_reads == 0 && !latched {
            counts.clean += 1;
        }
        let quarantined: BTreeSet<Bytes> =
            store.quarantined().into_iter().map(|d| d.scope).collect();
        // A crash right after: the quarantine is durable (open persists it synchronously,
        // run-time damage before the read returns).
        img.halt();
        drop(store);
        let img2 = img.crash();
        let reopened = match Store::open(&dir(), opts(&img2)) {
            Ok(o) => o,
            Err(Error::Corruption { .. }) if latched => {
                counts.refused += 1;
                continue;
            }
            Err(e) => return Err(format!("{ctx}: second open failed: {e} ({e:?})")),
        };
        let again: BTreeSet<Bytes> = reopened
            .quarantined
            .iter()
            .map(|d| d.scope.clone())
            .collect();
        if !quarantined.is_subset(&again) {
            return Err(format!(
                "{ctx}: the second open lost quarantined scopes: had {quarantined:?}, now {again:?}"
            ));
        }
        let store = reopened.store;
        check_reads(&store, cfg, model, &format!("{ctx} (second open)"))?;
        if store.requires_recovery() {
            continue;
        }
        // Release every quarantined scope: it must come back empty and serve again.
        let mut released = model.clone();
        for s in store.quarantined().into_iter().map(|d| d.scope) {
            let g = std::str::from_utf8(&s[1..3])
                .ok()
                .and_then(|n| n.parse::<u64>().ok())
                .ok_or_else(|| format!("{ctx}: unexpected scope {}", show(&s)))?;
            store
                .release_scope(&s)
                .map_err(|e| format!("{ctx}: release_scope {}: {e}", show(&s)))?;
            counts.releases += 1;
            release_batch(g).apply(&mut released);
            if store.quarantined().iter().any(|d| d.scope == s) {
                return Err(format!(
                    "{ctx}: {} still quarantined after release",
                    show(&s)
                ));
            }
        }
        check_reads(&store, cfg, &released, &format!("{ctx} (after release)"))?;
        store
            .close()
            .map_err(|e| format!("{ctx}: close after release: {e}"))?;
        let third = Store::open(&dir(), opts(&img2))
            .map_err(|e| format!("{ctx}: open after release failed: {e}"))?;
        if matches!(target, Target::GroupNeedle) && !third.quarantined.is_empty() {
            return Err(format!(
                "{ctx}: G1 quarantine came back after release_scope: {:?}",
                third.quarantined
            ));
        }
        check_reads(
            &third.store,
            cfg,
            &released,
            &format!("{ctx} (reopen after release)"),
        )?;
    }
    Ok(counts)
}
