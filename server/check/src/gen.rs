//! Generators backed by a choice tape, in the style of Hypothesis.
//!
//! Every draw records one `u64` choice on the tape. A fresh generator takes its choices from a
//! seeded [`SplitMix64`]; a replaying generator reads them back from a fixed tape, and once that
//! tape is exhausted every draw returns its minimal value (0, `false`, `lo`, the first item, an
//! empty collection). The shrinker in [`crate::prop`] edits tapes, so every draw is written so
//! that a smaller choice gives a simpler value.

use crate::rng::SplitMix64;

#[derive(Clone, Debug)]
enum Source {
    Fresh(SplitMix64),
    Replay(Vec<u64>),
}

#[derive(Clone, Debug)]
pub struct Gen {
    seed: u64,
    source: Source,
    tape: Vec<u64>,
}

impl Gen {
    /// A fresh generator whose choices come from `SplitMix64::new(seed)`.
    pub fn from_seed(seed: u64) -> Self {
        Self {
            seed,
            source: Source::Fresh(SplitMix64::new(seed)),
            tape: Vec::new(),
        }
    }

    /// A generator with seed 0 that replays `tape`, then returns minimal values once exhausted.
    pub fn from_tape(tape: Vec<u64>) -> Self {
        Self::from_seeded_tape(0, tape)
    }

    pub(crate) fn from_seeded_tape(seed: u64, tape: Vec<u64>) -> Self {
        Self {
            seed,
            source: Source::Replay(tape),
            tape: Vec::new(),
        }
    }

    /// The case seed, or 0 for a generator constructed with `from_tape`.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// The choices consumed so far, including the minimal values read past the end of a replayed
    /// tape. Replaying this tape reproduces the same draws.
    pub fn tape(&self) -> &[u64] {
        &self.tape
    }

    /// Takes the consumed tape.
    pub fn into_tape(self) -> Vec<u64> {
        self.tape
    }

    /// One choice in `[0, n)`, or any `u64` when `n` is `None`.
    fn choice(&mut self, n: Option<u64>) -> u64 {
        let v = match &mut self.source {
            Source::Fresh(rng) => match n {
                None => rng.next_u64(),
                Some(n) => uniform_below(rng, n),
            },
            Source::Replay(tape) => {
                let raw = tape.get(self.tape.len()).copied().unwrap_or(0);
                match n {
                    None => raw,
                    Some(n) => raw % n,
                }
            }
        };
        self.tape.push(v);
        v
    }

    pub fn u64(&mut self) -> u64 {
        self.choice(None)
    }

    /// Uniform in `[0, n)`. Panics when `n` is 0.
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "Gen::below(0)");
        self.choice(Some(n))
    }

    /// Uniform in `[lo, hi]`, inclusive. Shrinks toward `lo`. Panics when `lo > hi`.
    pub fn range_i64(&mut self, lo: i64, hi: i64) -> i64 {
        assert!(lo <= hi, "Gen::range_i64({lo}, {hi})");
        let span = (hi as u64).wrapping_sub(lo as u64).wrapping_add(1);
        let off = if span == 0 {
            self.u64()
        } else {
            self.below(span)
        };
        (lo as u64).wrapping_add(off) as i64
    }

    /// Shrinks toward `false`.
    pub fn bool(&mut self) -> bool {
        self.below(2) == 1
    }

    /// True with probability `num / den`. Shrinks toward `false` (unless `num >= den`).
    pub fn chance(&mut self, num: u32, den: u32) -> bool {
        assert!(den > 0, "Gen::chance with den 0");
        let num = num.min(den) as u64;
        self.below(den as u64) >= den as u64 - num
    }

    /// A uniform item. Shrinks toward the first. Panics when `items` is empty.
    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        assert!(!items.is_empty(), "Gen::pick on an empty slice");
        &items[self.below(items.len() as u64) as usize]
    }

    pub fn bytes(&mut self, max_len: usize) -> Vec<u8> {
        self.vec(max_len, |g| g.below(256) as u8)
    }

    /// Printable ASCII (0x20 to 0x7e). Characters shrink toward `'a'`, then the rest in order.
    pub fn ascii(&mut self, max_len: usize) -> String {
        const ORDER: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ \
!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~";
        self.vec(max_len, |g| *g.pick(ORDER) as char)
            .into_iter()
            .collect()
    }

    /// Up to `max_len` items. The length is drawn first, so the shrinker shortens a vector by
    /// lowering that one choice and deleting element choices after it.
    pub fn vec<T>(&mut self, max_len: usize, mut f: impl FnMut(&mut Gen) -> T) -> Vec<T> {
        let len = match (max_len as u64).checked_add(1) {
            Some(span) => self.below(span),
            None => self.u64(),
        } as usize;
        (0..len).map(|_| f(self)).collect()
    }
}

/// Lemire's nearly divisionless method, made exact by rejection.
fn uniform_below(rng: &mut SplitMix64, n: u64) -> u64 {
    let threshold = n.wrapping_neg() % n;
    loop {
        let m = (rng.next_u64() as u128) * (n as u128);
        if (m as u64) >= threshold {
            return (m >> 64) as u64;
        }
    }
}
