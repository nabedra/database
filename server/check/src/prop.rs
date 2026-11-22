//! Property runner with choice-tape shrinking.
//!
//! [`check`] runs a property on `cases` generated inputs. Case `i` gets its own seed, derived from
//! a base seed, so any single case replays alone: `NABEDRA_SEED=<case seed> NABEDRA_CASES=1` runs
//! exactly that case first. On the first failure the runner shrinks the failing choice tape with
//! [`shrink`] and panics with a report that names the property, the seed, the case, the shrunk tape
//! and the failure message. A panic inside the property counts as a failure.

use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Once;

use crate::r#gen::Gen;
use crate::rng::{GAMMA, mix};

/// The base seed when neither `Config::seed` nor `NABEDRA_SEED` sets one ("nabedra!").
pub const DEFAULT_SEED: u64 = 0x6e61_6265_6472_6121;
/// Cases per property when `NABEDRA_CASES` is unset.
pub const DEFAULT_CASES: u64 = 256;
/// Property runs the shrinker may spend on one failure.
pub const DEFAULT_MAX_SHRINK_STEPS: u64 = 5000;

#[derive(Clone, Debug)]
pub struct Config {
    pub cases: u64,
    /// The base seed. Case 0 runs with exactly this seed; `None` derives one from the property name.
    pub seed: Option<u64>,
    /// Property runs the shrinker may spend; 0 disables shrinking.
    pub max_shrink_steps: u64,
}

impl Default for Config {
    /// Reads `NABEDRA_CASES` (default 256) and `NABEDRA_SEED` (decimal or `0x` hex).
    /// Panics on an unparsable value, so a typo never silently runs a different seed.
    fn default() -> Self {
        Self {
            cases: env_u64("NABEDRA_CASES").unwrap_or(DEFAULT_CASES),
            seed: env_u64("NABEDRA_SEED"),
            max_shrink_steps: DEFAULT_MAX_SHRINK_STEPS,
        }
    }
}

fn env_u64(var: &str) -> Option<u64> {
    let raw = std::env::var(var).ok()?;
    match parse_u64(&raw) {
        Some(v) => Some(v),
        None => panic!("{var}={raw:?} is not a u64 (decimal or 0x hex)"),
    }
}

/// Parses a decimal or `0x`-prefixed hexadecimal `u64`, ignoring surrounding spaces and `_`.
pub fn parse_u64(raw: &str) -> Option<u64> {
    let s: String = raw.trim().chars().filter(|c| *c != '_').collect();
    match s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        Some(hex) => u64::from_str_radix(hex, 16).ok(),
        None => s.parse().ok(),
    }
}

/// The seed of case `case` under base seed `base`. Case 0 uses `base` itself, which is what makes
/// the printed replay line work.
pub fn case_seed(base: u64, case: u64) -> u64 {
    if case == 0 { base } else { mix(base.wrapping_add(case.wrapping_mul(GAMMA))) }
}

fn name_seed(name: &str) -> u64 {
    // FNV-1a, so each property gets its own stream under the default seed.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in name.bytes() {
        h = (h ^ b as u64).wrapping_mul(0x0000_0100_0000_01b3);
    }
    mix(DEFAULT_SEED ^ h)
}

pub fn check(name: &str, f: impl Fn(&mut Gen) -> Result<(), String>) {
    check_with(Config::default(), name, f)
}

pub fn check_with(cfg: Config, name: &str, f: impl Fn(&mut Gen) -> Result<(), String>) {
    let base = cfg.seed.unwrap_or_else(|| name_seed(name));
    for case in 0..cfg.cases {
        let seed = case_seed(base, case);
        let mut g = Gen::from_seed(seed);
        let Err(message) = run(&f, &mut g) else { continue };
        let original = g.into_tape();
        let original_len = original.len();
        let s = shrink(original, message, cfg.max_shrink_steps, &f);
        panic!("{}", report(name, seed, case, cfg.cases, original_len, &s));
    }
}

fn report(name: &str, seed: u64, case: u64, cases: u64, original_len: usize, s: &Shrunk) -> String {
    const SHOWN: usize = 64;
    let mut shown: Vec<String> = s.tape.iter().take(SHOWN).map(|c| c.to_string()).collect();
    if s.tape.len() > SHOWN {
        shown.push("...".into());
    }
    format!(
        "property `{name}` failed\n  seed: {seed} ({seed:#x})\n  case: {case} of {cases}\n  \
         shrunk tape length: {} (from {original_len}, {} shrink runs): [{}]\n  failure: {}\n  \
         replay: NABEDRA_SEED={seed} NABEDRA_CASES=1",
        s.tape.len(),
        s.steps,
        shown.join(", "),
        s.message,
    )
}

// ---------------------------------------------------------------------------------------------
// Running one input, with panics caught and their default output suppressed.

thread_local! {
    static QUIET: Cell<bool> = const { Cell::new(false) };
    static LAST_PANIC: RefCell<Option<String>> = const { RefCell::new(None) };
}
static HOOK: Once = Once::new();

/// Wraps the current panic hook once per process. While a property runs on this thread, the hook
/// records the panic text instead of printing it, so shrinking does not flood the test output.
/// Other threads and other code keep the previous hook's behaviour.
fn install_hook() {
    HOOK.call_once(|| {
        let prev = panic::take_hook();
        panic::set_hook(Box::new(move |info| {
            if QUIET.with(Cell::get) {
                LAST_PANIC.with(|p| *p.borrow_mut() = Some(info.to_string()));
            } else {
                prev(info);
            }
        }));
    });
}

/// Runs the property once. `Err` holds its failure message, or `panic: <text>` if it panicked.
pub fn run(f: &impl Fn(&mut Gen) -> Result<(), String>, g: &mut Gen) -> Result<(), String> {
    install_hook();
    let was_quiet = QUIET.with(|q| q.replace(true));
    LAST_PANIC.with(|p| p.borrow_mut().take());
    let out = panic::catch_unwind(AssertUnwindSafe(|| f(g)));
    QUIET.with(|q| q.set(was_quiet));
    match out {
        Ok(r) => r,
        Err(payload) => {
            let text = LAST_PANIC.with(|p| p.borrow_mut().take()).unwrap_or_else(|| {
                payload
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "non-string panic payload".into())
            });
            Err(format!("panic: {text}"))
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Shrinking.

/// The result of [`shrink`]: the smallest failing tape found, its failure message, and the number
/// of property runs spent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Shrunk {
    pub tape: Vec<u64>,
    pub message: String,
    pub steps: u64,
}

/// Shortlex order: a shorter tape is simpler; at equal length, the lexicographically smaller one.
/// Every accepted mutation strictly decreases the tape in this order, so shrinking terminates.
pub fn simpler(a: &[u64], b: &[u64]) -> bool {
    match a.len().cmp(&b.len()) {
        Ordering::Less => true,
        Ordering::Greater => false,
        Ordering::Equal => a < b,
    }
}

struct Shrinker<'a, F> {
    f: &'a F,
    best: Vec<u64>,
    message: String,
    steps: u64,
    max_steps: u64,
}

impl<F: Fn(&mut Gen) -> Result<(), String>> Shrinker<'_, F> {
    fn exhausted(&self) -> bool {
        self.steps >= self.max_steps
    }

    /// Runs the property on `cand`. Keeps the consumed tape if the property still fails and the
    /// tape is simpler than the best so far.
    fn attempt(&mut self, cand: Vec<u64>) -> bool {
        if self.exhausted() || cand == self.best {
            return false;
        }
        self.steps += 1;
        let mut g = Gen::from_tape(cand);
        match run(self.f, &mut g) {
            Ok(()) => false,
            Err(message) => {
                let used = g.into_tape();
                if simpler(&used, &self.best) {
                    self.best = used;
                    self.message = message;
                    true
                } else {
                    false
                }
            }
        }
    }

    fn with(&self, i: usize, v: u64) -> Vec<u64> {
        let mut t = self.best.clone();
        t[i] = v;
        t
    }

    /// ddmin-style deletion of chunks of 8, 4, 2 and 1 choices. After a plain deletion fails it
    /// also tries the deletion with the choice just before the chunk decremented, which shortens
    /// a `Gen::vec` by its length choice and one element at once.
    fn delete_chunks(&mut self) {
        for k in [8usize, 4, 2, 1] {
            let mut i = 0;
            while i + k <= self.best.len() && !self.exhausted() {
                let mut cand = self.best.clone();
                cand.drain(i..i + k);
                if self.attempt(cand.clone()) {
                    continue;
                }
                if i > 0 && cand[i - 1] > 0 {
                    cand[i - 1] -= 1;
                    if self.attempt(cand) {
                        continue;
                    }
                }
                i += 1;
            }
        }
    }

    /// Per choice: replace it with 0; else halve it while that still fails; else decrement it,
    /// first by binary search between the passing half and the failing value, then by one.
    fn minimize_choices(&mut self) {
        let mut i = 0;
        while i < self.best.len() && !self.exhausted() {
            let v = self.best[i];
            if v == 0 || self.attempt(self.with(i, 0)) {
                i += 1;
                continue;
            }
            while i < self.best.len() && self.best[i] > 1 && self.attempt(self.with(i, self.best[i] / 2)) {}
            if i < self.best.len() && self.best[i] > 1 {
                // best[i] fails and best[i] / 2 did not: search (lo, hi] for the smallest failure.
                let (mut lo, mut hi) = (self.best[i] / 2, self.best[i]);
                while hi - lo > 1 && i < self.best.len() && self.best[i] == hi && !self.exhausted() {
                    let mid = lo + (hi - lo) / 2;
                    if self.attempt(self.with(i, mid)) { hi = mid } else { lo = mid }
                }
            }
            while i < self.best.len() && self.best[i] > 0 && self.attempt(self.with(i, self.best[i] - 1)) {}
            i += 1;
        }
    }

    /// Swaps adjacent choices that are out of order.
    fn sort_pairs(&mut self) {
        let mut i = 0;
        while i + 1 < self.best.len() && !self.exhausted() {
            if self.best[i] > self.best[i + 1] {
                let mut cand = self.best.clone();
                cand.swap(i, i + 1);
                self.attempt(cand);
            }
            i += 1;
        }
    }

    /// Moves the whole value of a choice into one of the next 8 choices, so a total spread over
    /// several elements can collect in one and the emptied ones can then be deleted.
    fn redistribute(&mut self) {
        let mut i = 0;
        while i < self.best.len() && !self.exhausted() {
            let mut j = i + 1;
            while j < self.best.len().min(i + 9) && self.best[i] > 0 && !self.exhausted() {
                let mut cand = self.best.clone();
                cand[j] = cand[j].saturating_add(cand[i]);
                cand[i] = 0;
                self.attempt(cand);
                j += 1;
            }
            i += 1;
        }
    }
}

/// Shrinks a failing tape. `tape` and `message` are a failure of `f` (the tape as consumed by
/// [`Gen::tape`]). Runs the passes to a fixed point or until `max_steps` property runs are spent.
pub fn shrink(
    tape: Vec<u64>,
    message: String,
    max_steps: u64,
    f: &impl Fn(&mut Gen) -> Result<(), String>,
) -> Shrunk {
    let mut s = Shrinker { f, best: tape, message, steps: 0, max_steps };
    loop {
        let before = s.best.clone();
        s.delete_chunks();
        s.minimize_choices();
        s.sort_pairs();
        s.redistribute();
        if s.best == before || s.exhausted() {
            break;
        }
    }
    Shrunk { tape: s.best, message: s.message, steps: s.steps }
}
