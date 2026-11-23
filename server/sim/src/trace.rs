//! The run trace: an FNV-1a hash over every traced line, a bounded tail for failure
//! reports, and optionally the full trace (`NABEDRA_SIM_TRACE=full`).
//!
//! A harness traces frame metadata, probes, nemesis actions and history events, each
//! prefixed with virtual time. It never traces payload bytes that may embed `ring` nonces.
//! Two runs of one seed must trace the same lines in the same order.

use std::collections::VecDeque;
use std::fmt;

/// Lines kept for a failure report.
pub const TRACE_TAIL: usize = 64;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a, 64 bit: a stable hash with no per-process key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fnv(u64);

impl Default for Fnv {
    fn default() -> Self {
        Self(FNV_OFFSET)
    }
}

impl Fnv {
    pub fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(FNV_PRIME);
        }
    }

    pub fn finish(&self) -> u64 {
        self.0
    }
}

/// FNV-1a of `bytes`.
pub fn fnv(bytes: &[u8]) -> u64 {
    let mut hash = Fnv::default();
    hash.write(bytes);
    hash.finish()
}

/// True when `NABEDRA_SIM_TRACE=full` asks for the full trace.
pub fn full_trace_requested() -> bool {
    std::env::var("NABEDRA_SIM_TRACE").is_ok_and(|value| value.trim() == "full")
}

/// The trace of one run.
#[derive(Clone, Debug)]
pub struct Trace {
    hash: Fnv,
    events: u64,
    tail: VecDeque<String>,
    tail_len: usize,
    full: Option<Vec<String>>,
}

impl Default for Trace {
    fn default() -> Self {
        Self::new()
    }
}

impl Trace {
    /// An empty trace keeping the last [`TRACE_TAIL`] lines, and every line when
    /// `NABEDRA_SIM_TRACE=full`.
    pub fn new() -> Self {
        Self::with_tail(TRACE_TAIL).keep_full(full_trace_requested())
    }

    /// An empty trace keeping the last `tail_len` lines.
    pub fn with_tail(tail_len: usize) -> Self {
        Self {
            hash: Fnv::default(),
            events: 0,
            tail: VecDeque::with_capacity(tail_len),
            tail_len,
            full: None,
        }
    }

    /// Keeps (or stops keeping) every line from now on.
    pub fn keep_full(mut self, keep: bool) -> Self {
        self.full = keep.then(Vec::new);
        self
    }

    /// Adds `"<elapsed_ms> <line>\n"` to the hash. Byte-compatible with the Events
    /// simulator's `World::trace`.
    pub fn record(&mut self, elapsed_ms: u64, line: fmt::Arguments<'_>) {
        self.push(format!("{elapsed_ms} {line}"));
    }

    /// Adds a line that already carries its time prefix.
    pub fn push(&mut self, line: String) {
        self.hash.write(line.as_bytes());
        self.hash.write(b"\n");
        self.events += 1;
        if let Some(full) = &mut self.full {
            full.push(line.clone());
        }
        if self.tail_len == 0 {
            return;
        }
        if self.tail.len() == self.tail_len {
            self.tail.pop_front();
        }
        self.tail.push_back(line);
    }

    pub fn hash(&self) -> u64 {
        self.hash.finish()
    }

    /// Lines traced so far.
    pub fn events(&self) -> u64 {
        self.events
    }

    /// The last lines, oldest first.
    pub fn tail(&self) -> Vec<String> {
        self.tail.iter().cloned().collect()
    }

    /// Every line, when the trace keeps them.
    pub fn full(&self) -> Option<&[String]> {
        self.full.as_deref()
    }
}
