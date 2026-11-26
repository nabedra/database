//! Budgets: what one query may spend, and the RAM that codes and extensions share.
//!
//! A [`Meter`] counts a query's work against its [`Budget`] and is checked before the work or the
//! allocation happens. Running out is an explicit [`Error::OverBudget`] that ends the query; the
//! scan never returns what it found so far. CPU is counted in work units (one f32 multiply-add of
//! an exact score is one unit, a code estimate a fraction of that), so a budget ends the same
//! query at the same point on every run and every machine.
//!
//! [`Memory`] is the worker's RAM for codes. 1-bit codes are always charged and always stay; the
//! 4-bit extensions take what is left, and go to NVMe when it is gone (plan section 5).

use std::sync::{Mutex, PoisonError};

use crate::types::{Error, Result};

/// What one query may spend. `u64::MAX` is no limit.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Budget {
    /// Work units.
    pub cpu: u64,
    /// Bytes read from the vector store.
    pub io: u64,
    /// Bytes the query holds at once.
    pub memory: u64,
}

impl Budget {
    pub const UNLIMITED: Budget = Budget {
        cpu: u64::MAX,
        io: u64::MAX,
        memory: u64::MAX,
    };
}

/// What a query spent so far.
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub struct Usage {
    pub cpu: u64,
    pub io: u64,
    /// Held now, and the most held at once.
    pub memory: u64,
    pub peak: u64,
}

#[derive(Clone, Debug)]
pub struct Meter {
    budget: Budget,
    used: Usage,
    /// The resource that ran out. Every later charge fails too.
    over: Option<&'static str>,
}

impl Meter {
    pub fn new(budget: Budget) -> Meter {
        Meter {
            budget,
            used: Usage::default(),
            over: None,
        }
    }

    pub fn used(&self) -> Usage {
        self.used
    }

    /// The resource that ran out, if one did.
    pub fn over(&self) -> Option<&'static str> {
        self.over
    }

    fn charge(&mut self, what: &'static str, add: u64) -> Result<()> {
        if let Some(what) = self.over {
            return Err(Error::OverBudget(what));
        }
        let (used, limit) = match what {
            "cpu" => (&mut self.used.cpu, self.budget.cpu),
            "io" => (&mut self.used.io, self.budget.io),
            _ => (&mut self.used.memory, self.budget.memory),
        };
        match used.checked_add(add).filter(|n| *n <= limit) {
            Some(n) => {
                *used = n;
                self.used.peak = self.used.peak.max(self.used.memory);
                Ok(())
            }
            None => {
                self.over = Some(what);
                Err(Error::OverBudget(what))
            }
        }
    }

    /// Before work of `units`.
    pub fn cpu(&mut self, units: u64) -> Result<()> {
        self.charge("cpu", units)
    }

    /// Before reading `bytes` from the store.
    pub fn io(&mut self, bytes: u64) -> Result<()> {
        self.charge("io", bytes)
    }

    /// Before holding `bytes` more.
    pub fn grow(&mut self, bytes: u64) -> Result<()> {
        self.charge("memory", bytes)
    }

    /// After letting `bytes` go.
    pub fn shrink(&mut self, bytes: u64) {
        self.used.memory = self.used.memory.saturating_sub(bytes);
    }
}

/// Where an extension went.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Tier {
    Ram,
    Nvme,
}

/// The RAM a worker gives to codes. Shared by its cores; every charge has one release.
#[derive(Debug)]
pub struct Memory {
    limit: u64,
    /// 1-bit codes, then extensions in RAM.
    held: Mutex<(u64, u64)>,
}

impl Memory {
    pub fn new(limit: u64) -> Memory {
        Memory {
            limit,
            held: Mutex::new((0, 0)),
        }
    }

    fn held(&self) -> std::sync::MutexGuard<'_, (u64, u64)> {
        self.held.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// Bytes of 1-bit codes and of extensions in RAM.
    pub fn used(&self) -> (u64, u64) {
        *self.held()
    }

    /// 1-bit codes always stay in RAM, also beyond the limit, which then leaves extensions none.
    pub fn charge_codes(&self, bytes: u64) {
        let mut h = self.held();
        h.0 = h.0.saturating_add(bytes);
    }

    pub fn release_codes(&self, bytes: u64) {
        let mut h = self.held();
        h.0 = h.0.saturating_sub(bytes);
    }

    /// Takes `bytes` for an extension if the limit leaves room.
    pub fn try_extension(&self, bytes: u64) -> bool {
        let mut h = self.held();
        let total = h.0.checked_add(h.1).and_then(|t| t.checked_add(bytes));
        if total.is_some_and(|t| t <= self.limit) {
            h.1 += bytes;
            true
        } else {
            false
        }
    }

    pub fn release_extension(&self, bytes: u64) {
        let mut h = self.held();
        h.1 = h.1.saturating_sub(bytes);
    }
}
