//! The queue of one core: priority classes and admission (plan section 7).
//!
//! `apply` has 20% of the core reserved: while it has work and got less than a fifth of the work
//! served lately, it goes next. Otherwise the first class with work goes, in the order
//! interactive, maintenance, batch, exact, apply. Nothing idles while there is work, so apply
//! also takes what the others leave. Work is counted in the cost a task declares.
//!
//! Admission happens before work starts: a class whose queue holds its limit of tasks or of
//! cost refuses more. A refused query is `Overloaded` (`429`); a refused apply is `Busy`, which
//! pauses the feed and never touches a write.

use std::collections::VecDeque;

use crate::types::{Error, Result};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Class {
    Apply,
    Interactive,
    Maintenance,
    Batch,
    Exact,
}

impl Class {
    pub const ALL: [Class; 5] = [
        Class::Apply,
        Class::Interactive,
        Class::Maintenance,
        Class::Batch,
        Class::Exact,
    ];
    /// The order in which classes go once apply had its share.
    const PRIORITY: [Class; 5] = [
        Class::Interactive,
        Class::Maintenance,
        Class::Batch,
        Class::Exact,
        Class::Apply,
    ];

    fn index(self) -> usize {
        self as usize
    }
}

/// The share of work reserved to apply, in percent.
pub const APPLY_SHARE: u64 = 20;
/// Served work is halved past this, so the share follows recent work.
const WINDOW: u64 = 1 << 20;

/// Per class: tasks and cost a queue may hold.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Limits {
    pub tasks: [usize; 5],
    pub cost: [u64; 5],
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            tasks: [4096, 1024, 256, 256, 64],
            cost: [u64::MAX / 8; 5],
        }
    }
}

pub struct Scheduler<T> {
    limits: Limits,
    queues: [VecDeque<(u64, T)>; 5],
    queued: [u64; 5],
    served: [u64; 5],
}

impl<T> Scheduler<T> {
    pub fn new(limits: Limits) -> Self {
        Scheduler {
            limits,
            queues: Default::default(),
            queued: [0; 5],
            served: [0; 5],
        }
    }

    /// Queues a task, or refuses it before it starts. A task that costs more than its class may
    /// ever hold is refused too.
    pub fn submit(&mut self, class: Class, cost: u64, task: T) -> Result<()> {
        let i = class.index();
        let cost_after = self.queued[i].checked_add(cost);
        let full = self.queues[i].len() >= self.limits.tasks[i]
            || cost_after.is_none_or(|c| c > self.limits.cost[i]);
        if full {
            return Err(match class {
                Class::Apply => Error::Busy,
                _ => Error::Overloaded,
            });
        }
        self.queued[i] = cost_after.unwrap_or(u64::MAX);
        self.queues[i].push_back((cost, task));
        Ok(())
    }

    /// The task to run next.
    pub fn pop(&mut self) -> Option<(Class, T)> {
        let apply = Class::Apply.index();
        let starved = !self.queues[apply].is_empty()
            && u128::from(self.served[apply]) * 100 < self.served_total() * u128::from(APPLY_SHARE);
        let class = if starved {
            Class::Apply
        } else {
            *Class::PRIORITY
                .iter()
                .find(|c| !self.queues[c.index()].is_empty())?
        };
        let i = class.index();
        let (cost, task) = self.queues[i].pop_front()?;
        self.queued[i] = self.queued[i].saturating_sub(cost);
        // At least one unit, so that free tasks still count toward the shares.
        self.served[i] = self.served[i].saturating_add(cost.max(1));
        if self.served_total() > u128::from(WINDOW) {
            self.served.iter_mut().for_each(|s| *s /= 2);
        }
        Some((class, task))
    }

    /// Work served lately. Each class saturates at `u64::MAX`, and their sum in `u128` cannot
    /// overflow, whatever costs tasks declare.
    fn served_total(&self) -> u128 {
        self.served.iter().map(|&s| u128::from(s)).sum()
    }

    pub fn len(&self) -> usize {
        self.queues.iter().map(VecDeque::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Queued cost per class.
    pub fn queued(&self) -> [u64; 5] {
        self.queued
    }

    /// Takes every queued task, for a shutdown.
    pub fn drain(&mut self) -> Vec<T> {
        self.queued = [0; 5];
        let all = self.queues.iter_mut().flat_map(|q| q.drain(..));
        all.map(|(_, t)| t).collect()
    }
}
