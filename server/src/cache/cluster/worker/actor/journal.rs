//! A byte-budgeted FIFO of the actor's canonical effects, indexed by slot and epoch.
use std::collections::{HashMap, VecDeque};
use std::io;

use crate::cache::store::Effects;

type Key = (u16, u64, u64);

pub(super) struct Journal {
    order: VecDeque<(Key, usize)>,
    entries: HashMap<Key, Effects>,
    slots: usize,
    limit: usize,
    metadata: usize,
    charged: usize,
}

impl Journal {
    pub fn new(limit: usize) -> io::Result<Self> {
        // The fixed index and FIFO allocations are charged at startup, so
        // appending after the final lease check cannot need a reserve.
        let slots = (limit / 2048).clamp(1, 16_384);
        let mut order = VecDeque::new();
        let mut entries = HashMap::new();
        order.try_reserve_exact(slots).map_err(io::Error::other)?;
        entries.try_reserve(slots).map_err(io::Error::other)?;
        let metadata = slots * 768;
        Ok(Self {
            order,
            entries,
            slots,
            limit: limit - metadata,
            metadata,
            charged: 0,
        })
    }

    pub fn reserve_append(&mut self, bytes: usize) -> io::Result<()> {
        if bytes > self.limit {
            return Err(io::Error::other("cache journal effect exceeds capacity"));
        }
        // Hash-table tombstones can lower capacity after eviction. Refresh
        // the insertion reservation before linearizing the operation.
        self.entries.try_reserve(1).map_err(io::Error::other)
    }

    pub fn append(&mut self, slot: u16, epoch: u64, sequence: u64, effects: Effects, bytes: usize) {
        while self.charged + bytes > self.limit || self.order.len() == self.slots {
            let (key, cost) = self.order.pop_front().expect("journal reservation");
            self.entries.remove(&key);
            self.charged -= cost;
        }
        let key = (slot, epoch, sequence);
        self.order.push_back((key, bytes));
        self.entries.insert(key, effects);
        self.charged += bytes;
    }

    pub fn get(&self, slot: u16, epoch: u64, sequence: u64) -> Option<&Effects> {
        self.entries.get(&(slot, epoch, sequence))
    }

    pub fn charged_bytes(&self) -> usize {
        self.metadata + self.charged
    }

    pub fn clear(&mut self) {
        self.order.clear();
        self.entries.clear();
        self.charged = 0;
    }
}
