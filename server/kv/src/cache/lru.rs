//! A slab-backed LRU list with a hash index, shared by the block cache shards and the
//! descriptor cache. Every operation is O(1) apart from [`Lru::evict`], which walks at
//! most `max_steps` nodes from the cold end.

use std::collections::HashMap;
use std::hash::{BuildHasher, Hash};

const NIL: u32 = u32::MAX;

struct Node<K, V> {
    key: K,
    value: V,
    charge: usize,
    /// Towards the most recently used end.
    prev: u32,
    /// Towards the least recently used end.
    next: u32,
}

pub(crate) struct Lru<K, V, S> {
    map: HashMap<K, u32, S>,
    nodes: Vec<Option<Node<K, V>>>,
    free: Vec<u32>,
    /// Most recently used.
    head: u32,
    /// Least recently used.
    tail: u32,
    charge: usize,
}

impl<K: Copy + Eq + Hash, V, S: BuildHasher> Lru<K, V, S> {
    pub(crate) fn with_hasher(hasher: S) -> Self {
        Lru {
            map: HashMap::with_hasher(hasher),
            nodes: Vec::new(),
            free: Vec::new(),
            head: NIL,
            tail: NIL,
            charge: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    /// Sum of the charges of the resident entries.
    pub(crate) fn charge(&self) -> usize {
        self.charge
    }

    /// Looks `key` up and marks it most recently used.
    pub(crate) fn get(&mut self, key: &K) -> Option<&V> {
        let index = *self.map.get(key)?;
        self.touch(index);
        Some(&self.node(index).value)
    }

    /// Inserts `key`, replacing (and returning) an existing value. The entry becomes the
    /// most recently used one.
    pub(crate) fn insert(&mut self, key: K, value: V, charge: usize) -> Option<V> {
        let old = self.remove(&key);
        let node = Node {
            key,
            value,
            charge,
            prev: NIL,
            next: NIL,
        };
        let index = match self.free.pop() {
            Some(index) => {
                self.nodes[index as usize] = Some(node);
                index
            }
            None => {
                let index = u32::try_from(self.nodes.len()).expect("LRU larger than u32::MAX");
                self.nodes.push(Some(node));
                index
            }
        };
        self.push_front(index);
        self.map.insert(key, index);
        self.charge += charge;
        old
    }

    pub(crate) fn remove(&mut self, key: &K) -> Option<V> {
        let index = self.map.remove(key)?;
        self.unlink(index);
        let node = self.nodes[index as usize].take().expect("indexed LRU node");
        self.free.push(index);
        self.charge -= node.charge;
        Some(node.value)
    }

    /// Removes and returns the least recently used entry for which `evictable` holds,
    /// walking at most `max_steps` entries from the cold end. An entry it skips is moved
    /// to the hot end, so a later walk makes progress past it.
    pub(crate) fn evict(
        &mut self,
        max_steps: usize,
        mut evictable: impl FnMut(&V) -> bool,
    ) -> Option<(K, V, usize)> {
        for _ in 0..max_steps.min(self.map.len()) {
            let index = self.tail;
            if index == NIL {
                return None;
            }
            if evictable(&self.node(index).value) {
                let key = self.node(index).key;
                let charge = self.node(index).charge;
                let value = self.remove(&key).expect("tail is indexed");
                return Some((key, value, charge));
            }
            self.touch(index);
        }
        None
    }

    /// Every resident entry with its charge, in no particular order.
    pub(crate) fn entries(&self) -> impl Iterator<Item = (&K, &V, usize)> {
        self.nodes
            .iter()
            .flatten()
            .map(|node| (&node.key, &node.value, node.charge))
    }

    /// Keys from most to least recently used (tests only).
    #[cfg(test)]
    pub(crate) fn keys_mru(&self) -> Vec<K> {
        let mut out = Vec::new();
        let mut at = self.head;
        while at != NIL {
            out.push(self.node(at).key);
            at = self.node(at).next;
        }
        out
    }

    fn node(&self, index: u32) -> &Node<K, V> {
        self.nodes[index as usize].as_ref().expect("live LRU node")
    }

    fn node_mut(&mut self, index: u32) -> &mut Node<K, V> {
        self.nodes[index as usize].as_mut().expect("live LRU node")
    }

    fn touch(&mut self, index: u32) {
        if self.head != index {
            self.unlink(index);
            self.push_front(index);
        }
    }

    fn unlink(&mut self, index: u32) {
        let (prev, next) = {
            let node = self.node(index);
            (node.prev, node.next)
        };
        if prev == NIL {
            self.head = next;
        } else {
            self.node_mut(prev).next = next;
        }
        if next == NIL {
            self.tail = prev;
        } else {
            self.node_mut(next).prev = prev;
        }
        let node = self.node_mut(index);
        node.prev = NIL;
        node.next = NIL;
    }

    fn push_front(&mut self, index: u32) {
        let old_head = self.head;
        {
            let node = self.node_mut(index);
            node.prev = NIL;
            node.next = old_head;
        }
        if old_head == NIL {
            self.tail = index;
        } else {
            self.node_mut(old_head).prev = index;
        }
        self.head = index;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::RandomState;

    use super::*;

    #[test]
    fn lru_orders_touches_and_evicts_with_skips() {
        let mut lru: Lru<u32, u32, RandomState> = Lru::with_hasher(RandomState::new());
        for k in 0..5 {
            lru.insert(k, k * 10, 2);
        }
        assert_eq!(lru.charge(), 10);
        assert_eq!(lru.keys_mru(), vec![4, 3, 2, 1, 0]);
        assert_eq!(lru.get(&1), Some(&10));
        assert_eq!(lru.keys_mru(), vec![1, 4, 3, 2, 0]);
        // 0 is not evictable: it is skipped and moved to the hot end.
        assert_eq!(lru.evict(8, |v| *v != 0), Some((2, 20, 2)));
        assert_eq!(lru.keys_mru(), vec![0, 1, 4, 3]);
        assert_eq!(lru.evict(8, |_| false), None);
        assert_eq!(lru.insert(3, 33, 5), Some(30));
        assert_eq!(lru.charge(), 2 + 2 + 2 + 5);
        assert_eq!(lru.remove(&4), Some(40));
        assert_eq!(lru.remove(&4), None);
        assert_eq!(lru.len(), 3);
        // Slots are reused.
        lru.insert(9, 90, 1);
        assert_eq!(lru.nodes.len(), 5);
        assert_eq!(lru.entries().count(), 4);
    }
}
