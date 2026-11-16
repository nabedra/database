//! Disk key format 1. Prefixes delimit namespaces and table generations.
//! UTF-8 escaping preserves byte order; signed integers flip their sign bit.
use crate::protocol::{Key, Scalar};
use uuid::Uuid;

pub(crate) const MANIFEST: &[u8] = b"\x00m";
pub(crate) const ACTIVE: &[u8] = b"\x00g";

#[derive(Clone, Copy, Debug)]
pub(crate) struct Namespace(pub u64);
impl Namespace {
    pub(crate) fn prefix(self) -> Vec<u8> {
        let mut out = vec![1];
        out.extend_from_slice(&self.0.to_be_bytes());
        out
    }
    pub(crate) fn category(self, category: u8) -> Vec<u8> {
        let mut out = self.prefix();
        out.push(category);
        out
    }

    pub(crate) fn descriptor(self, id: Uuid) -> Vec<u8> {
        let mut out = self.category(b't');
        out.extend_from_slice(id.as_bytes());
        out
    }

    pub(crate) fn name(self, name: &str) -> Vec<u8> {
        let mut out = self.category(b'n');
        out.extend_from_slice(name.as_bytes());
        out
    }

    pub(crate) fn decision(self, id: Uuid) -> Vec<u8> {
        let mut out = self.category(b'd');
        out.extend_from_slice(id.as_bytes());
        out
    }

    pub(crate) fn rows(self, id: Uuid) -> Vec<u8> {
        let mut out = self.category(b'r');
        out.extend_from_slice(id.as_bytes());
        // Generation zero belongs to the standalone store. A distributed engine
        // requires a new manifest format and cannot silently reuse these keys.
        out.extend_from_slice(&0_u64.to_be_bytes());
        out
    }

    pub(crate) fn row(self, id: Uuid, key: &Key) -> Vec<u8> {
        let mut out = self.rows(id);
        match key {
            Key::Int64(value) => {
                out.push(1);
                out.extend_from_slice(&((*value as u64) ^ (1_u64 << 63)).to_be_bytes());
            }
            Key::Utf8(value) => {
                out.push(2);
                for byte in value.as_bytes() {
                    out.push(*byte);
                    if *byte == 0 {
                        out.push(255);
                    }
                }
                out.extend_from_slice(&[0, 0]);
            }
        }
        out
    }
}

pub(crate) fn end(prefix: &[u8]) -> Vec<u8> {
    let mut out = prefix.to_vec();
    for i in (0..out.len()).rev() {
        if out[i] != u8::MAX {
            out[i] += 1;
            out.truncate(i + 1);
            return out;
        }
    }
    unreachable!("all namespaces have a finite successor")
}

pub(crate) fn scalar_key(value: &Scalar) -> Key {
    match value {
        Scalar::Utf8(value) => Key::Utf8(value.clone()),
        Scalar::Int64(value) => Key::Int64(*value),
        _ => unreachable!("schema validation restricts primary keys"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn o03_integer_extrema_and_utf8_boundaries_keep_logical_order() {
        let id = Uuid::nil();
        let namespace = Namespace(0);
        let values = [i64::MIN, -1_000, -1, 0, 1, 255, i64::MAX];
        let encoded: Vec<_> = values
            .map(|value| namespace.row(id, &Key::Int64(value)))
            .into();
        assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
        let values = ["", "\0", "\0\0", "\0a", "a", "a\0", "aa", "é", "🦀"];
        let encoded: Vec<_> = values
            .map(|value| namespace.row(id, &Key::Utf8(value.into())))
            .into();
        assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(encoded.iter().all(|key| *key < end(&namespace.rows(id))));
    }
}
