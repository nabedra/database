//! Allocation-free compact-Thrift preflight for this codec's flat schema.
//! Full metadata semantics remain the pinned Parquet reader's responsibility.
use super::{ProtocolError, limit, stored};

const MAX_DEPTH: usize = 32;

pub(super) fn validate(bytes: &[u8], columns: usize) -> Result<(), ProtocolError> {
    let mut scan = Scanner { bytes };
    let mut last = 0;
    let mut schema_seen = false;
    while let Some((id, ty)) = scan.field(&mut last)? {
        if id == 2 {
            if ty != 9 {
                return Err(stored());
            }
            let (element, count) = scan.list()?;
            if element != 12 || count != columns + 1 {
                return Err(stored());
            }
            for i in 0..count {
                scan.schema_element(i == 0, columns)?;
            }
            schema_seen = true;
        } else {
            // Known top-level fields must have their actual wire types. In
            // particular, the dependency reads version without checking type.
            let expected = match id {
                1 => 5,
                3 => 6,
                4 | 5 | 7 => 9,
                6 | 9 => 8,
                8 => 12,
                _ => ty,
            };
            if ty != expected {
                return Err(stored());
            }
            scan.skip(ty, 0, true)?;
        }
    }
    if !schema_seen || !scan.bytes.is_empty() {
        return Err(stored());
    }
    Ok(())
}

struct Scanner<'a> {
    bytes: &'a [u8],
}
impl Scanner<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8], ProtocolError> {
        let value = self.bytes.get(..n).ok_or_else(stored)?;
        self.bytes = &self.bytes[n..];
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8, ProtocolError> {
        Ok(self.take(1)?[0])
    }
    fn varint(&mut self) -> Result<u64, ProtocolError> {
        let mut value = 0;
        for shift in (0..70).step_by(7) {
            let b = self.byte()?;
            if shift == 63 && b > 1 {
                return Err(stored());
            }
            value |= u64::from(b & 127) << shift;
            if b & 128 == 0 {
                return Ok(value);
            }
        }
        Err(stored())
    }
    fn integer(&mut self) -> Result<i64, ProtocolError> {
        let n = self.varint()?;
        Ok((n >> 1) as i64 ^ -((n & 1) as i64))
    }
    fn i32(&mut self) -> Result<i32, ProtocolError> {
        i32::try_from(self.integer()?).map_err(|_| stored())
    }
    fn field(&mut self, last: &mut i16) -> Result<Option<(i16, u8)>, ProtocolError> {
        let b = self.byte()?;
        if b == 0 {
            return Ok(None);
        }
        let ty = b & 15;
        if !(1..=12).contains(&ty) {
            return Err(stored());
        }
        let delta = i16::from(b >> 4);
        let id = if delta == 0 {
            i16::try_from(self.integer()?).map_err(|_| stored())?
        } else {
            last.checked_add(delta).ok_or_else(stored)?
        };
        // This writer emits ordered unique fields. Reject duplicates to avoid
        // validating one schema while the reader uses a later declaration.
        if id <= *last {
            return Err(stored());
        }
        *last = id;
        Ok(Some((id, ty)))
    }
    fn count(&self, n: u64) -> Result<usize, ProtocolError> {
        let n = usize::try_from(n).map_err(|_| stored())?;
        if n > self.bytes.len() || n > i32::MAX as usize {
            return Err(stored());
        }
        Ok(n)
    }
    fn list(&mut self) -> Result<(u8, usize), ProtocolError> {
        let b = self.byte()?;
        let count = if b >> 4 == 15 {
            self.varint()?
        } else {
            u64::from(b >> 4)
        };
        let n = self.count(count)?;
        let ty = b & 15;
        if !(1..=12).contains(&ty) && !(ty == 0 && n == 0) {
            return Err(stored());
        }
        Ok((ty, n))
    }
    fn schema_element(&mut self, root: bool, columns: usize) -> Result<(), ProtocolError> {
        let mut last = 0;
        let (mut physical, mut repetition, mut children, mut name) = (None, None, None, false);
        while let Some((id, ty)) = self.field(&mut last)? {
            match id {
                1 | 3 | 5 => {
                    if ty != 5 {
                        return Err(stored());
                    }
                    let n = self.i32()?;
                    match id {
                        1 => physical = Some(n),
                        3 => repetition = Some(n),
                        _ => children = Some(n),
                    }
                }
                4 => {
                    if ty != 8 {
                        return Err(stored());
                    }
                    self.skip(ty, 0, true)?;
                    name = true;
                }
                _ => {
                    if (matches!(id, 2 | 6..=9) && ty != 5) || (id == 10 && ty != 12) {
                        return Err(stored());
                    }
                    self.skip(ty, 0, true)?;
                }
            }
        }
        let valid = if root {
            physical.is_none() && children == Some(columns as i32)
        } else {
            matches!(physical, Some(0 | 2 | 5 | 6))
                && repetition == Some(0)
                && matches!(children, None | Some(0))
        };
        if !name || !valid {
            return Err(stored());
        }
        Ok(())
    }
    fn skip(&mut self, ty: u8, depth: usize, field: bool) -> Result<(), ProtocolError> {
        if depth > MAX_DEPTH {
            return Err(limit());
        }
        match ty {
            1 | 2 => {
                if !field {
                    // Parquet 60.1.0's generic skip consumes no bytes for
                    // collection booleans. Reject them so both parsers reach
                    // the same later fields, including schema child counts.
                    return Err(stored());
                }
            }
            3 => {
                self.take(1)?;
            }
            4 => {
                i16::try_from(self.integer()?).map_err(|_| stored())?;
            }
            5 => {
                self.i32()?;
            }
            6 => {
                self.integer()?;
            }
            7 => {
                self.take(8)?;
            }
            8 => {
                let n = self.varint()?;
                let n = self.count(n)?;
                self.take(n)?;
            }
            9 | 10 => {
                let (element, count) = self.list()?;
                for _ in 0..count {
                    self.skip(element, depth + 1, false)?;
                }
            }
            11 => {
                let n = self.varint()?;
                let count = self.count(n)?;
                if count > 0 {
                    let types = self.byte()?;
                    if count > self.bytes.len() / 2 {
                        return Err(stored());
                    }
                    for _ in 0..count {
                        self.skip(types >> 4, depth + 1, false)?;
                        self.skip(types & 15, depth + 1, false)?;
                    }
                }
            }
            12 => {
                let mut last = 0;
                while let Some((_, ty)) = self.field(&mut last)? {
                    self.skip(ty, depth + 1, true)?;
                }
            }
            _ => return Err(stored()),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tiny compact-Thrift FileMetaData containing only a schema. These are
    // deliberately tested directly, before any Parquet reader can run.
    fn fixture(root_children: &[u8], child_children: Option<u8>) -> Vec<u8> {
        let mut bytes = vec![0x29, 0x2c, 0x48, 1, b'r', 0x15];
        bytes.extend_from_slice(root_children);
        bytes.extend_from_slice(&[0, 0x15, 4, 0x25, 0, 0x18, 2, b'i', b'd']);
        if let Some(count) = child_children {
            bytes.extend_from_slice(&[0x15, count]);
        }
        bytes.extend_from_slice(&[0, 0]);
        bytes
    }

    #[test]
    fn rejects_schema_declarations_before_library_metadata_loading() {
        assert!(validate(&fixture(&[2], None), 1).is_ok());
        for bytes in [
            fixture(&[4], None),       // root declares two children, only one exists
            fixture(&[0x82, 2], None), // 129, modest and never passed to Parquet
            fixture(&[2], Some(2)),    // primitive declares a descendant
        ] {
            let e = validate(&bytes, 1).unwrap_err();
            assert_eq!(e.code, crate::protocol::ErrorCode::StorageUnavailable);
            assert_eq!(e.message, "invalid stored snapshot");
        }
    }

    #[test]
    fn bounds_skipping_of_other_footer_fields() {
        // Unknown field after schema, with declarations longer than its buffer.
        for tail in [vec![0x88, 127], vec![0x89, 0xfc, 127], vec![0x88, 0x80]] {
            let mut bytes = fixture(&[2], None);
            bytes.pop();
            bytes.extend(tail);
            assert_eq!(
                validate(&bytes, 1).unwrap_err().code,
                crate::protocol::ErrorCode::StorageUnavailable
            );
        }
        let mut bytes = fixture(&[2], None);
        bytes.pop();
        bytes.push(0x8c);
        bytes.extend(std::iter::repeat_n(0x1c, 34));
        bytes.extend(std::iter::repeat_n(0, 36));
        assert_eq!(
            validate(&bytes, 1).unwrap_err().code,
            crate::protocol::ErrorCode::LimitExceeded
        );
    }

    #[test]
    fn rejects_boolean_collections_before_library_metadata_loading() {
        // The pinned reader skips boolean collection items without consuming
        // their bytes. Reject these containers before reader interpretation.
        for container in [
            vec![0x69, 0x41],       // root field 11: list of four booleans
            vec![0x6a, 0x42],       // set of four booleans
            vec![0x6b, 2, 0x13],    // two boolean/byte map entries
            vec![0x6b, 2, 0x31],    // two byte/boolean map entries
            vec![0x69, 0x19, 0x41], // nested list of booleans
        ] {
            let base = fixture(&[2], None);
            let mut bytes = base[..7].to_vec();
            bytes.extend(container);
            // Modest replacement child count 129, tested only against guard.
            bytes.extend_from_slice(&[0x05, 0x0a, 0x82, 2]);
            bytes.extend_from_slice(&base[7..]);
            assert_eq!(
                validate(&bytes, 1).unwrap_err().code,
                crate::protocol::ErrorCode::StorageUnavailable
            );
        }
    }
}
