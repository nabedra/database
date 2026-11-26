//! Page-aligned buffers for `O_DIRECT`, without `unsafe`: a vector one page longer than needed,
//! used from its first aligned byte.

pub const PAGE: usize = 4096;

#[derive(Default)]
pub struct Pages {
    raw: Vec<u8>,
    start: usize,
    len: usize,
}

impl Pages {
    /// `n` zeroed pages.
    pub fn new(n: usize) -> Pages {
        let len = n.saturating_mul(PAGE);
        let raw = vec![0u8; len.saturating_add(PAGE)];
        let start = raw.as_ptr().align_offset(PAGE).min(PAGE);
        Pages { raw, start, len }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn bytes(&self) -> &[u8] {
        &self.raw[self.start..self.start + self.len]
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.raw[self.start..self.start + self.len]
    }

    /// Page `i`.
    pub fn page(&self, i: usize) -> &[u8] {
        &self.bytes()[i * PAGE..(i + 1) * PAGE]
    }
}

impl std::fmt::Debug for Pages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Pages({})", self.len / PAGE)
    }
}
