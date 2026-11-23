use super::{ChargedBytes, ScratchLease};
use std::{ops::Deref, sync::Arc};

pub const MAX_SECTIONS: usize = 3;

/// Move this wrapper into native work. Extracting a bare decoded value is not
/// exposed, so cancellation cannot drop its scratch owner while work continues.
pub struct Decoded<T> {
    value: T,
    _sections: [Option<ChargedBytes>; MAX_SECTIONS],
    _scratch: Arc<ScratchLease>,
}
impl<T> Decoded<T> {
    #[inline]
    pub fn new(
        value: T,
        sections: [Option<ChargedBytes>; MAX_SECTIONS],
        scratch: Arc<ScratchLease>,
    ) -> Self {
        Self {
            value,
            _sections: sections,
            _scratch: scratch,
        }
    }
}

impl<T> Deref for Decoded<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &self.value
    }
}
