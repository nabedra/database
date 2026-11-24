//! Protocol records and identities shared between OLTP components.

pub mod authority;
mod identity;
pub mod owned;
pub mod types;

pub use identity::{POOL_FORMAT_VERSION, PoolIdentity};

pub fn valid_database(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name.as_bytes()[0].is_ascii_alphanumeric()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
}
