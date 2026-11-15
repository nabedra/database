//! Observation of the actual constructed adapter, compiled only for unit tests.
use super::*;
pub(crate) fn assert_material(
    store: &HttpStore,
    base: &str,
    namespace: &str,
    token: &str,
    ca: Option<&[u8]>,
) {
    assert!(store.base == base);
    assert!(store.namespace.as_str() == namespace);
    assert!(store.authorization.as_bytes() == format!("Bearer {token}").as_bytes());
    assert!(store.authorization.is_sensitive());
    assert!(store.startup_ca_argument.as_deref() == ca);
}
