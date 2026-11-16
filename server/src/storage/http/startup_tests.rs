//! Observation of the actual constructed adapter, compiled only for unit tests.
pub(crate) fn assert_material(
    backend: &crate::storage::RuntimeStore,
    base: &str,
    namespace: &str,
    token: &str,
    ca: Option<&[u8]>,
) {
    let crate::storage::RuntimeStore::Http(store) = backend else {
        panic!("expected HTTP fixture")
    };
    assert!(store.base == base);
    assert!(store.namespace.as_str() == namespace);
    assert!(store.authorization.as_bytes() == format!("Bearer {token}").as_bytes());
    assert!(store.authorization.is_sensitive());
    assert!(store.startup_ca_argument.as_deref() == ca);
}
