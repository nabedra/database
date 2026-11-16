//! Immutable per-request grants with bounded client lifetime. No authorized
//! data or SDK identity cache is reused between credential snapshots.
use super::{
    RuntimeStore, Storage, StorageError,
    context::{RequestContext, StorageKey},
};
use std::sync::{Arc, Mutex};
const CLIENT_RESERVATION: usize = 256 * 1024;
struct State {
    entries: usize,
    bytes: usize,
}
struct Inner {
    state: Mutex<State>,
    max_entries: usize,
    max_bytes: usize,
    control: Option<StorageKey>,
    #[cfg(test)]
    fixture: Option<Fixture>,
}
#[derive(Clone)]
pub struct DatabaseRegistry {
    inner: Arc<Inner>,
}
#[cfg(test)]
struct Fixture {
    backends: std::collections::BTreeMap<StorageKey, super::HttpStore>,
    authorize: Arc<dyn Fn(&RequestContext) -> bool + Send + Sync>,
}
impl DatabaseRegistry {
    #[cfg(test)]
    pub(crate) fn new_fixture(
        max_entries: usize,
        max_bytes: usize,
        control_key: Option<StorageKey>,
        backends: std::collections::BTreeMap<StorageKey, super::HttpStore>,
        authorize: Arc<dyn Fn(&RequestContext) -> bool + Send + Sync>,
    ) -> Result<Self, StorageError> {
        let mut registry = Self::new(max_entries, max_bytes, control_key)?;
        Arc::get_mut(&mut registry.inner)
            .expect("new registry")
            .fixture = Some(Fixture {
            backends,
            authorize,
        });
        Ok(registry)
    }

    pub fn new(
        max_entries: usize,
        max_bytes: usize,
        control_key: Option<StorageKey>,
    ) -> Result<Self, StorageError> {
        if max_entries == 0 || max_bytes < CLIENT_RESERVATION {
            return Err(StorageError::Configuration);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    entries: 0,
                    bytes: 0,
                }),
                max_entries,
                max_bytes,
                control: control_key,
                #[cfg(test)]
                fixture: None,
            }),
        })
    }
    pub fn charged_bytes(&self) -> usize {
        self.inner.state.lock().unwrap().bytes
    }
    pub fn entries(&self) -> usize {
        self.inner.state.lock().unwrap().entries
    }
    fn reserve(&self, context: RequestContext, now: u64) -> Result<Reservation, StorageError> {
        context.validate(now)?;
        let key = context.storage_key()?;
        if self.inner.control.as_ref().is_some_and(|c| key.overlaps(c)) {
            return Err(StorageError::Configuration);
        }
        let context_bytes = serde_json::to_vec(&context)
            .map_err(|_| StorageError::Configuration)?
            .len();
        let bytes = CLIENT_RESERVATION
            .checked_add(
                context_bytes
                    .checked_mul(2)
                    .ok_or(StorageError::LimitExceeded)?,
            )
            .ok_or(StorageError::LimitExceeded)?;
        let mut state = self.inner.state.lock().unwrap();
        let total = state
            .bytes
            .checked_add(bytes)
            .ok_or(StorageError::LimitExceeded)?;
        if state.entries >= self.inner.max_entries || total > self.inner.max_bytes {
            return Err(StorageError::LimitExceeded);
        }
        state.entries += 1;
        state.bytes = total;
        Ok(Reservation {
            inner: self.inner.clone(),
            bytes,
            context,
            key,
            lifetime: Mutex::new(None),
        })
    }
    pub async fn open(
        &self,
        context: RequestContext,
        now: u64,
    ) -> Result<DatabaseHandle, StorageError> {
        self.open_accounted(context, now, None).await
    }
    pub(crate) async fn open_accounted(
        &self,
        context: RequestContext,
        now: u64,
        scheduler: Option<&crate::scheduling::ResourceScheduler>,
    ) -> Result<DatabaseHandle, StorageError> {
        let reservation = Arc::new(self.reserve(context, now)?);
        if let Some(scheduler) = scheduler {
            let charge = scheduler
                .charge_memory(0, reservation.bytes as u64)
                .map_err(|_| StorageError::LimitExceeded)?;
            *reservation.lifetime.lock().unwrap() = Some(Arc::new(charge));
        }
        #[cfg(test)]
        if let Some(fixture) = &self.inner.fixture {
            if !(fixture.authorize)(&reservation.context) {
                return Err(StorageError::Unavailable);
            }
            let backend = fixture
                .backends
                .get(&reservation.key)
                .ok_or(StorageError::Unavailable)?
                .clone();
            let storage = Arc::new(Storage::new(
                RuntimeStore::Fixture {
                    backend,
                    _reservation: reservation.clone(),
                },
                reservation.context.namespace()?,
            ));
            return Ok(DatabaseHandle {
                inner: Arc::new(HandleInner {
                    storage,
                    reservation,
                }),
            });
        }
        #[cfg(feature = "s3")]
        {
            let mut client = super::S3Store::from_context(&reservation.context).await?;
            client.retain_reservation(reservation.clone());
            let storage = Arc::new(Storage::new(
                RuntimeStore::S3(client),
                reservation.context.namespace()?,
            ));
            Ok(DatabaseHandle {
                inner: Arc::new(HandleInner {
                    storage,
                    reservation,
                }),
            })
        }
        #[cfg(not(feature = "s3"))]
        {
            let _ = reservation;
            Err(StorageError::Configuration)
        }
    }
}
struct Reservation {
    inner: Arc<Inner>,
    bytes: usize,
    context: RequestContext,
    key: StorageKey,
    lifetime: Mutex<Option<Arc<dyn Send + Sync>>>,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        state.entries -= 1;
        state.bytes -= self.bytes;
    }
}
struct HandleInner {
    storage: Arc<Storage<RuntimeStore>>,
    reservation: Arc<Reservation>,
}
#[derive(Clone)]
pub struct DatabaseHandle {
    inner: Arc<HandleInner>,
}
impl DatabaseHandle {
    pub fn storage(&self) -> &Arc<Storage<RuntimeStore>> {
        &self.inner.storage
    }
    pub fn key(&self) -> &StorageKey {
        &self.inner.reservation.key
    }
    pub fn context(&self) -> &RequestContext {
        &self.inner.reservation.context
    }
    pub fn charged_bytes(&self) -> usize {
        self.inner.reservation.bytes
    }
}
impl std::fmt::Debug for DatabaseHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DatabaseHandle { .. }")
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn context() -> RequestContext {
        serde_json::from_value(serde_json::json!({"version":1,"bucket":"app-data","region":"eu-west-1","prefix":"db/","credentials":{"access_key_id":"sentinel-key","secret_access_key":"sentinel-secret","session_token":null,"expires_at":null}})).unwrap()
    }
    #[test]
    fn sl02_control_overlap_capacity_and_release() {
        let control = StorageKey::new("app-data", "eu-west-1", "db/control").unwrap();
        let registry = DatabaseRegistry::new(1, 1 << 20, Some(control)).unwrap();
        assert!(matches!(
            registry.reserve(context(), 0),
            Err(StorageError::Configuration)
        ));
        assert_eq!(registry.charged_bytes(), 0);
        let registry = DatabaseRegistry::new(1, 1 << 20, None).unwrap();
        let grant = registry.reserve(context(), 0).unwrap();
        assert_eq!(registry.entries(), 1);
        assert!(registry.charged_bytes() > CLIENT_RESERVATION);
        assert!(matches!(
            registry.reserve(context(), 0),
            Err(StorageError::LimitExceeded)
        ));
        drop(grant);
        assert_eq!(registry.entries(), 0);
        assert_eq!(registry.charged_bytes(), 0);
    }
}

#[cfg(all(test, feature = "s3"))]
mod sdk_tests {
    use super::*;
    #[tokio::test]
    async fn sl02_sdk_grant_is_explicit_and_storage_clone_retains_charge() {
        let context: RequestContext = serde_json::from_value(serde_json::json!({"version":1,"bucket":"tenant-data","region":"eu-west-1","prefix":"db/","credentials":{"access_key_id":"ONLY_GATEWAY_ACCESS","secret_access_key":"ONLY_GATEWAY_SECRET","session_token":"ONLY_GATEWAY_SESSION","expires_at":null}})).unwrap();
        let registry = DatabaseRegistry::new(1, 1 << 20, None).unwrap();
        let handle = registry.open(context.clone(), 0).await.unwrap();
        let storage = handle.storage().clone();
        let crate::storage::RuntimeStore::S3(client) = storage.backend() else {
            panic!("expected explicit S3 provider")
        };
        let credentials = client.test_credentials().await;
        assert_eq!(credentials.access_key_id(), "ONLY_GATEWAY_ACCESS");
        assert_eq!(credentials.secret_access_key(), "ONLY_GATEWAY_SECRET");
        assert_eq!(credentials.session_token(), Some("ONLY_GATEWAY_SESSION"));
        assert_eq!(client.region(), "eu-west-1");
        drop(handle);
        assert_eq!(registry.entries(), 1);
        assert!(matches!(
            registry.open(context.clone(), 0).await,
            Err(StorageError::LimitExceeded)
        ));
        drop(storage);
        assert_eq!(registry.entries(), 0);
        assert_eq!(registry.charged_bytes(), 0);
        let next = registry.open(context, 0).await.unwrap();
        drop(next);
        assert_eq!(registry.entries(), 0);
    }
}

#[cfg(test)]
mod fixture_tests {
    use super::*;
    #[tokio::test]
    async fn sl02_fixture_rechecks_grant_and_retains_cloned_storage_lease() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let context: RequestContext = serde_json::from_value(serde_json::json!({"version":1,"bucket":"tenant-data","region":"eu-west-1","prefix":"db/","credentials":{"access_key_id":"TEST_ACCESS","secret_access_key":"TEST_SECRET","session_token":null,"expires_at":null}})).unwrap();
        let allowed = Arc::new(AtomicBool::new(true));
        let check = allowed.clone();
        let backend = super::super::HttpStore::new(
            "http://127.0.0.1:1/",
            context.namespace().unwrap(),
            "fixture-token",
            None,
        )
        .unwrap();
        let registry = DatabaseRegistry::new_fixture(
            2,
            1 << 20,
            None,
            [(context.storage_key().unwrap(), backend)]
                .into_iter()
                .collect(),
            Arc::new(move |_| check.load(Ordering::SeqCst)),
        )
        .unwrap();
        let handle = registry.open(context.clone(), 0).await.unwrap();
        let cloned_storage = handle.storage().clone();
        drop(handle);
        assert_eq!(registry.entries(), 1);
        allowed.store(false, Ordering::SeqCst);
        assert!(matches!(
            registry.open(context, 0).await,
            Err(StorageError::Unavailable)
        ));
        assert_eq!(registry.entries(), 1);
        drop(cloned_storage);
        assert_eq!(registry.entries(), 0);
        assert_eq!(registry.charged_bytes(), 0);
    }
}
