use super::{ByteClass, TrafficClass, WorkerIdentity};
use crate::oltp_replication::types::Text;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    sync::{Arc, Mutex},
    time::Duration,
};
use nabedra_rt::time::timeout;
use tokio::{
    sync::{Semaphore, watch},
    time::Instant,
};

const DATA_LOOKUPS: usize = 2;
const PROGRESS_LOOKUPS: usize = 2;
const CACHE_ENTRIES: usize = 128;
const MAX_ADDRESSES: usize = 32;
const WAIT: Duration = Duration::from_secs(1);
const POSITIVE_TTL: Duration = Duration::from_secs(1);
const NEGATIVE_TTL: Duration = Duration::from_millis(250);

#[derive(Debug)]
pub(super) enum ResolveError {
    LocallyUnavailable,
    NoAddresses,
}

#[cfg(test)]
type Lookup = dyn Fn(&str) -> Vec<SocketAddr> + Send + Sync;

pub(super) struct PeerResolver {
    entries: Mutex<Vec<Entry>>,
    data: Arc<Semaphore>,
    progress: Arc<Semaphore>,
    #[cfg(test)]
    lookup: Option<Arc<Lookup>>,
}

struct Entry {
    identity: WorkerIdentity,
    endpoint: Text<255>,
    result: watch::Receiver<Option<CachedAddresses>>,
}

struct CachedAddresses {
    addresses: Arc<[SocketAddr]>,
    expires: Instant,
}

impl Entry {
    fn fresh(&self, now: Instant) -> bool {
        match self.result.borrow().as_ref() {
            Some(result) => result.expires > now,
            None => self.result.has_changed().is_ok(),
        }
    }
}

impl Default for PeerResolver {
    fn default() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
            data: Arc::new(Semaphore::new(DATA_LOOKUPS)),
            progress: Arc::new(Semaphore::new(PROGRESS_LOOKUPS)),
            #[cfg(test)]
            lookup: None,
        }
    }
}

impl PeerResolver {
    pub(super) async fn resolve(
        &self,
        identity: WorkerIdentity,
        endpoint: &Text<255>,
        class: TrafficClass,
    ) -> Result<Arc<[SocketAddr]>, ResolveError> {
        let mut result = self.lookup_or_join(identity, endpoint, class)?;
        let cached = timeout(WAIT, result.wait_for(Option::is_some))
            .await
            .map_err(|_| ResolveError::LocallyUnavailable)?
            .map_err(|_| ResolveError::LocallyUnavailable)?;
        cached
            .as_ref()
            .filter(|cached| !cached.addresses.is_empty())
            .map(|cached| cached.addresses.clone())
            .ok_or(ResolveError::NoAddresses)
    }

    fn lookup_or_join(
        &self,
        identity: WorkerIdentity,
        endpoint: &Text<255>,
        class: TrafficClass,
    ) -> Result<watch::Receiver<Option<CachedAddresses>>, ResolveError> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| ResolveError::LocallyUnavailable)?;
        let now = nabedra_rt::time::now();
        if let Some(entry) = entries.iter().find(|entry| {
            entry.identity == identity && entry.endpoint == *endpoint && entry.fresh(now)
        }) {
            return Ok(entry.result.clone());
        }
        entries.retain(|entry| entry.fresh(now));
        // Share an exact lookup across traffic classes. A changed enrollment
        // can start its own lookup for a different hostname immediately.
        if entries.iter().any(|entry| {
            entry.result.borrow().is_none()
                && hostname(&entry.endpoint).eq_ignore_ascii_case(hostname(endpoint))
        }) {
            return Err(ResolveError::LocallyUnavailable);
        }
        // A new lookup uses its own lane. Data cannot borrow progress slots.
        let slots = if matches!(class.byte_class(), ByteClass::Progress) {
            &self.progress
        } else {
            &self.data
        };
        let permit = slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| ResolveError::LocallyUnavailable)?;
        if entries.len() == CACHE_ENTRIES {
            let completed = entries
                .iter()
                .position(|entry| entry.result.borrow().is_some())
                .ok_or(ResolveError::LocallyUnavailable)?;
            entries.remove(completed);
        }
        let (send, result) = watch::channel(None);
        entries.push(Entry {
            identity,
            endpoint: endpoint.clone(),
            result: result.clone(),
        });
        let endpoint = endpoint.clone();
        #[cfg(test)]
        let lookup = self.lookup.clone();
        // Dropping any or all waiters leaves the permit with getaddrinfo.
        drop(nabedra_rt::task::spawn_blocking(move || {
            #[cfg(test)]
            let addresses = if let Some(lookup) = lookup {
                lookup(endpoint.as_str())
            } else {
                resolve_host(endpoint.as_str())
            };
            #[cfg(not(test))]
            let addresses = resolve_host(endpoint.as_str());
            let ttl = if addresses.is_empty() {
                NEGATIVE_TTL
            } else {
                POSITIVE_TTL
            };
            drop(permit);
            let _ = send.send_replace(Some(CachedAddresses {
                addresses: addresses.into(),
                expires: nabedra_rt::time::now() + ttl,
            }));
        }));
        Ok(result)
    }

    #[cfg(test)]
    pub(super) fn with_lookup(
        lookup: impl Fn(&str) -> Vec<SocketAddr> + Send + Sync + 'static,
    ) -> Self {
        Self {
            lookup: Some(Arc::new(lookup)),
            ..Self::default()
        }
    }

    #[cfg(test)]
    pub(super) fn active_for_test(&self) -> [usize; 2] {
        [
            DATA_LOOKUPS - self.data.available_permits(),
            PROGRESS_LOOKUPS - self.progress.available_permits(),
        ]
    }
}

fn hostname(endpoint: &str) -> &str {
    endpoint
        .rsplit_once(':')
        .map_or(endpoint, |(host, _)| host)
        .trim_end_matches('.')
}

fn resolve_host(endpoint: &str) -> Vec<SocketAddr> {
    endpoint
        .to_socket_addrs()
        .map(|addresses| addresses.take(MAX_ADDRESSES).collect())
        .unwrap_or_default()
}
