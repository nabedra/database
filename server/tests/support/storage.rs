//! Conditional in-memory object backend shared by storage and later commit tests.
use meshdb_server::storage::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
#[derive(Default)]
pub struct Fake {
    pub state: Mutex<State>,
}
pub struct ReadGate {
    pub entered: tokio::sync::Semaphore,
    pub release: tokio::sync::Semaphore,
}
#[derive(Default)]
pub struct State {
    pub objects: BTreeMap<String, (Vec<u8>, String)>,
    pub generation: u64,
    pub denied: bool,
    pub race: Option<Vec<u8>>,
    pub puts: usize,
    pub lose_put_response: bool,
    pub put_faults: BTreeMap<String, (StorageError, bool)>,
    pub read_failure: bool,
    pub read_gate: Option<Arc<ReadGate>>,
    pub put_keys: Vec<String>,
    pub gets: usize,
}
impl Default for ReadGate {
    fn default() -> Self {
        Self {
            entered: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        }
    }
}
impl ObjectStore for Fake {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        let gate = self.state.lock().unwrap().read_gate.take();
        if let Some(gate) = gate {
            gate.entered.add_permits(1);
            gate.release.acquire().await.unwrap().forget();
        }
        let mut state = self.state.lock().unwrap();
        state.gets += 1;
        if state.denied || state.read_failure {
            return Err(StorageError::Unavailable);
        }
        let (bytes, etag) = state.objects.get(key).ok_or(StorageError::NotFound)?;
        Ok(Download::new(
            Chunks {
                chunks: bytes.chunks(8).map(bytes::Bytes::copy_from_slice).collect(),
            },
            etag.clone(),
        ))
    }

    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        let mut state = self.state.lock().unwrap();
        state.puts += 1;
        state.put_keys.push(key.into());
        let fault = state.put_faults.remove(key);
        if let Some((error, false)) = fault {
            return Err(error);
        }
        if state.denied {
            return Err(StorageError::Unavailable);
        }
        if let Some(winner) = state.race.take() {
            state
                .objects
                .insert(key.into(), (winner, "opaque-race".into()));
        }
        let existing = state.objects.get(key);
        match condition {
            WriteCondition::Create if existing.is_some() => return Err(StorageError::Conflict),
            WriteCondition::Match(etag) if existing.is_none_or(|(_, actual)| actual != etag) => {
                return Err(StorageError::Conflict);
            }
            _ => {}
        }
        state.generation += 1;
        let etag = format!("\"opaque-{}\"", state.generation);
        state.objects.insert(key.into(), (bytes, etag.clone()));
        if let Some((error, true)) = fault {
            return Err(error);
        }
        if state.lose_put_response {
            return Err(StorageError::Ambiguous);
        }
        Ok(etag)
    }
}

// Unknown total length, delivered in several frames, exercises the actual read loop.
struct Chunks {
    chunks: std::collections::VecDeque<bytes::Bytes>,
}
impl tokio::io::AsyncRead for Chunks {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if let Some(chunk) = self.chunks.front_mut() {
            let len = chunk.len().min(buffer.remaining());
            buffer.put_slice(&chunk.split_to(len));
            if chunk.is_empty() {
                self.chunks.pop_front();
            }
        }
        std::task::Poll::Ready(Ok(()))
    }
}
