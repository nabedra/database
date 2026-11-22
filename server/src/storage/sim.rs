//! In-memory `ObjectStore` with the documented conditional-write semantics and a
//! seeded fault plan. Test only. It is trusted only for behaviour that the shared
//! contract suite (`contract_tests`) also confirms on `HttpStore` and real S3.
//!
//! Non-fault semantics:
//! - GET is linearizable: every operation runs under one lock.
//! - Every applied PUT gets a fresh version that never repeats, even for identical bytes.
//! - `Create` applies only to an absent key and `Match` only to the current version;
//!   otherwise the PUT fails with `Conflict` and changes nothing.
//! - `Match` against an absent key fails with `NotFound` (S3 answers 404 there).
//! - A conditional PUT that is in flight when the same key is deleted fails with
//!   `Conflict` (S3 answers 409 ConditionalRequestConflict).
//! - LIST returns every key applied before it was called.
//!
//! Faults (`Fault`) fire on the operations a `Rule` selects, or at random from a seed.
use super::{Download, ObjectStore, StorageError, WriteCondition};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;

/// What an injected fault does to one operation.
#[derive(Clone)]
pub(crate) enum Fault {
    /// The server answered this status and applied nothing. 412 and 409 surface as
    /// `Conflict`, 404 as `NotFound`, and 503 as `Unavailable` on GET and
    /// `Ambiguous` on PUT (the adapters cannot prove a 5xx PUT did not apply).
    Status(u16),
    /// The write applied, then the reply was lost: the caller sees `Ambiguous`.
    LostResponse,
    /// The caller times out and sees `Ambiguous`, but the PUT stays queued in the
    /// store and applies `steps` operations later. Its condition is evaluated when
    /// it applies, as S3 evaluates conditions at completion time.
    DelayApply { steps: u64 },
    /// The PUT waits until the test adds a permit to the semaphore, then applies
    /// with its condition checked at that moment and replies normally.
    Hold(Arc<Semaphore>),
    /// Delta's "200 with an embedded error": the server said success but did not
    /// apply. A correct adapter cannot trust the status, so it reports `Ambiguous`.
    EmbeddedError,
}
impl std::fmt::Debug for Fault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Status(code) => write!(f, "status={code}"),
            Self::LostResponse => f.write_str("lost-response"),
            Self::DelayApply { steps } => write!(f, "delay-apply={steps}"),
            Self::Hold(_) => f.write_str("hold"),
            Self::EmbeddedError => f.write_str("embedded-error"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Op {
    Get,
    Put,
}

/// Fire `fault` on the `skip`+1-th operation of kind `op` whose key contains `key`.
#[derive(Debug, Clone)]
pub(crate) struct Rule {
    pub op: Op,
    pub key: String,
    pub skip: u64,
    pub fault: Fault,
}
impl Rule {
    pub fn put(key: &str, fault: Fault) -> Self {
        Self {
            op: Op::Put,
            key: key.into(),
            skip: 0,
            fault,
        }
    }
    pub fn get(key: &str, fault: Fault) -> Self {
        Self {
            op: Op::Get,
            key: key.into(),
            skip: 0,
            fault,
        }
    }
    pub fn after(mut self, skip: u64) -> Self {
        self.skip = skip;
        self
    }
}

/// Random faults: each operation draws one with probability `per_mille / 1000`.
struct Random {
    rng: nabedra_check::rng::SplitMix64,
    per_mille: u64,
    faults: Vec<Fault>,
}

#[derive(Debug, Clone)]
enum Cond {
    Create,
    Match(String),
}
struct Delayed {
    due: u64,
    key: String,
    bytes: Vec<u8>,
    cond: Cond,
    deletes: u64,
}

/// One applied PUT, for assertions about what reached the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Applied {
    pub key: String,
    pub etag: String,
    pub step: u64,
    pub bytes: Vec<u8>,
}

#[derive(Default)]
struct State {
    objects: BTreeMap<String, (Vec<u8>, String)>,
    next_version: u64,
    step: u64,
    /// Deletes per key, so an in-flight conditional PUT can detect a concurrent one.
    deletes: BTreeMap<String, u64>,
    rules: Vec<Rule>,
    random: Option<Random>,
    delayed: Vec<Delayed>,
    applied: Vec<Applied>,
    fired: Vec<(Op, String, String)>,
    requests: u64,
    /// Seeded scheduler noise: each operation first yields 0..=max times, so a
    /// current-thread runtime interleaves concurrent callers reproducibly.
    interleave: Option<(nabedra_check::rng::SplitMix64, u64)>,
}
impl State {
    fn yields(&mut self) -> u64 {
        match &mut self.interleave {
            Some((rng, max)) => rng.next_u64() % (*max + 1),
            None => 0,
        }
    }
    fn tick(&mut self) {
        self.requests += 1;
        self.step += 1;
        let step = self.step;
        let (due, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut self.delayed)
            .into_iter()
            .partition(|d| d.due <= step);
        self.delayed = rest;
        for d in due {
            let _ = self.apply(&d.key, d.bytes, &d.cond, d.deletes);
        }
    }
    fn take_fault(&mut self, op: Op, key: &str) -> Option<Fault> {
        if let Some(i) = self
            .rules
            .iter()
            .position(|r| r.op == op && key.contains(r.key.as_str()))
        {
            if self.rules[i].skip > 0 {
                self.rules[i].skip -= 1;
            } else {
                let fault = self.rules.remove(i).fault;
                self.fired.push((op, key.into(), format!("{fault:?}")));
                return Some(fault);
            }
        }
        let random = self.random.as_mut()?;
        if random.rng.next_u64() % 1000 >= random.per_mille || random.faults.is_empty() {
            return None;
        }
        let fault =
            random.faults[(random.rng.next_u64() % random.faults.len() as u64) as usize].clone();
        // A random hold would block forever without a test to release it.
        if matches!(fault, Fault::Hold(_)) || (op == Op::Get && !matches!(fault, Fault::Status(_)))
        {
            return None;
        }
        self.fired.push((op, key.into(), format!("{fault:?}")));
        Some(fault)
    }
    fn apply(
        &mut self,
        key: &str,
        bytes: Vec<u8>,
        cond: &Cond,
        deletes_at_start: u64,
    ) -> Result<String, StorageError> {
        if self.deletes.get(key).copied().unwrap_or(0) != deletes_at_start {
            return Err(StorageError::Conflict);
        }
        match (cond, self.objects.get(key)) {
            (Cond::Create, Some(_)) => return Err(StorageError::Conflict),
            (Cond::Match(_), None) => return Err(StorageError::NotFound),
            (Cond::Match(tag), Some((_, current))) if tag != current => {
                return Err(StorageError::Conflict);
            }
            _ => {}
        }
        self.next_version += 1;
        let etag = format!("\"s{}\"", self.next_version);
        self.applied.push(Applied {
            key: key.into(),
            etag: etag.clone(),
            step: self.step,
            bytes: bytes.clone(),
        });
        self.objects.insert(key.into(), (bytes, etag.clone()));
        Ok(etag)
    }
}

/// Cheap to clone; clones share one store.
#[derive(Clone, Default)]
pub(crate) struct SimObjectStore(Arc<Mutex<State>>);
impl SimObjectStore {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn inject(&self, rule: Rule) {
        self.0.lock().unwrap().rules.push(rule);
    }
    /// Seeded random faults on every operation, drawn from `faults`.
    pub fn random_faults(&self, seed: u64, per_mille: u64, faults: Vec<Fault>) {
        self.0.lock().unwrap().random = Some(Random {
            rng: nabedra_check::rng::SplitMix64::new(seed),
            per_mille,
            faults,
        });
    }
    /// Yield 0..=max times before each operation, drawn from `seed`.
    pub fn interleave(&self, seed: u64, max: u64) {
        self.0.lock().unwrap().interleave = Some((nabedra_check::rng::SplitMix64::new(seed), max));
    }
    async fn pause(&self) {
        let n = self.0.lock().unwrap().yields();
        for _ in 0..n {
            tokio::task::yield_now().await;
        }
    }
    pub fn clear_faults(&self) {
        let mut s = self.0.lock().unwrap();
        s.rules.clear();
        s.random = None;
    }
    /// Apply every queued late PUT now, in submission order.
    pub fn apply_delayed(&self) {
        let mut s = self.0.lock().unwrap();
        for d in std::mem::take(&mut s.delayed) {
            let _ = s.apply(&d.key, d.bytes, &d.cond, d.deletes);
        }
    }
    pub fn delayed_len(&self) -> usize {
        self.0.lock().unwrap().delayed.len()
    }
    pub fn applied(&self) -> Vec<Applied> {
        self.0.lock().unwrap().applied.clone()
    }
    /// `(op, key, fault)` for every fault that fired, for failure messages.
    pub fn fired(&self) -> Vec<(Op, String, String)> {
        self.0.lock().unwrap().fired.clone()
    }
    pub fn requests(&self) -> u64 {
        self.0.lock().unwrap().requests
    }
    pub fn object(&self, key: &str) -> Option<(Vec<u8>, String)> {
        self.0.lock().unwrap().objects.get(key).cloned()
    }
    /// Unconditional delete with S3 semantics: deleting an absent key succeeds.
    /// `if_match` on an absent key fails with `NotFound`, on another version with `Conflict`.
    pub fn delete(&self, key: &str, if_match: Option<&str>) -> Result<(), StorageError> {
        let mut s = self.0.lock().unwrap();
        s.tick();
        match (if_match, s.objects.get(key)) {
            (Some(_), None) => return Err(StorageError::NotFound),
            (Some(tag), Some((_, current))) if tag != current => {
                return Err(StorageError::Conflict);
            }
            _ => {}
        }
        if s.objects.remove(key).is_some() {
            *s.deletes.entry(key.into()).or_default() += 1;
        }
        Ok(())
    }
    pub fn list(&self, prefix: &str) -> Vec<String> {
        let mut s = self.0.lock().unwrap();
        s.tick();
        s.objects
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect()
    }
}

impl ObjectStore for SimObjectStore {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        self.pause().await;
        let mut s = self.0.lock().unwrap();
        s.tick();
        match s.take_fault(Op::Get, key) {
            Some(Fault::Status(404)) => return Err(StorageError::NotFound),
            Some(_) => return Err(StorageError::Unavailable),
            None => {}
        }
        let (bytes, etag) = s.objects.get(key).ok_or(StorageError::NotFound)?;
        Ok(Download::from_bytes(bytes.clone(), etag.clone()))
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        self.pause().await;
        let cond = match condition {
            WriteCondition::Create => Cond::Create,
            WriteCondition::Match(tag) => Cond::Match(tag.into()),
        };
        let (fault, deletes) = {
            let mut s = self.0.lock().unwrap();
            s.tick();
            let fault = s.take_fault(Op::Put, key);
            (fault, s.deletes.get(key).copied().unwrap_or(0))
        };
        match fault {
            None => self.0.lock().unwrap().apply(key, bytes, &cond, deletes),
            Some(Fault::Status(412 | 409)) => Err(StorageError::Conflict),
            Some(Fault::Status(404)) => Err(StorageError::NotFound),
            Some(Fault::Status(_) | Fault::EmbeddedError) => Err(StorageError::Ambiguous),
            Some(Fault::LostResponse) => {
                let _ = self.0.lock().unwrap().apply(key, bytes, &cond, deletes);
                Err(StorageError::Ambiguous)
            }
            Some(Fault::DelayApply { steps }) => {
                let mut s = self.0.lock().unwrap();
                let due = s.step + steps.max(1);
                s.delayed.push(Delayed {
                    due,
                    key: key.into(),
                    bytes,
                    cond,
                    deletes,
                });
                Err(StorageError::Ambiguous)
            }
            Some(Fault::Hold(gate)) => {
                gate.acquire()
                    .await
                    .map_err(|_| StorageError::Ambiguous)?
                    .forget();
                self.0.lock().unwrap().apply(key, bytes, &cond, deletes)
            }
        }
    }
}
