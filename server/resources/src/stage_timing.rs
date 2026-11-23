//! Per-stage latency histograms for the direct point path (PERF1).
//!
//! Every stage is a process-wide histogram of wall-clock durations in
//! power-of-two microsecond buckets, shown as `stage_timing` in
//! `/v1/runtime/stats`. A stage costs two clock reads and four relaxed
//! atomic adds per sample, so it stays on in release builds. Each worker
//! is its own process; in-process test clusters share the counters.
//!
//! Durations are wall-clock: a stage that waits (a fsync, a quorum, a
//! busy CPU) shows the wait, not CPU time. CPU attribution comes from
//! profiles, not from these counters.
use std::{
    sync::{
        Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

/// Bucket `i` counts samples below `2^i` microseconds; the last is open.
const BUCKETS: usize = 26;

pub struct Stage {
    name: &'static str,
    count: AtomicU64,
    sum_us: AtomicU64,
    max_us: AtomicU64,
    buckets: [AtomicU64; BUCKETS],
}

fn registry() -> &'static Mutex<Vec<&'static Stage>> {
    static REGISTRY: OnceLock<Mutex<Vec<&'static Stage>>> = OnceLock::new();
    REGISTRY.get_or_init(Mutex::default)
}

/// The stage called `name`, created on first use. Call sites with a fixed
/// name use [`perf_stage!`], which looks it up once.
pub fn stage(name: &'static str) -> &'static Stage {
    let mut stages = registry().lock().unwrap_or_else(|p| p.into_inner());
    if let Some(found) = stages.iter().find(|stage| stage.name == name) {
        return found;
    }
    let created: &'static Stage = Box::leak(Box::new(Stage {
        name,
        count: AtomicU64::new(0),
        sum_us: AtomicU64::new(0),
        max_us: AtomicU64::new(0),
        buckets: std::array::from_fn(|_| AtomicU64::new(0)),
    }));
    stages.push(created);
    created
}

/// The `&'static Stage` for a literal name, resolved once per call site.
#[macro_export]
macro_rules! perf_stage {
    ($name:literal) => {{
        static STAGE: std::sync::OnceLock<&'static $crate::stage_timing::Stage> =
            std::sync::OnceLock::new();
        *STAGE.get_or_init(|| $crate::stage_timing::stage($name))
    }};
}

impl Stage {
    pub fn record(&self, elapsed: Duration) {
        let us = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(us, Ordering::Relaxed);
        self.max_us.fetch_max(us, Ordering::Relaxed);
        let bucket = (u64::BITS - us.leading_zeros()) as usize;
        self.buckets[bucket.min(BUCKETS - 1)].fetch_add(1, Ordering::Relaxed);
    }
    pub fn since(&self, started: Instant) {
        self.record(nabedra_rt::time::elapsed(started));
    }
    /// Records the time until the guard drops (every return path).
    pub fn timer(&'static self) -> Timer {
        Timer {
            stage: self,
            started: nabedra_rt::time::now_std(),
        }
    }
}

pub struct Timer {
    stage: &'static Stage,
    started: Instant,
}
impl Drop for Timer {
    fn drop(&mut self) {
        self.stage.since(self.started);
    }
}

/// The stage `prefix` + the variant name of `value`'s `Debug` output (the
/// text before its first `(`, `{` or space), e.g. one stage per RPC method.
/// The set of variant names is finite, so their leaked names are bounded.
pub fn variant(prefix: &'static str, value: &impl std::fmt::Debug) -> &'static Stage {
    let debug = format!("{value:?}");
    let end = debug
        .find(|c: char| c == '(' || c == '{' || c == ' ')
        .unwrap_or(debug.len())
        .min(48);
    named(prefix, &debug[..end])
}

/// The stage `prefix` + the variant name of a serde-encoded enum value: the
/// first JSON string of `json` (`"current"` or `{"cold_pin":...}`).
pub fn json_variant(prefix: &'static str, json: &[u8]) -> &'static Stage {
    let text = std::str::from_utf8(&json[..json.len().min(64)]).unwrap_or("");
    let name = text
        .split('"')
        .nth(1)
        .filter(|name| name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'))
        .unwrap_or("other");
    named(prefix, name)
}

/// The stage `prefix` + `dir/file:line` of a call site (the last two path
/// components). Call sites are finite, so their leaked names are bounded.
pub fn site(
    prefix: &'static str,
    location: &'static std::panic::Location<'static>,
) -> &'static Stage {
    let path = location.file();
    let mut parts = path.rsplitn(3, ['/', '\\']);
    let file = parts.next().unwrap_or(path);
    let dir = parts.next().unwrap_or("");
    named(prefix, &format!("{dir}/{file}:{}", location.line()))
}

fn named(prefix: &'static str, suffix: &str) -> &'static Stage {
    static NAMES: OnceLock<Mutex<Vec<(String, &'static Stage)>>> = OnceLock::new();
    let name = format!("{prefix}{suffix}");
    let mut names = NAMES
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    if let Some((_, found)) = names.iter().find(|(known, _)| *known == name) {
        return found;
    }
    let created = stage(Box::leak(name.clone().into_boxed_str()));
    names.push((name, created));
    created
}

/// Awaits `future`, recording its duration in `stage`.
pub async fn timed<F: std::future::Future>(stage: &'static Stage, future: F) -> F::Output {
    let _timer = stage.timer();
    future.await
}

/// `{name: {count, sum_us, max_us, le_us: {bound: count}}}`, stages sorted
/// by name. `le_us` lists only nonzero buckets; `inf` is the open one.
pub fn snapshot() -> serde_json::Value {
    let mut stages: Vec<&'static Stage> =
        registry().lock().unwrap_or_else(|p| p.into_inner()).clone();
    stages.sort_by_key(|stage| stage.name);
    let mut out = serde_json::Map::new();
    for stage in stages {
        let mut buckets = serde_json::Map::new();
        for (index, bucket) in stage.buckets.iter().enumerate() {
            let count = bucket.load(Ordering::Relaxed);
            if count == 0 {
                continue;
            }
            let bound = if index == BUCKETS - 1 {
                "inf".to_owned()
            } else {
                (1u64 << index).to_string()
            };
            buckets.insert(bound, count.into());
        }
        out.insert(
            stage.name.to_owned(),
            serde_json::json!({
                "count": stage.count.load(Ordering::Relaxed),
                "sum_us": stage.sum_us.load(Ordering::Relaxed),
                "max_us": stage.max_us.load(Ordering::Relaxed),
                "le_us": buckets,
            }),
        );
    }
    serde_json::Value::Object(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn perf1_stage_buckets_are_power_of_two_microseconds() {
        let stage = stage("test.perf1.buckets");
        stage.record(Duration::from_micros(0));
        stage.record(Duration::from_micros(1));
        stage.record(Duration::from_micros(3));
        stage.record(Duration::from_millis(40));
        let snapshot = snapshot();
        let entry = &snapshot["test.perf1.buckets"];
        assert_eq!(entry["count"], 4);
        assert_eq!(entry["sum_us"], 40_004);
        assert_eq!(entry["max_us"], 40_000);
        // 0 us < 1; 1 us < 2; 3 us < 4; 40,000 us < 65,536.
        assert_eq!(entry["le_us"]["1"], 1);
        assert_eq!(entry["le_us"]["2"], 1);
        assert_eq!(entry["le_us"]["4"], 1);
        assert_eq!(entry["le_us"]["65536"], 1);
        assert!(std::ptr::eq(stage, super::stage("test.perf1.buckets")));
    }

    #[test]
    fn perf2c_site_stage_names_the_call_site() {
        let here = std::panic::Location::caller();
        let stage = site("test.perf2c.site.", here);
        let expected = format!("test.perf2c.site.src/stage_timing.rs:{}", here.line());
        assert_eq!(stage.name, expected);
        assert!(std::ptr::eq(stage, site("test.perf2c.site.", here)));
    }
}
