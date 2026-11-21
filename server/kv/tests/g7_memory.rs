//! G7 (NS11) measurement: the memory a freshly opened store holds after a few synced
//! puts, read from the process's own cgroup v2 files. The `[ci g7]` step of the kv job
//! runs it in its own systemd scope, at the base commit and at the head, on the runner's
//! disk (not tmpfs). It uses only the public API, so the same file builds at both.
//!
//! `cargo test -p nabedra-kv --test g7_memory -- --ignored --nocapture`, with `G7_DIR`
//! naming a directory on a disk-backed filesystem.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use nabedra_kv::fs::RealFs;
use nabedra_kv::{Background, BlockCache, Durability, MemtableBudget, Options, Store, WriteBatch};

fn cgroup_dir() -> Option<PathBuf> {
    let line = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let path = line.lines().find_map(|l| l.strip_prefix("0::"))?;
    Some(PathBuf::from("/sys/fs/cgroup").join(path.trim_start_matches('/')))
}

fn read(dir: &std::path::Path, name: &str) -> String {
    std::fs::read_to_string(dir.join(name)).unwrap_or_default()
}

fn stat(dir: &std::path::Path, field: &str) -> u64 {
    read(dir, "memory.stat")
        .lines()
        .find_map(|l| {
            l.strip_prefix(field)?
                .strip_prefix(' ')?
                .trim()
                .parse()
                .ok()
        })
        .unwrap_or(0)
}

fn sample(dir: &std::path::Path) -> String {
    format!(
        "{{\"memory_current\":{},\"memory_peak\":{},\"anon\":{},\"file\":{}}}",
        read(dir, "memory.current").trim(),
        read(dir, "memory.peak").trim(),
        stat(dir, "anon"),
        stat(dir, "file"),
    )
}

#[test]
#[ignore = "measurement for the [ci g7] step; needs Linux cgroup v2"]
fn g7_measure_store_memory_after_open() {
    let Some(cgroup) = cgroup_dir().filter(|dir| dir.join("memory.peak").exists()) else {
        eprintln!("g7_memory skipped: no cgroup v2 memory.peak");
        return;
    };
    let root = std::env::var_os("G7_DIR").map_or_else(std::env::temp_dir, PathBuf::from);
    let dir = root.join(format!("g7-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let before = sample(&cgroup);
    let background = Background::new(2).unwrap();
    let options = Options::new(
        Arc::new(RealFs),
        Arc::new(BlockCache::new(16 << 20)),
        Arc::new(MemtableBudget::new(16 << 20)),
        background.clone(),
    );
    let store = Store::open(&dir, options).unwrap().store;
    for i in 0..200u32 {
        let mut batch = WriteBatch::new();
        batch.put(format!("g7-{i:05}").as_bytes(), &[b'v'; 100]);
        store
            .submit(batch, Durability::Sync, Box::new(()))
            .unwrap()
            .wait()
            .unwrap();
    }
    std::thread::sleep(Duration::from_secs(2));
    let after = sample(&cgroup);
    println!("g7_memory {{\"before\":{before},\"after\":{after}}}");
    drop(store);
    let _ = std::fs::remove_dir_all(&dir);
}
