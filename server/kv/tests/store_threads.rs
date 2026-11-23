//! The store's own threads and the memory a completed write still holds (RV-NS6 F8, F9).
#![allow(clippy::disallowed_methods)] // integration tests are outside the phase 7 conversion, which covers kv/src

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

use common::*;
use nabedra_kv::{Background, Durability, Store, THREAD_STACK_BYTES, WriteBatch};

/// Records when the allocation at `WATCHED` is freed.
struct Watching;
static WATCHED: AtomicUsize = AtomicUsize::new(0);
static WATCHED_FREED: AtomicU8 = AtomicU8::new(0);

unsafe impl GlobalAlloc for Watching {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let watched = WATCHED.load(Ordering::SeqCst);
        if watched != 0 && ptr as usize == watched {
            WATCHED_FREED.store(1, Ordering::SeqCst);
        }
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: Watching = Watching;

/// The pool's batch charge, as `on_done`: records whether the batch buffer it
/// accounts for was already freed when the charge is released.
struct Charge(Arc<AtomicU8>);
impl Drop for Charge {
    fn drop(&mut self) {
        let freed = WATCHED_FREED.load(Ordering::SeqCst) == 1;
        self.0.store(if freed { 1 } else { 2 }, Ordering::SeqCst);
    }
}

/// M02 (RV-NS6 F8): the journal frees a written batch's buffer before it drops
/// `on_done`, which holds the charge for those bytes. A committer woken by the
/// release can build its next batch without the old buffer still allocated.
#[test]
fn m02_o05_a_released_batch_charge_has_freed_its_buffer() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(1).unwrap();
    let store = open(&dir, small_options(Arc::new(fs.clone()), bg.clone()));
    for (n, durability) in [Durability::NoSync, Durability::Sync]
        .into_iter()
        .enumerate()
    {
        let mut batch = WriteBatch::with_capacity_bytes(64 << 10);
        batch.set_scope(b"g01/");
        batch.put(format!("g01/k{n}").as_bytes(), &[7u8; 32 << 10]);
        WATCHED_FREED.store(0, Ordering::SeqCst);
        WATCHED.store(batch.data().as_ptr() as usize, Ordering::SeqCst);
        let seen = Arc::new(AtomicU8::new(0));
        store
            .submit(batch, durability, Box::new(Charge(seen.clone())))
            .unwrap()
            .wait()
            .unwrap();
        WATCHED.store(0, Ordering::SeqCst);
        assert_eq!(
            seen.load(Ordering::SeqCst),
            1,
            "write {n}: the charge was released while the batch buffer was still allocated"
        );
    }
}

const CHILD: &str = "NABEDRA_KV_STACK_CHILD";

/// Stack size of the calling thread, as the OS reports it.
#[cfg(target_os = "macos")]
fn current_stack_bytes() -> usize {
    unsafe { libc::pthread_get_stacksize_np(libc::pthread_self()) }
}
#[cfg(target_os = "linux")]
fn current_stack_bytes() -> usize {
    unsafe {
        let mut attr: libc::pthread_attr_t = std::mem::zeroed();
        assert_eq!(libc::pthread_getattr_np(libc::pthread_self(), &mut attr), 0);
        let mut size = 0;
        assert_eq!(libc::pthread_attr_getstacksize(&attr, &mut size), 0);
        libc::pthread_attr_destroy(&mut attr);
        size
    }
}

/// Runs on the journal thread: `on_done` drops there.
struct StackProbe(Arc<AtomicUsize>);
impl Drop for StackProbe {
    fn drop(&mut self) {
        self.0.store(current_stack_bytes(), Ordering::SeqCst);
    }
}

/// The child's workload: flushes, compactions, segment rolls and journal GC on the
/// store's threads, with `RUST_MIN_STACK` far below what they need.
fn stack_child() {
    let (fs, dir) = fault_fs();
    let bg = Background::new(2).unwrap();
    let store = open(&dir, small_options(Arc::new(fs.clone()), bg.clone()));
    for k in 0..2_000u64 {
        let mut batch = WriteBatch::new();
        let group = k % 4;
        batch.set_scope(&group_prefix(group));
        batch.put(&group_key(group, k), &[k as u8; 300]);
        store
            .submit(batch, Durability::NoSync, Box::new(()))
            .unwrap()
            .wait()
            .unwrap();
    }
    let journal_stack = Arc::new(AtomicUsize::new(0));
    let mut batch = WriteBatch::new();
    batch.set_scope(&group_prefix(1));
    batch.put(&group_key(1, 9_999), b"last");
    store
        .submit(
            batch,
            Durability::Sync,
            Box::new(StackProbe(journal_stack.clone())),
        )
        .unwrap()
        .wait()
        .unwrap();
    store.flush().unwrap();
    store.compact().unwrap();
    let _ = store.gc_journal().unwrap();
    let snapshot = store.snapshot();
    for k in (0..2_000u64).step_by(97) {
        let group = k % 4;
        assert_eq!(
            snapshot
                .get(&group_key(group, k))
                .unwrap()
                .unwrap()
                .as_ref(),
            [k as u8; 300]
        );
    }
    drop(snapshot);
    store.close().unwrap();
    let journal_stack = journal_stack.load(Ordering::SeqCst);
    assert!(
        (THREAD_STACK_BYTES..THREAD_STACK_BYTES + (1 << 20)).contains(&journal_stack),
        "kv-journal stack is {journal_stack} bytes"
    );
    let reopened = Store::open(&dir, small_options(Arc::new(fs.kill()), bg)).unwrap();
    assert_eq!(
        reopened
            .store
            .snapshot()
            .get(&group_key(1, 9_999))
            .unwrap()
            .unwrap()
            .as_ref(),
        b"last"
    );
    println!("stack child ok");
}

/// O05 (RV-NS6 F9): every store thread has a `THREAD_STACK_BYTES` stack, the
/// size the server's native thread budget reserves, whatever `RUST_MIN_STACK`
/// says. The child runs the store's flush, compaction and journal threads with
/// `RUST_MIN_STACK` at 16 KiB and measures the journal thread's stack.
#[cfg(any(target_os = "macos", target_os = "linux"))]
#[test]
fn o05_store_threads_have_their_own_stack_whatever_rust_min_stack_says() {
    if std::env::var_os(CHILD).is_some() {
        // The test harness thread has the tiny stack too: run on a thread of our own.
        std::thread::Builder::new()
            .stack_size(16 << 20)
            .spawn(stack_child)
            .unwrap()
            .join()
            .unwrap();
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "o05_store_threads_have_their_own_stack_whatever_rust_min_stack_says",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, "1")
        .env("RUST_MIN_STACK", "16384")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success() && stdout.contains("stack child ok"),
        "child failed ({:?})\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status
    );
}

/// O05 (RV-NS6 F9): a background thread the OS cannot start is an error from
/// `Background::new`, not a panic inside the worker's resource setup.
#[test]
fn o05_a_background_thread_that_cannot_start_is_an_error() {
    let result = std::panic::catch_unwind(|| Background::with_stack(3, 1 << 62));
    let result = result.expect("Background::with_stack panicked");
    assert!(result.is_err(), "{result:?}");
    // The normal size still starts.
    assert_eq!(Background::new(3).unwrap().threads(), 3);
}
