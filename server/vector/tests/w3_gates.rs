//! Busy-page read regression adapted from the W3 gates test in 25bc39ba.
//! The writer runs on a separate thread so the final read can hold the file lock.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::thread::{JoinHandle, ThreadId};

use common::*;
use nabedra_vector::store::{Backend, Disk, NvmeVectors, Req, Vectors, open};

/// A disk that, before each read of the reader thread, runs `hook` on a writer thread and waits
/// up to 300 ms for it. A writer that the reader itself holds back is not waited for longer.
struct Busy {
    inner: Arc<dyn Disk>,
    hook: OnceLock<Arc<dyn Fn() + Send + Sync>>,
    reader: Mutex<Option<ThreadId>>,
    writers: Mutex<Vec<JoinHandle<()>>>,
}

impl std::fmt::Debug for Busy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Busy")
    }
}

impl Disk for Busy {
    fn kind(&self) -> &'static str {
        self.inner.kind()
    }
    fn read(&self, reqs: &mut [Req]) {
        let me = Some(std::thread::current().id());
        let hook = self
            .hook
            .get()
            .filter(|_| *self.reader.lock().unwrap() == me);
        if let Some(hook) = hook.cloned() {
            let (done, wait) = std::sync::mpsc::channel();
            let writer = spawn_thread(move || {
                hook();
                let _ = done.send(());
            });
            let _ = wait.recv_timeout(std::time::Duration::from_millis(300));
            self.writers.lock().unwrap().push(writer);
        }
        self.inner.read(reqs)
    }
    fn write(&self, offset: u64, buf: &[u8]) -> std::io::Result<()> {
        self.inner.write(offset, buf)
    }
}

/// Found by the 50x repeat gate of `nt27` (49/50, W3 gates): a read of a record that never changes
/// fails with "a page kept changing" when a neighbour on its page is rewritten between each of its
/// four read rounds. Here the neighbour is rewritten, in place, before every read, so no optimistic
/// round can read the stable record: a reader must still get the exact vector, however busy its
/// page is. The rewrite runs on another thread, as a writer does: run on the reader's thread it
/// would wait for a lock the reader holds.
#[test]
fn nt27_a_stable_record_is_read_exactly_while_its_page_is_rewritten_before_every_read() {
    // 512 floats are 2 KiB: two records to a page.
    let dim = 512usize;
    let tmp = Tmp::new("w3-busy-page");
    let busy = Arc::new(Busy {
        inner: open(&tmp.join("raw-0"), Backend::Pread, 2).unwrap(),
        hook: OnceLock::new(),
        reader: Mutex::new(None),
        writers: Mutex::new(Vec::new()),
    });
    let s = Arc::new(NvmeVectors::on(vec![busy.clone() as Arc<dyn Disk>], dim).unwrap());
    s.put(key(0, 1), &vector(dim, 0)).unwrap();
    s.put(key(1, 1), &vector(dim, 1)).unwrap();
    s.flush().unwrap();
    let (weak, version) = (Arc::downgrade(&s), Arc::new(AtomicU64::new(1)));
    let (moved, one) = (version.clone(), Arc::new(Mutex::new(())));
    let _ = busy.hook.set(Arc::new(move || {
        let _one = one.lock().unwrap();
        let Some(s) = Weak::upgrade(&weak) else {
            return;
        };
        let v = moved.fetch_add(1, Ordering::SeqCst);
        // The released place is reused by the next put: the neighbour stays on the page.
        s.release(key(1, v));
        s.put(key(1, v + 1), &vector(dim, v + 1)).unwrap();
        s.flush().unwrap();
    }));
    let (reader, disk) = (s.clone(), busy.clone());
    let got = within(10, "a read of a stable record", move || {
        *disk.reader.lock().unwrap() = Some(std::thread::current().id());
        let got = reader.get(key(0, 1));
        *disk.reader.lock().unwrap() = None;
        got
    });
    for writer in std::mem::take(&mut *busy.writers.lock().unwrap()) {
        writer.join().unwrap();
    }
    assert!(
        matches!(&got, Ok(v) if bits(v) == bits(&vector(dim, 0))),
        "{:?}",
        got.err()
    );
    assert!(
        version.load(Ordering::SeqCst) >= 3,
        "the page changed under the reads"
    );
}
