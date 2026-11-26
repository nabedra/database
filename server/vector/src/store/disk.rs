//! Page I/O on one file: io_uring, or positioned reads on a thread pool where io_uring is not
//! available or is blocked (a container's seccomp profile). The choice is made at run time, and
//! both behave alike: a request moves all of its bytes or fails, and a short read is an error.

use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::path::Path;
use std::sync::mpsc;
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;

use nabedra_rt::fs::FsFile;
use nabedra_rt::fs::os;
use nabedra_rt::fs::uring::{Op, Ring};
use nabedra_rt::sync::Condvar;
use nabedra_rt::task;

use super::pages::Pages;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Backend {
    /// io_uring when the kernel lets us, otherwise positioned reads.
    Auto,
    Uring,
    Pread,
}

/// One read: whole pages at a page-aligned offset.
#[derive(Debug)]
pub struct Req {
    pub offset: u64,
    pub buf: Pages,
    pub result: io::Result<()>,
}

impl Req {
    pub fn new(offset: u64, pages: usize) -> Req {
        Req {
            offset,
            buf: Pages::new(pages),
            result: Ok(()),
        }
    }
}

pub trait Disk: Send + Sync + fmt::Debug {
    /// `"io_uring"` or `"pread"`.
    fn kind(&self) -> &'static str;
    /// Fills every request, and sets its result.
    fn read(&self, reqs: &mut [Req]);
    fn write(&self, offset: u64, buf: &[u8]) -> io::Result<()>;
}

fn short() -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, "short read")
}

/// A short read reads the same on both backends.
fn same(e: io::Error) -> io::Error {
    if e.kind() == io::ErrorKind::UnexpectedEof {
        short()
    } else {
        e
    }
}

pub struct UringDisk {
    ring: Ring,
    file: os::File,
}

impl fmt::Debug for UringDisk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("UringDisk")
    }
}

impl UringDisk {
    /// Finishes what a short completion left, one operation at a time.
    fn rest(&self, mut op: Op<'_>, mut done: usize) -> io::Result<()> {
        loop {
            let (len, next) = match &mut op {
                Op::Read { buf, offset } => (
                    buf.len(),
                    Op::Read {
                        buf: &mut buf[done..],
                        offset: *offset + done as u64,
                    },
                ),
                Op::Write { buf, offset } => (
                    buf.len(),
                    Op::Write {
                        buf: &buf[done..],
                        offset: *offset + done as u64,
                    },
                ),
            };
            if done >= len {
                return Ok(());
            }
            let write = matches!(next, Op::Write { .. });
            let mut ops = [next];
            let n = self.ring.run(&self.file, &mut ops).pop().unwrap_or(Ok(0))?;
            if n == 0 {
                return Err(if write {
                    io::ErrorKind::WriteZero.into()
                } else {
                    short()
                });
            }
            done += n;
        }
    }
}

impl Disk for UringDisk {
    fn kind(&self) -> &'static str {
        "io_uring"
    }

    fn read(&self, reqs: &mut [Req]) {
        let mut ops: Vec<Op<'_>> = reqs
            .iter_mut()
            .map(|r| Op::Read {
                buf: r.buf.bytes_mut(),
                offset: r.offset,
            })
            .collect();
        let results = self.ring.run(&self.file, &mut ops);
        drop(ops);
        for (r, res) in reqs.iter_mut().zip(results) {
            r.result = match res {
                Ok(n) if n == r.buf.len() => Ok(()),
                Ok(n) => {
                    let op = Op::Read {
                        buf: r.buf.bytes_mut(),
                        offset: r.offset,
                    };
                    self.rest(op, n)
                }
                Err(e) => Err(e),
            };
        }
    }

    fn write(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        let mut ops = [Op::Write { buf, offset }];
        let n = self.ring.run(&self.file, &mut ops).pop().unwrap_or(Ok(0))?;
        self.rest(Op::Write { buf, offset }, n)
    }
}

type Work = Box<dyn FnOnce() + Send>;

struct PoolInner {
    jobs: Mutex<(VecDeque<Work>, bool)>,
    wake: Condvar,
}

/// Threads that run positioned reads for one file.
struct Pool {
    inner: Arc<PoolInner>,
    threads: Vec<JoinHandle<()>>,
}

impl Pool {
    fn new(threads: usize, name: &str) -> io::Result<Pool> {
        let inner = Arc::new(PoolInner {
            jobs: Mutex::new((VecDeque::new(), false)),
            wake: Condvar::new(),
        });
        let mut pool = Pool {
            inner,
            threads: Vec::new(),
        };
        for i in 0..threads {
            let inner = pool.inner.clone();
            let t = task::spawn_service(format!("{name}-{i}"), move || work(&inner))?;
            pool.threads.push(t);
        }
        Ok(pool)
    }

    fn run(&self, job: Work) {
        let mut jobs = self.inner.jobs.lock().unwrap_or_else(PoisonError::into_inner);
        jobs.0.push_back(job);
        drop(jobs);
        self.inner.wake.notify_one();
    }
}

fn work(inner: &PoolInner) {
    loop {
        let jobs = inner.jobs.lock().unwrap_or_else(PoisonError::into_inner);
        let mut jobs = inner
            .wake
            .wait_while(jobs, |j| j.0.is_empty() && !j.1)
            .unwrap_or_else(PoisonError::into_inner);
        let Some(job) = jobs.0.pop_front() else {
            return;
        };
        drop(jobs);
        job();
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        let mut jobs = self.inner.jobs.lock().unwrap_or_else(PoisonError::into_inner);
        jobs.1 = true;
        drop(jobs);
        self.inner.wake.notify_all();
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

/// Positioned reads and writes; a batch of reads runs on the pool.
pub struct PreadDisk {
    file: Arc<dyn FsFile>,
    pool: Option<Pool>,
}

impl fmt::Debug for PreadDisk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PreadDisk")
    }
}

impl PreadDisk {
    /// `threads` 0 reads on the caller's thread.
    pub fn new(file: Arc<dyn FsFile>, threads: usize) -> io::Result<PreadDisk> {
        let pool = (threads > 0)
            .then(|| Pool::new(threads, "vector-pread"))
            .transpose()?;
        Ok(PreadDisk { file, pool })
    }
}

impl Disk for PreadDisk {
    fn kind(&self) -> &'static str {
        "pread"
    }

    fn read(&self, reqs: &mut [Req]) {
        let Some(pool) = self.pool.as_ref().filter(|_| reqs.len() > 1) else {
            for r in reqs {
                r.result = self.file.read_exact_at(r.buf.bytes_mut(), r.offset).map_err(same);
            }
            return;
        };
        let (tell, told) = mpsc::channel();
        for (i, r) in reqs.iter_mut().enumerate() {
            let (file, tell) = (self.file.clone(), tell.clone());
            let (offset, mut buf) = (r.offset, std::mem::take(&mut r.buf));
            pool.run(Box::new(move || {
                let result = file.read_exact_at(buf.bytes_mut(), offset).map_err(same);
                let _ = tell.send((i, buf, result));
            }));
            r.result = Err(io::Error::other("the read did not run"));
        }
        drop(tell);
        for (i, buf, result) in told.iter() {
            if let Some(r) = reqs.get_mut(i) {
                (r.buf, r.result) = (buf, result);
            }
        }
    }

    fn write(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        self.file.write_all_at(buf, offset)
    }
}

/// An `os::File` as an [`FsFile`], for positioned reads of a real file.
struct OsFile(os::File);

impl FsFile for OsFile {
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.0.write_all_at(buf, offset)
    }
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.0.read_exact_at(buf, offset)
    }
    fn sync_data(&self) -> io::Result<()> {
        self.0.sync_data()
    }
    fn sync_all(&self) -> io::Result<()> {
        self.0.sync_all()
    }
    fn preallocate(&self, _len: u64) -> io::Result<()> {
        Ok(())
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.0.set_len(len)
    }
    fn len(&self) -> io::Result<u64> {
        Ok(self.0.metadata()?.len())
    }
}

/// Creates or empties a file for page I/O, with `O_DIRECT` where the filesystem takes it (tmpfs
/// does not), and the backend that `backend` asks for and the kernel allows.
pub fn open(path: &Path, backend: Backend, threads: usize) -> io::Result<Arc<dyn Disk>> {
    let open = |direct: bool| {
        let mut o = os::OpenOptions::new();
        o.read(true).write(true).create(true).truncate(true);
        #[cfg(target_os = "linux")]
        if direct {
            o.custom_flags(os::O_DIRECT);
        }
        let _ = direct;
        o.open(path)
    };
    let file = match open(true) {
        Err(e) if e.raw_os_error() == Some(22) => open(false)?,
        other => other?,
    };
    if backend != Backend::Pread {
        match Ring::new(256) {
            Ok(ring) => return Ok(Arc::new(UringDisk { ring, file })),
            Err(e) if backend == Backend::Uring => return Err(e),
            Err(_) => {}
        }
    }
    Ok(Arc::new(PreadDisk::new(Arc::new(OsFile(file)), threads)?))
}
