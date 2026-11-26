//! io_uring for positioned reads and writes of an open [`os::File`], behind [`Ring`].
//!
//! A product build on Linux with the `uring` feature uses the `io-uring` crate. Elsewhere,
//! and under `sim`, [`Ring::new`] returns `Unsupported`, so callers fall back to positioned
//! reads and the simulator never sees a ring.
//!
//! A call submits its operations, waits for every completion the kernel owes and only then
//! returns, so the buffers it borrowed are never written after it. One ring serves one
//! caller at a time.

use super::os;

/// One operation of [`Ring::run`].
pub enum Op<'a> {
    Read { buf: &'a mut [u8], offset: u64 },
    Write { buf: &'a [u8], offset: u64 },
}

pub use imp::Ring;

/// Whether this build can use io_uring at all.
pub const BUILT: bool = cfg!(all(feature = "uring", target_os = "linux", not(feature = "sim")));

#[cfg(all(feature = "uring", target_os = "linux", not(feature = "sim")))]
mod imp {
    use std::io;
    use std::sync::{Mutex, PoisonError};

    use io_uring::{IoUring, opcode, types};

    use super::{Op, os};

    struct State {
        ring: IoUring,
        /// A submission failed with entries the kernel never took. They stay in the queue, so
        /// the ring is never entered again.
        broken: bool,
    }

    pub struct Ring {
        state: Mutex<State>,
    }

    fn retryable(e: &io::Error) -> bool {
        e.kind() == io::ErrorKind::Interrupted
            || matches!(e.raw_os_error(), Some(libc::EAGAIN | libc::EBUSY))
    }

    impl Ring {
        /// A ring with `entries` submission slots. Fails when the kernel has no io_uring or a
        /// seccomp profile blocks it; a no-op is submitted to find out.
        pub fn new(entries: u32) -> io::Result<Ring> {
            let mut ring = IoUring::new(entries.clamp(1, 4096))?;
            let nop = opcode::Nop::new().build().user_data(0);
            // SAFETY: a no-op names no memory.
            unsafe { ring.submission().push(&nop) }
                .map_err(|_| io::Error::other("the io_uring queue is full"))?;
            ring.submit_and_wait(1)?;
            match ring.completion().next().map(|c| c.result()) {
                Some(0) => {}
                Some(e) if e < 0 => return Err(io::Error::from_raw_os_error(-e)),
                _ => return Err(io::Error::other("io_uring did not complete a no-op")),
            }
            Ok(Ring {
                state: Mutex::new(State {
                    ring,
                    broken: false,
                }),
            })
        }

        /// Runs every operation on `file` and returns, per operation, the bytes it moved or its
        /// error. A count below the buffer's length is a short read or write.
        pub fn run(&self, file: &os::File, ops: &mut [Op<'_>]) -> Vec<io::Result<usize>> {
            let mut out: Vec<Option<io::Result<usize>>> = (0..ops.len()).map(|_| None).collect();
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let fd = types::Fd(file.raw_fd());
            let room = state.ring.params().sq_entries().max(1) as usize;
            let mut next = 0;
            while next < ops.len() && !state.broken {
                let end = ops.len().min(next + room);
                let mut pushed = 0;
                {
                    let mut sq = state.ring.submission();
                    for (i, op) in ops[next..end].iter_mut().enumerate() {
                        let entry = match op {
                            Op::Read { buf, offset } => u32::try_from(buf.len()).map(|len| {
                                opcode::Read::new(fd, buf.as_mut_ptr(), len)
                                    .offset(*offset)
                                    .build()
                            }),
                            Op::Write { buf, offset } => u32::try_from(buf.len()).map(|len| {
                                opcode::Write::new(fd, buf.as_ptr(), len)
                                    .offset(*offset)
                                    .build()
                            }),
                        };
                        let Ok(entry) = entry else {
                            out[next + i] = Some(Err(io::ErrorKind::InvalidInput.into()));
                            continue;
                        };
                        // SAFETY: the buffer is borrowed through `ops` for this whole call, and
                        // the call returns only after the kernel completed every entry it took;
                        // entries it did not take are never submitted (the ring is `broken`).
                        // The queue is empty here and holds `room` entries, so it has room.
                        let entry = entry.user_data((next + i) as u64);
                        if unsafe { sq.push(&entry) }.is_err() {
                            out[next + i] = Some(Err(io::Error::other("the io_uring queue is full")));
                            continue;
                        }
                        pushed += 1;
                    }
                }
                let mut done = 0;
                while done < pushed {
                    if let Err(e) = state.ring.submit_and_wait(pushed - done)
                        && !retryable(&e)
                    {
                        let waiting = state.ring.submission().len().min(pushed - done);
                        if pushed - done - waiting > 0 {
                            // The kernel holds pointers into borrowed buffers and cannot be
                            // waited for. Returning would let it write memory that is freed.
                            eprintln!("nabedra-rt: io_uring wait failed with I/O in flight: {e}");
                            std::process::abort();
                        }
                        state.broken = true;
                        break;
                    }
                    for cqe in state.ring.completion() {
                        let res = cqe.result();
                        if let Some(slot) = out.get_mut(cqe.user_data() as usize) {
                            *slot = Some(if res < 0 {
                                Err(io::Error::from_raw_os_error(-res))
                            } else {
                                Ok(res as usize)
                            });
                        }
                        done += 1;
                    }
                }
                next = end;
            }
            let failed = || io::Error::other("the io_uring instance failed earlier");
            out.into_iter().map(|r| r.unwrap_or_else(|| Err(failed()))).collect()
        }
    }
}

#[cfg(not(all(feature = "uring", target_os = "linux", not(feature = "sim"))))]
mod imp {
    use std::io;

    use super::{Op, os};

    /// io_uring is not built in, or this is a simulation: there is never a ring.
    pub struct Ring {
        never: std::convert::Infallible,
    }

    impl Ring {
        pub fn new(_entries: u32) -> io::Result<Ring> {
            Err(io::ErrorKind::Unsupported.into())
        }

        pub fn run(&self, _file: &os::File, _ops: &mut [Op<'_>]) -> Vec<io::Result<usize>> {
            match self.never {}
        }
    }
}
