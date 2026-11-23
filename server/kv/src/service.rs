//! Service mode is selected by the KV feature. Cargo may also enable runtime
//! simulation independently, such as when building the Events simulator.

#[cfg(feature = "sim")]
pub(crate) use nabedra_rt::{sync::Condvar, task::ServiceHandle};
#[cfg(not(feature = "sim"))]
pub(crate) use std::sync::Condvar;

#[cfg(not(feature = "sim"))]
#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct ServiceHandle(std::thread::JoinHandle<()>);

#[cfg(not(feature = "sim"))]
impl ServiceHandle {
    #[inline]
    pub(crate) fn join(self) -> std::thread::Result<()> {
        self.0.join()
    }

    #[inline]
    pub(crate) fn is_current(&self) -> bool {
        self.0.thread().id() == std::thread::current().id()
    }
}

#[cfg(not(feature = "sim"))]
#[inline]
pub(crate) fn spawn_thread(
    name: &str,
    stack_bytes: usize,
    run: impl FnOnce() + Send + 'static,
) -> std::io::Result<ServiceHandle> {
    nabedra_rt::task::spawn_service_with_stack(name, stack_bytes, run).map(ServiceHandle)
}
