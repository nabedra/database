//! Observe the applied position validated by a continuation's local read.
use nabedra_oltp_types::types::AppliedPosition;
use std::future::Future;

tokio::task_local! {
    static OBSERVER: Box<dyn Fn(AppliedPosition) + Send>;
}

pub async fn observe<T>(
    observer: impl Fn(AppliedPosition) + Send + 'static,
    read: impl Future<Output = T>,
) -> T {
    OBSERVER.scope(Box::new(observer), read).await
}

pub(crate) fn applied(position: AppliedPosition) {
    let _ = OBSERVER.try_with(|observer| observer(position));
}
