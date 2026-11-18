use super::*;
use serde::Serialize;

#[cfg(test)]
mod tests;

#[derive(Serialize)]
struct Report<'a, T> {
    worker: &'a WorkerIdentity,
    statistics: T,
}

impl WorkerRuntime {
    pub(in crate::oltp_runtime) async fn query_statistics_admin(
        self: Arc<Self>,
        request_id: Uuid,
    ) -> Result<EncodedResponse, ProtocolError> {
        let shared = self.resources().clone();
        let owner = shared.reserve_scratch(ByteClass::Data, 256 << 10)?;
        shared
            .execute(ExecutionClass::Control, 0, 0, move || {
                let _owner = owner;
                let report = Report {
                    worker: &self.services.local,
                    statistics: self.planner.query_statistics(),
                };
                Ok(response::encode_success(request_id, &report, false))
            })
            .await
    }
}
