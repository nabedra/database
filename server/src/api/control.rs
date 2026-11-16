//! Standalone fixed workers have no provisioning adapter. Reject administration
//! before reading or accepting request work, including while compute is busy.
use super::*;
pub(super) async fn unsupported<B: ObjectStore + 'static>(
    State(state): State<Arc<ApiState<B>>>,
    request: Request,
) -> Response {
    let started = Instant::now();
    if !authenticated(request.headers(), &state.token) {
        return failure(
            None,
            None,
            started,
            not_applied(ErrorCode::Unauthorized, "unauthorized"),
        );
    }
    let encoded=EncodedResponse{status:400,body:br#"{"request_id":null,"error":{"code":"unsupported_capability","message":"Standalone fixed workers have no provisioning adapter","outcome":"not_applied"}}"#.to_vec()};
    let mut response = complete(encoded, None, None, started, None);
    response
        .headers_mut()
        .insert(header::CONNECTION, HeaderValue::from_static("close"));
    response
}
