//! Synchronous API routing observes accepted work without submitting it again.
use super::{Result, route_failure, unknown};
use crate::{
    api::response::{EncodedResponse, encode_error},
    node::http::invalid,
    protocol,
    request_queue::{Receipt, RequestRecord, RequestState},
    storage::context::RequestContext,
};
use std::{future::Future, time::Duration};
use tokio::time::{Instant, timeout_at};
use uuid::Uuid;

pub(super) trait QueueAccess {
    fn submit(
        &self,
        body: Vec<u8>,
        context: Option<RequestContext>,
    ) -> impl Future<Output = Result<RequestRecord>> + Send;
    fn lookup(
        &self,
        run_id: Uuid,
        request_id: Uuid,
        context: Option<RequestContext>,
    ) -> impl Future<Output = Result<Receipt>> + Send;
}

// The first failed observation starts a recovery window. Healthy pending
// receipts reset it, so normal queue duration and result retention are unchanged.
const MAX_TRANSIENT_FAILURES: usize = 3;
const RECOVERY_WINDOW: Duration = Duration::from_secs(5);
fn transient(code: protocol::ErrorCode) -> bool {
    matches!(
        code,
        protocol::ErrorCode::StorageUnavailable
            | protocol::ErrorCode::Overloaded
            | protocol::ErrorCode::OutcomeUnknown
    )
}

pub(super) async fn route<Q: QueueAccess, S>(
    queue: &Q,
    bytes: Vec<u8>,
    context: Option<RequestContext>,
    staging: S,
) -> Result<EncodedResponse> {
    let parsed = protocol::parse_request(&bytes)?;
    let id = parsed.request_id;
    let mutation = parsed.operation.is_mutation();
    drop(parsed);
    let record = match queue.submit(bytes, context.clone()).await {
        Ok(record) => record,
        Err(error) => return Ok(route_failure(id, mutation, false, error)),
    };
    let run_id = record.run_id;
    let request_id = record.request_id;
    drop(record);
    drop(staging);
    let mut failures = 0usize;
    let mut recovery_end = None;
    loop {
        // QueueAccess::lookup reloads authority and signs a new exchange each
        // time. This helper has no path back to Submit after acceptance.
        let observation = if let Some(end) = recovery_end {
            if Instant::now() >= end {
                return Ok(encode_error(Some(id), unknown()));
            }
            match timeout_at(end, queue.lookup(run_id, request_id, context.clone())).await {
                Ok(result) if Instant::now() < end => result,
                _ => return Ok(encode_error(Some(id), unknown())),
            }
        } else {
            queue.lookup(run_id, request_id, context.clone()).await
        };
        let receipt = match observation {
            Ok(receipt) => receipt,
            Err(error) => {
                if !transient(error.code) {
                    return Ok(route_failure(id, mutation, true, error));
                }
                failures += 1;
                if failures >= MAX_TRANSIENT_FAILURES {
                    return Ok(encode_error(Some(id), unknown()));
                }
                let end = *recovery_end.get_or_insert_with(|| Instant::now() + RECOVERY_WINDOW);
                let backoff = Duration::from_millis(50 * (1u64 << (failures - 1)));
                if Instant::now() + backoff >= end {
                    return Ok(encode_error(Some(id), unknown()));
                }
                tokio::time::sleep(backoff).await;
                continue;
            }
        };
        if receipt.run_id != run_id || receipt.request_id != request_id {
            return Ok(route_failure(id, mutation, true, invalid()));
        }
        failures = 0;
        recovery_end = None;
        if receipt.state == RequestState::Abandoned {
            return Ok(encode_error(Some(id), unknown()));
        }
        if receipt.state.terminal() {
            return Ok(if let Some(error) = receipt.error {
                encode_error(Some(id), error)
            } else {
                let body =
                    serde_json::to_vec(&serde_json::json!({"request_id":id,"data":receipt.result}))
                        .map_err(|_| invalid())?;
                EncodedResponse { status: 200, body }
            });
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        node::http::failure,
        protocol::ErrorCode,
        request_queue::{QueueLimits, RequestQueue},
    };
    use std::{
        collections::VecDeque,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };
    struct IngressCharge(Arc<AtomicBool>);
    impl Drop for IngressCharge {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    struct LookupCharge(Arc<AtomicUsize>);
    impl Drop for LookupCharge {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    struct Script {
        accepted: RequestRecord,
        submissions: Mutex<Vec<(Vec<u8>, Option<RequestContext>)>>,
        observations: Mutex<Vec<(Uuid, Uuid, Option<RequestContext>, Instant)>>,
        replies: Mutex<VecDeque<(Duration, Result<Receipt>)>>,
        released: Arc<AtomicBool>,
        active: Arc<AtomicUsize>,
    }
    impl QueueAccess for Script {
        async fn submit(
            &self,
            body: Vec<u8>,
            context: Option<RequestContext>,
        ) -> Result<RequestRecord> {
            self.submissions.lock().unwrap().push((body, context));
            Ok(self.accepted.clone())
        }
        async fn lookup(
            &self,
            run: Uuid,
            id: Uuid,
            context: Option<RequestContext>,
        ) -> Result<Receipt> {
            assert!(
                self.released.load(Ordering::SeqCst),
                "ingress charge must be released before observation"
            );
            self.observations
                .lock()
                .unwrap()
                .push((run, id, context, Instant::now()));
            self.active.fetch_add(1, Ordering::SeqCst);
            let _charge = LookupCharge(self.active.clone());
            let (delay, result) = self
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected extra observation");
            tokio::time::sleep(delay).await;
            result
        }
    }
    fn fixture() -> (Script, Vec<u8>, RequestContext) {
        fixture_for(true)
    }
    fn fixture_for(mutation: bool) -> (Script, Vec<u8>, RequestContext) {
        let id = Uuid::new_v4();
        let member = Uuid::new_v4();
        let context:RequestContext=serde_json::from_value(serde_json::json!({"version":1,"bucket":"tenant-data","region":"eu-west-1","prefix":"db/","credentials":{"access_key_id":"accepted","secret_access_key":"fixture-secret","session_token":null,"expires_at":null}})).unwrap();
        let body=serde_json::to_vec(&serde_json::json!({"request_id":id,"operation":"table.create","payload":{"name":"original","schema":{"id":"int64"},"key":"id","format":"parquet"}})).unwrap();
        let body = if mutation {
            body
        } else {
            serde_json::to_vec(&serde_json::json!({"request_id":id,"operation":"table.get","payload":{"table_id":Uuid::new_v4(),"key":1}})).unwrap()
        };
        let mut queue = RequestQueue::new(
            Uuid::new_v4(),
            1,
            1,
            QueueLimits {
                max_pending_requests: 4,
                max_queue_bytes_per_replica: 1 << 20,
                max_retained_result_bytes_per_worker: 1 << 20,
                result_retention_ms: 1000,
            },
            [member].into(),
        )
        .unwrap();
        let record = queue
            .propose_with_context(body.clone(), Some(context.clone()), 0)
            .unwrap();
        let accepted = queue.acknowledge(record.identity(), member).unwrap();
        (
            Script {
                accepted,
                submissions: Mutex::new(Vec::new()),
                observations: Mutex::new(Vec::new()),
                replies: Mutex::new(VecDeque::new()),
                released: Arc::new(AtomicBool::new(false)),
                active: Arc::new(AtomicUsize::new(0)),
            },
            body,
            context,
        )
    }
    fn terminal(script: &Script) -> Receipt {
        Receipt {
            run_id: script.accepted.run_id,
            request_id: script.accepted.request_id,
            state: RequestState::Succeeded,
            result: Some(serde_json::json!({"original":17})),
            error: None,
        }
    }
    fn assert_once(script: &Script, body: &[u8], context: &RequestContext, observations: usize) {
        assert_eq!(
            *script.submissions.lock().unwrap(),
            vec![(body.to_vec(), Some(context.clone()))]
        );
        assert_eq!(
            script.active.load(Ordering::SeqCst),
            0,
            "observation ownership must settle even at the recovery deadline"
        );
        let calls = script.observations.lock().unwrap();
        assert_eq!(calls.len(), observations);
        for (run, id, grant, _) in calls.iter() {
            assert_eq!(
                (*run, *id),
                (script.accepted.run_id, script.accepted.request_id)
            );
            assert_eq!(grant.as_ref(), Some(context));
        }
    }
    #[tokio::test(start_paused = true)]
    async fn q04_transient_streak_has_exact_bound_backoff_and_unknown_exhaustion() {
        for code in [
            ErrorCode::StorageUnavailable,
            ErrorCode::Overloaded,
            ErrorCode::OutcomeUnknown,
        ] {
            let (script, body, context) = fixture();
            let began = Instant::now();
            script
                .replies
                .lock()
                .unwrap()
                .extend((0..3).map(|_| (Duration::ZERO, Err(failure(code)))));
            let response = route(
                &script,
                body.clone(),
                Some(context.clone()),
                IngressCharge(script.released.clone()),
            )
            .await
            .unwrap();
            assert_eq!(response.status, 503);
            let value: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
            assert_eq!(value["error"]["outcome"], "unknown");
            assert_eq!(value["request_id"], script.accepted.request_id.to_string());
            assert_once(&script, &body, &context, 3);
            let calls = script.observations.lock().unwrap();
            assert_eq!(calls[0].3, began);
            assert_eq!(calls[1].3 - began, Duration::from_millis(50));
            assert_eq!(calls[2].3 - began, Duration::from_millis(150));
        }
    }
    #[tokio::test(start_paused = true)]
    async fn q04_observation_deadline_cancels_slow_recovery_without_resubmission() {
        let (script, body, context) = fixture();
        let began = Instant::now();
        script.replies.lock().unwrap().extend([
            (Duration::ZERO, Err(failure(ErrorCode::StorageUnavailable))),
            (Duration::from_secs(30), Ok(terminal(&script))),
        ]);
        let response = route(
            &script,
            body.clone(),
            Some(context.clone()),
            IngressCharge(script.released.clone()),
        )
        .await
        .unwrap();
        assert_eq!(response.status, 503);
        assert_eq!(Instant::now() - began, RECOVERY_WINDOW);
        assert_once(&script, &body, &context, 2);
    }
    #[tokio::test(start_paused = true)]
    async fn u02_q06_nontransient_malformed_and_foreign_receipts_stop_immediately() {
        for code in [
            ErrorCode::Unauthorized,
            ErrorCode::ValidationError,
            ErrorCode::Conflict,
            ErrorCode::NotFound,
            ErrorCode::LimitExceeded,
            ErrorCode::UnsupportedOperation,
            ErrorCode::InternalError,
        ] {
            let (script, body, context) = fixture();
            script
                .replies
                .lock()
                .unwrap()
                .push_back((Duration::ZERO, Err(failure(code))));
            let response = route(
                &script,
                body.clone(),
                Some(context.clone()),
                IngressCharge(script.released.clone()),
            )
            .await
            .unwrap();
            assert_eq!(response.status, 503);
            assert_once(&script, &body, &context, 1);
        }
        let (script, body, context) = fixture();
        let mut foreign = terminal(&script);
        foreign.run_id = Uuid::new_v4();
        script
            .replies
            .lock()
            .unwrap()
            .push_back((Duration::ZERO, Ok(foreign)));
        assert_eq!(
            route(
                &script,
                body.clone(),
                Some(context.clone()),
                IngressCharge(script.released.clone())
            )
            .await
            .unwrap()
            .status,
            503
        );
        assert_once(&script, &body, &context, 1);
    }
    #[tokio::test(start_paused = true)]
    async fn q04_q06_healthy_pending_resets_streak_without_shortening_queue_wait_or_retrying_terminal_errors()
     {
        let (script, body, context) = fixture();
        let began = Instant::now();
        let mut pending = terminal(&script);
        pending.state = RequestState::Running;
        pending.result = None;
        script.replies.lock().unwrap().extend([
            (Duration::ZERO, Err(failure(ErrorCode::Overloaded))),
            (Duration::ZERO, Ok(pending.clone())),
            (Duration::from_secs(20), Ok(pending)),
            (Duration::ZERO, Err(failure(ErrorCode::OutcomeUnknown))),
            (Duration::ZERO, Ok(terminal(&script))),
        ]);
        let response = route(
            &script,
            body.clone(),
            Some(context.clone()),
            IngressCharge(script.released.clone()),
        )
        .await
        .unwrap();
        assert_eq!(response.status, 200);
        assert!(Instant::now() - began > RECOVERY_WINDOW);
        assert_once(&script, &body, &context, 5);
        let (script, body, context) = fixture();
        let mut failed = terminal(&script);
        failed.state = RequestState::Failed;
        failed.result = None;
        failed.error = Some(failure(ErrorCode::Overloaded));
        script
            .replies
            .lock()
            .unwrap()
            .push_back((Duration::ZERO, Ok(failed)));
        assert_eq!(
            route(
                &script,
                body.clone(),
                Some(context.clone()),
                IngressCharge(script.released.clone())
            )
            .await
            .unwrap()
            .status,
            429
        );
        assert_once(&script, &body, &context, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn q04_t04_transient_receipt_observation_never_resubmits() {
        for mutation in [false, true] {
            let (script, body, context) = fixture_for(mutation);
            let original = terminal(&script);
            script.replies.lock().unwrap().extend([
                (Duration::ZERO, Err(failure(ErrorCode::StorageUnavailable))),
                (Duration::ZERO, Ok(original.clone())),
            ]);
            let response = route(
                &script,
                body.clone(),
                Some(context.clone()),
                IngressCharge(script.released.clone()),
            )
            .await
            .unwrap();
            assert_eq!(
                response.status, 200,
                "a transient receipt error must recover the accepted result"
            );
            let value: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
            assert_eq!(value["data"], original.result.unwrap());
            assert_eq!(value["request_id"], original.request_id.to_string());
            assert_eq!(
                *script.submissions.lock().unwrap(),
                vec![(body, Some(context.clone()))]
            );
            let observations = script.observations.lock().unwrap();
            assert_eq!(observations.len(), 2);
            for (run, id, grant, _) in observations.iter() {
                assert_eq!((*run, *id), (original.run_id, original.request_id));
                assert_eq!(grant.as_ref(), Some(&context));
            }
        }
    }
}
