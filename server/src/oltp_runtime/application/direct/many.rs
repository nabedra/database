//! `table.upsert_many` and `table.delete_many` at the gateway
//! (`plans/oltp-vector.md`, section 2; VEC-G09).
//!
//! Every row is a request of its own: its id comes from the call's id and its
//! key, its body is the canonical `table.upsert` or `table.delete` of that
//! row. Rows are grouped by range into chunks, and a chunk goes to the range
//! leader as one `Method::Direct` request whose reply is one `DirectResponse`
//! per row. A row then follows the direct path's rules alone: it is resent
//! with its id after a lost reply, a stale route or a leader change, possibly
//! in another chunk, and it is settled at its key before it takes the queued
//! path. The call always ends with one outcome per row, and a row whose
//! outcome is not known is `outcome_unknown`.
use super::*;
use crate::{
    oltp_runtime::{application::PointRequest, http_runtime::HttpRuntime},
    protocol::{self, ErrorResponse, MAX_BATCH_ROWS, Request, Row, RowOutcome, RowOutcomes},
};
use serde::de::{SeqAccess, Visitor};
use std::{borrow::Cow, collections::HashSet};

/// Requests of one call in flight at once, over all ranges.
const CALL_REQUESTS: usize = 8;
/// Rows per chunk. Its body is at most `MAX_DIRECT_BATCH_BODY`, or one row.
const CHUNK_ROWS: usize = 1024;

/// The queued path, for rows that cannot run directly.
pub(crate) trait QueuedRows: Send + Sync {
    /// Runs one request to its terminal response. The gateway settled its id.
    fn run(&self, body: Vec<u8>) -> EndpointFuture<'_, EncodedResponse>;
}

/// The queued path of this process's HTTP runtime.
#[allow(dead_code)] // `http.rs` passes it.
pub(crate) struct HttpQueue(pub Arc<dyn HttpRuntime>);
impl QueuedRows for HttpQueue {
    fn run(&self, body: Vec<u8>) -> EndpointFuture<'_, EncodedResponse> {
        let runtime = self.0.clone();
        Box::pin(async move {
            let admitted = runtime.ingress().try_admit()?;
            let parsing = (3 * protocol::MAX_BODY_BYTES
                + crate::oltp_runtime::MAX_DECODED_REQUEST_BYTES) as u64;
            let charge = runtime
                .scheduler()
                .charge_memory(runtime.scheduler_node_id(), parsing)
                .map_err(|_| {
                    ProtocolError::new(
                        ErrorCode::Overloaded,
                        "request parsing memory capacity exhausted",
                        Outcome::NotApplied,
                    )
                })?;
            let queue = runtime.clone();
            let record = admitted
                .spawn(move || async move {
                    let parsed = protocol::parse_oltp_request(&body)?;
                    let point = PointRequest::from_operation(parsed.request_id, &parsed.operation);
                    drop(parsed);
                    queue.submit(body, charge, point).await
                })?
                .receive()
                .await?;
            // Accepted: a failed wait says nothing about the request.
            runtime.wait(&record).await.map_err(|_| unknown())
        })
    }
}

/// The body of a `*_many` call, its rows decoded and checked one at a time.
struct Call<'a> {
    id: Uuid,
    table_id: Uuid,
    upsert: bool,
    rows: Vec<CallRow<'a>>,
}
struct CallRow<'a> {
    key: Key,
    /// The row, or the key to delete, as JSON.
    raw: Cow<'a, str>,
    /// An earlier attempt of this row may have executed.
    uncertain: bool,
    outcome: Option<RowOutcome>,
}
/// Rows in key order and the route of the first, or why it has none.
type Chunk = (Vec<usize>, Result<Arc<TableRoute>, ProtocolError>);
/// What a row needs after its leader answered.
enum Next {
    Done(RowOutcome),
    /// Send it again, under the current route.
    Retry,
    /// Settle its id, then take the queued path.
    Settle,
    /// The marker is in place: take the queued path.
    Queue,
}

impl DirectGateway {
    pub(super) async fn many(
        &self,
        envelope: &Envelope<'_>,
        body_bytes: usize,
        asynchronous: bool,
        mode: DirectMode,
        queue: Option<&dyn QueuedRows>,
    ) -> Result<Routed, ProtocolError> {
        if asynchronous || (mode != DirectMode::Direct && queue.is_none()) {
            return Err(ProtocolError::new(
                ErrorCode::UnsupportedOperation,
                "the operation runs on /v1/operations with a declared Content-Length",
                Outcome::NotApplied,
            ));
        }
        GatewayStats::bump(&self.stats.requests);
        let deadline = nabedra_rt::time::now() + self.deadline;
        // One decoded row, as large as the body, and the chunks' encodings.
        let _charge = self
            .shared
            .reserve_scratch(ByteClass::Data, body_bytes.saturating_mul(2))?;
        let table_id = table_of(envelope.payload)
            .filter(|_| !envelope.request_id.is_nil())
            .ok_or_else(malformed)?;
        let table = match self.table(table_id, None, None, deadline).await? {
            Some(table) => table,
            None => return Err(not_found()),
        };
        let mut call = decode(envelope, table_id, &table)?;
        let _rows = self.shared.reserve_scratch(
            ByteClass::Data,
            call.rows.len() * std::mem::size_of::<CallRow<'_>>(),
        )?;
        let order = sorted(&call)?;
        let settle = if mode == DirectMode::Direct {
            self.send_all(&mut call, order, deadline).await
        } else {
            order.into_iter().map(|row| (row, Next::Settle)).collect()
        };
        self.queue_all(&mut call, settle, &table.key_column, queue, deadline)
            .await;
        GatewayStats::bump(&self.stats.direct_done);
        let outcomes = RowOutcomes {
            outcomes: call
                .rows
                .into_iter()
                .map(|row| row.outcome.unwrap_or_else(|| unknown().into()))
                .collect(),
        };
        Ok(Routed::Done(response::encode_success(
            call.id, &outcomes, true,
        )))
    }

    /// Sends `pending` (in key order) until every row has an outcome or needs
    /// the queued path. Returns the latter.
    async fn send_all(
        &self,
        call: &mut Call<'_>,
        mut pending: Vec<usize>,
        deadline: Instant,
    ) -> Vec<(usize, Next)> {
        let mut settle = Vec::new();
        let mut delay = Duration::ZERO;
        loop {
            let mut retry = Vec::new();
            let mut chunks = self.chunks(call, &pending, deadline).await.into_iter();
            let mut running = FuturesUnordered::new();
            let mut answers = Vec::new();
            loop {
                while running.len() < CALL_REQUESTS
                    && let Some((rows, route)) = chunks.next()
                {
                    let call = &*call;
                    running.push(async move {
                        let answer = match route {
                            Ok(table) => self.send_chunk(call, &rows, table, deadline).await,
                            Err(error) => {
                                (false, rows.iter().map(|_| Err(error.clone())).collect())
                            }
                        };
                        (rows, answer)
                    });
                }
                match running.next().await {
                    Some(answer) => answers.push(answer),
                    None => break,
                }
            }
            drop(running);
            for (rows, (uncertain, replies)) in answers {
                for (index, reply) in rows.into_iter().zip(replies) {
                    let row = &mut call.rows[index];
                    row.uncertain |= uncertain;
                    match row.next(reply) {
                        Next::Done(outcome) => row.outcome = Some(outcome),
                        Next::Retry => retry.push(index),
                        other => settle.push((index, other)),
                    }
                }
            }
            if retry.is_empty() {
                return settle;
            }
            GatewayStats::bump(&self.stats.resends);
            if !pause(deadline, &mut delay).await {
                for index in retry {
                    let row = &mut call.rows[index];
                    row.outcome = Some(row.refused(route_unavailable()));
                }
                return settle;
            }
            retry.sort_unstable_by(|a, b| compare_primary(&call.rows[*a].key, &call.rows[*b].key));
            pending = retry;
        }
    }

    /// `pending` (in key order) as runs of rows that share a range.
    async fn chunks(&self, call: &Call<'_>, pending: &[usize], deadline: Instant) -> Vec<Chunk> {
        let mut chunks: Vec<Chunk> = Vec::new();
        let mut bytes = 0;
        for (position, &index) in pending.iter().enumerate() {
            let row = &call.rows[index];
            let size = row.raw.len() + 1;
            let last = chunks.last().and_then(|(rows, route)| {
                let route = route.as_ref().ok()?;
                (route.covers(Some(&row.key))).then_some((rows.len(), route.clone()))
            });
            let route = match last {
                Some((rows, _))
                    if rows < CHUNK_ROWS && bytes + size <= MAX_DIRECT_BATCH_BODY - CHUNK_FRAME =>
                {
                    bytes += size;
                    chunks.last_mut().expect("a last chunk").0.push(index);
                    continue;
                }
                Some((_, route)) => Ok(route),
                None => self
                    .table(call.table_id, Some(&row.key), None, deadline)
                    .await
                    .and_then(|table| table.ok_or_else(not_found)),
            };
            if route.is_err() {
                // One failed lookup speaks for the keys after it.
                chunks.push((pending[position..].to_vec(), route));
                break;
            }
            bytes = size;
            chunks.push((vec![index], route));
        }
        chunks
    }

    /// Sends one chunk. Returns whether an attempt may have executed without
    /// an answer, and one answer per row.
    async fn send_chunk(
        &self,
        call: &Call<'_>,
        rows: &[usize],
        table: Arc<TableRoute>,
        deadline: Instant,
    ) -> (bool, Vec<Result<DirectResponse, ProtocolError>>) {
        let mut uncertain = false;
        let all = |reply: Result<DirectResponse, ProtocolError>| {
            rows.iter().map(|_| reply.clone()).collect()
        };
        let body = chunk_body(call, rows);
        let mut request = match self.request(call.id, &table, &body) {
            Ok(request) => request,
            Err(error) => return (false, all(Err(error))),
        };
        let (Some(first), Some(last)) = (rows.first(), rows.last()) else {
            return (false, Vec::new());
        };
        let (first, last) = (&call.rows[*first].key, &call.rows[*last].key);
        let result = self
            .call(
                first,
                &mut request,
                deadline,
                &mut uncertain,
                RouteAttempt {
                    table: table.clone(),
                    forward: false,
                    last: Some(last),
                },
            )
            .await;
        let replies = match result {
            Ok(Terminal::Done(data)) => {
                match serde_json::from_str::<Vec<DirectResponse>>(data.0.get()) {
                    Ok(replies) if replies.len() == rows.len() => {
                        self.follow(&table, first, &replies, deadline).await;
                        return (uncertain, replies.into_iter().map(Ok).collect());
                    }
                    _ => {
                        uncertain = true;
                        all(Err(unknown()))
                    }
                }
            }
            // The range changed under the chunk: its rows are grouped again.
            Ok(Terminal::Fallback(FallbackReason::Ineligible))
                if self
                    .cache
                    .get(call.table_id, Some(first))
                    .is_some_and(|route| !route.covers(Some(last))) =>
            {
                all(Ok(DirectResponse::StaleRoute {
                    schema_epoch: None,
                    routing_epoch: None,
                }))
            }
            Ok(Terminal::Fallback(reason)) => all(Ok(DirectResponse::Fallback { reason })),
            Ok(Terminal::Failed(error)) => all(Ok(DirectResponse::Failed(error))),
            Err(error) => all(Err(error)),
        };
        (uncertain, replies)
    }

    /// Learns the route from rows that name another one.
    async fn follow(
        &self,
        table: &Arc<TableRoute>,
        key: &Key,
        replies: &[DirectResponse],
        deadline: Instant,
    ) {
        if replies
            .iter()
            .any(|reply| matches!(reply, DirectResponse::StaleRoute { .. }))
        {
            GatewayStats::bump(&self.stats.stale_routes);
            let _ = self.refresh(table, key, deadline).await;
        }
        if replies
            .iter()
            .any(|reply| matches!(reply, DirectResponse::NotLeader { .. }))
        {
            GatewayStats::bump(&self.stats.not_leader);
            if let Some(node) = table.range.leader_hint() {
                table.range.forget_leader(node);
            }
        }
    }

    /// Settles each row at its key where needed, then runs it on the queued
    /// path, as `route` does for one request.
    async fn queue_all(
        &self,
        call: &mut Call<'_>,
        rows: Vec<(usize, Next)>,
        key_column: &str,
        queue: Option<&dyn QueuedRows>,
        deadline: Instant,
    ) {
        let mut waiting = rows.into_iter();
        let mut running = FuturesUnordered::new();
        let mut outcomes = Vec::new();
        loop {
            while running.len() < CALL_REQUESTS
                && let Some((index, next)) = waiting.next()
            {
                let call = &*call;
                running.push(async move {
                    let settle = matches!(next, Next::Settle);
                    let outcome = self
                        .queue_row(call, index, settle, key_column, queue, deadline)
                        .await
                        .map_or_else(RowOutcome::from, |()| RowOutcome::Applied);
                    (index, outcome)
                });
            }
            match running.next().await {
                Some(outcome) => outcomes.push(outcome),
                None => break,
            }
        }
        drop(running);
        for (index, outcome) in outcomes {
            call.rows[index].outcome = Some(outcome);
        }
    }

    /// `Ok` when the row applied, on either path.
    async fn queue_row(
        &self,
        call: &Call<'_>,
        index: usize,
        settle: bool,
        key_column: &str,
        queue: Option<&dyn QueuedRows>,
        deadline: Instant,
    ) -> Result<(), ProtocolError> {
        let row = &call.rows[index];
        // Until the id is settled it may have committed before.
        let unsettled = |_| unknown();
        let request = row_request(call, row, key_column)?;
        let body = serde_json::to_vec(&request).map_err(|_| malformed())?;
        if settle {
            let table = self
                .table(call.table_id, Some(&row.key), None, deadline)
                .await
                .map_err(unsettled)?
                .ok_or_else(unknown)?;
            let mut uncertain = row.uncertain;
            let mut direct = self
                .request(request.request_id, &table, &body)
                .map_err(unsettled)?;
            match self
                .settle(&table, &row.key, &mut direct, deadline, &mut uncertain)
                .await?
            {
                Routed::Done(_) => return Ok(()),
                Routed::Queued => {}
            }
        } else {
            GatewayStats::bump(&self.stats.queued);
        }
        // From here only the queued path knows the row: an earlier call may
        // have queued the same id.
        let queue = queue.ok_or_else(unknown)?;
        let reply = nabedra_rt::time::timeout_at(deadline, queue.run(body))
            .await
            .map_err(|_| unknown())??;
        if reply.status == 200 {
            return Ok(());
        }
        Err(serde_json::from_slice::<ErrorResponse>(&reply.body)
            .map_or_else(|_| unknown(), |reply| reply.error))
    }

    fn request(
        &self,
        request_id: Uuid,
        table: &TableRoute,
        body: &[u8],
    ) -> Result<DirectRequest, ProtocolError> {
        Ok(DirectRequest {
            database: self.database.clone(),
            table_id: table.table_id,
            schema_epoch: table.schema_epoch,
            routing_epoch: table.range.shard.routing_epoch,
            group: table.range.group().group_id,
            request_id,
            digest: digest(body),
            body: self.shared.copy_bytes(body)?,
            deadline: self.deadline,
        })
    }
}

impl CallRow<'_> {
    /// What a leader's answer, or the lack of one, means for the row.
    fn next(&mut self, reply: Result<DirectResponse, ProtocolError>) -> Next {
        match reply {
            Ok(DirectResponse::Done(_)) => Next::Done(RowOutcome::Applied),
            Ok(DirectResponse::Fallback {
                reason: FallbackReason::Forwarded,
            }) => Next::Queue,
            Ok(DirectResponse::Fallback { .. }) => Next::Settle,
            Ok(DirectResponse::StaleRoute { .. } | DirectResponse::NotLeader { .. }) => Next::Retry,
            Ok(DirectResponse::Failed(error)) => self.failed(error),
            // `call` ended without an answer it can rely on.
            Err(error) => Next::Done(self.refused(error)),
        }
    }
    fn failed(&mut self, error: ProtocolError) -> Next {
        if error.outcome == Outcome::Unknown {
            self.uncertain = true;
            return Next::Retry;
        }
        if error.code == ErrorCode::StorageUnavailable
            && !crate::oltp::pool::is_disk_reservation_refusal(&error)
        {
            Next::Retry
        } else if self.uncertain && error.code != ErrorCode::Conflict {
            // RV-FINAL L2, as in `route`: the leader may have refused before
            // its dedup lookup a row that committed.
            Next::Settle
        } else {
            Next::Done(error.into())
        }
    }
    /// A refusal of the last attempt, which says nothing about earlier ones.
    fn refused(&self, error: ProtocolError) -> RowOutcome {
        if self.uncertain { unknown() } else { error }.into()
    }
}

/// The envelope of `rows`, at most this long.
const CHUNK_FRAME: usize = 256;
fn chunk_body(call: &Call<'_>, rows: &[usize]) -> Vec<u8> {
    let (operation, list) = if call.upsert {
        ("table.upsert_many", "rows")
    } else {
        ("table.delete_many", "keys")
    };
    let mut body = format!(
        r#"{{"request_id":"{}","operation":"{operation}","payload":{{"table_id":"{}","{list}":["#,
        call.id, call.table_id
    );
    for (position, index) in rows.iter().enumerate() {
        if position != 0 {
            body.push(',');
        }
        body.push_str(&call.rows[*index].raw);
    }
    body.push_str("]}}");
    body.into_bytes()
}
fn row_request(
    call: &Call<'_>,
    row: &CallRow<'_>,
    key_column: &str,
) -> Result<Request, ProtocolError> {
    if call.upsert {
        let values: Row = protocol::decode_upsert_row(&row.raw)?;
        protocol::upsert_row_request(call.id, call.table_id, key_column, values)
    } else {
        protocol::delete_row_request(call.id, call.table_id, row.key.clone())
    }
}

fn table_of(payload: &RawValue) -> Option<Uuid> {
    #[derive(Deserialize)]
    struct Table {
        table_id: Uuid,
    }
    payload
        .get()
        .starts_with('{')
        .then(|| serde_json::from_str::<Table>(payload.get()).ok())?
        .map(|table| table.table_id)
}

/// Checks every row and keeps its key and its JSON, one decoded row at a time.
fn decode<'a>(
    envelope: &Envelope<'a>,
    table_id: Uuid,
    table: &TableRoute,
) -> Result<Call<'a>, ProtocolError> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Upsert<'a> {
        #[allow(dead_code)]
        table_id: Uuid,
        #[serde(borrow)]
        rows: &'a RawValue,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Delete<'a> {
        #[allow(dead_code)]
        table_id: Uuid,
        #[serde(borrow)]
        keys: &'a RawValue,
    }
    let upsert = envelope.operation == "table.upsert_many";
    let payload = envelope.payload.get();
    let list = if upsert {
        serde_json::from_str::<Upsert<'_>>(payload).map(|payload| payload.rows)
    } else {
        serde_json::from_str::<Delete<'_>>(payload).map(|payload| payload.keys)
    }
    .map_err(|_| malformed())?;
    let items = Items {
        row: |raw: &'a RawValue| {
            let key = if upsert {
                let row = protocol::decode_upsert_row(raw.get())?;
                protocol::row_key(&row, &table.key_column)?
            } else {
                serde_json::from_str(raw.get()).map_err(|_| malformed())?
            };
            key.validate(table.key_type)?;
            Ok(CallRow {
                key,
                raw: Cow::Borrowed(raw.get()),
                uncertain: false,
                outcome: None,
            })
        },
    };
    let mut decoder = serde_json::Deserializer::from_str(list.get());
    let rows =
        serde::Deserializer::deserialize_seq(&mut decoder, items).map_err(|_| malformed())??;
    Ok(Call {
        id: envelope.request_id,
        table_id,
        upsert,
        rows,
    })
}
struct Items<F> {
    row: F,
}
impl<'de, F> Visitor<'de> for Items<F>
where
    F: Fn(&'de RawValue) -> Result<CallRow<'de>, ProtocolError>,
{
    type Value = Result<Vec<CallRow<'de>>, ProtocolError>;
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("an array of rows")
    }
    fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<Self::Value, S::Error> {
        let mut rows = Vec::new();
        let mut failed = None;
        while let Some(raw) = seq.next_element::<&'de RawValue>()? {
            if failed.is_some() {
                continue;
            }
            if rows.len() == MAX_BATCH_ROWS {
                failed = Some(ProtocolError::new(
                    ErrorCode::LimitExceeded,
                    "batch exceeds 10000 rows",
                    Outcome::NotApplied,
                ));
                continue;
            }
            match (self.row)(raw) {
                Ok(row) => rows.push(row),
                Err(error) => failed = Some(error),
            }
        }
        Ok(failed.map_or(Ok(rows), Err))
    }
}
/// The rows in key order. A repeated key is the caller's error.
fn sorted(call: &Call<'_>) -> Result<Vec<usize>, ProtocolError> {
    let mut seen = HashSet::with_capacity(call.rows.len());
    if !call.rows.iter().all(|row| seen.insert(&row.key)) {
        return Err(protocol::duplicate_key());
    }
    let mut order: Vec<usize> = (0..call.rows.len()).collect();
    order.sort_unstable_by(|a, b| compare_primary(&call.rows[*a].key, &call.rows[*b].key));
    Ok(order)
}

fn malformed() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "malformed operation payload",
        Outcome::NotApplied,
    )
}
fn not_found() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::NotFound,
        "table or shard not found",
        Outcome::NotApplied,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every answer a row can get, with and without an earlier attempt that
    /// may have executed. A row in doubt is never `not_applied`, except for a
    /// conflict, which the leader finds after its dedup lookup.
    #[test]
    fn vs15_a_row_in_doubt_is_never_reported_as_not_applied() {
        let error = |code, outcome| ProtocolError::new(code, "m", outcome);
        let codes = [
            ErrorCode::ValidationError,
            ErrorCode::UnsupportedOperation,
            ErrorCode::Unauthorized,
            ErrorCode::NotFound,
            ErrorCode::Conflict,
            ErrorCode::LimitExceeded,
            ErrorCode::Overloaded,
            ErrorCode::StorageUnavailable,
            ErrorCode::OutcomeUnknown,
            ErrorCode::InternalError,
        ];
        for uncertain in [false, true] {
            let row = || CallRow {
                key: Key::Int64(1),
                raw: "{}".into(),
                uncertain,
                outcome: None,
            };
            for code in codes {
                for outcome in [Outcome::NotApplied, Outcome::Unknown] {
                    let error = error(code, outcome);
                    let definite = outcome == Outcome::NotApplied;
                    let mut answered = row();
                    let next = answered.next(Ok(DirectResponse::Failed(error.clone())));
                    assert_eq!(answered.uncertain, uncertain || !definite, "{code:?}");
                    match next {
                        Next::Retry => assert!(
                            !definite || code == ErrorCode::StorageUnavailable,
                            "{code:?}"
                        ),
                        Next::Settle => assert!(definite && uncertain, "{code:?}"),
                        Next::Done(RowOutcome::NotApplied { error: reported }) => {
                            assert!(definite && (!uncertain || code == ErrorCode::Conflict));
                            assert_eq!(reported, error);
                        }
                        _ => panic!("{code:?} {outcome:?}"),
                    }
                    let Next::Done(refused) = row().next(Err(error)) else {
                        panic!("an ended call is final");
                    };
                    assert_eq!(
                        matches!(refused, RowOutcome::NotApplied { .. }),
                        definite && !uncertain,
                        "{code:?}"
                    );
                }
            }
            let done = DirectResponse::Done(DirectData::from_json("false").unwrap());
            assert!(matches!(
                row().next(Ok(done)),
                Next::Done(RowOutcome::Applied)
            ));
            for reason in FallbackReason::ALL {
                let next = row().next(Ok(DirectResponse::Fallback { reason }));
                assert_eq!(
                    matches!(next, Next::Queue),
                    reason == FallbackReason::Forwarded
                );
                assert!(matches!(next, Next::Queue | Next::Settle), "{reason:?}");
            }
        }
    }
}
