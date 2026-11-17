use std::{collections::HashSet, fmt, sync::Arc};

use axum::{
    body::Body,
    extract::{FromRequest, Request},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use serde::{
    Deserialize, Serialize,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use serde_json::value::RawValue;
use tokio::{
    sync::OwnedSemaphorePermit,
    time::{Instant, timeout_at},
};
use uuid::Uuid;

use super::{
    ApiError, BackendError,
    partition::{MAX_BATCH_BYTES, MAX_BATCH_EVENTS, MAX_EVENT_BYTES, PublishEvent},
};

const MAX_DEPTH: usize = 64;
const MAX_DATA_NODES: usize = MAX_EVENT_BYTES;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InputEvent<'a> {
    event_id: Uuid,
    #[serde(borrow)]
    data: &'a RawValue,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InputBatch<'a> {
    #[serde(borrow, deserialize_with = "batch_events")]
    pub events: Vec<&'a RawValue>,
}

fn batch_events<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<&'de RawValue>, D::Error> {
    struct BoundedEvents;
    impl<'de> Visitor<'de> for BoundedEvents {
        type Value = Vec<&'de RawValue>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "up to {MAX_BATCH_EVENTS} events")
        }

        fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<Self::Value, S::Error> {
            let mut events = Vec::new();
            while let Some(event) = seq.next_element()? {
                if events.len() == MAX_BATCH_EVENTS {
                    return Err(de::Error::custom("too many events"));
                }
                events.push(event);
            }
            Ok(events)
        }
    }

    deserializer.deserialize_seq(BoundedEvents)
}

pub(super) fn publish_events(
    input: Vec<&RawValue>,
) -> Result<(Vec<PublishEvent>, usize), ApiError> {
    if input.is_empty() {
        return Err(ApiError::invalid(
            "a publish batch must contain at least one event",
        ));
    }
    let mut bytes = 2 + input.len() - 1;
    let mut events = Vec::with_capacity(input.len());
    for raw in input {
        let event: InputEvent<'_> = decode(raw.get().as_bytes())?;
        let mut remaining = MAX_DATA_NODES;
        validate_data(event.data, MAX_DEPTH, &mut remaining)
            .map_err(|_| ApiError::invalid("invalid, repeated, or deeply nested event data"))?;
        let data = serde_json::from_str(event.data.get())
            .map_err(|_| ApiError::invalid("invalid event data"))?;
        let event = PublishEvent {
            event_id: event.event_id,
            data,
        };
        let size = serde_json::to_vec(&event)
            .map_err(|_| BackendError::Internal)?
            .len();
        if size > MAX_EVENT_BYTES {
            return Err(ApiError::payload_too_large());
        }
        bytes += size;
        events.push(event);
    }
    if bytes + b"{\"events\":}".len() > MAX_BATCH_BYTES {
        return Err(ApiError::payload_too_large());
    }
    Ok((events, bytes))
}

fn validate_data(
    raw: &RawValue,
    depth: usize,
    remaining: &mut usize,
) -> Result<(), serde_json::Error> {
    struct Contents<'a> {
        remaining: &'a mut usize,
        depth: usize,
    }

    impl<'de> Visitor<'de> for Contents<'_> {
        type Value = ();

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("JSON event data")
        }

        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<(), M::Error> {
            let mut seen = HashSet::new();
            while let Some(key) = map.next_key::<String>()? {
                // serde_json's Value decoder assigns private meaning to these keys.
                if matches!(
                    key.as_str(),
                    "$serde_json::private::Number" | "$serde_json::private::RawValue"
                ) || !seen.insert(key)
                {
                    return Err(de::Error::custom("invalid or repeated field"));
                }
                let value: &RawValue = map.next_value()?;
                validate_data(value, self.depth, self.remaining).map_err(de::Error::custom)?;
            }
            Ok(())
        }

        fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<(), S::Error> {
            while let Some(value) = seq.next_element()? {
                validate_data(value, self.depth, self.remaining).map_err(de::Error::custom)?;
            }
            Ok(())
        }
    }

    if *remaining == 0 {
        return Err(de::Error::custom("event has too many values"));
    }
    *remaining -= 1;
    let raw = raw.get();
    let first = raw.as_bytes().first();
    if matches!(first, Some(b'{' | b'[')) {
        if depth == 0 {
            return Err(de::Error::custom("event data exceeds depth limit"));
        }
        let mut deserializer = serde_json::Deserializer::from_str(raw);
        let contents = Contents {
            remaining,
            depth: depth - 1,
        };
        return match first {
            Some(b'{') => serde::Deserializer::deserialize_map(&mut deserializer, contents),
            _ => serde::Deserializer::deserialize_seq(&mut deserializer, contents),
        };
    }

    if matches!(first, Some(b'-' | b'0'..=b'9')) {
        let number: serde_json::Number = serde_json::from_str(raw)?;
        let valid = if raw.bytes().any(|byte| matches!(byte, b'.' | b'e' | b'E')) {
            number.as_f64().is_some_and(f64::is_finite)
        } else {
            number.as_i64().is_some() || number.as_u64().is_some()
        };
        if !valid {
            return Err(de::Error::custom(
                "number is outside the supported JSON range",
            ));
        }
    }
    Ok(())
}

pub(super) async fn read_body(
    request: Request,
    deadline: Instant,
    allow_empty: bool,
) -> Result<Bytes, ApiError> {
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"));
    if request.headers().contains_key(header::CONTENT_ENCODING)
        || request
            .headers()
            .get_all(header::CONTENT_TYPE)
            .iter()
            .count()
            > 1
    {
        return Err(ApiError::invalid("invalid JSON content headers"));
    }
    if !allow_empty && !content_type {
        return Err(ApiError::invalid("content type must be application/json"));
    }

    let body = timeout_at(deadline, Bytes::from_request(request, &()))
        .await
        .map_err(|_| {
            ApiError::new(
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
                "request body was not received before the deadline",
            )
        })?
        .map_err(|error| {
            if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
                ApiError::payload_too_large()
            } else {
                ApiError::invalid("could not read request body")
            }
        })?;
    if !content_type && !body.is_empty() {
        return Err(ApiError::invalid("content type must be application/json"));
    }
    Ok(body)
}

pub(super) fn decode<'de, T: Deserialize<'de>>(body: &'de [u8]) -> Result<T, ApiError> {
    let first = body
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace());
    if first != Some(b'{') {
        return Err(ApiError::invalid("request body must be a JSON object"));
    }
    serde_json::from_slice(body).map_err(|_| ApiError::invalid("invalid JSON request"))
}

pub(super) fn decode_offset(value: &str) -> Result<u64, ApiError> {
    if value.len() > 20
        || value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(ApiError::invalid(
            "offset must be a canonical decimal string",
        ));
    }
    value
        .parse()
        .map_err(|_| ApiError::invalid("offset is out of range"))
}

pub(super) fn response(
    status: StatusCode,
    body: Vec<u8>,
    permit: Arc<OwnedSemaphorePermit>,
) -> Response {
    struct Payload {
        data: Vec<u8>,
        _permit: Arc<OwnedSemaphorePermit>,
    }

    impl AsRef<[u8]> for Payload {
        fn as_ref(&self) -> &[u8] {
            &self.data
        }
    }

    let payload = Bytes::from_owner(Payload {
        data: body,
        _permit: permit,
    });
    let mut response = (status, Body::from(payload)).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

pub(super) fn encode(body: &impl Serialize, limit: usize) -> Result<Vec<u8>, ApiError> {
    let bytes = serde_json::to_vec(body).map_err(|_| BackendError::Internal)?;
    if bytes.len() > limit {
        return Err(BackendError::Internal.into());
    }
    Ok(bytes)
}
