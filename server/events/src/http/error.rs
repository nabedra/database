use axum::{
    Json,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Serialize;

use super::{BackendError, partition};

#[derive(Clone, Debug)]
pub struct ApiError {
    status: StatusCode,
    body: ErrorBody,
}

#[derive(Clone, Debug, Serialize)]
struct ErrorBody {
    code: &'static str,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    partition: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    earliest_offset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    outcome: Option<&'static str>,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &'static str, message: &'static str) -> Self {
        Self {
            status,
            body: ErrorBody {
                code,
                message: message.into(),
                partition: None,
                earliest_offset: None,
                outcome: None,
            },
        }
    }

    pub(super) fn invalid(message: &'static str) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_request", message)
    }

    pub(super) fn payload_too_large() -> Self {
        Self::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "request exceeds its byte limit",
        )
    }

    pub(super) fn unauthorized() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer grant is missing or invalid",
        )
    }

    pub(super) fn forbidden() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "forbidden",
            "grant does not allow this operation",
        )
    }

    pub(super) fn lease_lost() -> Self {
        Self::new(
            StatusCode::CONFLICT,
            "lease_lost",
            "lease is expired or no longer held",
        )
    }

    pub(super) fn invalid_delivery() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "invalid_delivery",
            "delivery credential is invalid",
        )
    }

    pub(super) fn unknown() -> Self {
        let mut error = Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "outcome_unknown",
            "write may have committed",
        );
        error.body.outcome = Some("unknown");
        error
    }
}

impl From<BackendError> for ApiError {
    fn from(error: BackendError) -> Self {
        use BackendError as E;
        match error {
            E::NotFound => Self::new(
                StatusCode::NOT_FOUND,
                "not_found",
                "Events resource not found",
            ),
            E::Unavailable => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                "Events quorum or route is unavailable",
            ),
            E::Overloaded => Self::new(
                StatusCode::TOO_MANY_REQUESTS,
                "overloaded",
                "Events capacity is busy",
            ),
            E::InsufficientStorage => Self::new(
                StatusCode::INSUFFICIENT_STORAGE,
                "insufficient_storage",
                "broker storage is below its free-space limit",
            ),
            E::OutcomeUnknown => Self::unknown(),
            E::Internal => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "Events could not complete the request",
            ),
        }
    }
}

impl From<partition::Error> for ApiError {
    fn from(error: partition::Error) -> Self {
        use partition::ErrorCode as E;
        let (status, code) = match error.code {
            E::InvalidRequest => (StatusCode::BAD_REQUEST, "invalid_request"),
            E::PayloadTooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large"),
            E::InsufficientStorage => (StatusCode::INSUFFICIENT_STORAGE, "insufficient_storage"),
            E::Conflict => (StatusCode::CONFLICT, "conflict"),
            E::NotFound => (StatusCode::NOT_FOUND, "not_found"),
            E::LeaseBusy => (StatusCode::CONFLICT, "lease_busy"),
            E::LeaseLost => (StatusCode::CONFLICT, "lease_lost"),
            E::CheckpointExpired => (StatusCode::GONE, "checkpoint_expired"),
        };
        Self {
            status,
            body: ErrorBody {
                code,
                message: error.message,
                partition: error.partition,
                earliest_offset: error.earliest_offset.map(|value| value.to_string()),
                outcome: None,
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (self.status, Json(self.body)).into_response();
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        if self.status == StatusCode::TOO_MANY_REQUESTS {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        if self.status == StatusCode::UNAUTHORIZED {
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        response
    }
}
