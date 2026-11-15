//! Pure A11 startup inputs. Paths remain literal hints; no material is opened.
use super::{
    configuration::{self, ConfigurationError, Storage, Template, Timing, Tls},
    member::{nullable_uuid, uuid},
};
use crate::protocol::deserialize_object;
use serde::{Deserialize, Deserializer};
use std::fmt;
use uuid::Uuid;

pub(crate) const MAX_INPUT_BYTES: usize = 128 * 1024;

pub(crate) struct ApiConfig {
    pub(crate) cluster_id: Uuid,
    pub(crate) bind: String,
    pub(crate) advertise_url: String,
    pub(crate) auth_file: String,
    pub(crate) template: Template,
    pub(crate) lifecycle: Timing,
}
pub(crate) struct Bootstrap {
    pub(crate) cluster_id: Uuid,
    pub(crate) node_id: Uuid,
    pub(crate) request_id: Option<Uuid>,
    pub(crate) advertise_url: String,
    pub(crate) enrollment_token: String,
    pub(crate) template: Template,
}
macro_rules! redacted_debug {
    ($($name:ident),*) => { $(impl fmt::Debug for $name {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct(stringify!($name)).finish_non_exhaustive()
        }
    })* };
}
redacted_debug!(ApiConfig, Bootstrap);

fn nullable<'de, D: Deserializer<'de>, T: Deserialize<'de>>(d: D) -> Result<Option<T>, D::Error> {
    Option::deserialize(d)
}
// These wire records never escape decoding. Each nested object retains serde's
// duplicate detection, including Storage's original-byte RawValue options.
macro_rules! wire {
    ($name:ident { $($(#[$attr:meta])* $field:ident : $ty:ty),* $(,)? }) => {
        struct $name { $($field: $ty),* }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Wire { $($(#[$attr])* $field: $ty),* }
                let w: Wire = deserialize_object(d)?;
                Ok(Self { $($field: w.$field),* })
            }
        }
    }
}
wire!(Worker {
    listen: String,
    operation_slots: u64,
    partition_slots: u64,
    queue_size: u64,
    partition_size: String,
});
wire!(ApiWire {
    version: u64,
    #[serde(deserialize_with="uuid")]
    cluster_id: Uuid,
    bind: String,
    advertise_url: String,
    api_urls: Vec<String>,
    storage: Storage,
    auth_file: String,
    #[serde(deserialize_with="nullable")]
    tls: Option<Tls>,
    worker: Worker,
    #[serde(default, deserialize_with="lifecycle")]
    lifecycle: Timing,
});
wire!(BootstrapWire {
    version: u64,
    #[serde(deserialize_with="uuid")]
    cluster_id: Uuid,
    #[serde(deserialize_with="uuid")]
    node_id: Uuid,
    #[serde(deserialize_with="nullable_uuid")]
    request_id: Option<Uuid>,
    api_urls: Vec<String>,
    listen: String,
    advertise_url: String,
    storage: Storage,
    enrollment_token: String,
    #[serde(deserialize_with="nullable")]
    tls: Option<Tls>,
    operation_slots: u64,
    partition_slots: u64,
    queue_size: u64,
    partition_size: String,
});
fn supplied_integer<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
    u64::deserialize(d).map(Some)
}
macro_rules! lifecycle_fields {
    ($($field:ident),* $(,)?) => {
        fn lifecycle<'de, D: Deserializer<'de>>(d: D) -> Result<Timing, D::Error> {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Patch {
                $(#[serde(default, deserialize_with="supplied_integer")]
                $field: Option<u64>),*
            }
            let patch: Patch = deserialize_object(d)?;
            let mut timing = Timing::default();
            $(if let Some(value) = patch.$field { timing.$field = value; })*
            timing.validate().map_err(|_| serde::de::Error::custom("invalid lifecycle"))?;
            Ok(timing)
        }
    }
}
lifecycle_fields!(
    heartbeat_ms,
    expiry_ms,
    readiness_ms,
    drain_ms,
    lease_ms,
    renew_ms,
    stabilization_ms,
    cooldown_ms,
    retry_initial_ms,
    retry_max_ms,
    max_attempts
);

fn check(ok: bool) -> Result<(), ConfigurationError> {
    if ok {
        Ok(())
    } else {
        Err(ConfigurationError::InvalidRecord)
    }
}
fn literal_path(s: &str) -> bool {
    !s.is_empty() && s.len() <= 2048 && !s.contains('\0')
}
fn validate_template(template: &Template, advertise_url: &str) -> Result<(), ConfigurationError> {
    // Includes the bounded canonical 64 KiB serialization, not just field checks.
    template.validate()?;
    let scheme = if template.tls.is_some() {
        "https"
    } else {
        "http"
    };
    check(
        configuration::api_url(advertise_url)
            && std::iter::once(advertise_url)
                .chain(template.api_urls.iter().map(String::as_str))
                .all(|url| {
                    url.split_once("://")
                        .is_some_and(|(s, _)| s.eq_ignore_ascii_case(scheme))
                }),
    )?;
    if let Some(tls) = &template.tls {
        check(
            literal_path(&tls.cert_file)
                && literal_path(&tls.key_file)
                && literal_path(&tls.ca_file),
        )?;
    }
    if let Storage::Http(options) = &template.storage {
        check(
            literal_path(&options.token_file)
                && options.ca_file.as_deref().is_none_or(literal_path),
        )?;
    }
    Ok(())
}

pub(crate) fn decode_api(bytes: &[u8]) -> Result<ApiConfig, ConfigurationError> {
    configuration::input(bytes, MAX_INPUT_BYTES)?;
    let wire: ApiWire =
        serde_json::from_slice(bytes).map_err(|_| ConfigurationError::InvalidRecord)?;
    check(wire.version == 1 && configuration::listen(&wire.bind) && literal_path(&wire.auth_file))?;
    let template = Template {
        api_urls: wire.api_urls,
        listen: wire.worker.listen,
        operation_slots: wire.worker.operation_slots,
        partition_slots: wire.worker.partition_slots,
        queue_size: wire.worker.queue_size,
        partition_size: wire.worker.partition_size,
        storage: wire.storage,
        tls: wire.tls,
    };
    validate_template(&template, &wire.advertise_url)?;
    Ok(ApiConfig {
        cluster_id: wire.cluster_id,
        bind: wire.bind,
        advertise_url: wire.advertise_url,
        auth_file: wire.auth_file,
        template,
        lifecycle: wire.lifecycle,
    })
}
pub(crate) fn decode_bootstrap(bytes: &[u8]) -> Result<Bootstrap, ConfigurationError> {
    configuration::input(bytes, MAX_INPUT_BYTES)?;
    let wire: BootstrapWire =
        serde_json::from_slice(bytes).map_err(|_| ConfigurationError::InvalidRecord)?;
    check(
        wire.version == 1
            && (43..=4096).contains(&wire.enrollment_token.len())
            && wire
                .enrollment_token
                .bytes()
                .all(|b| (33..=126).contains(&b)),
    )?;
    let template = Template {
        api_urls: wire.api_urls,
        listen: wire.listen,
        operation_slots: wire.operation_slots,
        partition_slots: wire.partition_slots,
        queue_size: wire.queue_size,
        partition_size: wire.partition_size,
        storage: wire.storage,
        tls: wire.tls,
    };
    validate_template(&template, &wire.advertise_url)?;
    Ok(Bootstrap {
        cluster_id: wire.cluster_id,
        node_id: wire.node_id,
        request_id: wire.request_id,
        advertise_url: wire.advertise_url,
        enrollment_token: wire.enrollment_token,
        template,
    })
}
#[cfg(test)]
mod tests;
