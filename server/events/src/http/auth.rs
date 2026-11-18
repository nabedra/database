use std::collections::HashMap;

use anyhow::{Context, Result, ensure};
use axum::http::{HeaderMap, header::AUTHORIZATION};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::Sha256;
use uuid::Uuid;

use super::{ApiError, PartitionRoute, partition::valid_name};

const MAX_GRANT_MS: i64 = 24 * 60 * 60 * 1000;
const MAX_KEYS: usize = 8;
const MAX_TOKEN_BYTES: usize = 2048;
const MAX_CLOCK_SKEW_MS: i64 = 5_000;

type HmacSha256 = Hmac<Sha256>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyringConfig {
    pub cluster_id: Uuid,
    pub grant: SigningKeysConfig,
    pub session: SigningKeysConfig,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SigningKeysConfig {
    pub active: String,
    pub keys: Vec<SigningKeyConfig>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SigningKeyConfig {
    pub id: String,
    pub secret_hex: String,
    pub accept_until_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    pub tenant: String,
    pub subject: String,
    pub scope: GrantScope,
    pub expires_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum GrantScope {
    Publish { topic: String },
    Read { topic: String, subscription: String },
    Operator,
}

impl Grant {
    pub(super) fn authorize(&self, access: Access<'_>) -> Result<(), ApiError> {
        let allowed = match (&self.scope, access) {
            (GrantScope::Operator, Access::Operator) => true,
            (
                GrantScope::Publish { topic },
                Access::Discover(expected) | Access::Publish(expected),
            )
            | (GrantScope::Read { topic, .. }, Access::Discover(expected)) => topic == expected,
            (
                GrantScope::Read {
                    topic,
                    subscription,
                },
                Access::Read(expected_topic, expected_subscription),
            ) => topic == expected_topic && subscription == expected_subscription,
            _ => false,
        };
        if !allowed {
            return Err(ApiError::forbidden());
        }
        Ok(())
    }

    pub(super) fn owner(&self) -> String {
        // The separator isn't legal in either field, so pairs cannot alias.
        format!("{}/{}", self.tenant, self.subject)
    }

    fn validate(&self) -> bool {
        valid_name(&self.tenant)
            && valid_name(&self.subject)
            && match &self.scope {
                GrantScope::Publish { topic } => valid_name(topic),
                GrantScope::Read {
                    topic,
                    subscription,
                } => valid_name(topic) && valid_name(subscription),
                GrantScope::Operator => true,
            }
    }
}

pub(super) enum Access<'a> {
    Discover(&'a str),
    Publish(&'a str),
    Read(&'a str, &'a str),
    Operator,
}

struct SigningKey {
    secret: [u8; 32],
    accept_until_ms: i64,
}

struct SigningKeys {
    active: String,
    keys: HashMap<String, SigningKey>,
}

impl SigningKeys {
    fn new(config: SigningKeysConfig) -> Result<Self> {
        ensure!(valid_name(&config.active), "invalid active Events key ID");
        ensure!(
            (1..=MAX_KEYS).contains(&config.keys.len()),
            "invalid Events key count"
        );

        let mut keys = HashMap::with_capacity(config.keys.len());
        for key in config.keys {
            ensure!(
                valid_name(&key.id) && key.accept_until_ms > 0,
                "invalid Events signing key"
            );
            ensure!(
                key.secret_hex.len() == 64
                    && key.secret_hex.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "Events signing keys must contain 32 hex-encoded bytes"
            );
            let mut secret = [0_u8; 32];
            for (index, byte) in secret.iter_mut().enumerate() {
                *byte = u8::from_str_radix(&key.secret_hex[index * 2..index * 2 + 2], 16)
                    .context("invalid hex-encoded Events key")?;
            }
            ensure!(
                keys.insert(
                    key.id,
                    SigningKey {
                        secret,
                        accept_until_ms: key.accept_until_ms
                    },
                )
                .is_none(),
                "duplicate Events signing key ID"
            );
        }

        ensure!(
            keys.contains_key(&config.active),
            "active Events key is missing"
        );
        Ok(Self {
            active: config.active,
            keys,
        })
    }

    fn active(&self) -> &SigningKey {
        &self.keys[&self.active]
    }

    fn sign<T: Serialize>(&self, purpose: &[u8], token: &Token<T>) -> Result<String> {
        let key = self.active();
        ensure!(
            token.expires_at_ms <= key.accept_until_ms,
            "Events credential would outlive the signing key"
        );
        let message = URL_SAFE_NO_PAD.encode(serde_json::to_vec(token)?);
        let signature = mac(purpose, &self.active, &message, &key.secret)
            .finalize()
            .into_bytes();
        let token = format!(
            "{}.{}.{}",
            self.active,
            message,
            URL_SAFE_NO_PAD.encode(signature)
        );
        ensure!(
            token.len() <= MAX_TOKEN_BYTES,
            "Events credential too large"
        );
        Ok(token)
    }

    fn verify<T: DeserializeOwned>(
        &self,
        purpose: &[u8],
        token: &str,
        now_ms: i64,
    ) -> Option<Token<T>> {
        if token.len() > MAX_TOKEN_BYTES {
            return None;
        }
        let (key_id, rest) = token.split_once('.')?;
        let (message, signature) = rest.split_once('.')?;
        let key = self.keys.get(key_id)?;
        if now_ms >= key.accept_until_ms {
            return None;
        }
        let signature = URL_SAFE_NO_PAD.decode(signature).ok()?;
        mac(purpose, key_id, message, &key.secret)
            .verify_slice(&signature)
            .ok()?;
        let message = URL_SAFE_NO_PAD.decode(message).ok()?;
        let token: Token<T> = serde_json::from_slice(&message).ok()?;
        if token.issued_at_ms < 0
            || token.issued_at_ms > now_ms.saturating_add(MAX_CLOCK_SKEW_MS)
            || token.expires_at_ms <= token.issued_at_ms
            || token.expires_at_ms <= now_ms
            || token.expires_at_ms > key.accept_until_ms
        {
            return None;
        }
        Some(token)
    }
}

fn mac(purpose: &[u8], key_id: &str, message: &str, secret: &[u8; 32]) -> HmacSha256 {
    let mut mac = HmacSha256::new_from_slice(secret).expect("fixed-size HMAC key");
    mac.update(b"nabedra-events/v1\0");
    mac.update(purpose);
    mac.update(b"\0");
    mac.update(key_id.as_bytes());
    mac.update(b".");
    mac.update(message.as_bytes());
    mac
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Token<T> {
    cluster_id: Uuid,
    issued_at_ms: i64,
    expires_at_ms: i64,
    claims: T,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Session {
    pub topic_id: Uuid,
    pub partition: u32,
    pub subscription: String,
    pub owner: String,
    pub generation: u64,
}

impl Session {
    pub(super) fn new(
        route: &PartitionRoute,
        subscription: String,
        owner: String,
        generation: u64,
    ) -> Self {
        Self {
            topic_id: route.topic_id,
            partition: route.partition,
            subscription,
            owner,
            generation,
        }
    }

    pub(super) fn matches(&self, route: &PartitionRoute, subscription: &str, owner: &str) -> bool {
        self.topic_id == route.topic_id
            && self.partition == route.partition
            && self.subscription == subscription
            && self.owner == owner
            && self.generation != 0
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Delivery {
    pub session: Session,
    pub after: u64,
    pub last: u64,
}

pub struct Keyring {
    cluster_id: Uuid,
    grant: SigningKeys,
    session: SigningKeys,
}

impl Keyring {
    pub fn new(config: KeyringConfig) -> Result<Self> {
        ensure!(
            !config.cluster_id.is_nil(),
            "Events cluster identity cannot be nil"
        );
        let grant = SigningKeys::new(config.grant)?;
        let session = SigningKeys::new(config.session)?;
        ensure!(
            grant.keys.values().all(|grant| session
                .keys
                .values()
                .all(|session| grant.secret != session.secret)),
            "grant and session signing key material must be independent"
        );
        Ok(Self {
            cluster_id: config.cluster_id,
            grant,
            session,
        })
    }

    pub fn cluster_id(&self) -> Uuid {
        self.cluster_id
    }

    pub fn issue_grant(&self, grant: Grant, now_ms: i64) -> Result<String> {
        ensure!(grant.validate(), "invalid Events grant");
        ensure!(
            now_ms >= 0
                && grant.expires_at_ms > now_ms
                && grant.expires_at_ms <= now_ms.saturating_add(MAX_GRANT_MS),
            "Events grant lifetime must be positive and no greater than 24 hours"
        );
        self.grant.sign(
            b"grant",
            &Token {
                cluster_id: self.cluster_id,
                issued_at_ms: now_ms,
                expires_at_ms: grant.expires_at_ms,
                claims: grant,
            },
        )
    }

    pub(super) fn authorize(
        &self,
        headers: &HeaderMap,
        access: Access<'_>,
        now_ms: i64,
    ) -> Result<Grant, ApiError> {
        let bearer = one_header(headers, AUTHORIZATION.as_str())
            .and_then(|header| {
                let (scheme, credential) = header.split_once(' ')?;
                scheme.eq_ignore_ascii_case("Bearer").then_some(credential)
            })
            .ok_or_else(ApiError::unauthorized)?;
        let token: Token<Grant> = self
            .grant
            .verify(b"grant", bearer, now_ms)
            .ok_or_else(ApiError::unauthorized)?;
        if token.cluster_id != self.cluster_id
            || token.claims.expires_at_ms != token.expires_at_ms
            || token.expires_at_ms - token.issued_at_ms > MAX_GRANT_MS
            || !token.claims.validate()
        {
            return Err(ApiError::unauthorized());
        }
        token.claims.authorize(access)?;
        Ok(token.claims)
    }

    pub(super) fn session_expiry(&self, requested_ms: i64, grant: &Grant) -> i64 {
        requested_ms
            .min(grant.expires_at_ms)
            .min(self.session.active().accept_until_ms)
    }

    pub(super) fn issue_lease(
        &self,
        session: Session,
        now_ms: i64,
        expires_ms: i64,
    ) -> Result<String> {
        self.issue_session(b"lease", session, now_ms, expires_ms)
    }

    pub(super) fn lease(&self, headers: &HeaderMap, now_ms: i64) -> Result<Session, ApiError> {
        let credential = one_header(headers, "x-events-lease").ok_or_else(ApiError::lease_lost)?;
        self.verify_session(b"lease", credential, now_ms)
            .ok_or_else(ApiError::lease_lost)
    }

    pub(super) fn issue_delivery(
        &self,
        delivery: Delivery,
        now_ms: i64,
        expires_ms: i64,
    ) -> Result<String> {
        self.issue_session(b"delivery", delivery, now_ms, expires_ms)
    }

    pub(super) fn delivery(&self, credential: &str, now_ms: i64) -> Result<Delivery, ApiError> {
        self.verify_session(b"delivery", credential, now_ms)
            .ok_or_else(ApiError::invalid_delivery)
    }

    fn issue_session<T: Serialize>(
        &self,
        purpose: &[u8],
        claims: T,
        now_ms: i64,
        expires_ms: i64,
    ) -> Result<String> {
        ensure!(
            now_ms >= 0 && expires_ms > now_ms,
            "Events session expiry must be in the future"
        );
        ensure!(
            expires_ms - now_ms <= super::partition::MAX_LEASE_MS + MAX_CLOCK_SKEW_MS,
            "Events session lifetime exceeds the limit"
        );
        self.session.sign(
            purpose,
            &Token {
                cluster_id: self.cluster_id,
                issued_at_ms: now_ms,
                expires_at_ms: expires_ms,
                claims,
            },
        )
    }

    fn verify_session<T: DeserializeOwned>(
        &self,
        purpose: &[u8],
        credential: &str,
        now_ms: i64,
    ) -> Option<T> {
        let token = self.session.verify(purpose, credential, now_ms)?;
        if token.cluster_id != self.cluster_id
            || token.expires_at_ms - token.issued_at_ms
                > super::partition::MAX_LEASE_MS + MAX_CLOCK_SKEW_MS
        {
            return None;
        }
        Some(token.claims)
    }
}

pub fn authorize_operator(
    headers: &HeaderMap,
    keyring: &Keyring,
    now_ms: i64,
) -> Result<Grant, ApiError> {
    keyring.authorize(headers, Access::Operator, now_ms)
}

fn one_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let values = headers.get_all(name);
    let mut matching = values.iter();
    let value = matching.next()?.to_str().ok()?;
    matching.next().is_none().then_some(value)
}
