//! Cache client authentication and user-specific command and connection limits.
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use ring::hmac;
use ring::rand::{SecureRandom, SystemRandom};
use serde::Deserialize;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::command::{CommandKind, Credentials, DataCommand, MAX_KEY_BYTES};

const DEFAULT_CONNECTION_LIMIT: usize = 128;
const MAX_USERNAME_BYTES: usize = 128;
const MAX_PASSWORD_BYTES: usize = 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserConfig {
    pub username: String,
    pub password: String,
    pub key_prefixes: Vec<String>,
    pub commands: Vec<CommandKind>,
    pub max_connections: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclConfigError {
    Username,
    Password,
    KeyPrefix,
    ConnectionLimit,
    DuplicateUser,
    RandomUnavailable,
}

impl fmt::Display for AclConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Username => "invalid cache ACL username",
            Self::Password => "invalid cache ACL password length",
            Self::KeyPrefix => "invalid cache ACL key prefix",
            Self::ConnectionLimit => "invalid cache ACL connection limit",
            Self::DuplicateUser => "duplicate cache ACL username",
            Self::RandomUnavailable => "cache ACL random source unavailable",
        })
    }
}

impl std::error::Error for AclConfigError {}

pub struct Acl {
    users: HashMap<String, Identity>,
    password_key: hmac::Key,
    dummy_tag: hmac::Tag,
}

impl Acl {
    pub fn new(users: Vec<UserConfig>) -> Result<Self, AclConfigError> {
        // Hash passwords once at load and compare fixed-size tags in constant time.
        let mut secret = [0; 32];
        SystemRandom::new()
            .fill(&mut secret)
            .map_err(|_| AclConfigError::RandomUnavailable)?;
        let password_key = hmac::Key::new(hmac::HMAC_SHA256, &secret);
        let dummy_tag = hmac::sign(&password_key, b"");
        let mut identities = HashMap::with_capacity(users.len());
        for user in users {
            let connection_limit = user.max_connections.unwrap_or(DEFAULT_CONNECTION_LIMIT);
            if user.username.is_empty()
                || user.username.len() > MAX_USERNAME_BYTES
                || !user
                    .username
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_.@-".contains(&b))
            {
                return Err(AclConfigError::Username);
            }
            if user.password.is_empty() || user.password.len() > MAX_PASSWORD_BYTES {
                return Err(AclConfigError::Password);
            }
            if user
                .key_prefixes
                .iter()
                .any(|prefix| prefix.is_empty() || prefix.len() > MAX_KEY_BYTES)
            {
                return Err(AclConfigError::KeyPrefix);
            }
            if connection_limit == 0 || connection_limit > Semaphore::MAX_PERMITS {
                return Err(AclConfigError::ConnectionLimit);
            }
            let commands = user
                .commands
                .into_iter()
                .fold(0, |bits, command| bits | command_bit(command));
            let identity = Identity {
                name: user.username.clone(),
                tag: hmac::sign(&password_key, user.password.as_bytes()),
                key_prefixes: user.key_prefixes,
                commands,
                connections: Arc::new(Semaphore::new(connection_limit)),
            };
            if identities.insert(user.username, identity).is_some() {
                return Err(AclConfigError::DuplicateUser);
            }
        }
        Ok(Self {
            users: identities,
            password_key,
            dummy_tag,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.users.is_empty()
    }

    pub fn authenticate(&self, credentials: &Credentials) -> Option<&Identity> {
        if credentials.password.len() > MAX_PASSWORD_BYTES {
            return None;
        }
        let username = credentials.username.as_deref().unwrap_or(b"default");
        let user = std::str::from_utf8(username)
            .ok()
            .filter(|name| name.len() <= MAX_USERNAME_BYTES)
            .and_then(|name| self.users.get(name));
        let expected = user.map_or(&self.dummy_tag, |user| &user.tag);
        hmac::verify(&self.password_key, &credentials.password, expected.as_ref())
            .ok()
            .and(user)
    }
}

pub struct Identity {
    name: String,
    tag: hmac::Tag,
    key_prefixes: Vec<String>,
    commands: u16,
    connections: Arc<Semaphore>,
}

impl Identity {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn allows(&self, command: &DataCommand) -> bool {
        self.commands & command_bit(command.kind()) != 0
            && command.keys().all(|key| self.allows_key(key))
    }

    pub fn allows_key(&self, key: &[u8]) -> bool {
        self.key_prefixes
            .iter()
            .any(|prefix| prefix == "*" || key.starts_with(prefix.as_bytes()))
    }

    pub fn try_connect(&self) -> Option<OwnedSemaphorePermit> {
        self.connections.clone().try_acquire_owned().ok()
    }
}

fn command_bit(command: CommandKind) -> u16 {
    1 << command as u16
}

#[cfg(test)]
mod tests;
