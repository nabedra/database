//! Versioned archive object envelopes. The keyring is local operator config.
use super::*;
use ring::{
    aead::{self, AES_256_GCM, Aad, LessSafeKey, Nonce, Tag, UnboundKey},
    rand::{SecureRandom, SystemRandom},
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

const MAGIC: &[u8; 16] = b"NABEDRA-ARCHIVE\0";
const VERSION: u8 = 1;
const PREFIX_LEN: usize = MAGIC.len() + 2;
const MAX_KEY_IDS: usize = 16;
const MAX_KEY_ID_LEN: usize = 64;
const MAX_HEADER_LEN: usize = PREFIX_LEN + MAX_KEY_ID_LEN + aead::NONCE_LEN;
const MAX_OBJECT_NAME_LEN: usize = "part-00000000000000000000".len();
pub(super) const MAX_OVERHEAD: usize = MAX_HEADER_LEN + aead::MAX_TAG_LEN;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArchiveEncryptionConfig {
    #[serde(default)]
    write_key: Option<String>,
    keyring: Vec<ArchiveKeyFile>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchiveKeyFile {
    id: String,
    file: PathBuf,
}

enum WriteMode {
    Plaintext,
    ReadOnly,
    Encrypted(String),
}

pub(crate) struct ArchiveKeys {
    write: WriteMode,
    read: BTreeMap<String, LessSafeKey>,
}

impl ArchiveKeys {
    pub(crate) fn load(
        config: Option<&ArchiveEncryptionConfig>,
    ) -> Result<Arc<Self>, ProtocolError> {
        let mut read = BTreeMap::new();
        let write = match config {
            None => WriteMode::Plaintext,
            Some(config) => {
                if config.keyring.len() > MAX_KEY_IDS {
                    return Err(invalid_keys());
                }
                for entry in &config.keyring {
                    if !valid_key_id(&entry.id) || read.contains_key(&entry.id) {
                        return Err(invalid_keys());
                    }
                    read.insert(entry.id.clone(), read_key_file(&entry.file)?);
                }
                match &config.write_key {
                    Some(id) if read.contains_key(id) => WriteMode::Encrypted(id.clone()),
                    Some(_) => return Err(invalid_keys()),
                    None => WriteMode::ReadOnly,
                }
            }
        };
        Ok(Arc::new(Self { write, read }))
    }

    pub(crate) fn writes_encrypted(&self) -> bool {
        matches!(self.write, WriteMode::Encrypted(_))
    }

    pub(crate) fn require_write(&self) -> Result<(), ProtocolError> {
        if matches!(self.write, WriteMode::ReadOnly) {
            return Err(ProtocolError::new(
                ErrorCode::UnsupportedOperation,
                "archive writes require a selected archive encryption key",
                Outcome::NotApplied,
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(super) enum ObjectProtection {
    Plaintext,
    Encrypted,
}

/// The only repository used by archive producers and readers. The sealed
/// transport owns conditional writes and durable acknowledgments.
#[derive(Clone)]
pub(crate) struct ArchiveRepository {
    raw: Arc<dyn Repository>,
    keys: Arc<ArchiveKeys>,
}

impl ArchiveRepository {
    pub(crate) fn new(raw: Arc<dyn Repository>, keys: Arc<ArchiveKeys>) -> Self {
        Self { raw, keys }
    }

    pub(crate) fn shared(&self) -> &Arc<WorkerSharedResources> {
        self.raw.shared()
    }

    pub(super) fn write_protection(&self) -> ObjectProtection {
        if self.keys.writes_encrypted() {
            ObjectProtection::Encrypted
        } else {
            ObjectProtection::Plaintext
        }
    }

    pub(crate) async fn put(
        &self,
        scope: ArchiveKey,
        name: String,
        body: ChargedBytes,
    ) -> Result<Digest, ProtocolError> {
        let limit = plain_limit(&name);
        if !scope.valid() || body.is_empty() || body.len() > limit {
            return Err(invalid());
        }
        self.keys.require_write()?;
        let WriteMode::Encrypted(id) = &self.keys.write else {
            return self.raw.put(scope, name, body).await;
        };
        let key = self.keys.read.get(id).ok_or_else(missing_key)?;
        let sealed = self.seal(scope, &name, id, key, body.as_ref())?;
        match self.raw.put(scope, name.clone(), sealed).await {
            Err(error) if error == conflict() => {
                // A retry uses a new nonce. Only the transport's confirmed
                // conflict permits reconciliation, and only authenticated,
                // byte-for-byte identical plaintext proves this write.
                let existing = self
                    .raw
                    .get(scope, name.clone(), limit + MAX_OVERHEAD)
                    .await?;
                if !existing.as_ref().starts_with(MAGIC) {
                    return Err(error);
                }
                let opened = self.open(scope, &name, existing.as_ref(), limit)?;
                if opened.as_ref() != body.as_ref() {
                    return Err(error);
                }
                Ok(hash(existing.as_ref()))
            }
            result => result,
        }
    }

    pub(super) async fn get_root(
        &self,
        scope: ArchiveKey,
        limit: usize,
    ) -> Result<(ChargedBytes, Digest, ObjectProtection), ProtocolError> {
        if limit == 0 || limit > MAX_ROOT_BYTES {
            return Err(invalid());
        }
        let stored = self
            .raw
            .get(scope, ROOT_OBJECT.into(), limit + MAX_OVERHEAD)
            .await?;
        let digest = hash(stored.as_ref());
        if stored.as_ref().starts_with(MAGIC) {
            let body = self.open(scope, ROOT_OBJECT, stored.as_ref(), limit)?;
            Ok((body, digest, ObjectProtection::Encrypted))
        } else if stored.len() <= limit {
            Ok((stored, digest, ObjectProtection::Plaintext))
        } else {
            Err(corrupt())
        }
    }

    pub(crate) async fn get_root_bytes(
        &self,
        scope: ArchiveKey,
        limit: usize,
    ) -> Result<ChargedBytes, ProtocolError> {
        Ok(self.get_root(scope, limit).await?.0)
    }

    pub(super) async fn get_part(
        &self,
        scope: ArchiveKey,
        name: String,
        limit: usize,
        protection: ObjectProtection,
    ) -> Result<ChargedBytes, ProtocolError> {
        match protection {
            ObjectProtection::Plaintext => self.raw.get(scope, name, limit).await,
            ObjectProtection::Encrypted => {
                let stored = self
                    .raw
                    .get(scope, name.clone(), limit + MAX_OVERHEAD)
                    .await?;
                self.open(scope, &name, stored.as_ref(), limit)
            }
        }
    }

    fn seal(
        &self,
        scope: ArchiveKey,
        name: &str,
        id: &str,
        key: &LessSafeKey,
        bytes: &[u8],
    ) -> Result<ChargedBytes, ProtocolError> {
        let mut nonce = [0; aead::NONCE_LEN];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| unavailable())?;
        let nonce_offset = PREFIX_LEN + id.len();
        let header_len = nonce_offset + nonce.len();
        let mut buffer = self.shared().allocate_bytes(
            ByteClass::Data,
            header_len + bytes.len() + aead::MAX_TAG_LEN,
        )?;
        let (header, remainder) = buffer.as_mut_slice().split_at_mut(header_len);
        let (encrypted, tag) = remainder.split_at_mut(bytes.len());
        header[..MAGIC.len()].copy_from_slice(MAGIC);
        header[MAGIC.len()] = VERSION;
        header[MAGIC.len() + 1] = id.len() as u8;
        header[PREFIX_LEN..nonce_offset].copy_from_slice(id.as_bytes());
        header[nonce_offset..].copy_from_slice(&nonce);
        encrypted.copy_from_slice(bytes);
        let sealed = key
            .seal_in_place_separate_tag(
                Nonce::assume_unique_for_key(nonce),
                associated_data(scope, name, header),
                encrypted,
            )
            .map_err(|_| unavailable())?;
        tag.copy_from_slice(sealed.as_ref());
        Ok(buffer.freeze())
    }

    fn open(
        &self,
        scope: ArchiveKey,
        name: &str,
        stored: &[u8],
        limit: usize,
    ) -> Result<ChargedBytes, ProtocolError> {
        if !stored.starts_with(MAGIC) || stored.get(MAGIC.len()) != Some(&VERSION) {
            return Err(corrupt());
        }
        let id_len = usize::from(*stored.get(MAGIC.len() + 1).ok_or_else(corrupt)?);
        let nonce_offset = PREFIX_LEN + id_len;
        let header_len = nonce_offset + aead::NONCE_LEN;
        let len = stored
            .len()
            .checked_sub(header_len + aead::MAX_TAG_LEN)
            .filter(|&len| len > 0 && len <= limit)
            .ok_or_else(corrupt)?;
        let id = std::str::from_utf8(&stored[PREFIX_LEN..nonce_offset]).map_err(|_| corrupt())?;
        if !valid_key_id(id) {
            return Err(corrupt());
        }
        let key = self.keys.read.get(id).ok_or_else(missing_key)?;
        let nonce = Nonce::try_assume_unique_for_key(&stored[nonce_offset..header_len])
            .map_err(|_| corrupt())?;
        let tag = Tag::try_from(&stored[header_len + len..]).map_err(|_| corrupt())?;
        let mut buffer = self.shared().allocate_bytes(ByteClass::Data, len)?;
        buffer
            .as_mut_slice()
            .copy_from_slice(&stored[header_len..header_len + len]);
        key.open_in_place_separate_tag(
            nonce,
            associated_data(scope, name, &stored[..header_len]),
            tag,
            buffer.as_mut_slice(),
            0..,
        )
        .map_err(|_| corrupt())?;
        Ok(buffer.freeze())
    }
}

fn associated_data(
    scope: ArchiveKey,
    name: &str,
    header: &[u8],
) -> Aad<[u8; MAX_HEADER_LEN + 32 + MAX_OBJECT_NAME_LEN]> {
    let mut aad = [0; MAX_HEADER_LEN + 32 + MAX_OBJECT_NAME_LEN];
    aad[..header.len()].copy_from_slice(header);
    aad[MAX_HEADER_LEN..MAX_HEADER_LEN + 16].copy_from_slice(scope.cluster_id.as_bytes());
    aad[MAX_HEADER_LEN + 16..MAX_HEADER_LEN + 32].copy_from_slice(scope.archive_id.as_bytes());
    aad[MAX_HEADER_LEN + 32..MAX_HEADER_LEN + 32 + name.len()].copy_from_slice(name.as_bytes());
    Aad::from(aad)
}

fn valid_key_id(id: &str) -> bool {
    (1..=MAX_KEY_ID_LEN).contains(&id.len())
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

#[cfg(unix)]
fn read_key_file(path: &Path) -> Result<LessSafeKey, ProtocolError> {
    use std::{
        fs::OpenOptions,
        io::{ErrorKind, Read},
        os::unix::fs::{MetadataExt, OpenOptionsExt},
    };

    if !path.is_absolute() || path.as_os_str().len() > 4096 {
        return Err(invalid_keys());
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
        .map_err(|error| match error.kind() {
            ErrorKind::NotFound => missing_key(),
            _ => invalid_keys(),
        })?;
    let metadata = file.metadata().map_err(|_| invalid_keys())?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || !matches!(metadata.mode() & 0o7777, 0o400 | 0o600)
        || metadata.len() != 32
    {
        return Err(invalid_keys());
    }

    let mut bytes = [0; 32];
    let mut extra = [0];
    let key = (|| {
        file.read_exact(&mut bytes).map_err(|_| invalid_keys())?;
        if file.read(&mut extra).map_err(|_| invalid_keys())? != 0 {
            return Err(invalid_keys());
        }
        UnboundKey::new(&AES_256_GCM, &bytes)
            .map(LessSafeKey::new)
            .map_err(|_| invalid_keys())
    })();
    bytes.fill(0);
    key
}

#[cfg(not(unix))]
fn read_key_file(_path: &Path) -> Result<LessSafeKey, ProtocolError> {
    Err(invalid_keys())
}

fn missing_key() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::StorageUnavailable,
        "archive encryption key is unavailable",
        Outcome::NotApplied,
    )
}

fn invalid_keys() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::ValidationError,
        "invalid private archive encryption configuration",
        Outcome::NotApplied,
    )
}
