//! Synchronous, retained local material. C08b2 will compose these concrete records.
use super::super::{
    configuration::{self, ConfigurationError, Tls},
    startup_config::{self, ApiConfig, Bootstrap, MAX_INPUT_BYTES},
};
use crate::protocol::deserialize_object;
use nabedra_rt::time;
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
    sign::CertifiedKey,
};
use serde::{Deserialize, Deserializer};
use std::{
    fmt,
    fs::{self, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};
const AGGREGATE: usize = 8 * 1024 * 1024;
const AUTH: usize = 16 * 1024;
const TOKEN: usize = 4 * 1024;
const KEY: usize = 256 * 1024;
const CERTS: usize = 1024 * 1024;
const DER: usize = 64 * 1024;
const INVALID: ConfigurationError = ConfigurationError::InvalidRecord;
const LIMIT: ConfigurationError = ConfigurationError::LimitExceeded;
macro_rules! redacted {
    ($($ty:ident),*) => { $(impl fmt::Debug for $ty {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.debug_struct(stringify!($ty)).finish_non_exhaustive() }
    })* };
}
// C08b2 callers will consume retained bytes and parsed records without reopening.
#[allow(dead_code)]
pub(crate) struct LoadedInput<T> {
    pub(crate) bytes: Arc<[u8]>,
    pub(crate) value: T,
}
impl<T> fmt::Debug for LoadedInput<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LoadedInput { .. }")
    }
}
pub(crate) struct Auth {
    pub(crate) bytes: Arc<[u8]>,
    pub(crate) admin_token: String,
    pub(crate) data_token: String,
    pub(crate) enrollment_token: String,
}
impl<'de> Deserialize<'de> for Auth {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            admin_token: String,
            data_token: String,
            enrollment_token: String,
        }
        let w: Wire = deserialize_object(d)?;
        Ok(Self {
            bytes: Arc::from([]),
            admin_token: w.admin_token,
            data_token: w.data_token,
            enrollment_token: w.enrollment_token,
        })
    }
}
#[allow(dead_code)] // C08b2 retains both original file and effective token.
pub(crate) struct HttpToken {
    pub(crate) bytes: Arc<[u8]>,
    pub(crate) value: String,
}
#[allow(dead_code)] // C08b2 identities use sorted DER, constructors use these same roots/PEM.
pub(crate) struct CaMaterial {
    pub(crate) pem: Arc<[u8]>,
    pub(crate) der: Vec<CertificateDer<'static>>,
    pub(crate) roots: rustls::RootCertStore,
}
#[allow(dead_code)] // C08b2 reuses the validated provider-owned signing key and ordered chain.
pub(crate) struct TlsMaterial {
    pub(crate) chain_pem: Arc<[u8]>,
    pub(crate) key_bytes: Arc<[u8]>,
    pub(crate) ca: CaMaterial,
    pub(crate) certified_key: CertifiedKey,
}
struct Cached {
    path: PathBuf,
    bytes: Arc<[u8]>,
    protected: bool,
}
pub(crate) struct Reader {
    base: PathBuf,
    input: PathBuf,
    deadline: Instant,
    total: usize,
    cache: Vec<Cached>,
    #[cfg(test)]
    pub(super) system_candidates: Option<Vec<PathBuf>>,
}
redacted!(Auth, HttpToken, CaMaterial, TlsMaterial, Reader, Cached);
impl Reader {
    pub(crate) fn new(input: &Path, deadline: Instant) -> Result<Self, ConfigurationError> {
        if !input.is_absolute() {
            return Err(INVALID);
        }
        Ok(Self {
            base: input.parent().ok_or(INVALID)?.into(),
            input: input.into(),
            deadline,
            total: 0,
            cache: Vec::new(),
            #[cfg(test)]
            system_candidates: None,
        })
    }
    fn check(&self) -> Result<(), ConfigurationError> {
        if time::now_std() >= self.deadline {
            Err(LIMIT)
        } else {
            Ok(())
        }
    }
    fn resolve(&self, hint: &str) -> Result<PathBuf, ConfigurationError> {
        if hint.is_empty() || hint.len() > 2048 || hint.contains('\0') {
            return Err(INVALID);
        }
        Ok(self.base.join(hint))
    }
    pub(crate) fn api(&mut self) -> Result<LoadedInput<ApiConfig>, ConfigurationError> {
        let path = self.input.clone();
        let bytes = self.read_path(&path, MAX_INPUT_BYTES, false, || {})?;
        let value = startup_config::decode_api(&bytes)?;
        self.check()?;
        Ok(LoadedInput { bytes, value })
    }
    pub(crate) fn bootstrap(&mut self) -> Result<LoadedInput<Bootstrap>, ConfigurationError> {
        let path = self.input.clone();
        let bytes = self.read_path(&path, MAX_INPUT_BYTES, true, || {})?;
        let value = startup_config::decode_bootstrap(&bytes)?;
        self.check()?;
        Ok(LoadedInput { bytes, value })
    }
    fn read(
        &mut self,
        hint: &str,
        cap: usize,
        secret: bool,
    ) -> Result<Arc<[u8]>, ConfigurationError> {
        self.read_with(hint, cap, secret, || {})
    }
    fn read_with(
        &mut self,
        hint: &str,
        cap: usize,
        secret: bool,
        before_open: impl FnOnce(),
    ) -> Result<Arc<[u8]>, ConfigurationError> {
        let path = self.resolve(hint)?;
        self.read_path(&path, cap, secret, before_open)
    }
    fn read_path(
        &mut self,
        path: &Path,
        cap: usize,
        secret: bool,
        before_open: impl FnOnce(),
    ) -> Result<Arc<[u8]>, ConfigurationError> {
        self.read_optional(path, cap, secret, before_open)?
            .ok_or(INVALID)
    }
    fn read_optional(
        &mut self,
        path: &Path,
        cap: usize,
        secret: bool,
        before_open: impl FnOnce(),
    ) -> Result<Option<Arc<[u8]>>, ConfigurationError> {
        #[cfg(test)]
        READ_ATTEMPTS.with(|slot| {
            if let Some(paths) = slot.borrow_mut().as_mut() {
                assert!(paths.len() < 32, "read observation bound");
                paths.push(path.to_owned());
            }
        });
        self.check()?;
        if let Some(cached) = self.cache.iter().find(|v| v.path == path) {
            if secret && !cached.protected {
                return Err(INVALID);
            }
            if cached.bytes.len() > cap {
                return Err(LIMIT);
            }
            return Ok(Some(cached.bytes.clone()));
        }
        if self.cache.len() >= 16 {
            return Err(LIMIT);
        }
        if !fs::metadata(path).is_ok_and(|m| m.is_file()) {
            return Ok(None);
        }
        self.check()?;
        before_open();
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NONBLOCK);
        }
        let mut file = match options.open(path) {
            Ok(file) => file,
            Err(_) => return Ok(None),
        };
        self.check()?;
        let meta = file.metadata().map_err(|_| INVALID)?;
        if !meta.is_file() {
            return Err(INVALID);
        }
        #[cfg(unix)]
        let protected = {
            use std::os::unix::fs::PermissionsExt;
            meta.permissions().mode() & 0o077 == 0
        };
        #[cfg(not(unix))]
        let protected = true;
        if secret && !protected {
            return Err(INVALID);
        }
        let mut bytes = Vec::new();
        let mut buf = [0u8; 8192];
        loop {
            self.check()?;
            let size = buf
                .len()
                .min(cap.checked_add(1).ok_or(LIMIT)?.saturating_sub(bytes.len()))
                .min(AGGREGATE.saturating_sub(self.total).saturating_add(1));
            if size == 0 {
                return Err(LIMIT);
            }
            let n = file.read(&mut buf[..size]).map_err(|_| INVALID)?;
            self.check()?;
            self.total = self.total.checked_add(n).ok_or(LIMIT)?;
            if self.total > AGGREGATE {
                return Err(LIMIT);
            }
            if n == 0 {
                break;
            }
            bytes.extend_from_slice(&buf[..n]);
            if bytes.len() > cap {
                return Err(LIMIT);
            }
        }
        if bytes.is_empty() {
            return Err(INVALID);
        }
        let bytes: Arc<[u8]> = bytes.into();
        self.cache.push(Cached {
            path: path.into(),
            bytes: bytes.clone(),
            protected,
        });
        self.check()?;
        Ok(Some(bytes))
    }
    pub(crate) fn auth(&mut self, hint: &str) -> Result<Auth, ConfigurationError> {
        let bytes = self.read(hint, AUTH, true)?;
        configuration::input(&bytes, AUTH)?;
        let mut auth: Auth = serde_json::from_slice(&bytes).map_err(|_| INVALID)?;
        let tokens = [&auth.admin_token, &auth.data_token, &auth.enrollment_token];
        if !tokens
            .iter()
            .all(|s| (43..=4096).contains(&s.len()) && printable(s.as_bytes()))
            || tokens[0] == tokens[1]
            || tokens[0] == tokens[2]
            || tokens[1] == tokens[2]
        {
            return Err(INVALID);
        }
        auth.bytes = bytes;
        self.check()?;
        Ok(auth)
    }
    pub(crate) fn http_token(&mut self, hint: &str) -> Result<HttpToken, ConfigurationError> {
        let bytes = self.read(hint, TOKEN, true)?;
        let value = bytes.strip_suffix(b"\n").unwrap_or(&bytes);
        if value.is_empty() || !printable(value) {
            return Err(INVALID);
        }
        let value = String::from_utf8(value.to_vec()).map_err(|_| INVALID)?;
        self.check()?;
        Ok(HttpToken { bytes, value })
    }
    pub(crate) fn http_ca(
        &mut self,
        https: bool,
        supplied: Option<&str>,
    ) -> Result<Option<CaMaterial>, ConfigurationError> {
        #[cfg(test)]
        if let Some(candidates) = self.system_candidates.clone() {
            let paths: Vec<_> = candidates.iter().map(PathBuf::as_path).collect();
            return self.http_ca_candidates(https, supplied, &paths);
        }
        self.http_ca_candidates(
            https,
            supplied,
            &[
                Path::new("/etc/ssl/cert.pem"),
                Path::new("/etc/ssl/certs/ca-certificates.crt"),
                Path::new("/etc/pki/tls/certs/ca-bundle.crt"),
            ],
        )
    }
    fn http_ca_candidates(
        &mut self,
        https: bool,
        supplied: Option<&str>,
        candidates: &[&Path],
    ) -> Result<Option<CaMaterial>, ConfigurationError> {
        self.check()?;
        if let Some(hint) = supplied {
            return self.ca(hint).map(Some);
        }
        if !https {
            return Ok(None);
        }
        for path in candidates {
            self.check()?;
            // Once opened, read/cap/parse failure is terminal. Only an
            // unreadable or nonregular candidate permits selecting the next.
            if let Some(bytes) = self.read_optional(path, CERTS, false, || {})? {
                return self.parse_ca(bytes).map(Some);
            }
        }
        Err(INVALID)
    }
    fn ca(&mut self, hint: &str) -> Result<CaMaterial, ConfigurationError> {
        let bytes = self.read(hint, CERTS, false)?;
        self.parse_ca(bytes)
    }
    fn parse_ca(&self, pem: Arc<[u8]>) -> Result<CaMaterial, ConfigurationError> {
        let mut der = self.certificates(&pem, 1024)?;
        der.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
        der.dedup();
        let mut roots = rustls::RootCertStore::empty();
        for cert in &der {
            self.check()?;
            roots.add(cert.clone()).map_err(|_| INVALID)?;
        }
        self.check()?;
        Ok(CaMaterial { pem, der, roots })
    }
    fn certificates(
        &self,
        pem: &[u8],
        count: usize,
    ) -> Result<Vec<CertificateDer<'static>>, ConfigurationError> {
        let blocks = pem_blocks(pem)?;
        let mut certs = Vec::new();
        for block in blocks {
            self.check()?;
            if certs.len() == count {
                return Err(LIMIT);
            }
            let cert = CertificateDer::from_pem_slice(block).map_err(|_| INVALID)?;
            if cert.len() > DER {
                return Err(LIMIT);
            }
            // Parse each DER, including non-leaf chain entries, using rustls.
            let mut roots = rustls::RootCertStore::empty();
            roots.add(cert.clone()).map_err(|_| INVALID)?;
            certs.push(cert);
        }
        if certs.is_empty() {
            return Err(INVALID);
        }
        self.check()?;
        Ok(certs)
    }
    pub(crate) fn tls(&mut self, hints: &Tls) -> Result<TlsMaterial, ConfigurationError> {
        let chain_pem = self.read(&hints.cert_file, CERTS, false)?;
        let certs = self.certificates(&chain_pem, 64)?;
        let key_bytes = self.read(&hints.key_file, KEY, true)?;
        let key = if key_bytes.first() == Some(&0x30) {
            PrivateKeyDer::try_from(key_bytes.to_vec()).map_err(|_| INVALID)?
        } else {
            let blocks = pem_blocks(&key_bytes)?;
            if blocks.len() != 1 {
                return Err(INVALID);
            }
            PrivateKeyDer::from_pem_slice(blocks[0]).map_err(|_| INVALID)?
        };
        complete_der_sequence(key.secret_der())?;
        self.check()?;
        let provider = rustls::crypto::ring::default_provider();
        let certified_key = CertifiedKey::from_der(certs, key, &provider).map_err(|_| INVALID)?;
        certified_key.keys_match().map_err(|_| INVALID)?;
        self.check()?;
        let ca = self.ca(&hints.ca_file)?;
        self.check()?;
        Ok(TlsMaterial {
            chain_pem,
            key_bytes,
            ca,
            certified_key,
        })
    }
}
// Provider loaders may consume only the first DER object. Require exactly one
// definite, minimally encoded outer SEQUENCE for every supported key encoding.
// Validation of the sequence's key contents remains with the existing provider.
fn complete_der_sequence(bytes: &[u8]) -> Result<(), ConfigurationError> {
    if bytes.first() != Some(&0x30) {
        return Err(INVALID);
    }
    let first = *bytes.get(1).ok_or(INVALID)?;
    let (header, length) = if first < 0x80 {
        (2usize, usize::from(first))
    } else {
        let count = usize::from(first & 0x7f);
        if count == 0 || count > std::mem::size_of::<usize>() {
            return Err(INVALID);
        }
        let octets = bytes.get(2..2 + count).ok_or(INVALID)?;
        if octets[0] == 0 {
            return Err(INVALID);
        }
        let length = octets.iter().try_fold(0usize, |value, octet| {
            value
                .checked_mul(256)
                .and_then(|value| value.checked_add(usize::from(*octet)))
                .ok_or(INVALID)
        })?;
        if length < 128 {
            return Err(INVALID);
        }
        (2 + count, length)
    };
    if header.checked_add(length) != Some(bytes.len()) {
        return Err(INVALID);
    }
    Ok(())
}
fn printable(bytes: &[u8]) -> bool {
    bytes.iter().all(|b| (33..=126).contains(b))
}
// rustls's PEM iterator skips unrelated text and unknown labels. Validate the
// envelope first so trailing junk and nested/truncated sections cannot disappear.
fn pem_blocks(bytes: &[u8]) -> Result<Vec<&[u8]>, ConfigurationError> {
    let text = std::str::from_utf8(bytes).map_err(|_| INVALID)?;
    let mut rest = text.trim_ascii();
    let mut blocks = Vec::new();
    while !rest.is_empty() {
        let header_end = rest.find('\n').ok_or(INVALID)?;
        let header = rest[..header_end].trim_end_matches('\r');
        let label = header
            .strip_prefix("-----BEGIN ")
            .and_then(|s| s.strip_suffix("-----"))
            .ok_or(INVALID)?;
        if !matches!(
            label,
            "CERTIFICATE" | "PRIVATE KEY" | "RSA PRIVATE KEY" | "EC PRIVATE KEY"
        ) {
            return Err(INVALID);
        }
        let end = format!("-----END {label}-----");
        let pos = rest.find(&end).ok_or(INVALID)?;
        let body = &rest[header_end + 1..pos];
        if body.is_empty()
            || !body
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"+/=\r\n".contains(&b))
        {
            return Err(INVALID);
        }
        let len = pos + end.len();
        blocks.push(&rest.as_bytes()[..len]);
        rest = rest[len..].trim_ascii();
    }
    if blocks.is_empty() {
        return Err(INVALID);
    }
    Ok(blocks)
}
#[cfg(test)]
pub(crate) mod tests;

#[cfg(test)]
thread_local! {
    static READ_ATTEMPTS: std::cell::RefCell<Option<Vec<PathBuf>>> = const { std::cell::RefCell::new(None) };
}
/// Scoped to one synchronous call on this thread; unwind also releases ownership.
#[cfg(test)]
pub(crate) fn observe_reads<T>(call: impl FnOnce() -> T) -> (T, Vec<PathBuf>) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            READ_ATTEMPTS.with(|slot| {
                slot.replace(None);
            });
        }
    }
    READ_ATTEMPTS.with(|slot| {
        assert!(slot.borrow().is_none());
        slot.replace(Some(Vec::new()));
    });
    let reset = Reset;
    let value = call();
    let paths = READ_ATTEMPTS.with(|slot| slot.replace(None).unwrap());
    drop(reset);
    (value, paths)
}
