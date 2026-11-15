//! Pure identities of retained material. These do not certify backend construction.
use super::{
    super::configuration::{ConfigurationError, Template, Timing, bounded},
    files::{Auth, CaMaterial, HttpToken, TlsMaterial},
};
use crate::{partition_size::PartitionSize, storage::Namespace};
use rustls::pki_types::CertificateDer;
use serde::{Serialize, Serializer};
use sha2::{Digest, Sha256};
use std::fmt;
use uuid::Uuid;

const MAX_BYTES: usize = 16 * 1024 * 1024;
const SECRET: &str = "meshdb.config-secret.v1";
const ROOTS: &str = "meshdb.config-roots.v1";
const CHAIN: &str = "meshdb.config-chain.v1";
const STORAGE: &str = "meshdb.control-storage.v1";
const AUTH: &str = "meshdb.control-auth.v1";
const LIFECYCLE: &str = "meshdb.control-lifecycle.v1";
const WORKER: &str = "meshdb.control-worker-semantic.v1";

#[derive(PartialEq, Eq)]
pub(crate) struct Fingerprint(String);
impl fmt::Debug for Fingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Fingerprint { .. }")
    }
}
impl Fingerprint {
    // C08b2b/C08c consume these values; no Display or Serialize on the result.
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(DIGITS[(byte >> 4) as usize] as char);
        result.push(DIGITS[(byte & 15) as usize] as char);
    }
    result
}
fn hash(label: &str, value: &impl Serialize) -> Result<Fingerprint, ConfigurationError> {
    let bytes = bounded(value, MAX_BYTES)?;
    let mut digest = Sha256::new();
    digest.update(label.as_bytes());
    digest.update([0]);
    digest.update(bytes);
    Ok(Fingerprint(hex(&digest.finalize())))
}
// Only these fixed wire records serialize, never the loaded material or inputs.
#[derive(Serialize)]
struct Secret<'a> {
    role: &'a str,
    sha256: String,
}
#[derive(Serialize)]
struct Certificates<'a> {
    certificates: Vec<Hex<'a>>,
}
struct Hex<'a>(&'a [u8]);
impl Serialize for Hex<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // One bounded certificate at a time; the shared writer counts all syntax.
        if self.0.len() > MAX_BYTES / 2 {
            return Err(serde::ser::Error::custom("configuration limit"));
        }
        serializer.serialize_str(&hex(self.0))
    }
}
#[derive(Serialize)]
struct Http<'a> {
    base_url: &'a str,
    ca: Option<&'a str>,
    driver: &'a str,
    namespace: &'a str,
    token: &'a str,
}
#[derive(Serialize)]
struct S3<'a> {
    bucket: &'a str,
    driver: &'a str,
    endpoint_policy: &'a str,
    namespace: &'a str,
    region: &'a str,
}
#[derive(Serialize)]
struct AuthWire<'a> {
    admin: &'a str,
    cluster_id: &'a str,
    data: &'a str,
    enrollment: &'a str,
}
#[derive(Serialize)]
struct TlsWire<'a> {
    ca: &'a str,
    certificate_chain: &'a str,
}
#[derive(Serialize)]
struct Worker<'a> {
    api_urls: &'a [String],
    listen: &'a str,
    operation_slots: u64,
    partition_bytes: usize,
    partition_slots: u64,
    queue_size: u64,
    storage_fingerprint: &'a str,
    tls: Option<TlsWire<'a>>,
}
pub(crate) enum Role {
    Admin,
    Data,
    Enrollment,
    Http,
}
pub(crate) fn secret(role: Role, bytes: &[u8]) -> Result<Fingerprint, ConfigurationError> {
    let role = match role {
        Role::Admin => "admin",
        Role::Data => "data",
        Role::Enrollment => "enrollment",
        Role::Http => "storage.http",
    };
    hash(
        SECRET,
        &Secret {
            role,
            sha256: hex(&Sha256::digest(bytes)),
        },
    )
}
fn certificates(
    label: &str,
    certs: &[CertificateDer<'_>],
    sorted: bool,
) -> Result<Fingerprint, ConfigurationError> {
    let mut certs: Vec<&[u8]> = certs.iter().map(AsRef::as_ref).collect();
    if sorted {
        certs.sort_unstable();
        certs.dedup();
    }
    hash(
        label,
        &Certificates {
            certificates: certs.into_iter().map(Hex).collect(),
        },
    )
}
fn roots(ca: &CaMaterial) -> Result<Fingerprint, ConfigurationError> {
    certificates(ROOTS, &ca.der, true)
}
pub(crate) fn http(
    base_url: &str,
    namespace: &Namespace,
    token: &HttpToken,
    ca: Option<&CaMaterial>,
) -> Result<Fingerprint, ConfigurationError> {
    let token = secret(Role::Http, token.value.as_bytes())?;
    let ca = ca.map(roots).transpose()?;
    hash(
        STORAGE,
        &Http {
            base_url: base_url.trim_end_matches('/'),
            ca: ca.as_ref().map(Fingerprint::as_str),
            driver: "http",
            namespace: namespace.as_str(),
            token: token.as_str(),
        },
    )
}
#[allow(dead_code)] // C08b2c must independently prove the standard endpoint policy before calling.
pub(crate) fn s3(
    bucket: &str,
    namespace: &Namespace,
    region: &str,
) -> Result<Fingerprint, ConfigurationError> {
    hash(
        STORAGE,
        &S3 {
            bucket,
            driver: "s3",
            endpoint_policy: "aws-s3-standard.v1",
            namespace: namespace.as_str(),
            region,
        },
    )
}
pub(crate) fn authentication(
    cluster_id: Uuid,
    auth: &Auth,
) -> Result<Fingerprint, ConfigurationError> {
    let admin = secret(Role::Admin, auth.admin_token.as_bytes())?;
    let data = secret(Role::Data, auth.data_token.as_bytes())?;
    let enrollment = secret(Role::Enrollment, auth.enrollment_token.as_bytes())?;
    hash(
        AUTH,
        &AuthWire {
            admin: admin.as_str(),
            cluster_id: &cluster_id.to_string(),
            data: data.as_str(),
            enrollment: enrollment.as_str(),
        },
    )
}
pub(crate) fn lifecycle(timing: &Timing) -> Result<Fingerprint, ConfigurationError> {
    hash(LIFECYCLE, timing)
}
pub(crate) fn worker(
    template: &Template,
    storage: &Fingerprint,
    tls: Option<&TlsMaterial>,
) -> Result<Fingerprint, ConfigurationError> {
    let tls = tls
        .map(|tls| {
            Ok::<_, ConfigurationError>((
                roots(&tls.ca)?,
                certificates(CHAIN, &tls.certified_key.cert, false)?,
            ))
        })
        .transpose()?;
    let partition_bytes = template
        .partition_size
        .parse::<PartitionSize>()
        .map_err(|_| ConfigurationError::InvalidRecord)?
        .bytes();
    hash(
        WORKER,
        &Worker {
            api_urls: &template.api_urls,
            listen: &template.listen,
            operation_slots: template.operation_slots,
            partition_bytes,
            partition_slots: template.partition_slots,
            queue_size: template.queue_size,
            storage_fingerprint: storage.as_str(),
            tls: tls.as_ref().map(|(ca, chain)| TlsWire {
                ca: ca.as_str(),
                certificate_chain: chain.as_str(),
            }),
        },
    )
}
#[cfg(test)]
mod tests;
