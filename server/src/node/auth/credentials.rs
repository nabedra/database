//! Memory-owned signing keys. Signatures prove possession and bytes, never permission.
//! Seeds are never persisted or exported. No memory zeroization guarantee is made.
use super::{Auth, EnrollmentUnsigned, Error, ReplyAuth, Role};
use ring::{
    digest, hkdf,
    rand::{SecureRandom, SystemRandom},
    signature::{Ed25519KeyPair, KeyPair},
};
use uuid::Uuid;

#[allow(dead_code)] // Boot owner and R03a2b callers pending.
pub(crate) struct WorkerSigningKey {
    pair: Ed25519KeyPair,
}
#[allow(dead_code)] // Boot owner and R03a2b callers pending.
pub(crate) struct ApiSigningKey {
    cluster: Uuid,
    pair: Ed25519KeyPair,
}
impl std::fmt::Debug for WorkerSigningKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WorkerSigningKey")
    }
}
impl std::fmt::Debug for ApiSigningKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ApiSigningKey")
    }
}
struct Len32;
impl hkdf::KeyType for Len32 {
    fn len(&self) -> usize {
        32
    }
}
fn pair(seed: &[u8; 32]) -> Result<Ed25519KeyPair, Error> {
    Ed25519KeyPair::from_seed_unchecked(seed).map_err(|_| Error::Invalid)
}
fn public(pair: &Ed25519KeyPair) -> [u8; 32] {
    let mut bytes = [0; 32];
    bytes.copy_from_slice(pair.public_key().as_ref());
    bytes
}
// Closed domains: no caller can select arbitrary domain text.
enum Domain {
    Enrollment,
    Request,
    Response,
}
fn sign(pair: &Ed25519KeyPair, domain: Domain, body: &[u8]) -> Result<[u8; 64], Error> {
    let (prefix, cap): (&[u8], usize) = match domain {
        Domain::Enrollment => (b"meshdb.enroll.v1\0", 16 * 1024),
        Domain::Request => (b"meshdb.http-request.v1\0", 4096),
        Domain::Response => (b"meshdb.http-response.v1\0", 4096),
    };
    if body.len() > cap {
        return Err(Error::Limit);
    }
    let mut input = Vec::with_capacity(prefix.len() + body.len());
    input.extend_from_slice(prefix);
    input.extend_from_slice(body);
    let mut signature = [0; 64];
    signature.copy_from_slice(pair.sign(&input).as_ref());
    Ok(signature)
}
#[allow(dead_code)] // Boot owner retains this key across enrollment retries.
impl WorkerSigningKey {
    pub(crate) fn new() -> Result<Self, Error> {
        let mut seed = [0; 32];
        SystemRandom::new()
            .fill(&mut seed)
            .map_err(|_| Error::Invalid)?;
        Ok(Self { pair: pair(&seed)? })
    }
    #[cfg(test)]
    fn from_test_seed(seed: [u8; 32]) -> Result<Self, Error> {
        Ok(Self { pair: pair(&seed)? })
    }
    pub(crate) fn public_key(&self) -> [u8; 32] {
        public(&self.pair)
    }
    pub(crate) fn sign_enrollment(
        &self,
        enrollment: &EnrollmentUnsigned,
    ) -> Result<[u8; 64], Error> {
        if enrollment.public_key()? != self.public_key() {
            return Err(Error::Invalid);
        }
        sign(&self.pair, Domain::Enrollment, &enrollment.encode()?)
    }
    pub(crate) fn sign_request(&self, auth: &Auth) -> Result<[u8; 64], Error> {
        if auth.sender().role() != Role::Worker || auth.sender().public_key()? != self.public_key()
        {
            return Err(Error::Invalid);
        }
        sign(&self.pair, Domain::Request, &auth.canonical_bytes()?)
    }
    pub(crate) fn sign_response(&self, auth: &ReplyAuth) -> Result<[u8; 64], Error> {
        if auth.sender().role() != Role::Worker || auth.sender().public_key()? != self.public_key()
        {
            return Err(Error::Invalid);
        }
        sign(&self.pair, Domain::Response, &auth.canonical_bytes()?)
    }
}
#[allow(dead_code)] // API boot owner consumes retained admin material later.
impl ApiSigningKey {
    pub(super) fn cluster_id(&self) -> Uuid {
        self.cluster
    }

    /// Exact retained admin text, with A11's bounded printable-ASCII invariant.
    /// Format checks cannot establish the deployment requirement of 256-bit entropy.
    pub(crate) fn derive(cluster: Uuid, admin_token: &str) -> Result<Self, Error> {
        if !(43..=4096).contains(&admin_token.len())
            || !admin_token.bytes().all(|b| (33..=126).contains(&b))
        {
            return Err(Error::Invalid);
        }
        let prk =
            hkdf::Salt::new(hkdf::HKDF_SHA256, cluster.as_bytes()).extract(admin_token.as_bytes());
        let mut seed = [0; 32];
        prk.expand(&[b"meshdb.api-signing.v1"], Len32)
            .map_err(|_| Error::Invalid)?
            .fill(&mut seed)
            .map_err(|_| Error::Invalid)?;
        Ok(Self {
            cluster,
            pair: pair(&seed)?,
        })
    }
    pub(crate) fn public_key(&self) -> [u8; 32] {
        public(&self.pair)
    }
    pub(crate) fn sign_request(&self, auth: &Auth) -> Result<[u8; 64], Error> {
        if auth.cluster_id() != self.cluster
            || auth.sender().role() != Role::Api
            || auth.sender().public_key()? != self.public_key()
        {
            return Err(Error::Invalid);
        }
        sign(&self.pair, Domain::Request, &auth.canonical_bytes()?)
    }
}
/// Raw SHA256 bytes, ordered cluster/node/incarnation/session UUID bytes then key.
/// Later Session construction converts these 32 bytes to 64 lowercase hex digits.
/// This public-binding digest is neither a bearer credential nor a MAC.
#[allow(dead_code)] // R03a2b claimed-key and later enrollment callers pending.
pub(crate) fn worker_verifier(
    cluster: Uuid,
    node: Uuid,
    incarnation: Uuid,
    session: Uuid,
    public_key: &[u8; 32],
) -> [u8; 32] {
    let mut hash = digest::Context::new(&digest::SHA256);
    hash.update(b"meshdb.worker-verifier.v1\0");
    for id in [cluster, node, incarnation, session] {
        hash.update(id.as_bytes());
    }
    hash.update(public_key);
    let mut bytes = [0; 32];
    bytes.copy_from_slice(hash.finish().as_ref());
    bytes
}
#[cfg(test)]
mod tests;
