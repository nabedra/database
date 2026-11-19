//! Mutual TLS material for worker peer transports. Only a CA-authenticated
//! certificate and key pair can construct it; callers choose the ALPN list.
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use sha2::{Digest, Sha256};
use std::{io, sync::Arc};
use tokio_rustls::{TlsAcceptor, TlsConnector};

#[derive(Clone)]
pub struct TlsMaterial {
    client: Arc<rustls::ClientConfig>,
    server: Arc<rustls::ServerConfig>,
    certificate_sha256: String,
}
impl TlsMaterial {
    /// Only a CA-authenticated mutual TLS configuration can construct a transport.
    pub fn from_pem(certificate: &[u8], private_key: &[u8], ca: &[u8]) -> io::Result<Self> {
        let certificates: Vec<_> = CertificateDer::pem_slice_iter(certificate)
            .collect::<Result<_, _>>()
            .map_err(|_| invalid("invalid peer certificate"))?;
        let fingerprint = certificates
            .first()
            .map(|cert| hex(&Sha256::digest(cert.as_ref())))
            .ok_or_else(|| invalid("missing peer certificate"))?;
        let key = PrivateKeyDer::from_pem_slice(private_key)
            .map_err(|_| invalid("invalid peer private key"))?;
        let mut roots = rustls::RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(ca) {
            roots
                .add(cert.map_err(|_| invalid("invalid peer CA"))?)
                .map_err(|_| invalid("invalid peer CA"))?;
        }
        if roots.is_empty() {
            return Err(invalid("missing peer CA"));
        }
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots.clone()),
            provider.clone(),
        )
        .build()
        .map_err(|_| invalid("invalid client certificate verifier"))?;
        let server = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(|_| invalid("invalid peer TLS protocols"))?
            .with_client_cert_verifier(verifier)
            .with_single_cert(certificates.clone(), key.clone_key())
            .map_err(|_| invalid("peer certificate and key differ"))?;
        let client = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|_| invalid("invalid peer TLS protocols"))?
            .with_root_certificates(roots)
            .with_client_auth_cert(certificates, key)
            .map_err(|_| invalid("peer certificate and key differ"))?;
        Ok(Self {
            client: Arc::new(client),
            server: Arc::new(server),
            certificate_sha256: fingerprint,
        })
    }
    pub fn certificate_sha256(&self) -> &str {
        &self.certificate_sha256
    }
    /// Offer the protocols in preference order. The server selects the first
    /// of its own list that the client offers, so an old peer that offers only
    /// the one-shot protocol still negotiates it.
    pub(super) fn transports(&self, alpn: &[&[u8]]) -> (TlsConnector, TlsAcceptor) {
        let mut client = (*self.client).clone();
        let mut server = (*self.server).clone();
        client.alpn_protocols = alpn.iter().map(|protocol| protocol.to_vec()).collect();
        server.alpn_protocols = client.alpn_protocols.clone();
        (
            TlsConnector::from(Arc::new(client)),
            TlsAcceptor::from(Arc::new(server)),
        )
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
