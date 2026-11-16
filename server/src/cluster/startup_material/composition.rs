//! Local HTTP preparation. Runtime integration and S3 connection are later slices.
use super::{
    files::{Auth, CaMaterial, HttpToken, LoadedInput, Reader, TlsMaterial},
    identity::{self, Fingerprint},
};
use crate::{
    cluster::{
        configuration::{ConfigurationError, Storage, Template},
        startup_config::{ApiConfig, Bootstrap},
    },
    storage::{Namespace, http::HttpStore},
};
use std::{fmt, path::Path, time::Instant};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum StartupError {
    InvalidConfig,
    IncompatibleCluster,
    StorageUnavailable,
}
impl fmt::Display for StartupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidConfig => "invalid_config",
            Self::IncompatibleCluster => "incompatible_cluster",
            Self::StorageUnavailable => "storage_unavailable",
        })
    }
}
impl std::error::Error for StartupError {}
impl From<ConfigurationError> for StartupError {
    fn from(_: ConfigurationError) -> Self {
        Self::InvalidConfig
    }
}
#[allow(dead_code)] // Retained until startup consumes these bundles.
pub(crate) struct ApiPrepared {
    pub(crate) input: LoadedInput<ApiConfig>,
    pub(crate) auth: Auth,
    pub(crate) tls: Option<TlsMaterial>,
    backend: HttpMaterial,
}
#[allow(dead_code)]
pub(crate) struct BootstrapPrepared {
    pub(crate) input: LoadedInput<Bootstrap>,
    pub(crate) tls: Option<TlsMaterial>,
    backend: HttpMaterial,
}
struct HttpMaterial {
    base: String,
    namespace: Namespace,
    token: HttpToken,
    ca: Option<CaMaterial>,
    s3: Option<crate::cluster::configuration::S3Options>,
}
#[allow(dead_code)]
pub(crate) struct ConnectedHttp {
    pub(crate) client: crate::storage::RuntimeStore,
    pub(crate) storage: Fingerprint,
    pub(crate) worker: Fingerprint,
}
#[allow(dead_code)]
pub(crate) struct ApiReady {
    pub(crate) prepared: ApiPrepared,
    pub(crate) backend: ConnectedHttp,
    pub(crate) authentication: Fingerprint,
    pub(crate) lifecycle: Fingerprint,
}
#[allow(dead_code)]
pub(crate) struct BootstrapReady {
    pub(crate) prepared: BootstrapPrepared,
    pub(crate) backend: ConnectedHttp,
}
macro_rules! redacted { ($($ty:ident),*) => { $(impl fmt::Debug for $ty { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.debug_struct(stringify!($ty)).finish_non_exhaustive() } })* }; }
redacted!(
    ApiPrepared,
    BootstrapPrepared,
    HttpMaterial,
    ConnectedHttp,
    ApiReady,
    BootstrapReady
);
impl ApiReady {
    /// Compare outgoing hints to the exact retained HTTP adapter declaration.
    /// Credential/CA paths are never resolved or opened here.
    pub(crate) fn matches_backend(&self, template: &Template) -> bool {
        if let Storage::S3(options) = &template.storage {
            return self.prepared.backend.s3.as_ref().is_some_and(|s| {
                s.bucket == options.bucket
                    && s.region == options.region
                    && Namespace::new(&options.namespace)
                        .is_ok_and(|n| n.as_str() == self.prepared.backend.namespace.as_str())
            });
        }
        let Storage::Http(options) = &template.storage else {
            return false;
        };
        options.base_url.trim_end_matches('/') == self.prepared.backend.base.trim_end_matches('/')
            && Namespace::new(&options.namespace).is_ok_and(|namespace| {
                namespace.as_str() == self.prepared.backend.namespace.as_str()
            })
    }
}
#[allow(dead_code)]
pub(crate) fn prepare_api(path: &Path, deadline: Instant) -> Result<ApiPrepared, StartupError> {
    api(&mut Reader::new(path, deadline)?)
}
#[allow(dead_code)]
pub(crate) fn prepare_bootstrap(
    path: &Path,
    deadline: Instant,
) -> Result<BootstrapPrepared, StartupError> {
    bootstrap(&mut Reader::new(path, deadline)?)
}
fn api(reader: &mut Reader) -> Result<ApiPrepared, StartupError> {
    let input = reader.api()?;
    let auth = reader.auth(&input.value.auth_file)?;
    let tls = input
        .value
        .template
        .tls
        .as_ref()
        .map(|hints| reader.tls(hints))
        .transpose()?;
    let backend = material(reader, &input.value.template)?;
    Ok(ApiPrepared {
        input,
        auth,
        tls,
        backend,
    })
}
fn bootstrap(reader: &mut Reader) -> Result<BootstrapPrepared, StartupError> {
    let input = reader.bootstrap()?;
    let tls = input
        .value
        .template
        .tls
        .as_ref()
        .map(|hints| reader.tls(hints))
        .transpose()?;
    let backend = material(reader, &input.value.template)?;
    Ok(BootstrapPrepared {
        input,
        tls,
        backend,
    })
}
fn material(reader: &mut Reader, template: &Template) -> Result<HttpMaterial, StartupError> {
    let Storage::Http(options) = &template.storage else {
        let Storage::S3(options) = &template.storage else {
            unreachable!()
        };
        return Ok(HttpMaterial {
            base: String::new(),
            namespace: Namespace::new(&options.namespace)
                .map_err(|_| StartupError::InvalidConfig)?,
            token: HttpToken {
                bytes: std::sync::Arc::from(&b""[..]),
                value: String::new(),
            },
            ca: None,
            s3: Some(options.clone()),
        });
    };
    let namespace = Namespace::new(&options.namespace).map_err(|_| StartupError::InvalidConfig)?;
    let token = reader.http_token(&options.token_file)?;
    let https = options
        .base_url
        .split_once("://")
        .is_some_and(|(scheme, _)| scheme.eq_ignore_ascii_case("https"));
    let ca = reader.http_ca(https, options.ca_file.as_deref())?;
    Ok(HttpMaterial {
        base: options.base_url.clone(),
        namespace,
        token,
        ca,
        s3: None,
    })
}
impl HttpMaterial {
    async fn connect_async(
        &self,
        template: &Template,
        tls: Option<&TlsMaterial>,
    ) -> Result<ConnectedHttp, StartupError> {
        if let Some(options) = &self.s3 {
            #[cfg(feature = "s3")]
            {
                let client = crate::storage::S3Store::connect_region(
                    &options.bucket,
                    self.namespace.clone(),
                    options.region.as_deref(),
                )
                .await
                .map_err(|_| StartupError::StorageUnavailable)?;
                let storage = identity::s3(&options.bucket, &self.namespace, client.region())?;
                let worker = identity::worker(template, &storage, tls)?;
                return Ok(ConnectedHttp {
                    client: crate::storage::RuntimeStore::S3(client),
                    storage,
                    worker,
                });
            }
            #[cfg(not(feature = "s3"))]
            {
                let _ = options;
                return Err(StartupError::InvalidConfig);
            }
        }
        self.connect(template, tls)
    }

    fn connect(
        &self,
        template: &Template,
        tls: Option<&TlsMaterial>,
    ) -> Result<ConnectedHttp, StartupError> {
        let client = HttpStore::new(
            &self.base,
            self.namespace.clone(),
            &self.token.value,
            self.ca.as_ref().map(|ca| ca.pem.as_ref()),
        )
        .map_err(|_| StartupError::InvalidConfig)?;
        let storage = identity::http(&self.base, &self.namespace, &self.token, self.ca.as_ref())?;
        let worker = identity::worker(template, &storage, tls)?;
        Ok(ConnectedHttp {
            client: crate::storage::RuntimeStore::Http(client),
            storage,
            worker,
        })
    }
}
#[allow(dead_code)]
impl ApiPrepared {
    pub(crate) async fn connect_async(self) -> Result<ApiReady, StartupError> {
        let backend = self
            .backend
            .connect_async(&self.input.value.template, self.tls.as_ref())
            .await?;
        let authentication = identity::authentication(self.input.value.cluster_id, &self.auth)?;
        let lifecycle = identity::lifecycle(&self.input.value.lifecycle)?;
        Ok(ApiReady {
            prepared: self,
            backend,
            authentication,
            lifecycle,
        })
    }

    pub(crate) fn connect(self) -> Result<ApiReady, StartupError> {
        let backend = self
            .backend
            .connect(&self.input.value.template, self.tls.as_ref())?;
        let authentication = identity::authentication(self.input.value.cluster_id, &self.auth)?;
        let lifecycle = identity::lifecycle(&self.input.value.lifecycle)?;
        Ok(ApiReady {
            prepared: self,
            backend,
            authentication,
            lifecycle,
        })
    }
}
#[allow(dead_code)]
impl BootstrapPrepared {
    pub(crate) async fn connect_async(self) -> Result<BootstrapReady, StartupError> {
        let backend = self
            .backend
            .connect_async(&self.input.value.template, self.tls.as_ref())
            .await?;
        Ok(BootstrapReady {
            prepared: self,
            backend,
        })
    }

    pub(crate) fn connect(self) -> Result<BootstrapReady, StartupError> {
        let backend = self
            .backend
            .connect(&self.input.value.template, self.tls.as_ref())?;
        Ok(BootstrapReady {
            prepared: self,
            backend,
        })
    }
}
#[cfg(test)]
mod tests;
