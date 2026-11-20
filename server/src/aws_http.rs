//! HTTPS client for the AWS SDK on rustls with the `ring` provider.
//!
//! `ring` is the only TLS provider in every build (BLD5). The SDK's default
//! HTTPS client is hard-wired to aws-lc, so its default features are off and
//! every S3 config gets this client instead. It matches the SDK default for
//! `BehaviorVersion::latest()` in everything but the crypto provider: native
//! root certificates, proxy settings from the environment and the SDK's
//! per-settings connector cache.
use aws_smithy_http_client::{Builder, Connector, proxy::ProxyConfig, tls};

pub(crate) fn https_client() -> aws_sdk_s3::config::SharedHttpClient {
    Builder::new().build_with_connector_fn(|settings, runtime_components| {
        let mut connector = Connector::builder().tls_provider(tls::Provider::Rustls(
            tls::rustls_provider::CryptoMode::Ring,
        ));
        connector.set_connector_settings(settings.cloned());
        if let Some(components) = runtime_components {
            connector.set_sleep_impl(components.sleep_impl());
        }
        connector.set_proxy_config(Some(ProxyConfig::from_env()));
        connector.build()
    })
}
