use std::sync::{Arc, OnceLock};

use rustls::{ClientConfig, RootCertStore};

use crate::opts::Opts;

/// Returns `opts.tls_config`, or the shared native config when it is unset.
pub fn client_config(opts: &Opts) -> std::io::Result<Arc<ClientConfig>> {
    match &opts.tls_config {
        Some(config) => Ok(Arc::clone(config)),
        None => native_client_config(),
    }
}

/// Returns the rustls client config shared by all TLS connections without
/// `Opts::tls_config`.
///
/// The OS trust store is loaded on the first call and reused afterwards.
fn native_client_config() -> std::io::Result<Arc<ClientConfig>> {
    static CONFIG: OnceLock<Arc<ClientConfig>> = OnceLock::new();
    if let Some(config) = CONFIG.get() {
        return Ok(Arc::clone(config));
    }
    let config = Arc::new(build_client_config()?);
    Ok(Arc::clone(CONFIG.get_or_init(|| config)))
}

fn build_client_config() -> std::io::Result<ClientConfig> {
    let native_certs = rustls_native_certs::load_native_certs();
    let mut roots = RootCertStore::empty();
    let (added, _) = roots.add_parsable_certificates(native_certs.certs);
    if added == 0 {
        return Err(std::io::Error::other(format!(
            "no native root certificates found: {:?}",
            native_certs.errors
        )));
    }
    for error in native_certs.errors {
        log::warn!("failed to load some native root certificates: {error}");
    }

    // An explicit provider avoids a panic when both aws-lc-rs and ring are
    // enabled in the dependency graph and no process default is installed.
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    Ok(ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(std::io::Error::other)?
        .with_root_certificates(roots)
        .with_no_client_auth())
}

#[cfg(any(feature = "sync-tls", feature = "tokio-tls"))]
pub fn server_name(host: &str) -> std::io::Result<rustls::pki_types::ServerName<'static>> {
    rustls::pki_types::ServerName::try_from(host.to_owned()).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid TLS server name {host:?}: {e}"),
        )
    })
}
