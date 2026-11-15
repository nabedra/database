//! Compatible observation only. Run bounded synchronous comparison on the finite
//! coordinator executor, before A04 preparation. No ownership or activation.
use super::{
    startup_material::{
        composition::{ApiReady, StartupError},
        identity,
    },
    state::Provisioning,
    store::LoadedControl,
};
use crate::storage::StorageError;

/// Keeps the exact observed base/token and actual local client/material alive.
/// This value grants no readiness, controller or callback authority.
#[derive(Debug)]
#[allow(dead_code)] // Runtime consumes the retained pair in a later assignment.
pub(crate) struct AttachedConfig {
    loaded: LoadedControl,
    local: ApiReady,
}

/// For C04b1 load/initialize results with an already validated initial candidate.
/// Its bounded codec and body-limit failures cannot be attached. All remaining
/// outcomes fail as unavailable, including ambiguous writes: never infer absence
/// or nonapplication and never resend here. The caller owns deadline/cancellation.
#[allow(dead_code)] // Pending startup caller.
pub(crate) fn control_result(
    result: Result<LoadedControl, StorageError>,
) -> Result<LoadedControl, StartupError> {
    result.map_err(|error| match error {
        StorageError::InvalidCatalog
        | StorageError::UnsupportedCatalog
        | StorageError::LimitExceeded => StartupError::IncompatibleCluster,
        _ => StartupError::StorageUnavailable,
    })
}

/// `expected_name` is supervisor-validated; provisioning is explicitly derived
/// from callback declarations (absent means External), never from this root.
/// Use identically after initial Create, existing observation and race recovery.
#[allow(dead_code)] // Pending startup caller.
pub(crate) fn check_attach(
    loaded: LoadedControl,
    local: ApiReady,
    expected_name: &str,
    expected_provisioning: Provisioning,
) -> Result<AttachedConfig, StartupError> {
    let root = loaded.root();
    let config = &root.config;
    let incompatible = || StartupError::IncompatibleCluster;
    let adapter = match expected_provisioning {
        Provisioning::Managed => Some("meshdb.callbacks.v1"),
        Provisioning::External => None,
    };
    if root.cluster_id != local.prepared.input.value.cluster_id
        || root.name != expected_name
        || config.generation != 2
        || root.provisioning != expected_provisioning
        || config.adapter_contract.as_deref() != adapter
        || config.storage_fingerprint != local.backend.storage.as_str()
        || config.auth_fingerprint != local.authentication.as_str()
        || config.lifecycle_fingerprint != local.lifecycle.as_str()
        || config.worker_semantic_fingerprint != local.backend.worker.as_str()
        || root.template.tls.is_some() != local.prepared.tls.is_some()
        || !local.matches_backend(&root.template)
    {
        return Err(incompatible());
    }
    // The stored exact Template authenticates only itself. Local path spellings
    // are intentionally absent from this comparison and from semantic hashing.
    if root.template.fingerprint().map_err(|_| incompatible())? != config.template_fingerprint
        || identity::lifecycle(&root.timing)
            .map_err(|_| incompatible())?
            .as_str()
            != config.lifecycle_fingerprint
        || identity::worker(
            &root.template,
            &local.backend.storage,
            local.prepared.tls.as_ref(),
        )
        .map_err(|_| incompatible())?
        .as_str()
            != config.worker_semantic_fingerprint
    {
        return Err(incompatible());
    }
    Ok(AttachedConfig { loaded, local })
}
#[cfg(test)]
pub(super) mod tests;

impl AttachedConfig {
    pub(crate) fn local(&self) -> &ApiReady {
        &self.local
    }
    pub(crate) fn root(&self) -> &super::state::Control {
        self.loaded.root()
    }
    #[allow(dead_code)] // C05 callback reconstruction; runtime caller follows.
    pub(super) fn callback_enrollment(
        &self,
        current: &super::state::Control,
    ) -> Result<&str, StartupError> {
        let retained = self.loaded.root();
        current
            .validate()
            .map_err(|_| StartupError::IncompatibleCluster)?;
        if current.cluster_id != retained.cluster_id
            || current.name != retained.name
            || current.config != retained.config
            || current.provisioning != retained.provisioning
            || current.template != retained.template
            || current.timing != retained.timing
        {
            return Err(StartupError::IncompatibleCluster);
        }
        Ok(&self.local.prepared.auth.enrollment_token)
    }
}
