//! Explicit maintenance commands, separate from process enrollment and startup.
use crate::{
    cluster::startup_material::composition::prepare_api,
    storage::{Namespace, ObjectKey, Storage},
};
use std::{path::Path, time::Duration};

pub async fn doctor(path: &Path, json: bool) -> Result<(), &'static str> {
    let prepared = prepare_api(path, nabedra_rt::time::now_std() + Duration::from_secs(30))
        .map_err(|_| "configuration invalid")?;
    let ready = prepared
        .connect_async()
        .await
        .map_err(|_| "storage initialization failed")?;
    let namespace = match &ready.prepared.input.value.template.storage {
        crate::cluster::configuration::Storage::Http(o) => &o.namespace,
        crate::cluster::configuration::Storage::S3(o) => &o.namespace,
    };
    let storage = Storage::new(
        ready.backend.client,
        Namespace::new(namespace).map_err(|_| "namespace invalid")?,
    );
    // This probe never calls an initialization, migration, fence or PUT method.
    match storage
        .read(
            &ObjectKey::control(),
            crate::cluster::state::MAX_CONTROL_BYTES,
        )
        .await
    {
        Ok(_) => {
            if json {
                println!(
                    "{{\"ok\":true,\"storage_read\":\"verified\",\"storage_write\":\"unverified\"}}"
                );
            } else {
                println!("control storage read verified; write permissions unverified");
            }
        }
        Err(crate::storage::StorageError::NotFound) => {
            if json {
                println!(
                    "{{\"ok\":true,\"storage_read\":\"object_absent\",\"storage_write\":\"unverified\"}}"
                );
            } else {
                println!("control object absent; read/write permissions unverified");
            }
        }
        Err(_) => return Err("control storage read failed"),
    }
    Ok(())
}
