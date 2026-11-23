//! Explicit maintenance commands, separate from process enrollment and startup.
#[cfg(feature = "s3")]
use crate::storage::CatalogFence;
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
#[cfg(feature = "s3")]
pub async fn migrate(
    uri: &str,
    region: &str,
    cluster_id: uuid::Uuid,
    confirmed: bool,
) -> Result<(), &'static str> {
    if cluster_id.is_nil() {
        return Err("cluster ID invalid");
    }
    if !confirmed {
        return Err("--confirm-writers-stopped is required");
    }
    let target = uri
        .strip_prefix("s3://")
        .and_then(|s| s.split_once('/'))
        .ok_or("storage URI invalid")?;
    let namespace = Namespace::new(target.1).map_err(|_| "storage URI invalid")?;
    let client = crate::storage::S3Store::connect_region(target.0, namespace.clone(), Some(region))
        .await
        .map_err(|_| "storage initialization failed")?;
    let storage = Storage::new(client, namespace);
    storage
        .initialize_owned_cluster_catalog(
            cluster_id,
            CatalogFence {
                run_id: nabedra_rt::ids::uuid_v4(),
                scheduler_term: 1,
            },
            true,
            false,
        )
        .await
        .map_err(|_| "catalog migration failed")?;
    println!("catalog migration completed");
    Ok(())
}
#[cfg(not(feature = "s3"))]
pub async fn migrate(_: &str, _: &str, _: uuid::Uuid, _: bool) -> Result<(), &'static str> {
    Err("S3 support unavailable")
}
