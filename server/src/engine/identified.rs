//! Local execution captures a private catalog and forwards only immutable object
//! uploads. The live root changes once, with its durable decision and owner fence.
use super::*;
use crate::{
    api::response::{self, EncodedResponse},
    protocol::Request,
    storage::{
        Catalog, CatalogFence, Decision, Download, HistoryBudget, HistoryLookup,
        LoadedClusterCatalog, ObjectKey, PreparedPublication, PublicationOutcome, WriteCondition,
    },
};
use std::sync::Mutex;
use tokio::time::Duration;

struct PrivateCatalog {
    catalog: Catalog,
    etag: u64,
}
struct CapturedStore<B: ObjectStore> {
    source: Arc<Coordinator<B>>,
    private: Arc<Mutex<PrivateCatalog>>,
}
impl<B: ObjectStore> ObjectStore for CapturedStore<B> {
    async fn get(&self, key: &str) -> Result<Download, StorageError> {
        let relative = key
            .strip_prefix("captured/")
            .ok_or(StorageError::InvalidPath)?;
        if relative == "catalog.json" {
            let private = self.private.lock().map_err(|_| StorageError::Unavailable)?;
            return Ok(Download::from_bytes(
                private.catalog.encode()?,
                private.etag.to_string(),
            ));
        }
        let object = self
            .source
            .storage()
            .read(
                &ObjectKey::parse_relative(relative)?,
                crate::snapshot::MAX_ENCODED_SNAPSHOT_BYTES,
            )
            .await?;
        Ok(Download::from_bytes(object.bytes, object.etag))
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        condition: WriteCondition<'_>,
    ) -> Result<String, StorageError> {
        let relative = key
            .strip_prefix("captured/")
            .ok_or(StorageError::InvalidPath)?;
        if relative == "catalog.json" {
            let candidate = Catalog::decode(&bytes)?;
            let mut private = self.private.lock().map_err(|_| StorageError::Unavailable)?;
            if !matches!(condition, WriteCondition::Match(etag) if etag == private.etag.to_string())
            {
                return Err(StorageError::Conflict);
            }
            private.etag = private
                .etag
                .checked_add(1)
                .ok_or(StorageError::LimitExceeded)?;
            private.catalog = candidate;
            return Ok(private.etag.to_string());
        }
        let key = ObjectKey::parse_relative(relative)?;
        // No control, decision, or mutable root can escape this private adapter.
        if !relative.starts_with("tables/") || !matches!(condition, WriteCondition::Create) {
            return Err(StorageError::InvalidPath);
        }
        self.source
            .storage()
            .put(&key, bytes, WriteCondition::Create)
            .await
    }
}
fn unavailable() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::OutcomeUnknown,
        "request outcome requires reconciliation",
        Outcome::Unknown,
    )
}
fn terminal(decision: Decision) -> EncodedResponse {
    EncodedResponse {
        status: decision.response_status,
        body: decision.response_body.into_bytes(),
    }
}
impl<B: ObjectStore + 'static> Engine<B> {
    /// Used by the receipt runtime for both synchronous and detached submissions.
    /// A read's pin is captured and replicated before calling this method. Caller
    /// retains ownership across disconnects and settles all queued/worker work.
    pub async fn execute_identified(
        &self,
        body: Vec<u8>,
        fence: CatalogFence,
        pinned: Option<LoadedClusterCatalog>,
    ) -> Result<EncodedResponse, ProtocolError> {
        let Request {
            request_id,
            operation,
        } = protocol::parse_request(&body)?;
        let mutation = operation.is_mutation();
        if !mutation && pinned.is_none() {
            return Err(failure(
                ErrorCode::ValidationError,
                "read snapshot is required",
            ));
        }
        let budget = HistoryBudget::new(
            4096,
            64 * 1024 * 1024,
            nabedra_rt::time::now() + Duration::from_secs(60),
        )
        .map_err(|_| unavailable())?;
        for _ in 0..3 {
            let loaded = if mutation {
                self.storage()
                    .load_cluster_catalog()
                    .await
                    .map_err(|_| unavailable())?
            } else {
                pinned.clone().ok_or_else(unavailable)?
            };
            if mutation {
                match self
                    .storage()
                    .lookup_decision(&loaded, request_id, &body, budget)
                    .await
                    .map_err(|_| unavailable())?
                {
                    HistoryLookup::Found(decision) => return Ok(terminal(decision)),
                    HistoryLookup::IdentityConflict => {
                        return Err(failure(
                            ErrorCode::Conflict,
                            "request identity has a different body",
                        ));
                    }
                    HistoryLookup::Absent => {}
                }
                if loaded.catalog().fence != Some(fence) {
                    return Err(unavailable());
                }
            }
            let base = loaded.catalog();
            let private = Arc::new(Mutex::new(PrivateCatalog {
                catalog: Catalog {
                    format_version: 2,
                    revision: base.revision,
                    commit_id: base.commit_id,
                    tables: base.tables.clone(),
                },
                etag: 1,
            }));
            let captured = CapturedStore {
                source: self.coordinator.clone(),
                private: private.clone(),
            };
            let engine = Engine::new(
                Storage::new(
                    captured,
                    crate::storage::Namespace::new("captured").map_err(|_| unavailable())?,
                ),
                self.worker_pool.clone(),
                self.partition_size,
            );
            let calculated = engine.execute(operation.clone()).await;
            let encoded = match calculated {
                Ok(value) => response::encode_success(request_id, &value, false),
                Err(error) => response::encode_error(Some(request_id), error),
            };
            if !mutation {
                return Ok(encoded);
            }
            if encoded.status != 200 && !matches!(encoded.status, 400 | 404 | 409 | 413) {
                return Err(unavailable());
            }
            // Response size/semantics are known before the live publication. A
            // fallback error publishes no table changes, even after orphan uploads.
            let candidate = private.lock().map_err(|_| unavailable())?.catalog.clone();
            let changed = encoded.status == 200 && candidate.tables != base.tables;
            let tables = if changed {
                candidate.tables
            } else {
                base.tables.clone()
            };
            let decision = Decision {
                version: 1,
                id: if changed {
                    candidate.commit_id
                } else {
                    nabedra_rt::ids::uuid_v4()
                },
                parent: base.decision_head,
                base_revision: base.revision,
                revision: base.revision.checked_add(1).ok_or_else(|| {
                    failure(ErrorCode::LimitExceeded, "catalog revision exhausted")
                })?,
                request_id,
                request_body: String::from_utf8(body.clone()).map_err(|_| unavailable())?,
                response_status: encoded.status,
                response_body: String::from_utf8(encoded.body).map_err(|_| unavailable())?,
            };
            let unchanged = base.tables.clone();
            let candidate = match PreparedPublication::prepare_fenced(
                loaded.clone(),
                tables,
                decision.clone(),
                fence,
            ) {
                Ok(candidate) => candidate,
                // OLAP-3: a changed root above the catalog cap is a definite
                // limit, published as a 413 decision with unchanged tables.
                Err(StorageError::LimitExceeded) if decision.response_status == 200 => {
                    let fallback = response::encode_error(
                        Some(request_id),
                        failure(ErrorCode::LimitExceeded, "catalog size limit exceeded"),
                    );
                    let decision = Decision {
                        id: nabedra_rt::ids::uuid_v4(),
                        response_status: fallback.status,
                        response_body: String::from_utf8(fallback.body)
                            .map_err(|_| unavailable())?,
                        ..decision
                    };
                    PreparedPublication::prepare_fenced(loaded, unchanged, decision, fence)
                        .map_err(|_| unavailable())?
                }
                Err(_) => return Err(unavailable()),
            };
            match candidate
                .publish(self.storage(), budget)
                .await
                .map_err(|_| unavailable())?
            {
                PublicationOutcome::Terminal(decision) => return Ok(terminal(decision)),
                PublicationOutcome::IdentityConflict => {
                    return Err(failure(
                        ErrorCode::Conflict,
                        "request identity has a different body",
                    ));
                }
                PublicationOutcome::Unknown => return Err(unavailable()),
                PublicationOutcome::NeedsRecompute => {}
            }
        }
        Err(unavailable())
    }
}
