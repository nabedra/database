//! Incremental archives copy the exact child image before Status effects.
use super::*;
use crate::{
    oltp::{
        backup::history::split_child_scope,
        mvcc::store::{
            image_archive::CommittedImage,
            scan::{ScanDomain, ScanPage, ScanRequest},
            split_closure::{ArchivedSplitPublication, CatalogSplitClosed, split_closure_row},
        },
    },
    oltp_replication::authority::status::{IdentityLookup, TerminalIdentity},
};

impl WorkerRuntime {
    async fn split_publication(
        &self,
        catalog: &ParticipantHandle,
        row: &CatalogSplitClosed,
        version: CommitTs,
        cut: CommitTs,
    ) -> Result<ArchivedSplitPublication, ProtocolError> {
        let publication = if let Some(restored) = &row.restored {
            restored.clone()
        } else {
            let guard = row.guard.binding();
            let Some(IdentityLookup::Terminal(TerminalIdentity::Committed { decision, .. })) = self
                .services
                .status
                .lookup_identity(row.split_id, Some(guard.request.digest))
                .await?
            else {
                return Err(unknown());
            };
            let resolution = catalog
                .strong_resolution(guard.txn, decision.manifest)
                .await?;
            ArchivedSplitPublication {
                status: decision,
                catalog: resolution.into_evidence(),
                first_archive: None,
            }
        };
        publication.validate(row, catalog.identity(), version, cut)?;
        Ok(publication)
    }

    pub(super) async fn export_native_split_publication(
        &self,
        export: &mut Export,
        catalog: &ParticipantHandle,
        image: &CommittedImage,
        cut: CommitTs,
    ) -> Result<(), ProtocolError> {
        if let Some(row) = split_closure_row(&image.key, image.value.as_ref())? {
            let proof = self
                .split_publication(catalog, &row, image.commit_ts, cut)
                .await?;
            export
                .frame(HistoryFrame::CatalogSplitPublication(proof))
                .await?;
        }
        Ok(())
    }

    pub(super) async fn export_native_split_baselines(
        &self,
        pin: &PinnedCatalog,
        scope: &HistoryScope,
        export: &mut Export,
    ) -> Result<(), ProtocolError> {
        let base = scope.base_exclusive.ok_or_else(unknown)?;
        let catalog = self.services.catalog.participant();
        let mut after = None;
        let mut after_owner = None;
        loop {
            let page = catalog
                .scan_page(ScanRequest {
                    domain: ScanDomain::CatalogSplits,
                    read_ts: pin.read_ts,
                    after: after.clone(),
                    after_owner: after_owner.clone(),
                    lower: None,
                    upper: None,
                    max_items: 32,
                    max_bytes: 256 << 10,
                })
                .await?;
            match page.payload()? {
                ScanPage::Blocked { txn, manifest } => {
                    self.help_intent(IntentRef {
                        txn: *txn,
                        manifest: *manifest,
                    })
                    .await?;
                }
                ScanPage::Records { records, exhausted } => {
                    for record in records.iter() {
                        let Some(version) = record.version else {
                            continue;
                        };
                        let row = split_closure_row(&record.key, record.value.as_ref())?
                            .ok_or_else(unknown)?;
                        if version <= base {
                            continue;
                        }
                        let publication = self
                            .split_publication(catalog, &row, version, pin.read_ts)
                            .await?;
                        export
                            .frame(HistoryFrame::BeginSplit {
                                row: Box::new(row.clone()),
                                publication,
                            })
                            .await?;
                        for index in 0..row.children.len() {
                            let child = split_child_scope(&row, index)?;
                            let participant =
                                self.services.resolver.participant(&child.group).await?;
                            self.export_native_image_stream(
                                export,
                                participant,
                                child,
                                row.source_pin.read_ts,
                                None,
                            )
                            .await?;
                        }
                    }
                    if *exhausted {
                        return Ok(());
                    }
                    let next = &records.last().ok_or_else(unknown)?.key;
                    if after.as_ref() == Some(next) {
                        return Err(unknown());
                    }
                    after = Some(next.clone());
                    after_owner = Some(page.decode_owner()?);
                }
            }
        }
    }
}
