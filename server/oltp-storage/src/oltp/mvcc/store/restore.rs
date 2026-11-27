//! Install verified fixed committed images before a new target begins Raft.
//! Source request execution, Prepare and mutation decisions are never replayed.
use super::image_archive::CommittedImage;
use super::*;
use crate::oltp::{
    backup::{
        history::GroupImageScope,
        restore::{OfflineRestoreTarget, RestoreGenesisStatement},
    },
    pool::GroupStore,
};
pub(super) const RESTORE_GENESIS: &[u8] = b"mvcc/restore-genesis/v1";
const RESTORE_INACTIVE: &[u8] = b"mvcc/restore-inactive/v1";
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParticipantGenesis {
    pub restore_id: Uuid,
    pub source_cluster: Uuid,
    pub target_group: GroupIdentity,
    pub catalog_group_id: Uuid,
    pub schema_epoch: u64,
    pub routing_epoch: u64,
    pub span: KeySpan,
    pub cut: CommitTs,
}
impl ParticipantGenesis {
    fn new(
        scope: &GroupImageScope,
        target: &RestoreGenesisStatement,
    ) -> Result<Self, ProtocolError> {
        validate_archive_scope(scope)?;
        if scope.group.cluster_id != target.source_cluster
            || scope.group.database != target.database
        {
            return Err(invalid());
        }
        let mut group = scope.group.clone();
        group.cluster_id = target.target_pool.cluster_id;
        Ok(Self {
            restore_id: target.restore_id,
            source_cluster: target.source_cluster,
            target_group: group,
            catalog_group_id: target.catalog_group,
            schema_epoch: scope.schema_epoch,
            routing_epoch: scope.routing_epoch,
            span: scope.span.clone(),
            cut: target.target_cut,
        })
    }
}

pub fn validate_archive_scope(scope: &GroupImageScope) -> Result<(), ProtocolError> {
    scope.group.validate().map_err(|_| invalid())?;
    if !matches!(
        scope.group.kind,
        GroupKind::Catalog | GroupKind::Data { .. }
    ) || scope.schema_epoch == 0
        || scope.routing_epoch == 0
        || !valid_span(&scope.span)
    {
        return Err(invalid());
    }
    Ok(())
}

pub fn validate_archive_image(scope: &GroupImageScope, key: &MvccKey) -> Result<(), ProtocolError> {
    if !owns_image(&scope.group, &scope.span, key) {
        return Err(invalid());
    }
    key_length(key)?;
    Ok(())
}

pub async fn install_images(
    store: &GroupStore,
    target: Arc<OfflineRestoreTarget>,
    scope: GroupImageScope,
    active: Option<GroupImageScope>,
    mut images: Vec<crate::oltp::backup::history::OwnedImage>,
) -> Result<(), ProtocolError> {
    let historical = ParticipantGenesis::new(&scope, target.statement())?;
    let seed = active
        .as_ref()
        .map(|scope| ParticipantGenesis::new(scope, target.statement()))
        .transpose()?;
    if &historical.target_group != store.identity()
        || images.len() > 128
        || seed.as_ref().is_some_and(|seed| {
            seed.target_group != historical.target_group || seed.span != historical.span
        })
    {
        return Err(invalid());
    }
    for image in &mut images {
        image.map_catalog(
            store.resources(),
            historical.source_cluster,
            historical.target_group.cluster_id,
        )?;
    }
    let group = store.identity().clone();
    store
        .restore_genesis_batch(
            target,
            TransactionBudget {
                scratch_bytes: 32 << 20,
                batch_bytes: 32 << 20,
                output_bytes: 4096,
            },
            move |tx| {
                images.sort_unstable_by(crate::oltp::backup::history::OwnedImage::compare);
                for pair in images.windows(2) {
                    let a = pair[0].image()?;
                    let b = pair[1].image()?;
                    if a.key == b.key && a.commit_ts == b.commit_ts && a.value != b.value {
                        return Err(recovery_unknown());
                    }
                }
                let config: Option<Config> = load(tx, CONFIG)?;
                if config.is_some() {
                    return Err(recovery_unknown());
                }
                if let Some(seed) = seed {
                    if tx.pinned(RESTORE_INACTIVE)?.is_some() {
                        return Err(invalid());
                    }
                    if let Some(previous) = load::<ParticipantGenesis>(tx, RESTORE_GENESIS)? {
                        if previous != seed {
                            return Err(invalid());
                        }
                    } else {
                        tx.put_serialized(RESTORE_GENESIS, &seed)?;
                    }
                } else {
                    if tx.pinned(RESTORE_GENESIS)?.is_some() {
                        return Err(invalid());
                    }
                    tx.put_serialized(RESTORE_INACTIVE, &historical.restore_id)?;
                }
                for owned in &images {
                    let image: &CommittedImage = owned.image()?;
                    if image.commit_ts > historical.cut {
                        continue;
                    }
                    install_image(tx, &group, &historical.span, image)?;
                }
                Ok(())
            },
        )
        .await
}

/// Write one committed image into a restore target, idempotently. Dedup
/// images go to the `'D'` namespace; every other key becomes a version.
pub(super) fn install_image(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    span: &KeySpan,
    image: &CommittedImage,
) -> Result<(), ProtocolError> {
    if !owns_image(group, span, &image.key) {
        return Err(invalid());
    }
    if matches!(image.key, MvccKey::DirectDedup { .. }) {
        return super::direct::import_image(tx, image);
    }
    let length = key_length(&image.key)?;
    tx.with_scratch_buffer(length + 9, |tx, buffer| {
        encode_key(&image.key, &mut buffer[1..length + 1]);
        let end = length + 1;
        buffer[0] = HEAD;
        let head = load::<Head>(tx, &buffer[..end])?.unwrap_or_default();
        if head.intent.is_some() {
            return Err(recovery_unknown());
        }
        buffer[0] = VALUE;
        buffer[end..].copy_from_slice(&(!image.commit_ts.get()).to_be_bytes());
        let raw = image
            .value
            .as_ref()
            .map(|value| serde_json::from_str::<&RawValue>(value.as_str()).map_err(|_| invalid()))
            .transpose()?;
        #[derive(Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Version<'a> {
            #[serde(borrow)]
            value: Option<&'a RawValue>,
        }
        if let Some(previous) = tx.pinned(buffer)? {
            let previous: Version<'_> =
                serde_json::from_slice(&previous).map_err(|_| recovery_unknown())?;
            if previous.value.map(RawValue::get) != raw.map(RawValue::get) {
                return Err(recovery_unknown());
            }
        } else {
            tx.put_serialized(buffer, &Version { value: raw })?;
        }
        if head
            .version
            .is_none_or(|version| version <= image.commit_ts)
        {
            buffer[0] = HEAD;
            tx.put_serialized(
                &buffer[..end],
                &Head {
                    version: Some(image.commit_ts),
                    intent: None,
                },
            )?;
        }
        Ok(())
    })?;
    Ok(())
}

/// An imported seed may only supply the exact first-fence fields. Ordinary
/// empty groups retain their normal initial-fence validation.
pub(super) fn validate_initial_fence(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    catalog_group_id: &Uuid,
    schema_epoch: &u64,
    routing_epoch: &u64,
    span: &KeySpan,
) -> Result<u64, ProtocolError> {
    if tx.pinned(RESTORE_INACTIVE)?.is_some() {
        return Err(invalid());
    }
    let Some(seed) = load::<ParticipantGenesis>(tx, RESTORE_GENESIS)? else {
        return Ok(0);
    };
    if &seed.target_group != group
        || seed.restore_id.is_nil()
        || seed.source_cluster.is_nil()
        || &seed.catalog_group_id != catalog_group_id
        || &seed.schema_epoch != schema_epoch
        || &seed.routing_epoch != routing_epoch
        || &seed.span != span
    {
        return Err(invalid());
    }
    Ok(seed.cut.get())
}
impl ParticipantMachine {
    pub async fn restore_seed(&self) -> Result<Option<ParticipantGenesis>, ProtocolError> {
        let raw = self
            .store
            .read_metadata(
                TransactionBudget {
                    scratch_bytes: 64 << 10,
                    batch_bytes: 0,
                    output_bytes: 64 << 10,
                },
                |view| {
                    let seed = view
                        .pinned(RESTORE_GENESIS)?
                        .map(|raw| {
                            serde_json::from_slice::<ParticipantGenesis>(&raw)
                                .map_err(|_| invalid())
                        })
                        .transpose()?;
                    view.encode(&seed)
                },
            )
            .await?;
        serde_json::from_slice(raw.as_slice()).map_err(|_| invalid())
    }
}
