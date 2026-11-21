use super::*;

const PAGE_ITEMS: usize = 32;
const PAGE_BYTES: usize = 512 << 10;

impl ParticipantHandle {
    pub(crate) async fn next_stored_index(
        &self,
        after: Option<Uuid>,
    ) -> Result<Option<Uuid>, ProtocolError> {
        if let Some(remote) = &self.remote {
            return remote.next_stored_index(after).await;
        }
        let GroupKind::Data { table_id, .. } = self.identity().kind else {
            return Err(invalid());
        };
        self.local_consensus().ensure_linearizable_lease().await?;
        let result = self
            .local_machine()
            .store()
            .read(
                TransactionBudget {
                    scratch_bytes: 64 << 10,
                    batch_bytes: 0,
                    output_bytes: 128,
                },
                move |view| {
                    let start = match after {
                        Some(after) => prefix_end(&index_prefix(HEAD, table_id, after)),
                        None => index_prefix(HEAD, table_id, Uuid::nil()).to_vec(),
                    };
                    let prefix = index_prefix(HEAD, table_id, Uuid::nil());
                    if !start.starts_with(&prefix[..18]) {
                        return view.encode(&None::<Uuid>);
                    }
                    let next = view
                        .first_from(&prefix[..18], &start)?
                        .map(|entry| {
                            let id = entry.key().get(18..34).ok_or_else(recovery_unknown)?;
                            Uuid::from_slice(id).map_err(|_| recovery_unknown())
                        })
                        .transpose()?;
                    view.encode(&next)
                },
            )
            .await?;
        serde_json::from_slice(result.as_slice()).map_err(|_| recovery_unknown())
    }
}

/// Issued for a committed Retired Catalog record and an applied Timestamp
/// retention floor. Catalog never reuses an index UUID within a table.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReclaimIndex {
    pub authority: Authority,
    pub catalog_group_id: Uuid,
    pub table_id: Uuid,
    pub index_id: Uuid,
    pub retired_at: CommitTs,
    pub safe_ts: ReadTs,
    pub routing_version: Option<CommitTs>,
}

fn index_prefix(tag: u8, table_id: Uuid, index_id: Uuid) -> [u8; 34] {
    let mut prefix = [0; 34];
    prefix[0] = tag;
    prefix[1] = 10;
    prefix[2..18].copy_from_slice(table_id.as_bytes());
    prefix[18..].copy_from_slice(index_id.as_bytes());
    prefix
}

fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let last = prefix
        .iter()
        .rposition(|&byte| byte != 255)
        .expect("fixed native key tag");
    let mut end = prefix[..=last].to_vec();
    end[last] += 1;
    end
}

pub(super) fn apply(
    tx: &mut GroupTransaction<'_>,
    group: &GroupIdentity,
    config: Option<Config>,
    command: ReclaimIndex,
) -> Result<Transition, ProtocolError> {
    let Some(mut config) = config else {
        return reject(tx, Fault::NotInitialized);
    };
    let GroupKind::Data { table_id, .. } = group.kind else {
        return reject(tx, Fault::InvalidBinding);
    };
    if config.authority != command.authority {
        return reject(tx, Fault::StaleAuthority);
    }
    if command.catalog_group_id != config.catalog_group_id
        || command.table_id != table_id
        || command.index_id.is_nil()
        || command.safe_ts <= command.retired_at
    {
        return reject(tx, Fault::InvalidBinding);
    }
    let stable = |publication: Option<CommitTs>| {
        publication.is_some_and(|publication| publication <= command.safe_ts)
    };
    if config
        .split_source
        .as_ref()
        .is_some_and(|source| !stable(source.published_at))
        || config.split_child.is_some_and(|child| {
            // Split holds the table's exclusive lifecycle guard from its
            // image cut through routing publication and source settlement.
            // A later routing version therefore covers publication even
            // for old children without a stored publication timestamp.
            let publication = child.published_at.or(command
                .routing_version
                .filter(|&version| version > child.read_ts));
            !child.complete || !stable(publication)
        })
        || (config.direct_pause.is_some() && config.split_source.is_none())
        || tx
            .first_from(
                &index_prefix(INTENT, table_id, command.index_id),
                &index_prefix(INTENT, table_id, command.index_id),
            )?
            .is_some()
    {
        return reject(tx, Fault::Incomplete);
    }

    // Delete versions first. The heads keep the index discoverable until
    // every version has been reclaimed, even after a restart or leader move.
    let (versions, exhausted) = remove_page(tx, VALUE, table_id, command.index_id, PAGE_ITEMS)?;
    let (heads, complete) = if exhausted && versions < PAGE_ITEMS {
        remove_page(tx, HEAD, table_id, command.index_id, PAGE_ITEMS - versions)?
    } else {
        (0, false)
    };
    let deleted = versions + heads;
    if deleted != 0 {
        config.gc_floor = config.gc_floor.max(command.safe_ts.get());
        tx.put_serialized(CONFIG, &config)?;
    }
    commit(tx, &ParticipantReply::IndexReclaimed { deleted, complete })
}

fn remove_page(
    tx: &mut GroupTransaction<'_>,
    tag: u8,
    table_id: Uuid,
    index_id: Uuid,
    limit: usize,
) -> Result<(usize, bool), ProtocolError> {
    let prefix = index_prefix(tag, table_id, index_id);
    let mut bytes = 0usize;
    let progress = tx.scan_page_until(&prefix, limit, |key, value| {
        bytes += key.len() + value.len();
        Ok(bytes >= PAGE_BYTES)
    })?;
    if progress.visited != 0 {
        // The next key after a fully encoded entry is outside the deleted
        // range. The prefix end also covers UUIDs that end in 0xff.
        let end = if let Some(last) = progress.last_key.as_ref() {
            [last.as_slice(), &[0]].concat()
        } else {
            prefix_end(&prefix)
        };
        tx.delete_range(&prefix, &end)?;
    }
    Ok((progress.visited, progress.exhausted))
}
