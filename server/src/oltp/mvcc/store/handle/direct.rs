//! Leader-local reads for the direct executor (plan section 6, T5).
//!
//! The executor takes one linearizable barrier per attempt
//! (`direct_barrier`, lease first) and then reads the dedup record, the
//! group's epochs and each key's newest version from this replica's applied
//! state without another round trip. These reads are only served on a local
//! handle; a remote handle refuses them.
// T6 (gateway) and T7 (activation) construct and call this; until then only tests do.
#![cfg_attr(not(test), allow(dead_code))]
use super::*;
use crate::oltp_replication::authority::consensus::ReadBarrier;

/// The newest committed state of one key on this replica.
#[derive(Debug)]
pub(crate) enum LatestRead {
    /// `version` is the key's exact head version (a tombstone keeps its
    /// timestamp) and `value` is `None` for no row or a tombstone.
    Value {
        version: Option<CommitTs>,
        value: Option<Box<RawValue>>,
    },
    /// A queued transaction holds an intent on the key.
    Blocked,
    /// The key is outside this group's span, or split routing moved it.
    NotOwned,
    /// A split source awaits routing publication.
    SplitInProgress,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LatestReply<'a> {
    Value {
        version: Option<CommitTs>,
        #[serde(borrow)]
        value: Option<&'a RawValue>,
    },
    Blocked,
    NotOwned,
    SplitInProgress,
}

impl ParticipantHandle {
    fn direct_local(&self) -> Result<(), ProtocolError> {
        if self.remote.is_some() || !matches!(self.identity.kind, GroupKind::Data { .. }) {
            return Err(invalid());
        }
        Ok(())
    }

    /// The range clock of a local Data replica.
    pub(crate) fn direct_clock(
        &self,
    ) -> Result<&Arc<crate::oltp::mvcc::store::direct::RangeClock>, ProtocolError> {
        self.direct_local()?;
        Ok(self.local_machine().range_clock())
    }

    /// The leader this replica last observed, and its own node id.
    pub(crate) fn direct_leadership(&self) -> (Option<u64>, Option<u64>) {
        match &self.consensus {
            Some(consensus) => (consensus.current_leader(), consensus.local_node()),
            None => (None, None),
        }
    }

    /// The leader's read barrier: a valid lease, else ReadIndex.
    pub(crate) async fn direct_barrier(&self) -> Result<ReadBarrier, ProtocolError> {
        self.direct_local()?;
        self.local_consensus().ensure_linearizable_lease().await
    }

    /// `direct_dedup` without its own barrier: call after `direct_barrier`.
    pub(crate) async fn direct_dedup_after_barrier(
        &self,
        table_id: Uuid,
        anchor: Key,
        request_id: Uuid,
    ) -> Result<Option<crate::oltp::mvcc::store::direct::DedupLookup>, ProtocolError> {
        self.direct_local()?;
        self.local_machine()
            .direct_dedup(table_id, anchor, request_id)
            .await
    }

    /// The group's fence and epochs from local applied state, no barrier.
    /// Apply checks them again, so a stale value only costs a rejection.
    pub(crate) async fn direct_epochs(&self) -> Result<ParticipantEpochs, ProtocolError> {
        self.direct_local()?;
        let raw = self
            .local_machine()
            .store
            .read_metadata(META, |view| {
                let raw = view.pinned(CONFIG)?.ok_or_else(invalid)?;
                if raw.len() > METADATA_BYTES {
                    return Err(recovery_unknown());
                }
                let epochs = serde_json::from_slice::<ParticipantEpochs>(&raw)
                    .map_err(|_| recovery_unknown())?;
                view.encode(&epochs)
            })
            .await?;
        serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())
    }

    /// The leader's read barrier index: a valid lease, else ReadIndex.
    /// Pass it to `direct_snapshot`, which checks it in its own snapshot.
    pub(crate) async fn direct_lease_index(&self) -> Result<u64, ProtocolError> {
        self.direct_local()?;
        self.local_consensus().lease_read_index().await
    }

    /// One snapshot of this replica's applied state for the direct leader
    /// (PERF2b): the applied position checked against `barrier` (from
    /// `direct_lease_index`), the group's epochs, the dedup record of
    /// `request_id` at `anchor`, and the newest version of `anchor`'s row.
    /// It replaces `direct_epochs`, the barrier's applied read,
    /// `direct_dedup_after_barrier` and `direct_latest`, one native job each,
    /// with one Query-class job. `None` means this replica has not applied
    /// the barrier's index yet. Each part keeps its own error, so callers
    /// map them exactly as they mapped the separate reads.
    pub(crate) async fn direct_snapshot(
        &self,
        barrier: u64,
        table_id: Uuid,
        anchor: Key,
        request_id: Uuid,
    ) -> Result<Option<DirectSnapshot>, ProtocolError> {
        self.direct_local()?;
        let machine = self.local_machine();
        let key = MvccKey::Row {
            table_id,
            primary_key: anchor.clone(),
        };
        machine.range_clock().wait_latest(&key).await?;
        let group = self.identity.clone();
        let store = machine.store.clone();
        let raw = machine
            .store
            .read(
                TransactionBudget {
                    scratch_bytes: 32 << 20,
                    batch_bytes: 0,
                    // `direct_latest`'s bound, the largest Query reply. A
                    // dedup result is at most 48 KiB, and a row at most a
                    // request body, far below it.
                    output_bytes: 8 << 20,
                },
                move |view| {
                    // As `read_barrier`: the snapshot must hold the
                    // barrier's index, or the caller retries.
                    let applied = store.applied_position_in(view)?.map(|at| at.index);
                    if applied.is_none_or(|applied| applied < barrier) {
                        tracing::error!(group=%group.group_id, ?applied, required=barrier,
                            "direct read barrier not applied in the read snapshot");
                        return view.encode(&SnapshotReply::<'_>::default());
                    }
                    let raw = view.pinned(CONFIG)?.ok_or_else(invalid)?;
                    if raw.len() > METADATA_BYTES {
                        return Err(recovery_unknown());
                    }
                    let config: Config =
                        serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
                    drop(raw);
                    let epochs = ParticipantEpochs {
                        authority: config.authority,
                        catalog_group_id: config.catalog_group_id,
                        schema_epoch: config.schema_epoch,
                        routing_epoch: config.routing_epoch,
                        direct_schema: config.direct_schema,
                    };
                    let (dedup, dedup_error) = match crate::oltp::mvcc::store::direct::dedup_in(
                        view, &group, &config, table_id, anchor, request_id,
                    ) {
                        Ok(found) => (found, None),
                        Err(error) => (None, Some(error)),
                    };
                    latest_in(view, &group, &config, &key, |view, latest| {
                        let (latest, latest_error) = match latest {
                            // A dedup hit answers both callers from the
                            // record: the row value is not needed, and the
                            // reply stays within one row's bound.
                            Ok(LatestReply::Value { version, .. }) if dedup.is_some() => (
                                Some(LatestReply::Value {
                                    version,
                                    value: None,
                                }),
                                None,
                            ),
                            Ok(latest) => (Some(latest), None),
                            Err(error) => (None, Some(error)),
                        };
                        view.encode(&SnapshotReply {
                            epochs: Some(epochs),
                            dedup,
                            dedup_error,
                            latest,
                            latest_error,
                        })
                    })
                },
            )
            .await?;
        let reply: SnapshotReply<'_> =
            serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())?;
        let Some(epochs) = reply.epochs else {
            return Ok(None);
        };
        let latest = match (reply.latest, reply.latest_error) {
            (_, Some(error)) => Err(error),
            (Some(latest), None) => Ok(latest.into()),
            (None, None) => return Err(recovery_unknown()),
        };
        Ok(Some(DirectSnapshot {
            epochs,
            dedup: match reply.dedup_error {
                Some(error) => Err(error),
                None => Ok(reply.dedup),
            },
            latest,
        }))
    }

    /// The newest version of `key` on this replica, after every commit this
    /// leader registered on the key applied (range clock `wait_latest`). The
    /// wait fails with `StorageUnavailable` when leadership changes.
    pub(crate) async fn direct_latest(&self, key: MvccKey) -> Result<LatestRead, ProtocolError> {
        self.direct_local()?;
        let machine = self.local_machine();
        machine.range_clock().wait_latest(&key).await?;
        let group = self.identity.clone();
        let raw = machine
            .store
            .read(
                TransactionBudget {
                    scratch_bytes: 32 << 20,
                    batch_bytes: 0,
                    output_bytes: 8 << 20,
                },
                move |view| {
                    let raw = view.pinned(CONFIG)?.ok_or_else(invalid)?;
                    if raw.len() > METADATA_BYTES {
                        return Err(recovery_unknown());
                    }
                    let config: Config =
                        serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
                    drop(raw);
                    latest_in(view, &group, &config, &key, |view, reply| {
                        view.encode(&reply?)
                    })
                },
            )
            .await?;
        let reply: LatestReply<'_> =
            serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())?;
        Ok(reply.into())
    }
}

/// What `direct_snapshot` read.
pub(crate) struct DirectSnapshot {
    pub epochs: ParticipantEpochs,
    pub dedup: Result<Option<crate::oltp::mvcc::store::direct::DedupLookup>, ProtocolError>,
    pub latest: Result<LatestRead, ProtocolError>,
}

/// `direct_snapshot`'s native reply. No `epochs`: the barrier index was not
/// applied in the snapshot.
#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotReply<'a> {
    epochs: Option<ParticipantEpochs>,
    dedup: Option<crate::oltp::mvcc::store::direct::DedupLookup>,
    dedup_error: Option<ProtocolError>,
    #[serde(borrow)]
    latest: Option<LatestReply<'a>>,
    latest_error: Option<ProtocolError>,
}

impl From<LatestReply<'_>> for LatestRead {
    fn from(reply: LatestReply<'_>) -> Self {
        match reply {
            LatestReply::Value { version, value } => LatestRead::Value {
                version,
                value: value.map(ToOwned::to_owned),
            },
            LatestReply::Blocked => LatestRead::Blocked,
            LatestReply::NotOwned => LatestRead::NotOwned,
            LatestReply::SplitInProgress => LatestRead::SplitInProgress,
        }
    }
}

/// The newest committed state of `key` in `view`'s snapshot, handed to
/// `finish` (which encodes it while the value is still borrowed). A failed
/// lookup reaches `finish` as `Err`; only `finish`'s own errors and a
/// scratch allocation failure return from here directly.
fn latest_in<'a, R>(
    view: &mut crate::oltp::pool::GroupRead<'a>,
    group: &GroupIdentity,
    config: &Config,
    key: &MvccKey,
    finish: impl for<'r> FnOnce(
        &mut crate::oltp::pool::GroupRead<'a>,
        Result<LatestReply<'r>, ProtocolError>,
    ) -> Result<R, ProtocolError>,
) -> Result<R, ProtocolError> {
    let latest = match ReadTs::new(u64::MAX) {
        Ok(latest) => latest,
        Err(_) => return finish(view, Err(invalid())),
    };
    match lineage::validate_read(config, latest) {
        Ok(()) => {}
        Err(error) if error.code == ErrorCode::SplitPublicationPending => {
            return finish(view, Ok(LatestReply::SplitInProgress));
        }
        Err(error) if error.code == ErrorCode::StorageUnavailable => {
            return finish(view, Ok(LatestReply::NotOwned));
        }
        Err(error) => return finish(view, Err(error)),
    }
    if !owns(group, &config.span, key) {
        return finish(view, Ok(LatestReply::NotOwned));
    }
    let length = match key_length(key) {
        Ok(length) => length,
        Err(error) => return finish(view, Err(error)),
    };
    view.with_scratch_buffer(length + 9, |view, buffer| {
        encode_key(key, &mut buffer[1..1 + length]);
        let end = length + 1;
        buffer[0] = HEAD;
        let blocked = view.pinned(&buffer[..end]).and_then(|raw| match raw {
            Some(raw) => {
                let head: Head = serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
                Ok(head.intent.is_some())
            }
            None => Ok(false),
        });
        match blocked {
            Err(error) => return finish(view, Err(error)),
            Ok(true) => return finish(view, Ok(LatestReply::Blocked)),
            Ok(false) => {}
        }
        buffer[0] = VALUE;
        buffer[end..].copy_from_slice(&0u64.to_be_bytes());
        let record = match view.first_from(&buffer[..end], buffer) {
            Err(error) => return finish(view, Err(error)),
            Ok(None) => {
                return finish(
                    view,
                    Ok(LatestReply::Value {
                        version: None,
                        value: None,
                    }),
                );
            }
            Ok(Some(record)) => record,
        };
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Version<'a> {
            #[serde(borrow)]
            value: Option<&'a RawValue>,
        }
        let decoded = (|| {
            if record.key().len() != buffer.len() {
                return Err(recovery_unknown());
            }
            let version = CommitTs::new(!u64::from_be_bytes(
                record.key()[end..]
                    .try_into()
                    .map_err(|_| recovery_unknown())?,
            ))
            .map_err(|_| recovery_unknown())?;
            let value: Version<'_> =
                serde_json::from_slice(record.value()).map_err(|_| recovery_unknown())?;
            Ok((version, value.value))
        })();
        match decoded {
            Err(error) => finish(view, Err(error)),
            Ok((version, value)) => finish(
                view,
                Ok(LatestReply::Value {
                    version: Some(version),
                    value,
                }),
            ),
        }
    })
}
