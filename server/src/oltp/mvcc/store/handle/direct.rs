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
                    let latest = ReadTs::new(u64::MAX).map_err(|_| invalid())?;
                    match lineage::validate_read(&config, latest) {
                        Ok(()) => {}
                        Err(error) if error.code == ErrorCode::SplitPublicationPending => {
                            return view.encode(&LatestReply::SplitInProgress);
                        }
                        Err(error) if error.code == ErrorCode::StorageUnavailable => {
                            return view.encode(&LatestReply::NotOwned);
                        }
                        Err(error) => return Err(error),
                    }
                    if !owns(&group, &config.span, &key) {
                        return view.encode(&LatestReply::NotOwned);
                    }
                    let length = key_length(&key)?;
                    view.with_scratch_buffer(length + 9, |view, buffer| {
                        encode_key(&key, &mut buffer[1..1 + length]);
                        let end = length + 1;
                        buffer[0] = HEAD;
                        if let Some(raw) = view.pinned(&buffer[..end])? {
                            let head: Head =
                                serde_json::from_slice(&raw).map_err(|_| recovery_unknown())?;
                            if head.intent.is_some() {
                                return view.encode(&LatestReply::Blocked);
                            }
                        }
                        buffer[0] = VALUE;
                        buffer[end..].copy_from_slice(&0u64.to_be_bytes());
                        let Some(record) = view.first_from(&buffer[..end], buffer)? else {
                            return view.encode(&LatestReply::Value {
                                version: None,
                                value: None,
                            });
                        };
                        if record.key().len() != buffer.len() {
                            return Err(recovery_unknown());
                        }
                        let version = CommitTs::new(!u64::from_be_bytes(
                            record.key()[end..]
                                .try_into()
                                .map_err(|_| recovery_unknown())?,
                        ))
                        .map_err(|_| recovery_unknown())?;
                        #[derive(Deserialize)]
                        #[serde(deny_unknown_fields)]
                        struct Version<'a> {
                            #[serde(borrow)]
                            value: Option<&'a RawValue>,
                        }
                        let value: Version<'_> = serde_json::from_slice(record.value())
                            .map_err(|_| recovery_unknown())?;
                        view.encode(&LatestReply::Value {
                            version: Some(version),
                            value: value.value,
                        })
                    })
                },
            )
            .await?;
        let reply: LatestReply<'_> =
            serde_json::from_slice(raw.as_slice()).map_err(|_| recovery_unknown())?;
        Ok(match reply {
            LatestReply::Value { version, value } => LatestRead::Value {
                version,
                value: value.map(ToOwned::to_owned),
            },
            LatestReply::Blocked => LatestRead::Blocked,
            LatestReply::NotOwned => LatestRead::NotOwned,
            LatestReply::SplitInProgress => LatestRead::SplitInProgress,
        })
    }
}
