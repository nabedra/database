//! The timestamp group retains the exact split request and its ordered cuts.
use super::*;
use crate::{
    oltp::mvcc::store::{
        KeySpan,
        lifecycle::{GuardMode, SealedLifecycleGuard},
        lineage::SplitBinding,
    },
    oltp_replication::authority::{
        control::{GroupRecord, group_root, group_seed, group_step},
        status::{RequestKind, SealedSettlement},
    },
    protocol::Key,
};
use uuid::Uuid;

const JOB: &[u8] = b"ts/shard-split/v1/";
const RETAINED: &[u8] = b"ts/split-retained/v1/";
const RETAINED_KEY_BYTES: usize = RETAINED.len() + 24;
const RELEASE_BATCH: usize = 32;
pub(crate) const MAX_METADATA: usize = 48 << 10;
pub(crate) const AUTOMATIC_ATTEMPTS: u8 = 3;
pub(super) const BUDGET: TransactionBudget = TransactionBudget {
    scratch_bytes: 256 << 10,
    batch_bytes: 128 << 10,
    output_bytes: MAX_METADATA,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SplitIntent {
    pub split_id: Uuid,
    pub source: GroupIdentity,
    pub boundary: Key,
    pub page_bytes: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub automatic: Option<AutomaticSplitOrigin>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutomaticSplitOrigin {
    pub epoch: crate::oltp::mvcc::store::split_usage::ShardEpoch,
    pub slot: u8,
}
impl AutomaticSplitOrigin {
    pub(crate) fn id(self, source: &GroupIdentity) -> Uuid {
        use sha2::{Digest as _, Sha256};
        let mut hash = Sha256::new();
        hash.update(b"nabedra/automatic-shard-split/v1\0");
        hash.update(source.group_id.as_bytes());
        hash.update(self.epoch.schema.to_be_bytes());
        hash.update(self.epoch.routing.to_be_bytes());
        hash.update([self.slot]);
        Uuid::from_slice(&hash.finalize()[..16]).expect("fixed 16-byte split ID")
    }
}
impl SplitIntent {
    pub(crate) fn digest(&self) -> Result<Digest, ProtocolError> {
        crate::oltp_replication::owned::serialized_digest(&(b"nabedra/manual-split/v1", self))
    }
    pub(crate) fn validate(&self) -> Result<(), ProtocolError> {
        self.source.validate().map_err(|_| invalid())?;
        if self.split_id.is_nil()
            || !matches!(self.source.kind, GroupKind::Data { .. })
            || !(64 << 10..=crate::oltp::mvcc::MAX_FIXED_COMMAND_BYTES - (64 << 10))
                .contains(&self.page_bytes)
            || matches!(&self.boundary, Key::Utf8(key) if key.len() > MAX_METADATA / 6)
            || self.automatic.is_some_and(|origin| {
                origin.epoch.schema == 0
                    || origin.epoch.routing == 0
                    || !(1..=AUTOMATIC_ATTEMPTS).contains(&origin.slot)
                    || origin.id(&self.source) != self.split_id
            })
        {
            return Err(invalid());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ImageCut {
    pub pin: StoragePin,
    pub guard: SealedLifecycleGuard,
    pub children: Option<[SplitBinding; 2]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement: Option<SplitPlacement>,
}

pub(crate) const MAX_SPLIT_VOTERS: usize = 5;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SplitVoter {
    pub member: ReplicaMember,
    pub worker: WorkerIdentity,
    pub registered_pool_generation: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SplitSourcePlacement {
    pub record: GroupRecord,
    pub voters: Page<SplitVoter>,
    pub domains: Option<Digest>,
}

impl SplitSourcePlacement {
    pub(crate) fn validate(&self, source: &GroupIdentity) -> Result<(), ProtocolError> {
        let voters = &self.voters;
        let count = voters.len() as u64;
        if self.record.identity != *source
            || self.record.generation.is_nil()
            || !matches!(voters.len(), 1 | 3 | MAX_SPLIT_VOTERS)
            || self.record.replica_count != count
            || self.record.replica_root
                != group_root(
                    count,
                    voters.iter().fold(group_seed(), |prior, voter| {
                        group_step(prior, &voter.member)
                    }),
                )
            || voters
                .windows(2)
                .any(|pair| pair[0].member.node_id >= pair[1].member.node_id)
            || voters.iter().enumerate().any(|(index, voter)| {
                voter.member.validate().is_err()
                    || voter.worker.validate().is_err()
                    || voter.member.worker_id != voter.worker.worker_id
                    || voters[..index].iter().any(|old| {
                        old.worker.worker_id == voter.worker.worker_id
                            || old.member.pool_id == voter.member.pool_id
                    })
                    || voter.registered_pool_generation.is_some_and(|generation| {
                        generation == 0 || self.domains.is_none() || count != 3
                    })
            })
        {
            return Err(invalid());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SplitPlacement {
    pub source: SplitSourcePlacement,
    pub pool_generations: Page<u64>,
    pub required_bytes: u64,
}

impl SplitPlacement {
    fn validate(&self, intent: &SplitIntent) -> Result<(), ProtocolError> {
        self.source.validate(&intent.source)?;
        if intent.automatic.is_none()
            || self.required_bytes == 0
            || self.pool_generations.len() != self.source.voters.len()
            || self
                .pool_generations
                .iter()
                .zip(self.source.voters.iter())
                .any(|(generation, voter)| {
                    *generation == 0
                        || voter
                            .registered_pool_generation
                            .is_some_and(|expected| expected != *generation)
                })
        {
            return Err(invalid());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SplitAttempt {
    pub intent: SplitIntent,
    pub authority: Authority,
    pub catalog_group_id: Uuid,
    pub request: RequestBinding,
    pub txn: TxnId,
    pub snapshot: StoragePin,
    pub image: Option<ImageCut>,
    pub closed: Option<TxnDecision>,
}
impl SplitAttempt {
    pub(crate) fn exact_guard(&self, guard: &SealedLifecycleGuard) -> bool {
        let binding = guard.binding();
        guard.mode() == GuardMode::Exclusive
            && matches!(self.intent.source.kind, GroupKind::Data { table_id, .. } if table_id == binding.table_id)
            && binding.catalog_group_id == self.catalog_group_id
            && binding.authority == self.authority
            && binding.txn == self.txn
            && binding.request == self.request
            && binding.read_ts == self.snapshot.read_ts
    }
    fn exact_children(&self, children: &[SplitBinding; 2], cut: &ImageCut) -> bool {
        let [left, right] = children;
        children.iter().all(|child| {
            child.split_id == self.intent.split_id
                && child.source == self.intent.source
                && child.read_ts == cut.pin.read_ts
                && child.guard == cut.guard
        }) && left.source_schema_epoch == right.source_schema_epoch
            && left.source_routing_epoch == right.source_routing_epoch
            && self.intent.automatic.is_none_or(|origin| {
                origin.epoch.schema == left.source_schema_epoch
                    && origin.epoch.routing == left.source_routing_epoch
            })
            && left.source_span == right.source_span
            && left.target_span
                == (KeySpan {
                    start: left.source_span.start.clone(),
                    end: Some(self.intent.boundary.clone()),
                })
            && right.target_span
                == (KeySpan {
                    start: Some(self.intent.boundary.clone()),
                    end: right.source_span.end.clone(),
                })
            && left.target != right.target
    }
    pub(crate) fn children(&self) -> Result<&[SplitBinding; 2], ProtocolError> {
        self.image
            .as_ref()
            .and_then(|cut| cut.children.as_ref())
            .ok_or_else(invalid)
    }
    pub(crate) fn placement(&self) -> Option<&SplitPlacement> {
        self.image.as_ref().and_then(|cut| cut.placement.as_ref())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum SplitTimestampCommand {
    Open {
        authority: Authority,
        intent: SplitIntent,
        catalog_group_id: Uuid,
        reservation: IdentityReservation,
        pin_id: Uuid,
    },
    Cut {
        authority: Authority,
        split_id: Uuid,
        guard: SealedLifecycleGuard,
        pin_id: Uuid,
    },
    Bind {
        authority: Authority,
        split_id: Uuid,
        children: Box<[SplitBinding; 2]>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        placement: Option<Box<SplitPlacement>>,
    },
    Close {
        authority: Authority,
        split_id: Uuid,
        settled: SealedSettlement,
    },
}
impl SplitTimestampCommand {
    pub(super) fn heap_bytes(&self) -> Result<usize, ProtocolError> {
        // Variable fields are restricted to the bounded split metadata.
        crate::oltp::mvcc::store::encoded_length(self, MAX_METADATA)
    }
    pub(crate) fn id(&self) -> Uuid {
        match self {
            Self::Open { intent, .. } => intent.split_id,
            Self::Cut { split_id, .. }
            | Self::Bind { split_id, .. }
            | Self::Close { split_id, .. } => *split_id,
        }
    }
    pub(crate) fn authority(&self) -> Authority {
        match self {
            Self::Open { authority, .. }
            | Self::Cut { authority, .. }
            | Self::Bind { authority, .. }
            | Self::Close { authority, .. } => *authority,
        }
    }
}

pub(super) fn key(id: Uuid) -> Vec<u8> {
    [JOB, id.as_bytes()].concat()
}
pub(super) fn apply(
    tx: &mut GroupTransaction<'_>,
    state: Option<State>,
    scope: &GroupIdentity,
    command: &SplitTimestampCommand,
    position: AppliedPosition,
) -> Result<Transition, ProtocolError> {
    let Some(mut state) = state else {
        return reject(tx, Fault::NotInitialized);
    };
    if state.authority != command.authority() || state.ready.ready_count == 0 {
        return reject(tx, Fault::StaleAuthority);
    }
    let key = key(command.id());
    let current = load::<SplitAttempt>(tx, &key)?;
    if let SplitTimestampCommand::Open {
        authority,
        intent,
        catalog_group_id,
        reservation,
        pin_id,
    } = command
    {
        if intent.validate().is_err()
            || intent.source.cluster_id != scope.cluster_id
            || intent.source.database != scope.database
            || catalog_group_id.is_nil()
            || reservation.request_id != intent.split_id
            || reservation.kind != RequestKind::Mutation
            || reservation.original_run != authority.run_id
            || reservation.digest != intent.digest()?
            || reservation.reservation_id.is_nil()
            || pin_id.is_nil()
        {
            return reject(tx, Fault::InvalidBinding);
        }
        if let Some(existing) = current {
            return if existing.intent == *intent && existing.catalog_group_id == *catalog_group_id {
                commit(tx, &TimestampReply::Snapshot(existing.snapshot))
            } else {
                reject(tx, Fault::TransitionConflict)
            };
        }
        let Some(snapshot) = next_pin(&mut state, *pin_id) else {
            return reject(tx, Fault::TimestampExhausted);
        };
        let txn = TxnId {
            request_id: intent.split_id,
            attempt: 1,
        };
        let identity = PinIdentity {
            run_id: reservation.original_run,
            request_id: reservation.request_id,
            digest: reservation.digest,
            nonce: reservation.reservation_id,
        };
        let request = RequestBinding {
            run_id: identity.run_id,
            request_id: identity.request_id,
            digest: identity.digest,
            accepted_context: crate::oltp_replication::owned::serialized_digest(&(
                b"nabedra/manual-split-context/v1",
                intent,
                catalog_group_id,
                snapshot,
                authority,
            ))?,
        };
        let pin_key = pin_key(identity.nonce);
        if tx.get(&pin_key)?.is_some() || tx.get(&rejected_key(identity.nonce))?.is_some() {
            return reject(tx, Fault::InvalidBinding);
        }
        put(
            tx,
            &pin_key,
            &PinRecord {
                identity: identity.clone(),
                reservation: *reservation,
                request: Some(request),
                pin: snapshot,
                state: PinState::Active,
                purpose: PinPurpose::SplitMetadata { txn },
            },
        )?;
        tx.put(&active_key(snapshot), identity.nonce.as_bytes())?;
        put(
            tx,
            &key,
            &SplitAttempt {
                intent: intent.clone(),
                authority: *authority,
                catalog_group_id: *catalog_group_id,
                request,
                txn,
                snapshot,
                image: None,
                closed: None,
            },
        )?;
        put(tx, STATE, &state)?;
        return commit(tx, &TimestampReply::Snapshot(snapshot));
    }
    let Some(mut attempt) = current else {
        return reject(tx, Fault::UnknownPin);
    };
    match command {
        SplitTimestampCommand::Open { .. } => unreachable!("handled above"),
        SplitTimestampCommand::Cut { guard, pin_id, .. } => {
            if attempt.authority != state.authority
                || !attempt.exact_guard(guard)
                || attempt.closed.is_some()
                || pin_id.is_nil()
            {
                return reject(tx, Fault::InvalidBinding);
            }
            if let Some(cut) = attempt.image {
                return if cut.guard == *guard {
                    commit(tx, &TimestampReply::Snapshot(cut.pin))
                } else {
                    reject(tx, Fault::TransitionConflict)
                };
            }
            let Some(pin) = next_pin(&mut state, *pin_id) else {
                return reject(tx, Fault::TimestampExhausted);
            };
            attempt.image = Some(ImageCut {
                pin,
                guard: guard.clone(),
                children: None,
                placement: None,
            });
            tx.put(&maintenance_active_key(pin), &key)?;
            put(tx, &key, &attempt)?;
            put(tx, STATE, &state)?;
            commit(tx, &TimestampReply::Snapshot(pin))
        }
        SplitTimestampCommand::Bind {
            children,
            placement,
            ..
        } => {
            let Some(cut) = attempt.image.as_ref() else {
                return reject(tx, Fault::UnknownPin);
            };
            if attempt.authority != state.authority
                || attempt.closed.is_some()
                || !attempt.exact_children(children, cut)
                || placement
                    .as_ref()
                    .is_some_and(|value| value.validate(&attempt.intent).is_err())
            {
                return reject(tx, Fault::InvalidBinding);
            }
            let pin = cut.pin;
            if let Some(prior) = &cut.children {
                return if prior == children.as_ref()
                    && cut.placement.as_ref() == placement.as_deref()
                {
                    commit(tx, &TimestampReply::Snapshot(pin))
                } else {
                    reject(tx, Fault::TransitionConflict)
                };
            }
            let cut = attempt.image.as_mut().ok_or_else(unknown)?;
            cut.children = Some(children.as_ref().clone());
            cut.placement = placement.as_deref().cloned();
            if crate::oltp::mvcc::store::encoded_length(&attempt, MAX_METADATA).is_err() {
                return reject(tx, Fault::InvalidBinding);
            }
            put(tx, &key, &attempt)?;
            commit(tx, &TimestampReply::Snapshot(pin))
        }
        SplitTimestampCommand::Close { settled, .. } => {
            let proof = settled.statement();
            if settled
                .validate_for(attempt.txn, &attempt.request, &proof.manifest)
                .is_err()
            {
                return reject(tx, Fault::InvalidBinding);
            }
            let decision = proof.decision.decision;
            if let Some(closed) = attempt.closed {
                return if closed == decision {
                    commit(tx, &TimestampReply::Released)
                } else {
                    reject(tx, Fault::TransitionConflict)
                };
            }
            if let TxnDecision::Commit { commit_ts } = decision {
                let Some(cut) = attempt.image.as_ref() else {
                    return reject(tx, Fault::InvalidBinding);
                };
                if cut.children.is_none()
                    || commit_ts <= cut.pin.read_ts
                    || proof.manifest.participant_count != 1
                {
                    return reject(tx, Fault::InvalidBinding);
                }
                tx.put(&retained_key(cut.pin), &commit_ts.get().to_be_bytes())?;
            }
            if let Some(cut) = &attempt.image {
                tx.delete(&maintenance_active_key(cut.pin))?;
            }
            let nonce = active_nonce(tx, attempt.snapshot)?;
            let key_for_pin = pin_key(nonce);
            let Some(mut pin) = load::<PinRecord>(tx, &key_for_pin)? else {
                return Err(unknown());
            };
            if pin.pin != attempt.snapshot
                || pin.request != Some(attempt.request)
                || pin.purpose != (PinPurpose::SplitMetadata { txn: attempt.txn })
                || !matches!(pin.state, PinState::Active)
            {
                return reject(tx, Fault::InvalidBinding);
            }
            pin.state = PinState::MetadataReleased {
                decision: crate::oltp_replication::owned::serialized_digest(settled)?,
                applied_at: position,
            };
            put(tx, &key_for_pin, &pin)?;
            tx.delete(&active_key(attempt.snapshot))?;
            attempt.closed = Some(decision);
            put(tx, &key, &attempt)?;
            commit(tx, &TimestampReply::Released)
        }
    }
}

pub(super) fn retained_key(pin: StoragePin) -> [u8; RETAINED_KEY_BYTES] {
    let mut key = [0; RETAINED_KEY_BYTES];
    key[..RETAINED.len()].copy_from_slice(RETAINED);
    key[RETAINED.len()..RETAINED.len() + 8].copy_from_slice(&pin.read_ts.get().to_be_bytes());
    key[RETAINED.len() + 8..].copy_from_slice(pin.pin_id.as_bytes());
    key
}

/// A base taken during import may be newer than the image cut yet older than
/// publication. Keep the image cut until every independent pin covers publication.
pub(super) fn advance_retention(
    tx: &mut GroupTransaction<'_>,
    independent: u64,
) -> Result<u64, ProtocolError> {
    let mut released = [[0; RETAINED_KEY_BYTES]; RELEASE_BATCH];
    let mut count = 0;
    let mut floor = independent;
    tx.scan_page(RETAINED, None, RELEASE_BATCH + 1, |key, value| {
        let key: [u8; RETAINED_KEY_BYTES] = key.try_into().map_err(|_| unknown())?;
        let cut = u64::from_be_bytes(
            key[RETAINED.len()..RETAINED.len() + 8]
                .try_into()
                .map_err(|_| unknown())?,
        );
        let published = u64::from_be_bytes(value.try_into().map_err(|_| unknown())?);
        if cut == 0 || cut >= published {
            return Err(unknown());
        }
        if published <= independent && count < released.len() {
            released[count] = key;
            count += 1;
        } else {
            floor = floor.min(cut);
        }
        Ok(())
    })?;
    for key in &released[..count] {
        tx.delete(key)?;
    }
    Ok(floor)
}

fn next_pin(state: &mut State, pin_id: Uuid) -> Option<StoragePin> {
    let read_ts = Timestamp::new(state.high_watermark.checked_add(1)?).ok()?;
    state.high_watermark = read_ts.get();
    Some(StoragePin {
        pin_id,
        read_ts,
        allocator_epoch: state.allocator_epoch,
    })
}

fn active_nonce(tx: &mut GroupTransaction<'_>, pin: StoragePin) -> Result<Uuid, ProtocolError> {
    let key = active_key(pin);
    let raw = tx.get(&key)?.ok_or_else(unknown)?;
    Uuid::from_slice(raw.as_slice()).map_err(|_| unknown())
}

impl TimestampHandle {
    pub(crate) async fn split_attempt_raw(&self, id: Uuid) -> Result<ChargedBytes, ProtocolError> {
        self.consensus.ensure_linearizable().await?;
        self.machine
            .store()
            .read_metadata(BUDGET, move |view| {
                let raw = view.get(&key(id))?;
                let value: Option<SplitAttempt> = raw
                    .as_ref()
                    .map(|raw| serde_json::from_slice(raw.as_slice()).map_err(|_| unknown()))
                    .transpose()?;
                view.encode(&value)
            })
            .await
    }
    pub(crate) async fn split_timestamp(
        &self,
        command: SplitTimestampCommand,
    ) -> Result<TimestampReply, ProtocolError> {
        Ok(self
            .submit(TimestampCommand::SplitMetadata { command })
            .await?
            .reply)
    }
}
