use super::*;
use nabedra_oltp_types::{authority::ReplicaRetiredStamp, types::GroupKind};
use uuid::Uuid;

pub(super) fn learner_key(group: Uuid) -> Vec<u8> {
    group_key(group, b'v', &[])
}

impl OltpPool {
    pub async fn unused_placement_target(&self, group: GroupIdentity) -> Result<(), ProtocolError> {
        let pool = self.clone();
        self.resources()
            .execute(ExecutionClass::Control, 0, 0, move || {
                pool.healthy()?;
                let snapshot = pool.snapshot();
                if snapshot
                    .get(&identity_key(group.group_id))
                    .map_err(storage_error)?
                    .is_some()
                    || snapshot
                        .get(&learner_key(group.group_id))
                        .map_err(storage_error)?
                        .is_some()
                {
                    return Err(failure(
                        ErrorCode::Conflict,
                        "target pool already retains this group",
                    ));
                }
                Ok(())
            })
            .await
    }

    pub async fn placement_learner(
        &self,
        group: GroupIdentity,
        binding: Digest,
    ) -> Result<GroupStore, ProtocolError> {
        if !matches!(group.kind, GroupKind::Data { .. }) {
            return Err(storage_error_message());
        }
        self.open_group(group, true, Some(binding)).await
    }

    pub(super) fn check_placement_learner(
        &self,
        group: &GroupIdentity,
        binding: Digest,
    ) -> Result<(), ProtocolError> {
        let snapshot = self.snapshot();
        let prior = snapshot
            .get(&learner_key(group.group_id))
            .map_err(storage_error)?;
        let retained = snapshot
            .get(&identity_key(group.group_id))
            .map_err(storage_error)?
            .is_some();
        match (prior, retained) {
            (Some(prior), true) if prior.as_ref() == binding.as_bytes() => Ok(()),
            (None, false) => Ok(()),
            _ => Err(failure(
                ErrorCode::Conflict,
                "retained target has a different replica move binding",
            )),
        }
    }
}

impl GroupStore {
    pub async fn replica_retirement(
        &self,
        generation: Uuid,
    ) -> Result<Option<ReplicaRetiredStamp>, ProtocolError> {
        let group = self.clone();
        self.resources()
            .execute(ExecutionClass::Control, 0, 0, move || {
                group.pool().healthy()?;
                let key = group_key(group.identity().group_id, b'x', generation.as_bytes());
                group
                    .pool()
                    .get_latest(&key)?
                    .map(|bytes| {
                        serde_json::from_slice(bytes.as_ref()).map_err(|_| recovery_unknown())
                    })
                    .transpose()
            })
            .await
    }

    pub async fn record_replica_retirement(
        &self,
        stamp: ReplicaRetiredStamp,
    ) -> Result<(), ProtocolError> {
        let group = self.clone();
        self.resources()
            .execute(ExecutionClass::Control, 0, 0, move || {
                group.pool().healthy()?;
                if group.identity() != &stamp.group
                    || group.pool().identity().pool_id != stamp.replica.pool_id
                    || group.pool().identity().worker_id != stamp.worker.worker_id
                {
                    return Err(recovery_unknown());
                }
                let key = group_key(group.identity().group_id, b'x', stamp.generation.as_bytes());
                let value = group
                    .resources()
                    .encode_in(ByteClass::Progress, &stamp, 4096)?;
                if let Some(prior) = group.pool().get_latest(&key)? {
                    return if prior.as_ref() == value.as_slice() {
                        Ok(())
                    } else {
                        Err(recovery_unknown())
                    };
                }
                let mut batch = WriteBatch::new();
                batch.put(&key, value.as_slice());
                group
                    .pool()
                    .durable_write(&group_scope(group.identity().group_id), batch)
            })
            .await
    }
}
