//! Bounded usage and live pool probes for an exact source placement.
use super::*;
use crate::oltp::{
    failure,
    mvcc::store::split_usage::{ShardEpoch, ShardUsage, ShardUsageScan},
    pool::mount::{MountedPoolObservation, PoolMountProfile},
};
use crate::protocol::{ErrorCode, Key};

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutomaticSplitPolicy {
    pub shard_target_bytes: u64,
    pub minimum_free_bytes: u64,
}

impl AutomaticSplitPolicy {
    pub(crate) fn validate(self) -> Result<(), ProtocolError> {
        if self.shard_target_bytes == 0 || self.minimum_free_bytes == 0 {
            return Err(invalid());
        }
        self.required_free(self.shard_target_bytes).map(|_| ())
    }

    fn required_free(self, observed_bytes: u64) -> Result<u64, ProtocolError> {
        observed_bytes
            .checked_mul(3)
            .and_then(|copy| copy.checked_add(self.minimum_free_bytes))
            .ok_or_else(|| capacity("split headroom calculation exceeds the native byte range"))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CapacityRequest {
    pub source: SplitSourcePlacement,
    pub authority: Authority,
    pub required_bytes: u64,
    pub page_bytes: usize,
}

#[derive(Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub(super) enum CapacityReply {
    Ready(u64),
    Disk {
        generation: u64,
        required_bytes: u64,
        available_bytes: u64,
    },
}

#[derive(Clone, Serialize)]
pub(crate) struct SplitPoolDeficit {
    pub worker: WorkerIdentity,
    pub pool: Uuid,
    pub pool_generation: u64,
    pub required_bytes: u64,
    pub available_bytes: u64,
}

pub(crate) enum SplitCapacity {
    Ready(SplitPlacement),
    Disk {
        required_bytes: u64,
        pools: Page<SplitPoolDeficit>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct UsageRequest {
    pub source: SplitSourcePlacement,
    pub authority: Authority,
    pub target: u64,
    pub token: Option<Uuid>,
}

pub(crate) struct UsageResume {
    worker: WorkerIdentity,
    token: Uuid,
}

pub(crate) enum Usage {
    Inactive,
    Frozen {
        epoch: ShardEpoch,
        split_id: Uuid,
    },
    Scanning(UsageResume),
    Observed {
        epoch: ShardEpoch,
        bytes: u64,
        largest_image: usize,
        pivot: Option<Key>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum UsageReply {
    Inactive,
    Frozen {
        epoch: ShardEpoch,
        split_id: Uuid,
    },
    Scanning {
        token: Uuid,
    },
    Observed {
        epoch: ShardEpoch,
        bytes: u64,
        largest_image: usize,
        pivot: Option<Key>,
    },
}

pub(super) struct LocalUsageScan {
    key: (Uuid, Uuid, Authority, u64),
    token: Uuid,
    scan: ShardUsageScan,
}

impl LocalUsageScan {
    pub(super) async fn observe(
        slot: &tokio::sync::Mutex<Option<Self>>,
        request: UsageRequest,
        machine: ParticipantMachine,
    ) -> Result<UsageReply, ProtocolError> {
        let key = (
            request.source.record.identity.group_id,
            request.source.record.generation,
            request.authority,
            request.target,
        );
        let mut slot = slot.lock().await;
        let (token, scan) = if let Some(token) = request.token {
            let scan = slot
                .take()
                .filter(|scan| scan.token == token && scan.key == key)
                .ok_or_else(unavailable)?;
            (token, Some(scan.scan))
        } else {
            *slot = None;
            (nabedra_rt::ids::uuid_v4(), None)
        };
        Ok(
            match machine
                .observe_split_usage(request.authority, request.target, scan)
                .await?
            {
                ShardUsage::Inactive => UsageReply::Inactive,
                ShardUsage::Frozen { epoch, split_id } => UsageReply::Frozen { epoch, split_id },
                ShardUsage::Scanning(scan) => {
                    *slot = Some(Self { key, token, scan });
                    UsageReply::Scanning { token }
                }
                ShardUsage::Observed {
                    epoch,
                    bytes,
                    largest_image,
                    pivot,
                } => UsageReply::Observed {
                    epoch,
                    bytes,
                    largest_image,
                    pivot,
                },
            },
        )
    }
}

impl SplitClient {
    pub(crate) async fn source_placement(
        &self,
        record: &GroupRecord,
        authority: Authority,
    ) -> Result<SplitSourcePlacement, ProtocolError> {
        self.control.split_source(record, authority).await
    }

    pub(super) async fn verify_source_placement(
        &self,
        expected: &SplitSourcePlacement,
        authority: Authority,
    ) -> Result<(), ProtocolError> {
        if self.source_placement(&expected.record, authority).await? != *expected {
            return Err(unavailable());
        }
        Ok(())
    }

    pub(crate) async fn observe_usage(
        &self,
        source: &SplitSourcePlacement,
        authority: Authority,
        target: u64,
        resume: Option<UsageResume>,
    ) -> Result<Usage, ProtocolError> {
        let payload = self.shared().encode_in(
            ByteClass::Progress,
            &UsageRequest {
                source: source.clone(),
                authority,
                target,
                token: resume.as_ref().map(|resume| resume.token),
            },
            MAX_METADATA,
        )?;
        let mut last = unavailable();
        for voter in source.voters.iter().filter(|voter| {
            resume
                .as_ref()
                .is_none_or(|resume| resume.worker == voter.worker)
        }) {
            let reply = match self
                .request(&voter.member, Operation::Usage, &payload)
                .await
            {
                Ok((reply, _)) if reply.remote_identity() == voter.worker => reply,
                Ok(_) => return Err(unauthorized()),
                Err(error) if transient(&error) => {
                    last = error;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let (usage, _) = self.decode::<UsageReply>(reply).await?;
            return Ok(match usage {
                UsageReply::Inactive => Usage::Inactive,
                UsageReply::Frozen { epoch, split_id } => Usage::Frozen { epoch, split_id },
                UsageReply::Scanning { token } => Usage::Scanning(UsageResume {
                    worker: voter.worker,
                    token,
                }),
                UsageReply::Observed {
                    epoch,
                    bytes,
                    largest_image,
                    pivot,
                } => Usage::Observed {
                    epoch,
                    bytes,
                    largest_image,
                    pivot,
                },
            });
        }
        Err(last)
    }

    pub(crate) async fn check_capacity(
        &self,
        source: &SplitSourcePlacement,
        authority: Authority,
        policy: AutomaticSplitPolicy,
        observed: u64,
        page_bytes: usize,
    ) -> Result<SplitCapacity, ProtocolError> {
        let required_bytes = policy.required_free(observed)?;
        self.probe_capacity(source, authority, required_bytes, page_bytes)
            .await
    }

    pub(crate) async fn verify_capacity(
        &self,
        placement: &SplitPlacement,
        authority: Authority,
        page_bytes: usize,
    ) -> Result<(), ProtocolError> {
        let actual = self
            .probe_capacity(
                &placement.source,
                authority,
                placement.required_bytes,
                page_bytes,
            )
            .await?;
        match actual {
            SplitCapacity::Ready(actual) if actual == *placement => Ok(()),
            SplitCapacity::Ready(_) => Err(capacity("automatic split pool generation changed")),
            SplitCapacity::Disk { .. } => Err(disk_capacity()),
        }
    }

    async fn probe_capacity(
        &self,
        source: &SplitSourcePlacement,
        authority: Authority,
        required_bytes: u64,
        page_bytes: usize,
    ) -> Result<SplitCapacity, ProtocolError> {
        let payload = self.shared().encode_in(
            ByteClass::Progress,
            &CapacityRequest {
                source: source.clone(),
                authority,
                required_bytes,
                page_bytes,
            },
            MAX_METADATA,
        )?;
        let mut generations = Vec::with_capacity(source.voters.len());
        let mut deficits = Vec::with_capacity(source.voters.len());
        for voter in source.voters.iter() {
            let (reply, _) = self
                .request(&voter.member, Operation::Capacity, &payload)
                .await?;
            if reply.remote_identity() != voter.worker {
                return Err(unauthorized());
            }
            let (reply, _) = self.decode::<CapacityReply>(reply).await?;
            let generation = match reply {
                CapacityReply::Ready(generation) => generation,
                CapacityReply::Disk {
                    generation,
                    required_bytes: required,
                    available_bytes,
                } => {
                    if available_bytes >= required
                        || (required != required_bytes
                            && required_bytes.checked_mul(source.voters.len() as u64)
                                != Some(required))
                    {
                        return Err(invalid());
                    }
                    deficits.push(SplitPoolDeficit {
                        worker: voter.worker,
                        pool: voter.member.pool_id,
                        pool_generation: generation,
                        required_bytes: required,
                        available_bytes,
                    });
                    generation
                }
            };
            if generation == 0
                || voter
                    .registered_pool_generation
                    .is_some_and(|expected| generation != expected)
            {
                return Err(capacity("automatic split pool generation changed"));
            }
            generations.push(generation);
        }
        if deficits.is_empty() {
            Ok(SplitCapacity::Ready(SplitPlacement {
                source: source.clone(),
                pool_generations: Page::new(generations).map_err(|_| invalid())?,
                required_bytes,
            }))
        } else {
            Ok(SplitCapacity::Disk {
                required_bytes,
                pools: Page::new(deficits).map_err(|_| invalid())?,
            })
        }
    }
}

pub(super) fn check_headroom(
    shared: &crate::oltp::pool::WorkerSharedResources,
    mounted: &MountedPoolObservation,
    replica_count: usize,
    required_bytes: u64,
    page_bytes: usize,
) -> Result<CapacityReply, ProtocolError> {
    if required_bytes == 0
        || !(64 << 10..=MAX_FIXED_COMMAND_BYTES - (64 << 10)).contains(&page_bytes)
    {
        return Err(invalid());
    }
    let (used, limit) = shared.resident_group_usage();
    if used.checked_add(2).is_none_or(|required| required > limit) {
        return Err(capacity(
            "automatic split needs two available resident Data replica slots",
        ));
    }
    let required = disk_requirement(mounted, replica_count, required_bytes)?;
    let working = page_bytes
        .checked_mul(3)
        .and_then(|bytes| bytes.checked_add(256 << 10))
        .ok_or_else(|| capacity("automatic split image memory calculation overflowed"))?;
    drop(shared.reserve_scratch(ByteClass::Data, working)?);
    let generation = mounted.expected.generation;
    Ok(if mounted.available_bytes < required {
        CapacityReply::Disk {
            generation,
            required_bytes: required,
            available_bytes: mounted.available_bytes,
        }
    } else {
        CapacityReply::Ready(generation)
    })
}

pub(super) fn check_disk_headroom(
    mounted: &MountedPoolObservation,
    replica_count: usize,
    required_bytes: u64,
) -> Result<(), ProtocolError> {
    if mounted.available_bytes < disk_requirement(mounted, replica_count, required_bytes)? {
        return Err(disk_capacity());
    }
    Ok(())
}

fn disk_requirement(
    mounted: &MountedPoolObservation,
    replica_count: usize,
    required_bytes: u64,
) -> Result<u64, ProtocolError> {
    if matches!(mounted.expected.profile, PoolMountProfile::Docker { .. }) {
        // Different Docker volumes can share one backing filesystem.
        required_bytes
            .checked_mul(replica_count as u64)
            .ok_or_else(|| capacity("automatic split Docker capacity calculation overflowed"))
    } else {
        Ok(required_bytes)
    }
}

fn disk_capacity() -> ProtocolError {
    capacity("automatic split is waiting for mounted filesystem capacity")
}

fn capacity(message: &'static str) -> ProtocolError {
    failure(ErrorCode::LimitExceeded, message)
}
