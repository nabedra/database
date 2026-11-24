use super::*;
use crate::{
    oltp::pool::mount::MountedPoolObservation,
    oltp_replication::authority::control::{RestartIntent, RestartMountKey, VerifiedExclusion},
};
use ring::rand::{SecureRandom, SystemRandom};

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RollingStart {
    pub preparation_id: Uuid,
    pub run_id: Uuid,
}

pub(super) fn validate(config: &RegisteredConfig) -> Result<(), ProtocolError> {
    let Some(rolling) = config.rolling else {
        return Ok(());
    };
    if rolling.preparation_id.is_nil()
        || rolling.run_id.is_nil()
        || config.required_voters < 3
        || config.cold.is_some()
        || config.restore.is_some()
        || !config
            .mount
            .as_ref()
            .is_some_and(|mount| matches!(mount.profile, PoolMountProfile::Docker { .. }))
    {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) struct RestartMount {
    pub key: RestartMountKey,
    pub generation: u64,
}

impl RestartMount {
    // The caller holds both the registered worker lock and the opened pool's
    // verified mount lock for the process lifetime.
    pub(super) fn open(
        owner: &DirectoryOwner,
        mount: Option<&MountedPoolObservation>,
        require_existing: bool,
    ) -> Result<Option<Self>, ProtocolError> {
        let Some(mount) =
            mount.filter(|mount| matches!(mount.expected.profile, PoolMountProfile::Docker { .. }))
        else {
            return Ok(None);
        };
        let path = owner.path.join("NABEDRA_RESTART_KEY");
        let mut seed = match bounded_read(&path, 32) {
            Ok(seed) => seed,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && !require_existing => {
                let mut seed = vec![0u8; 32];
                SystemRandom::new().fill(&mut seed).map_err(|_| invalid())?;
                let temporary = owner
                    .path
                    .join(format!("restart-key-{}.next", nabedra_rt::ids::uuid_v4()));
                let mut output = private_file(&temporary, true).map_err(storage)?;
                output.write_all(&seed).map_err(storage)?;
                output.sync_all().map_err(storage)?;
                fs::rename(temporary, &path).map_err(storage)?;
                fs::sync_dir(&owner.path).map_err(storage)?;
                seed
            }
            Err(error) => return Err(storage(error)),
        };
        #[cfg(unix)]
        {
            let metadata = fs::symlink_metadata(&path).map_err(storage)?;
            if !metadata.is_file() || metadata.nlink() != 1 || metadata.mode() & 0o077 != 0 {
                return Err(invalid());
            }
        }
        let key = RestartMountKey::from_seed(&seed);
        seed.fill(0);
        Ok(Some(Self {
            key: key?,
            generation: mount.expected.generation,
        }))
    }
}

pub(crate) struct OpenedRestart {
    pub intent: RestartIntent,
    owner: Arc<DirectoryOwner>,
    record: RetainedIdentity,
}

impl OpenedRestart {
    pub(super) fn new(
        config: &RegisteredConfig,
        owner: Arc<DirectoryOwner>,
        previous: Option<WorkerIdentity>,
        record: &RetainedIdentity,
    ) -> Result<Option<Self>, ProtocolError> {
        let Some(profile) = config.rolling else {
            return Ok(None);
        };
        let previous = previous.ok_or_else(invalid)?;
        let expected = config
            .control_enrollments
            .iter()
            .find(|enrollment| enrollment.worker_id == previous.worker_id)
            .ok_or_else(invalid)?;
        if WorkerIdentity::from(expected) != previous {
            return Err(invalid());
        }
        let intent = RestartIntent {
            id: profile.preparation_id,
            run_id: profile.run_id,
            previous,
            next: config.enrollment.clone(),
            pool_id: config.pool_id,
            mount_generation: config.mount.as_ref().ok_or_else(invalid)?.generation,
        };
        intent.validate()?;
        Ok(Some(Self {
            intent,
            owner,
            record: record.clone(),
        }))
    }

    pub(crate) fn retain(&self) -> Result<(), ProtocolError> {
        retain_identity(&self.owner, &self.record)
    }
}

impl RegisteredWorker {
    pub(crate) fn restart_exclusion(
        &self,
        operation: Uuid,
    ) -> Result<VerifiedExclusion, ProtocolError> {
        let rolling = self.rolling.as_ref().ok_or_else(invalid)?;
        let mount = self.restart_mount.as_ref().ok_or_else(invalid)?;
        if mount.generation != rolling.intent.mount_generation
            || self.pool.identity().pool_id != rolling.intent.pool_id
        {
            return Err(invalid());
        }
        self.operator.exclude_previous_before_open(
            &OperatorPolicy {
                policy: self.bootstrap_policy,
                certificate_pins: [self.enrollment.certificate_sha256].into_iter().collect(),
            },
            operation,
            rolling.intent.previous,
        )
    }
}
