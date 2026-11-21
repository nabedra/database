use super::*;
use crate::oltp_replication::authority::control::VerifiedExclusion;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ColdStart {
    pub previous_run: Uuid,
    pub next_run: Uuid,
    pub transition_id: Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RetainedCold {
    profile: ColdStart,
    previous: WorkerIdentity,
}

pub(crate) struct OpenedCold {
    pub profile: ColdStart,
    pub previous: WorkerIdentity,
    owner: Arc<DirectoryOwner>,
    record: RetainedIdentity,
}

pub(super) fn validate(config: &RegisteredConfig) -> Result<(), ProtocolError> {
    let Some(cold) = config.cold else {
        return Ok(());
    };
    if cold.previous_run.is_nil()
        || cold.next_run.is_nil()
        || cold.transition_id.is_nil()
        || cold.previous_run == cold.next_run
        || cold.next_run == config.initial_run
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

impl OpenedCold {
    pub(super) fn new(
        config: &RegisteredConfig,
        owner: Arc<DirectoryOwner>,
        previous: Option<WorkerIdentity>,
        record: &RetainedIdentity,
    ) -> Result<Option<Self>, ProtocolError> {
        let Some(profile) = config.cold else {
            if previous.is_some() && config.rolling.is_none() && config.system_replacement.is_none() {
                return Err(invalid());
            }
            return Ok(None);
        };
        let last_opened = previous.ok_or_else(invalid)?;
        let previous = match &record.cold {
            Some(cold) if cold.profile == profile => cold.previous,
            Some(cold) if cold.profile.next_run != profile.previous_run => return Err(invalid()),
            _ => last_opened,
        };
        let mut record = record.clone();
        record.cold = Some(RetainedCold { profile, previous });
        Ok(Some(Self {
            profile,
            previous,
            owner,
            record,
        }))
    }

    // Write before enrollment. If that command is interrupted, the original
    // queue holder and the most recent opened incarnation both remain known.
    pub(crate) fn retain(&self) -> Result<(), ProtocolError> {
        retain_identity(&self.owner, &self.record)
    }
}

impl RegisteredWorker {
    pub(crate) fn cold_exclusion(
        &self,
        operation_id: Uuid,
        previous: WorkerIdentity,
    ) -> Result<VerifiedExclusion, ProtocolError> {
        let cold = self.cold.as_ref().ok_or_else(invalid)?;
        if previous != cold.previous && Some(previous) != self.previous_process {
            return Err(invalid());
        }
        self.operator.exclude_previous_before_open(
            &OperatorPolicy {
                policy: self.bootstrap_policy,
                certificate_pins: [self.enrollment.certificate_sha256].into_iter().collect(),
            },
            operation_id,
            previous,
        )
    }
}
