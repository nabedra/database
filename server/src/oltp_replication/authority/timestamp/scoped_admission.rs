use super::*;
use nabedra_oltp_types::queue::OriginalAuthorization;
use uuid::Uuid;

const ORIGIN_PREFIX: &[u8] = b"ts/scoped-origin/";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ScopedAdmissionFailure {
    ScopeSuperseded,
}

/// Immutable evidence that Timestamp closed an unbound application pin.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScopedAdmissionClosure {
    pub identity: PinIdentity,
    pub reservation: IdentityReservation,
    pub pin: StoragePin,
    pub failure: ScopedAdmissionFailure,
    pub applied_at: AppliedPosition,
}

impl ScopedAdmissionClosure {
    fn from_record(record: &PinRecord, applied_at: AppliedPosition) -> Self {
        Self {
            identity: record.identity.clone(),
            reservation: record.reservation,
            pin: record.pin,
            failure: ScopedAdmissionFailure::ScopeSuperseded,
            applied_at,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Command {
    Begin {
        epoch: ScopedTimestampEpoch,
        view: AuthorityView,
        identity: PinIdentity,
        reservation: IdentityReservation,
        pin_id: Uuid,
        origin: OriginalAuthorization,
    },
    Bind {
        epoch: ScopedTimestampEpoch,
        view: AuthorityView,
        identity: PinIdentity,
        pin: StoragePin,
        request: RequestBinding,
    },
    CloseSuperseded {
        epoch: ScopedTimestampEpoch,
        view: AuthorityView,
        identity: PinIdentity,
        reservation: IdentityReservation,
        pin: StoragePin,
    },
}

impl Command {
    pub(super) fn name(&self) -> &'static str {
        match self {
            Self::Begin { .. } => "timestamp.begin_scoped_snapshot",
            Self::Bind { .. } => "timestamp.bind_scoped_snapshot_context",
            Self::CloseSuperseded { .. } => "timestamp.close_superseded_scoped_admission",
        }
    }

    pub(super) fn heap_bytes(&self) -> usize {
        match self {
            Self::Begin { origin, .. } => origin.database.capacity_bytes(),
            Self::Bind { .. } | Self::CloseSuperseded { .. } => 0,
        }
    }
}

pub(super) fn apply(
    tx: &mut GroupTransaction<'_>,
    scope: &GroupIdentity,
    state: Option<State>,
    command: &Command,
    position: AppliedPosition,
) -> Result<Transition, ProtocolError> {
    let (epoch, can_seed) = match command {
        Command::Begin { epoch, .. } => (*epoch, true),
        Command::Bind { epoch, .. } | Command::CloseSuperseded { epoch, .. } => (*epoch, false),
    };
    if !epoch.valid() {
        return reject(tx, Fault::InvalidBinding);
    }
    let epoch_key = epoch.key();
    let stored = load::<ScopedTimestampEpoch>(tx, &epoch_key)?;
    let seed = can_seed && stored.is_none();
    if !seed && stored != Some(epoch) {
        return reject(tx, Fault::StaleReadyView);
    }
    match command {
        Command::Begin {
            view,
            identity,
            reservation,
            pin_id,
            origin,
            ..
        } => {
            if identity.run_id != epoch.view.run_id
                || !epoch.admits(reservation.scoped)
                || origin.database != scope.database
                || origin.policy_revision == 0
                || origin.ingress_id.is_nil()
                || origin.ingress_incarnation.is_nil()
            {
                return reject(tx, Fault::InvalidBinding);
            }
            // A rejection discards the seed along with any other staged writes.
            if seed {
                put(tx, &epoch_key, &epoch)?;
            }
            begin(
                tx,
                state,
                *view,
                identity,
                reservation,
                *pin_id,
                Some(origin),
            )
        }
        Command::Bind {
            view,
            identity,
            pin,
            request,
            ..
        } => {
            if identity.run_id != epoch.view.run_id {
                return reject(tx, Fault::InvalidBinding);
            }
            bind(tx, state, *view, identity, *pin, *request, Some(epoch))
        }
        Command::CloseSuperseded {
            view,
            identity,
            reservation,
            pin,
            ..
        } => {
            let Some(state) = state else {
                return reject(tx, Fault::NotInitialized);
            };
            if state.ready.view != *view || state.authority != view.authority() {
                return reject(tx, Fault::StaleReadyView);
            }
            if identity.run_id != view.run_id
                || identity.run_id != epoch.view.run_id
                || !epoch.admits(reservation.scoped)
            {
                return reject(tx, Fault::InvalidBinding);
            }
            close_superseded(tx, identity, reservation, *pin, position)
        }
    }
}

fn close_superseded(
    tx: &mut GroupTransaction<'_>,
    identity: &PinIdentity,
    reservation: &IdentityReservation,
    pin: StoragePin,
    position: AppliedPosition,
) -> Result<Transition, ProtocolError> {
    let key = pin_key(identity.nonce);
    let Some(mut record) = load::<PinRecord>(tx, &key)? else {
        return reject(tx, Fault::UnknownPin);
    };
    if record.identity != *identity
        || record.reservation != *reservation
        || record.pin != pin
        || record.purpose != PinPurpose::Application
        || record.request.is_some()
    {
        return reject(tx, Fault::InvalidBinding);
    }
    let active = active_key(pin);
    let applied_at = match record.state {
        PinState::Active => {
            if !tx
                .get(&active)?
                .is_some_and(|nonce| nonce.as_slice() == identity.nonce.as_bytes())
            {
                return Err(unknown());
            }
            record.state = PinState::ScopeSuperseded {
                applied_at: position,
            };
            put(tx, &key, &record)?;
            tx.delete(&active)?;
            position
        }
        PinState::ScopeSuperseded { applied_at } => {
            if tx.get(&active)?.is_some() {
                return Err(unknown());
            }
            applied_at
        }
        _ => return reject(tx, Fault::PinExpired),
    };
    commit(
        tx,
        &TimestampReply::ScopedAdmissionClosed(ScopedAdmissionClosure::from_record(
            &record, applied_at,
        )),
    )
}

impl TimestampHandle {
    pub(crate) async fn close_superseded_scoped_admission(
        &self,
        view: AuthorityView,
        identity: PinIdentity,
        reservation: IdentityReservation,
        pin: StoragePin,
        epoch: ScopedTimestampEpoch,
    ) -> Result<ScopedAdmissionClosure, ProtocolError> {
        match self
            .submit(TimestampCommand::ScopedSnapshot(Command::CloseSuperseded {
                epoch,
                view,
                identity,
                reservation,
                pin,
            }))
            .await?
            .reply
        {
            TimestampReply::ScopedAdmissionClosed(closure) => Ok(closure),
            reply => Err(super::handle::rejected(reply)),
        }
    }

    pub(crate) async fn scoped_admission_closure(
        &self,
        identity: PinIdentity,
        reservation: IdentityReservation,
    ) -> Result<ChargedBytes, ProtocolError> {
        self.consensus.ensure_linearizable().await?;
        self.machine
            .store()
            .read_metadata(BUDGET, move |view| {
                let Some(raw) = view.get(&pin_key(identity.nonce))? else {
                    return view.encode(&None::<ScopedAdmissionClosure>);
                };
                let record: PinRecord =
                    serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())?;
                if record.identity != identity
                    || record.reservation != reservation
                    || record.purpose != PinPurpose::Application
                {
                    return Err(invalid());
                }
                let PinState::ScopeSuperseded { applied_at } = record.state else {
                    return view.encode(&None::<ScopedAdmissionClosure>);
                };
                if record.request.is_some()
                    || view.get(&active_key(record.pin))?.is_some()
                    || applied_at.term == 0
                    || applied_at.index == 0
                {
                    return Err(unknown());
                }
                view.encode(&Some(ScopedAdmissionClosure::from_record(
                    &record, applied_at,
                )))
            })
            .await
    }
}

pub(super) fn begin(
    tx: &mut GroupTransaction<'_>,
    state: Option<State>,
    view: AuthorityView,
    identity: &PinIdentity,
    reservation: &IdentityReservation,
    pin_id: Uuid,
    origin: Option<&OriginalAuthorization>,
) -> Result<Transition, ProtocolError> {
    let Some(mut state) = state else {
        return reject(tx, Fault::NotInitialized);
    };
    if state.ready.view != view {
        return reject(tx, Fault::StaleReadyView);
    }
    if state.ready.ready_count == 0
        || pin_id.is_nil()
        || identity.nonce.is_nil()
        || reservation.request_id.is_nil()
        || reservation.reservation_id.is_nil()
        || identity.nonce != reservation.reservation_id
        || identity.run_id != state.authority.run_id
        || identity.run_id != reservation.original_run
        || identity.request_id != reservation.request_id
        || identity.digest != reservation.digest
    {
        return reject(tx, Fault::InvalidBinding);
    }
    let key = pin_key(identity.nonce);
    if tx.get(&rejected_key(identity.nonce))?.is_some() {
        return reject(tx, Fault::PinExpired);
    }
    let (pin, bound) = if let Some(existing) = load::<PinRecord>(tx, &key)? {
        if existing.identity != *identity
            || existing.reservation != *reservation
            || existing.purpose != PinPurpose::Application
        {
            return reject(tx, Fault::InvalidBinding);
        }
        if !matches!(existing.state, PinState::Active) {
            return reject(tx, Fault::PinExpired);
        }
        (existing.pin, existing.request.is_some())
    } else {
        let Some(next) = state.high_watermark.checked_add(1) else {
            return reject(tx, Fault::TimestampExhausted);
        };
        let pin = StoragePin {
            pin_id,
            read_ts: Timestamp::new(next).map_err(|_| unknown())?,
            allocator_epoch: state.allocator_epoch,
        };
        let record = PinRecord {
            identity: identity.clone(),
            reservation: *reservation,
            request: None,
            pin,
            state: PinState::Active,
            purpose: PinPurpose::Application,
            scoped_copy_proof: None,
            retry_pins: None,
        };
        put(tx, &key, &record)?;
        tx.put(&active_key(pin), identity.nonce.as_bytes())?;
        state.high_watermark = next;
        put_state(tx, &state)?;
        (pin, false)
    };
    let Some(proposed) = origin else {
        return commit(tx, &TimestampReply::Snapshot(pin));
    };
    let key = origin_key(identity.nonce);
    let origin = match load::<OriginalAuthorization>(tx, &key)? {
        Some(origin) => origin,
        // A legacy bound digest cannot attest to a proposed authorization.
        None if bound => return reject(tx, Fault::InvalidBinding),
        None => {
            put(tx, &key, proposed)?;
            proposed.clone()
        }
    };
    commit(tx, &TimestampReply::ScopedSnapshot { pin, origin })
}

pub(super) fn bind(
    tx: &mut GroupTransaction<'_>,
    state: Option<State>,
    view: AuthorityView,
    identity: &PinIdentity,
    pin: StoragePin,
    request: RequestBinding,
    epoch: Option<ScopedTimestampEpoch>,
) -> Result<Transition, ProtocolError> {
    let Some(state) = state else {
        return reject(tx, Fault::NotInitialized);
    };
    if state.ready.view != view {
        return reject(tx, Fault::StaleReadyView);
    }
    if request.run_id != state.authority.run_id
        || request.run_id != identity.run_id
        || request.request_id != identity.request_id
        || request.digest != identity.digest
    {
        return reject(tx, Fault::InvalidBinding);
    }
    let key = pin_key(identity.nonce);
    let Some(mut record) = load::<PinRecord>(tx, &key)? else {
        return reject(tx, Fault::UnknownPin);
    };
    if record.identity != *identity || record.pin != pin {
        return reject(tx, Fault::InvalidBinding);
    }
    if let Some(epoch) = epoch {
        if !epoch.admits(record.reservation.scoped)
            || record.purpose != PinPurpose::Application
            || tx.get(&origin_key(identity.nonce))?.is_none()
        {
            return reject(tx, Fault::InvalidBinding);
        }
    } else if record.reservation.scoped.is_some() && tx.get(&origin_key(identity.nonce))?.is_some()
    {
        // Legacy commands can replay old pins, but cannot bind pins with an origin.
        return reject(tx, Fault::InvalidBinding);
    }
    if !matches!(record.state, PinState::Active) {
        return reject(tx, Fault::PinExpired);
    }
    if let Some(existing) = record.request {
        return if existing == request {
            commit(tx, &TimestampReply::ContextBound(pin))
        } else {
            reject(tx, Fault::InvalidBinding)
        };
    }
    record.request = Some(request);
    put(tx, &key, &record)?;
    commit(tx, &TimestampReply::ContextBound(pin))
}

fn origin_key(nonce: Uuid) -> [u8; ORIGIN_PREFIX.len() + 16] {
    let mut key = [0; ORIGIN_PREFIX.len() + 16];
    key[..ORIGIN_PREFIX.len()].copy_from_slice(ORIGIN_PREFIX);
    key[ORIGIN_PREFIX.len()..].copy_from_slice(nonce.as_bytes());
    key
}
