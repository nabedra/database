use super::*;
use crate::{oltp::pool::GroupRead, protocol::Outcome};
use nabedra_resources::memory::Decoded;

#[derive(Clone, Copy)]
pub(crate) enum PointEvidenceRead {
    Seal(ScopedCatalogRead),
    Rejection(ScopedCatalogRead),
    Completion,
}

impl ParticipantHandle {
    pub(crate) async fn read_timestamp_point<T: Send + 'static>(
        &self,
        input: Decoded<T>,
        locator_of: fn(&T) -> &TimestampDecisionLocator,
        authority: Authority,
        kind: PointEvidenceRead,
    ) -> Result<ChargedBytes, ProtocolError> {
        let locator = locator_of(&input);
        locator.validate()?;
        let group = self.identity().clone();
        if self.remote.is_some() || locator.binding.participant.group != group {
            return Err(invalid());
        }
        let barrier = self
            .local_consensus()
            .ensure_linearizable()
            .await
            .map_err(point_read_error)?;
        self.local_machine()
            .store
            .read_metadata(META, move |view| {
                let locator = locator_of(&input);
                let config: Config = timestamp_resolution::read_record(view, CONFIG)?;
                if config.authority != authority {
                    return Err(recovery_unknown());
                }
                let scope = match kind {
                    PointEvidenceRead::Seal(scope) | PointEvidenceRead::Rejection(scope) => scope,
                    PointEvidenceRead::Completion => {
                        return timestamp_resolution::read_completion(
                            view,
                            locator,
                            barrier.applied,
                        );
                    }
                };
                if !timestamp_resolution::eligible_config(&config, locator)
                    || !timestamp_resolution::matches_scope(locator, scope)
                    || !scoped_queue::read_current(view, scope, &group, authority, locator)?
                {
                    return Err(recovery_unknown());
                }
                match kind {
                    PointEvidenceRead::Seal(_) => {
                        read_seal(view, locator, authority, barrier.applied)
                    }
                    PointEvidenceRead::Rejection(_) => timestamp_resolution::read_rejection(
                        view,
                        locator,
                        authority,
                        barrier.applied,
                    ),
                    PointEvidenceRead::Completion => unreachable!(),
                }
            })
            .await
            .map_err(point_read_error)
    }
}

fn read_seal(
    view: &mut GroupRead<'_>,
    locator: &TimestampDecisionLocator,
    authority: Authority,
    applied: AppliedPosition,
) -> Result<ChargedBytes, ProtocolError> {
    let txn = locator.binding.txn;
    let state: Txn = timestamp_resolution::read_record(view, &txn_key(b't', txn))?;
    let seal = timestamp_resolution::check_seal(locator, &state, authority)?;
    let page_root = view.pinned(&ordinal_key(b'p', txn, 0))?;
    if page_root.as_ref().map(|root| root.as_ref()) != Some(state.root.as_bytes())
        || state.decision.is_some()
        || state.resolved != 0
        || !timestamp_resolution::at_or_before(seal.sealed_at, applied)
        || view.pinned(&txn_key(b'b', txn))?.is_some()
        || view.pinned(&txn_key(b'd', txn))?.is_some()
    {
        return Err(recovery_unknown());
    }
    view.encode(seal)
}

fn point_read_error(mut error: ProtocolError) -> ProtocolError {
    error.outcome = Outcome::Unknown;
    error
}
