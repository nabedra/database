//! Owner: E. `Tail` over `ParticipantMachine::change_page` and `SnapshotOpen/Page/Close`
//! over a pinned view (`store/search_image.rs`), contract-w2 2.3.
//!
//! Skeleton (S): every method answers `UnsupportedOperation`. `plan` runs in
//! preflight after the role check and returns the receive plan (the default
//! is passed in); `handle` decodes section 0 with `SearchHeader::decode` and
//! answers a `SearchReplyFrame` in section 0, vectors in section 1.
use super::*;

pub(super) fn plan(
    _endpoint: &SearchRpcEndpoint,
    _session: &AuthenticatedSession,
    _prologue: &SearchPrologue,
    _default: ReceivePlan,
) -> Result<ReceivePlan, ProtocolError> {
    Err(unsupported())
}

pub(super) async fn handle(
    _endpoint: &SearchRpcEndpoint,
    _session: AuthenticatedSession,
    _prologue: SearchPrologue,
    _sections: ChargedSections,
) -> Result<ChargedReply, ProtocolError> {
    Err(unsupported())
}
