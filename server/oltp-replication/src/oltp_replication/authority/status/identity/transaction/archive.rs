//! Strong bounded history export. A TSO closure proves that no previously
//! allocated timestamp at/below the cut can acquire a later COMMIT decision.
//! UUID history has a separately recorded Status applied cut; it is never
//! described as an untimestamped MVCC snapshot at the application data cut.
use super::timestamp_point::projection::read_for_transaction;
use super::*;
use crate::oltp::backup::history::{ARCHIVE_PAGE_WIRE_BYTES, ArchivePageBudget};
use crate::oltp_replication::authority::timestamp::VerifiedArchiveClosure;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StatusArchiveCutStatement {
    pub group: GroupIdentity,
    pub authority: Authority,
    pub applied: AppliedPosition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_root: Option<Digest>,
}
pub(crate) struct VerifiedStatusArchiveCut {
    statement: StatusArchiveCutStatement,
    owner: Arc<ScratchLease>,
}
impl VerifiedStatusArchiveCut {
    pub(crate) fn statement(&self) -> &StatusArchiveCutStatement {
        &self.statement
    }
    pub(crate) fn owner(&self) -> Arc<ScratchLease> {
        self.owner.clone()
    }
    pub(crate) fn from_authenticated(
        context: crate::oltp_replication::service_rpc::AuthenticatedMetadataReply,
        statement: StatusArchiveCutStatement,
        authority: Authority,
    ) -> Result<Self, ProtocolError> {
        context.status(19, &statement)?;
        if context.group() != &statement.group || statement.authority != authority {
            return Err(unknown());
        }
        Ok(Self {
            statement,
            owner: context.owner(),
        })
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct ArchiveCursor {
    pub base_exclusive: Option<CommitTs>,
    pub archive_id: Uuid,
    pub data_cut: CommitTs,
    pub status_applied_at: AppliedPosition,
    pub request_id: Uuid,
    pub attempt: u64,
}
pub(crate) use nabedra_oltp_types::authority::{
    ArchivedDecision, ArchivedIdentity, ArchivedIdentityDisposition,
};
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct CommittedArchivePage {
    pub status_group: GroupIdentity,
    pub cut: CommitTs,
    pub base_exclusive: Option<CommitTs>,
    pub status_applied_at: AppliedPosition,
    pub after: Option<ArchiveCursor>,
    pub exhausted: bool,
    pub items: Page<ArchivedDecision>,
    #[serde(skip)]
    owner: Option<Arc<ScratchLease>>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct IdentityArchiveCursor {
    pub archive_id: Uuid,
    pub data_cut: CommitTs,
    pub status_applied_at: AppliedPosition,
    pub request_id: Uuid,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[doc(hidden)]
pub struct IdentityArchivePage {
    pub status_group: GroupIdentity,
    pub data_cut: CommitTs,
    pub status_applied_at: AppliedPosition,
    pub after: Option<IdentityArchiveCursor>,
    pub exhausted: bool,
    pub items: Page<ArchivedIdentity>,
    #[serde(skip)]
    owner: Option<Arc<ScratchLease>>,
}
impl CommittedArchivePage {
    pub(crate) fn retain_owner(&mut self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        let mut items = std::mem::replace(
            &mut self.items,
            Page::new(Vec::new()).map_err(|_| unknown())?,
        )
        .into_inner();
        for item in &mut items {
            item.retain_owner(owner.clone());
        }
        self.items = Page::new(items).map_err(|_| unknown())?;
        self.owner = Some(owner);
        Ok(())
    }
}
impl IdentityArchivePage {
    pub(crate) fn retain_owner(&mut self, owner: Arc<ScratchLease>) -> Result<(), ProtocolError> {
        let mut items = std::mem::replace(
            &mut self.items,
            Page::new(Vec::new()).map_err(|_| unknown())?,
        )
        .into_inner();
        for item in &mut items {
            item.retain_owner(owner.clone());
        }
        self.items = Page::new(items).map_err(|_| unknown())?;
        self.owner = Some(owner);
        Ok(())
    }
}
pub(super) const ARCHIVE_BUDGET: TransactionBudget = TransactionBudget {
    scratch_bytes: 2 << 20,
    batch_bytes: 0,
    output_bytes: ARCHIVE_PAGE_WIRE_BYTES,
};
impl StatusHandle {
    pub(crate) async fn archive_cut(
        &self,
        authority: Authority,
    ) -> Result<VerifiedStatusArchiveCut, ProtocolError> {
        self.consensus.ensure_linearizable().await?;
        let group = self.identity().clone();
        let raw = self
            .machine
            .store()
            .read_archive(ARCHIVE_BUDGET, move |view, applied| {
                if read::<Authority>(view, STATE)?.is_none_or(|current| current != authority) {
                    return Err(unknown());
                }
                view.encode(&StatusArchiveCutStatement {
                    group,
                    authority,
                    applied,
                    identity_count: None,
                    identity_root: None,
                })
            })
            .await?;
        let owner = Arc::new(
            self.resources()
                .reserve_scratch(ByteClass::Data, 16 << 10)?,
        );
        let mut statement: StatusArchiveCutStatement =
            serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())?;
        let (count, root) = super::archive_images::identity_inventory(self, &statement).await?;
        statement.identity_count = Some(count);
        statement.identity_root = Some(root);
        Ok(VerifiedStatusArchiveCut { statement, owner })
    }
    pub(crate) async fn committed_page(
        &self,
        closure: &VerifiedArchiveClosure,
        base_exclusive: Option<CommitTs>,
        after: Option<ArchiveCursor>,
    ) -> Result<CommittedArchivePage, ProtocolError> {
        if !closure.covers_status(self.identity()) || base_exclusive != closure.base_exclusive() {
            return Err(invalid());
        }
        self.consensus.ensure_linearizable().await?;
        let cut = closure.cut();
        let authority = closure.authority();
        let group = self.identity().clone();
        let applied = closure.status_applied_at();
        let archive_id = closure.archive_id();
        if after.is_some_and(|cursor| {
            cursor.archive_id != archive_id
                || cursor.base_exclusive != base_exclusive
                || cursor.data_cut != cut
                || cursor.status_applied_at != applied
        }) {
            return Err(invalid());
        }
        let shared = self.machine.store().resources().clone();
        let decode_bound = ARCHIVE_BUDGET
            .output_bytes
            .checked_mul(258)
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(unknown)?;
        let owner = Arc::new(
            shared
                .reserve_scratch_wait(ByteClass::Data, decode_bound, 0)
                .await?,
        );
        let raw = self
            .machine
            .store()
            .read_archive(ARCHIVE_BUDGET, move |view, current_applied| {
                if current_applied.index < applied.index {
                    return Err(unknown());
                }
                if read::<Authority>(view, STATE)?
                    .is_none_or(|current| current.owner_term < authority.owner_term)
                {
                    return Err(unknown());
                }
                let mut prefix = TXNS.to_vec();
                prefix.push(b't');
                let mut cursor = after
                    .map(|c| {
                        txn_key(
                            b't',
                            TxnId {
                                request_id: c.request_id,
                                attempt: c.attempt,
                            },
                            &[],
                        )
                    })
                    .unwrap_or_else(|| prefix.clone());
                if after.is_some() {
                    cursor.push(0);
                }
                let mut items = Vec::new();
                let mut budget = ArchivePageBudget::new();
                let mut next = after;
                let mut examined = 0;
                while examined < 64 {
                    let Some(record) = view.first_from(&prefix, &cursor)? else {
                        break;
                    };
                    let native_key = record.key();
                    if native_key.len() != prefix.len() + 24 || record.value().len() > 64 << 10 {
                        return Err(unknown());
                    }
                    let suffix = &native_key[prefix.len()..];
                    let txn = TxnId {
                        request_id: Uuid::from_slice(&suffix[..16]).map_err(|_| unknown())?,
                        attempt: u64::from_be_bytes(
                            suffix[16..].try_into().map_err(|_| unknown())?,
                        ),
                    };
                    let state: Transaction =
                        serde_json::from_slice(record.value()).map_err(|_| unknown())?;
                    if state.txn != txn {
                        return Err(unknown());
                    }
                    if let Some(statement) = state.decision {
                        if let TxnDecision::Commit { commit_ts } = statement.decision {
                            if commit_ts <= cut
                                && base_exclusive.is_none_or(|base| commit_ts > base)
                            {
                                if statement.decided_at.index > applied.index
                                    || state.settled_at.is_some_and(|at| at.index > applied.index)
                                {
                                    return Err(unknown());
                                }
                                if state.resolved != state.expected
                                    || state.manifest != Some(statement.manifest)
                                    || state.settled_at.is_none()
                                {
                                    return Err(unknown());
                                }
                                let identity: IdentityRecord =
                                    read(view, &key(IDENTITIES, txn.request_id))?
                                        .ok_or_else(unknown)?;
                                let Some(TerminalIdentity::Committed {
                                    decision, outcome, ..
                                }) = &identity.terminal
                                else {
                                    return Err(unknown());
                                };
                                if *decision != statement || *outcome != state.outcome {
                                    return Err(unknown());
                                }
                                let item = ArchivedDecision::new(
                                    identity.reservation,
                                    statement,
                                    state.outcome,
                                    state.read_ts,
                                    state.settled_at.ok_or_else(unknown)?,
                                );
                                if !budget.include(view, &item)? {
                                    break;
                                }
                                items.push(item);
                            }
                        }
                    }
                    next = Some(ArchiveCursor {
                        base_exclusive,
                        archive_id,
                        data_cut: cut,
                        status_applied_at: applied,
                        request_id: txn.request_id,
                        attempt: txn.attempt,
                    });
                    cursor = native_key.to_vec();
                    cursor.push(0);
                    examined += 1;
                }
                let exhausted = view.first_from(&prefix, &cursor)?.is_none();
                view.encode(&CommittedArchivePage {
                    status_group: group,
                    cut,
                    base_exclusive,
                    status_applied_at: applied,
                    after: next,
                    exhausted,
                    items: Page::new(items).map_err(|_| unknown())?,
                    owner: None,
                })
            })
            .await?;
        shared
            .execute(ExecutionClass::Query, 0, 0, move || {
                owner.shrink_unused(crate::oltp::mvcc::json_decode_budget(raw.as_slice())?)?;
                let mut page: CommittedArchivePage =
                    serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())?;
                let mut items = page.items.into_inner();
                for item in &mut items {
                    item.retain_owner(owner.clone());
                }
                page.items = Page::new(items).map_err(|_| unknown())?;
                page.owner = Some(owner);
                Ok(page)
            })
            .await
    }
    pub(crate) async fn identity_history_page(
        &self,
        closure: &VerifiedArchiveClosure,
        after: Option<IdentityArchiveCursor>,
    ) -> Result<IdentityArchivePage, ProtocolError> {
        if !closure.covers_status(self.identity()) {
            return Err(invalid());
        }
        self.consensus.ensure_linearizable().await?;
        let cut = closure.cut();
        let authority = closure.authority();
        let group = self.identity().clone();
        let applied = closure.status_applied_at();
        let archive_id = closure.archive_id();
        if after.is_some_and(|cursor| {
            cursor.archive_id != archive_id
                || cursor.data_cut != cut
                || cursor.status_applied_at != applied
        }) {
            return Err(invalid());
        }
        let shared = self.machine.store().resources().clone();
        let decode_bound = ARCHIVE_BUDGET
            .output_bytes
            .checked_mul(258)
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(unknown)?;
        let owner = Arc::new(
            shared
                .reserve_scratch_wait(ByteClass::Data, decode_bound, 0)
                .await?,
        );
        let raw = self
            .machine
            .store()
            .read_archive(ARCHIVE_BUDGET, move |view, current_applied| {
                if current_applied.index < applied.index {
                    return Err(unknown());
                }
                if read::<Authority>(view, STATE)?.is_none_or(|current| current.owner_term < authority.owner_term) {
                    return Err(unknown());
                }
                let mut cursor = after
                    .map(|cursor| key(IDENTITIES, cursor.request_id))
                    .unwrap_or_else(|| IDENTITIES.to_vec());
                if after.is_some() {
                    cursor.push(0);
                }
                let mut items = Vec::new();
                let mut budget = ArchivePageBudget::new();
                let mut next = after;
                for _ in 0..64 {
                    let Some(row) = view.first_from(IDENTITIES, &cursor)? else {
                        break;
                    };
                    if row.key().len() != IDENTITIES.len() + 16 || row.value().len() > 64 << 10 {
                        return Err(unknown());
                    }
                    let identity: IdentityRecord =
                        serde_json::from_slice(row.value()).map_err(|_| unknown())?;
                    if row.key() != key(IDENTITIES, identity.reservation.request_id) {
                        return Err(unknown());
                    }
                    if identity.restore_origin.is_none() && identity
                        .reserved_at
                        .is_some_and(|position| position.index > applied.index)
                    {
                        next = Some(IdentityArchiveCursor {
                            archive_id,
                            data_cut: cut,
                            status_applied_at: applied,
                            request_id: identity.reservation.request_id,
                        });
                        cursor = row.key().to_vec();
                        cursor.push(0);
                        continue;
                    }
                    if let Some(TerminalIdentity::Committed{decision,..})=&identity.terminal {
                        if matches!(decision.decision,TxnDecision::Commit{commit_ts} if commit_ts<=cut) && identity.restore_origin.is_none() && decision.decided_at.index>applied.index{return Err(unknown());}
                    }
                    let identity = super::archive_images::project_identity(identity, applied)
                        .ok_or_else(unknown)?;
                    let disposition = identity_disposition(
                        identity.reservation,
                        identity.terminal,
                        identity.rejection,
                        cut,
                    );
                    let point_projection = if identity.restore_origin.is_none() {
                        match &disposition {
                            ArchivedIdentityDisposition::PreCutCommitted { decision, .. }
                            | ArchivedIdentityDisposition::CurrentFailed {
                                terminal: TerminalIdentity::Committed { decision, .. },
                            } => read_for_transaction(
                                view,
                                &group,
                                decision.txn,
                                Some(current_applied),
                            )?,
                            _ => None,
                        }
                    } else {
                        None
                    };
                    let item = ArchivedIdentity::new(
                        identity.restore_origin,
                        identity.reservation,
                        identity.reserved_at,
                        disposition,
                        point_projection.map(Box::new),
                    );
                    if !budget.include(view, &item)? {
                        break;
                    }
                    next = Some(IdentityArchiveCursor {
                        archive_id,
                        data_cut: cut,
                        status_applied_at: applied,
                        request_id: item.reservation.request_id,
                    });
                    items.push(item);
                    cursor = row.key().to_vec();
                    cursor.push(0);
                }
                let exhausted = view.first_from(IDENTITIES, &cursor)?.is_none();
                view.encode(&IdentityArchivePage {
                    status_group: group,
                    data_cut: cut,
                    status_applied_at: applied,
                    after: next,
                    exhausted,
                    items: Page::new(items).map_err(|_| unknown())?,
                    owner: None,
                })
            })
            .await?;
        shared
            .execute(ExecutionClass::Query, 0, 0, move || {
                owner.shrink_unused(crate::oltp::mvcc::json_decode_budget(raw.as_slice())?)?;
                let mut page: IdentityArchivePage =
                    serde_json::from_slice(raw.as_slice()).map_err(|_| unknown())?;
                let mut items = page.items.into_inner();
                for item in &mut items {
                    item.retain_owner(owner.clone());
                }
                page.items = Page::new(items).map_err(|_| unknown())?;
                page.owner = Some(owner);
                Ok(page)
            })
            .await
    }
}
pub(super) fn identity_disposition(
    reservation: IdentityReservation,
    terminal: Option<TerminalIdentity>,
    rejection: Option<RejectedIdentity>,
    cut: CommitTs,
) -> ArchivedIdentityDisposition {
    match terminal {
        Some(TerminalIdentity::Committed {
            decision, outcome, ..
        }) => match decision.decision {
            TxnDecision::Commit { commit_ts } if commit_ts <= cut => {
                ArchivedIdentityDisposition::PreCutCommitted { decision, outcome }
            }
            TxnDecision::Commit { .. } => ArchivedIdentityDisposition::RestoreExcluded {
                source_decision: decision,
                restore_cut: cut,
            },
            TxnDecision::Abort { .. } => ArchivedIdentityDisposition::CurrentFailed {
                terminal: TerminalIdentity::Committed {
                    reservation,
                    decision,
                    outcome,
                },
            },
        },
        Some(terminal @ TerminalIdentity::Failed { .. }) => {
            ArchivedIdentityDisposition::CurrentFailed { terminal }
        }
        Some(terminal @ TerminalIdentity::ReadExpired { .. }) => {
            ArchivedIdentityDisposition::ExpiredRead { terminal }
        }
        None => rejection.map_or(ArchivedIdentityDisposition::Reserved, |rejection| {
            ArchivedIdentityDisposition::CurrentRejected { rejection }
        }),
    }
}

macro_rules! debug_fields {
    ($name:ident,$($field:ident),+)=>{impl std::fmt::Debug for $name {fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result{f.debug_struct(stringify!($name))$(.field(stringify!($field),&self.$field))+.finish()}}};
}
debug_fields!(
    CommittedArchivePage,
    status_group,
    cut,
    base_exclusive,
    status_applied_at,
    after,
    exhausted,
    items
);
debug_fields!(
    IdentityArchivePage,
    status_group,
    data_cut,
    status_applied_at,
    after,
    exhausted,
    items
);
