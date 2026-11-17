//! Consensus-backed timestamp service. Every mutation below returns only after
//! its native state and applied metadata have committed in the same sync batch.
use super::*;
use crate::{
    oltp::pool::ByteClass,
    oltp_replication::authority::consensus::{Applied, Consensus},
};

#[derive(Clone)]
pub(crate) struct TimestampHandle {
    machine: TimestampMachine,
    consensus: Arc<dyn Consensus<TimestampCommand, TimestampReply>>,
    release_reader:Arc<std::sync::OnceLock<ReleaseReader>>,
}
#[derive(Clone)]
struct ReleaseReader {control:Arc<super::super::control::ControlClient>,queue:Arc<crate::oltp_runtime::worker_endpoint::QueuePeerClient>}
impl TimestampHandle {
    pub(crate) async fn current_authority(&self)->Result<Option<Authority>,ProtocolError>{
        self.consensus.ensure_linearizable().await?;
        let raw=self.machine.store().read_metadata(BUDGET,|view|{
            let state:Option<State>=view.get(STATE)?.map(|bytes|serde_json::from_slice(bytes.as_slice()).map_err(|_|unknown())).transpose()?;
            view.encode(&state.map(|state|state.authority))
        }).await?;
        serde_json::from_slice(raw.as_slice()).map_err(|_|unknown())
    }
    pub(crate) fn new(
        machine: TimestampMachine,
        consensus: Arc<dyn Consensus<TimestampCommand, TimestampReply>>,
    ) -> Arc<Self> {
        Arc::new(Self { machine, consensus,release_reader:Arc::new(std::sync::OnceLock::new()) })
    }
    pub(crate) fn identity(&self) -> &GroupIdentity {
        self.machine.store().identity()
    }
    pub(crate) fn install_release_reader(&self,control:Arc<super::super::control::ControlClient>,queue:Arc<crate::oltp_runtime::worker_endpoint::QueuePeerClient>)->Result<(),ProtocolError>{
        self.release_reader.set(ReleaseReader{control,queue}).map_err(|_|unknown())
    }
    pub(crate) async fn release(&self,statement:TerminalCopyStatement)->Result<VerifiedPinRelease,ProtocolError>{
        use crate::oltp_runtime::{replica::QueueScope,worker_endpoint::AuthorityReader};
        use super::super::{control::InventoryPage,proof::{AuthenticatedAck,TerminalVerifier}};
        self.consensus.ensure_linearizable().await?;
        let reader=self.release_reader.get().ok_or_else(unknown)?;
        let scope=QueueScope{cluster_id:self.identity().cluster_id,database:self.identity().database.clone()};
        let current=reader.control.current(&scope).await?;
        let mut verifier=TerminalVerifier::begin(statement.clone(),current.ready).map_err(|_|unknown())?;
        let shared=self.machine.store().resources().clone();let mut after=None;
        loop {
            let reply=reader.control.ready_page(statement.ready,after).await?;
            let bytes=reply.sections[0].as_ref().ok_or_else(unknown)?.clone();
            let required=shared.execute(crate::oltp::pool::ExecutionClass::Control,0,0,move||crate::oltp::mvcc::json_decode_budget(bytes.as_slice())).await?;
            let reply=reply.reserve_reply_decode(&shared,required).await?;
            let page=shared.execute(crate::oltp::pool::ExecutionClass::Control,0,0,move||reply.decode_section::<InventoryPage<WorkerIdentity>>(0)).await?;
            if page.revision!=statement.ready.view.membership_revision||(!page.done&&page.items.is_empty()){return Err(unknown())}
            for worker in page.items.iter().copied(){
                if after.is_some_and(|previous|worker.worker_id<=previous){return Err(unknown())}
                let observation=reader.queue.terminal_state(worker,scope.clone(),&statement,self.identity().group_id).await?;
                let (member,observed,nonce)=observation.into_parts();
                if observed!=statement{return Err(unknown())}
                let ack=AuthenticatedAck::from_transport(member,super::super::proof::statement_digest(&observed).map_err(|_|unknown())?,nonce);
                verifier.accept(worker,&ack).map_err(|_|unknown())?;after=Some(worker.worker_id);
            }
            if page.after!=after{return Err(unknown())}
            let done=page.done;drop(page);if done{break}
        }
        let current=reader.control.current(&scope).await?;
        let proof=verifier.finish(current.ready).map_err(|_|unknown())?.into_sealed();
        let confirmed=self.submit(TimestampCommand::Release{proof:proof.clone()}).await?;
        match confirmed.reply{TimestampReply::Released=>Ok(VerifiedPinRelease{proof,confirmed_at:confirmed.position}),reply=>Err(rejected(reply))}
    }
    async fn submit(
        &self,
        command: TimestampCommand,
    ) -> Result<Applied<TimestampReply>, ProtocolError> {
        let command = OwnedCommand::new_owned(
            command,
            self.machine.store().resources(),
            ByteClass::Progress,
        )?;
        self.consensus.propose(command).await
    }
    pub(crate) async fn begin_snapshot(
        &self,
        view: AuthorityView,
        identity: PinIdentity,
        reservation: IdentityReservation,
    ) -> Result<StoragePin, ProtocolError> {
        match self
            .submit(TimestampCommand::BeginSnapshot {
                view,
                identity,
                reservation,
                pin_id: uuid::Uuid::new_v4(),
            })
            .await?
            .reply
        {
            TimestampReply::Snapshot(pin) => Ok(pin),
            reply => Err(rejected(reply)),
        }
    }
    pub(crate) async fn bind_snapshot_context(
        &self,
        view: AuthorityView,
        identity: PinIdentity,
        pin: StoragePin,
        request: RequestBinding,
    ) -> Result<(), ProtocolError> {
        match self
            .submit(TimestampCommand::BindSnapshotContext {
                view,
                identity,
                pin,
                request,
            })
            .await?
            .reply
        {
            TimestampReply::ContextBound(bound) if bound == pin => Ok(()),
            reply => Err(rejected(reply)),
        }
    }
    pub(crate) async fn allocate_commit(
        &self,
        permit: SealedCommitPermit,
    ) -> Result<CommitStamp, ProtocolError> {
        permit.validate()?;
        match self
            .submit(TimestampCommand::AllocateCommit { permit })
            .await?
            .reply
        {
            TimestampReply::Commit(stamp) => Ok(stamp),
            reply => Err(rejected(reply)),
        }
    }
    pub(crate) async fn fence_ready(
        &self,
        transition_id: uuid::Uuid,
        expected: ReadyMembership,
        next: ReadyMembership,
    ) -> Result<ReadyFenceReceipt, ProtocolError> {
        match self
            .submit(TimestampCommand::FenceReady {
                transition_id,
                expected,
                next,
            })
            .await?
            .reply
        {
            TimestampReply::ReadyFenced(receipt)
                if receipt.transition_id == transition_id
                    && receipt.expected == expected
                    && receipt.next == next =>
            {
                Ok(receipt)
            }
            reply => Err(rejected(reply)),
        }
    }
    pub(crate) async fn fence(
        &self,
        transition_id: uuid::Uuid,
        expected: Option<Authority>,
        next: Authority,
        ready: ReadyMembership,
    ) -> Result<Applied<TimestampReply>, ProtocolError> {
        self.submit(TimestampCommand::Fence {
            transition_id,
            expected,
            next,
            ready,
        })
        .await
    }
}
fn rejected(reply: TimestampReply) -> ProtocolError {
    ProtocolError::new(
        ErrorCode::Conflict,
        format!("timestamp transition rejected: {reply:?}"),
        Outcome::NotApplied,
    )
}
