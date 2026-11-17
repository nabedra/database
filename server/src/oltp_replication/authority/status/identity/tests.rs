use super::*;
use crate::{config::WorkerResources,oltp::pool::{OltpPool,PoolIdentity,SharedResourceConfig,WorkerSharedResources}};

struct Dir(std::path::PathBuf);
impl Drop for Dir{fn drop(&mut self){let _=std::fs::remove_dir_all(&self.0);}}
struct Fixture{machine:StatusMachine,pool:OltpPool,dir:Dir,index:u64}
impl Fixture {
    async fn new()->Self {
        let dir=Dir(std::env::temp_dir().join(format!("status-identity-{}",Uuid::new_v4())));
        let shared=WorkerSharedResources::new(3,WorkerResources{max_threads:4,..Default::default()},SharedResourceConfig::default()).unwrap();
        let pool=OltpPool::open_development(&dir.0,PoolIdentity{engine:"oltp".into(),pool_format_version:1,cluster_id:Uuid::new_v4(),worker_id:Uuid::new_v4(),pool_id:Uuid::new_v4()},shared).unwrap();
        let group=pool.group(GroupIdentity{cluster_id:pool.identity().cluster_id,database:Text::try_from("default").unwrap(),group_id:Uuid::new_v4(),kind:GroupKind::Status{bucket:0},group_format_version:1}).await.unwrap();
        Self{machine:StatusMachine::new(group).unwrap(),pool,dir,index:0}
    }
    async fn apply(&mut self,command:StatusCommand)->StatusReply {
        let command=OwnedCommand::new_owned(command,self.pool.resources(),ByteClass::Progress).unwrap();
        let meta=ApplyMeta{position:AppliedPosition{term:1,node_id:1,index:self.index},command_digest:serialized_digest(&command).unwrap(),membership:None};self.index+=1;
        let reply=self.machine.apply(meta,command).await.unwrap();serde_json::from_slice(reply.as_slice()).unwrap()
    }
    async fn reopen(self)->Self {
        let Self{machine,pool,dir,index}=self;
        let identity=pool.identity().clone();let group=machine.store().identity().clone();let shared=pool.resources().clone();
        drop(machine);drop(pool);
        let pool=OltpPool::open_development(&dir.0,identity,shared).unwrap();
        let machine=StatusMachine::new(pool.group(group).await.unwrap()).unwrap();
        Self{machine,pool,dir,index}
    }
}
fn reserve(authority:Authority,id:Uuid,digest:Digest)->StatusCommand {StatusCommand::ReserveIdentity{authority,request_id:id,digest,kind:RequestKind::Mutation,reservation_id:Uuid::new_v4()}}

/// R04/R05/O10: permanent identity metadata prevents an unchanged delayed POST
/// from acquiring a new run after all RAM is lost. No executable body is logged.
#[tokio::test]
async fn r04_r05_o10_reserved_uuid_survives_reopen_and_never_relabels_into_a_new_run() {
    let mut f=Fixture::new().await;let authority=Authority{run_id:Uuid::new_v4(),owner_term:1};
    f.apply(StatusCommand::Fence{transition_id:Uuid::new_v4(),expected:None,next:authority}).await;
    let request=Uuid::new_v4();let digest=Digest::from_bytes([3;32]);
    let first=f.apply(reserve(authority,request,digest)).await;
    let StatusReply::Identity(IdentityLookup::Reserved(reservation))=first else{panic!("reservation")};
    assert_eq!(reservation.original_run,authority.run_id);
    assert_eq!(f.apply(reserve(authority,request,digest)).await,StatusReply::Identity(IdentityLookup::Reserved(reservation)));
    let mut f=f.reopen().await;
    let owner=Authority{owner_term:2,..authority};
    f.apply(StatusCommand::Fence{transition_id:Uuid::new_v4(),expected:Some(authority),next:owner}).await;
    assert_eq!(f.apply(reserve(owner,request,digest)).await,StatusReply::Identity(IdentityLookup::Reserved(reservation)));
    let cold=Authority{run_id:Uuid::new_v4(),owner_term:3};
    f.apply(StatusCommand::Fence{transition_id:Uuid::new_v4(),expected:Some(owner),next:cold}).await;
    assert_eq!(f.apply(reserve(cold,request,digest)).await,StatusReply::Identity(IdentityLookup::Abandoned(reservation)));
    assert_eq!(f.apply(reserve(owner,Uuid::new_v4(),digest)).await,StatusReply::Rejected(StatusFault::StaleAuthority));
    assert_eq!(f.apply(reserve(cold,request,Digest::from_bytes([4;32]))).await,StatusReply::Rejected(StatusFault::DigestConflict));
    let encoded=serde_json::to_string(&reserve(cold,request,digest)).unwrap();
    for forbidden in ["operation","payload","query","authorization","queue_order"] {assert!(!encoded.contains(forbidden),"{forbidden}")}
}

/// O10/O29: an unreadable permanent identity is Unknown, never treated as absent
/// so the same UUID cannot be admitted as a fresh operation.
#[tokio::test]
async fn o10_o29_corrupt_identity_cannot_be_recomputed_as_absent() {
    let mut f=Fixture::new().await;let authority=Authority{run_id:Uuid::new_v4(),owner_term:1};
    f.apply(StatusCommand::Fence{transition_id:Uuid::new_v4(),expected:None,next:authority}).await;
    let request=Uuid::new_v4();let digest=Digest::from_bytes([5;32]);f.apply(reserve(authority,request,digest)).await;
    let index=f.index;f.index+=1;
    f.machine.store().transaction(ApplyMeta{position:AppliedPosition{term:1,node_id:1,index},command_digest:Digest::from_bytes([6;32]),membership:None},BUDGET,move|tx|{
        tx.put(&key(IDENTITIES,request),b"invalid identity bytes")?;Ok(Transition::Commit(tx.copy_output(b"corrupted model")?))
    }).await.unwrap();
    let command=OwnedCommand::new_owned(reserve(authority,request,digest),f.pool.resources(),ByteClass::Progress).unwrap();
    let result=f.machine.apply(ApplyMeta{position:AppliedPosition{term:1,node_id:1,index:f.index},command_digest:serialized_digest(&command).unwrap(),membership:None},command).await.unwrap_err();
    assert_eq!(result.outcome,Outcome::Unknown);
}
