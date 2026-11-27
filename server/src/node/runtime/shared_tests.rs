//! SL01/SL02 local conditional-object evidence through the real worker queue.
use super::*;
use crate::{
    node::queue_runtime::{Command as QueueCommand, Reply as QueueReply},
    request_queue::{QueueLimits, RequestKey, RequestQueue},
    storage::{
        CatalogFence, ObjectKey, WriteCondition, context::RequestContext,
        registry::DatabaseRegistry,
    },
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, io::BufRead};
struct StoreProcess {
    child: std::process::Child,
    path: PathBuf,
    url: String,
}
impl StoreProcess {
    fn start() -> Self {
        let path =
            std::env::temp_dir().join(format!("nabedra-shared-{}", nabedra_rt::ids::uuid_v4()));
        std::fs::create_dir(&path).unwrap();
        let mut child = std::process::Command::new("python3.11")
            .args([
                "-B",
                "../scripts/local_store.py",
                "--port",
                "0",
                "--database",
            ])
            .arg(path.join("objects.sqlite"))
            .env("NABEDRA_STORE_TOKEN", "test-secret")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let mut line = String::new();
        std::io::BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let v: Value = serde_json::from_str(&line).unwrap();
        Self {
            child,
            path,
            url: format!("http://127.0.0.1:{}", v["port"]),
        }
    }
    fn adapter(&self, prefix: &str) -> crate::storage::HttpStore {
        crate::storage::HttpStore::new(
            &self.url,
            Namespace::new(prefix).unwrap(),
            "test-secret",
            None,
        )
        .unwrap()
    }
    fn write(&self, name: &str, bytes: impl AsRef<[u8]>) {
        let p = self.path.join(name);
        std::fs::write(&p, bytes).unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
}
impl Drop for StoreProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
fn context(bucket: &str, key: &str) -> RequestContext {
    serde_json::from_value(json!({"version":1,"bucket":bucket,"region":"eu-west-1","prefix":"db/","credentials":{"access_key_id":key,"secret_access_key":"fixture-secret","session_token":null,"expires_at":null}})).unwrap()
}
fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sl01_sl02_shared_worker_routes_independent_databases_and_reauthorizes_receipts() {
    shared_worker_case(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sl01_first_use_absent_catalog_two_workers_metadata_create_and_list() {
    shared_worker_case(true).await;
}
async fn shared_worker_case(two_workers: bool) {
    nabedra_rt::time::timeout(Duration::from_secs(60),async {
 let service=StoreProcess::start(); let data_a=StoreProcess::start(); let data_b=StoreProcess::start(); let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let endpoint=format!("http://{}",listener.local_addr().unwrap());
 let resources=crate::config::WorkerResources::default();
 let template=json!({"mode":"serverless","api_urls":["http://127.0.0.1:1"],"listen":"127.0.0.1:0","storage":{"driver":"http","options":{"base_url":service.url,"namespace":"control/","token_file":"token","ca_file":null}},"tls":null,"operation_slots":4,"partition_slots":4,"queue_size":64,"partition_size":"4 KiB","resources":resources});
 let mut bootstrap=template.clone();bootstrap["version"]=json!(2);bootstrap["cluster_id"]=json!(id(1));bootstrap["node_id"]=json!(id(3));bootstrap["request_id"]=Value::Null;bootstrap["advertise_url"]=json!(endpoint);bootstrap["enrollment_token"]=json!("E".repeat(43));
 service.write("bootstrap.json",serde_json::to_vec(&bootstrap).unwrap());service.write("token",b"test-secret");
 let ready=Arc::new(Box::pin(prepare_bootstrap(&service.path.join("bootstrap.json"),(nabedra_rt::time::now()+Duration::from_secs(10)).into_std()).unwrap().connect_async()).await.unwrap());
 let key=Arc::new(auth::WorkerSigningKey::new().unwrap());let sender=Arc::new(auth::Sender::worker(id(3),id(4),id(5),http::hex(&key.public_key())).unwrap());
 let slots=super::super::queue_runtime::coordination_capacity(&ready.prepared.input.value.template).unwrap();let job_slots=super::super::queue_runtime::job_coordination_capacity(&ready.prepared.input.value.template).unwrap();
 let typed:crate::cluster::configuration::Template=serde_json::from_value(template.clone()).unwrap();
 let mut root:crate::cluster::state::Control=serde_json::from_value(json!({"version":1,"cluster_id":id(1),"name":"Shared_fixture","config":{"generation":2,"storage_fingerprint":ready.backend.storage.as_str(),"auth_fingerprint":"b".repeat(64),"template_fingerprint":typed.fingerprint().unwrap(),"lifecycle_fingerprint":"c".repeat(64),"worker_semantic_fingerprint":"d".repeat(64),"adapter_contract":null},"provisioning":"external","template":template,"timing":crate::cluster::configuration::Timing::default(),"revision":1,"change_id":id(20),"policy":{"revision":1,"mode":"fixed","desired":1,"min_nodes":1,"max_nodes":1},"controller":{"owner_id":null,"epoch":0,"lease_state":"released","lease_until_ms":null},"scheduler":{"coordinator_node":id(3),"scheduler_term":1,"cooldown_until_ms":0,"backoff_until_ms":0,"failures":0,"low_since_ms":null},"admin_seq":0,"admin_head":null,"nodes":[{"node_id":id(3),"launch_id":null,"resource_id":format!("external:{}",id(3)),"ownership":"external","state":"ready","incarnation":id(4),"retired_incarnations":[],"endpoint":endpoint,"operation_slots":slots,"partition_slots":job_slots,"resources":resources,"queue_size":64,"registration":"verified","session":{"id":id(5),"verifier":http::hex(&auth::worker_verifier(id(1),id(3),id(4),id(5),&key.public_key()))},"drain":null}],"intents":[]})).unwrap();
 let storage=Arc::new(Storage::new(RuntimeStore::Http(service.adapter("control/")),Namespace::new("control/").unwrap()));storage.put(&ObjectKey::control(),root.encode().unwrap(),WriteCondition::Create).await.unwrap();
 let fence=CatalogFence{run_id:id(10),scheduler_term:1}; storage.initialize_fenced_cluster_catalog(fence,false).await.unwrap();
 let (pool,threads)=WorkerPool::start_with_resources(resources,tokio::runtime::Handle::current()).unwrap();
 let local=LocalAdmission::new_with_job_slots(id(1),&key,sender.clone(),id(30),slots,job_slots,64).unwrap();let boot=local.boot();let receiver=Arc::new(Mutex::new(ReceiverState::distributed(local)));
 let owners=Arc::new(PayloadOwners::from_receiver_with_streams(pool.clone(),receiver.clone(),32)); let scratch=service.path.join("scratch");std::fs::create_dir(&scratch).unwrap();
 let mut process=owners.start_boot(&boot,id(30),scratch,SpoolLimits::new(2*1024*1024*1024,220000).unwrap()).unwrap();let receipt=process.process_ready().await.unwrap();assert!(receiver.lock().unwrap().fixture_initialize(receipt));
 let a=context("tenant-one","first");let b=context("tenant-two","second");let allowed=Arc::new(AtomicBool::new(true));let check=allowed.clone();
 let registry=DatabaseRegistry::new_fixture(64,64<<20,None, BTreeMap::from([(a.storage_key().unwrap(),data_a.adapter("db/")),(b.storage_key().unwrap(),data_b.adapter("db/"))]),Arc::new(move |_|check.load(Ordering::SeqCst))).unwrap();
 // Fixture adapters retain their mapped namespace; production adapters use the context namespace.
 let budget=crate::scheduling::ResourceScheduler::new(vec![(0,resources)]).unwrap();
 let worker=Arc::new(Worker{test_job_barrier:Mutex::new(None),test_terminal_replication_barrier:Mutex::new(None),initialization:id(30),ready,storage:storage.clone(),registry,cpu:pool.clone(),control_cpu:pool.clone(),control_budget:budget.clone(),progress_budget:budget,coordination_slots:slots,signer:Arc::new(http::Signer::worker(id(1),(*sender).clone(),key).unwrap()),client:http::HttpClient::new(None).unwrap(),owners:owners.clone(),receiver,pin:[7;32],process:Arc::new(process),operations:Executor::new(slots,0).unwrap(),partitions:Executor::new(job_slots,0).unwrap(),controls:Executor::new(8,0).unwrap(),sending:Executor::new(job_slots,0).unwrap(),operation_transport:Arc::new(Semaphore::new(slots)),queue:super::super::queue_runtime::QueueRuntime::new(),remote_scheduler:super::super::coordinator::RemoteScheduler::new(),partition_transport:Arc::new(Semaphore::new(job_slots)),control_transport:Arc::new(Semaphore::new(8)),accepting:AtomicBool::new(true)});
 let mut second=None;
 if two_workers {
   let listener2=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let endpoint2=format!("http://{}",listener2.local_addr().unwrap());
   let key2=Arc::new(auth::WorkerSigningKey::new().unwrap());let sender2=Arc::new(auth::Sender::worker(id(6),id(7),id(8),http::hex(&key2.public_key())).unwrap());
   let mut node=root.nodes[0].clone();node.node_id=id(6);node.resource_id=Some(format!("external:{}",id(6)));node.incarnation=Some(id(7));node.endpoint=Some(endpoint2.clone());node.session.as_mut().unwrap().id=id(8);node.session.as_mut().unwrap().verifier=http::hex(&auth::worker_verifier(id(1),id(6),id(7),id(8),&key2.public_key()));root.nodes.push(node);root.policy.desired=2;root.policy.min_nodes=2;root.policy.max_nodes=2;root.revision+=1;
   let loaded=storage.read(&ObjectKey::control(),crate::cluster::state::MAX_CONTROL_BYTES).await.unwrap();storage.put(&ObjectKey::control(),root.encode().unwrap(),WriteCondition::Match(&loaded.etag)).await.unwrap();
   let mut boot2=bootstrap.clone();boot2["node_id"]=json!(id(6));boot2["advertise_url"]=json!(endpoint2);service.write("bootstrap2.json",serde_json::to_vec(&boot2).unwrap());let ready2=Arc::new(Box::pin(prepare_bootstrap(&service.path.join("bootstrap2.json"),(nabedra_rt::time::now()+Duration::from_secs(10)).into_std()).unwrap().connect_async()).await.unwrap());
   let (pool2,threads2)=WorkerPool::start_with_resources(resources,tokio::runtime::Handle::current()).unwrap();let local2=LocalAdmission::new_with_job_slots(id(1),&key2,sender2.clone(),id(30),slots,job_slots,64).unwrap();let boot=local2.boot();let receiver2=Arc::new(Mutex::new(ReceiverState::distributed(local2)));let owners2=Arc::new(PayloadOwners::from_receiver_with_streams(pool2.clone(),receiver2.clone(),32));let scratch=service.path.join("scratch2");std::fs::create_dir(&scratch).unwrap();let mut process2=owners2.start_boot(&boot,id(30),scratch,SpoolLimits::new(2*1024*1024*1024,220000).unwrap()).unwrap();let receipt=process2.process_ready().await.unwrap();assert!(receiver2.lock().unwrap().fixture_initialize(receipt));
   let w2=Arc::new(Worker{test_job_barrier:Mutex::new(None),test_terminal_replication_barrier:Mutex::new(None),initialization:id(30),ready:ready2,storage:storage.clone(),registry:worker.registry.clone(),cpu:pool2.clone(),control_cpu:pool2.clone(),control_budget:worker.control_budget.clone(),progress_budget:worker.progress_budget.clone(),coordination_slots:slots,signer:Arc::new(http::Signer::worker(id(1),(*sender2).clone(),key2).unwrap()),client:http::HttpClient::new(None).unwrap(),owners:owners2.clone(),receiver:receiver2,pin:[7;32],process:Arc::new(process2),operations:Executor::new(slots,0).unwrap(),partitions:Executor::new(job_slots,0).unwrap(),controls:Executor::new(8,0).unwrap(),sending:Executor::new(job_slots,0).unwrap(),operation_transport:Arc::new(Semaphore::new(slots)),queue:super::super::queue_runtime::QueueRuntime::new(),remote_scheduler:super::super::coordinator::RemoteScheduler::new(),partition_transport:Arc::new(Semaphore::new(job_slots)),control_transport:Arc::new(Semaphore::new(8)),accepting:AtomicBool::new(true)});
   w2.queue.install_fixture_queue(RequestQueue::new(fence.run_id,1,root.revision,QueueLimits{max_pending_requests:64,max_queue_bytes_per_replica:16<<20,max_retained_result_bytes_per_worker:16<<20,result_retention_ms:300000},[id(3),id(6)].into_iter().collect()).unwrap());
   let mut app=Router::new();for path in ["operations","jobs","dependencies/open","dependencies/close"] {app=app.route(&format!("/internal/v1/{path}"),post(internal));}let app=app.route(super::super::queue_transport::PATH,post(super::super::queue_runtime::handler)).with_state(w2.clone());let server=nabedra_rt::task::spawn(async move {axum::serve(listener2,app).await.unwrap()});second=Some((w2,owners2,threads2,pool2,server));
 }
 worker.queue.install_fixture_queue(RequestQueue::new(fence.run_id,1,root.revision,QueueLimits{max_pending_requests:64,max_queue_bytes_per_replica:16<<20,max_retained_result_bytes_per_worker:16<<20,result_retention_ms:300000},root.nodes.iter().map(|n|n.node_id).collect()).unwrap());
 let mut app=Router::new();for path in ["operations","jobs","dependencies/open","dependencies/close"] {app=app.route(&format!("/internal/v1/{path}"),post(internal));}let worker_for_server=worker.clone();let server=nabedra_rt::task::spawn(async move { axum::serve(listener,app.with_state(worker_for_server)).await.unwrap() });
 let request_id=id(100);let body=serde_json::to_vec(&json!({"request_id":request_id,"operation":"table.create","payload":{"name":"same","schema":{"id":"int64","value":"utf8"},"key":"id","format":"parquet"}})).unwrap();
 for ctx in [&a,&b] {
  let fresh=Box::pin(worker.open_database(ctx.clone())).await.unwrap();assert!(matches!(fresh.storage().load_cluster_catalog().await,Err(crate::storage::StorageError::NotFound)));drop(fresh);
  Box::pin(worker.queue_command(&root,None,QueueCommand::Submit{body:body.clone(),context:Some(ctx.clone())})).await.unwrap();let key=RequestKey::new(request_id,Some(ctx)).unwrap();let record=worker.queue.record(key.clone()).unwrap();
  let arrived=Arc::new(Semaphore::new(0));let release=Arc::new(Semaphore::new(0));*worker.test_terminal_replication_barrier.lock().unwrap()=Some((arrived.clone(),release.clone()));
  let w=worker.clone();let r=root.clone();let execution=nabedra_rt::task::spawn(async move {Box::pin(w.run_queued(r,record)).await});arrived.acquire().await.unwrap().forget();
  assert!(worker.queue.record(key.clone()).unwrap().state.terminal());
  let receipt=Box::pin(worker.queue_command(&root,None,QueueCommand::Lookup{run_id:fence.run_id,request_id,context:Some(ctx.clone())})).await;
  release.add_permits(1);execution.await.unwrap().unwrap();
  assert!(matches!(receipt,Ok(QueueReply::Receipt{receipt}) if receipt.state==crate::request_queue::RequestState::Succeeded),"durable terminal result must survive concurrent receipt before replication finishes");
  let record=worker.queue.record(key).unwrap();assert_eq!(record.response_status,Some(200),"{:?}",record.response_body);assert!(record.state.terminal());
 }
 for ctx in [&a,&b] {
  let bytes=serde_json::to_vec(&json!({"request_id":id(99),"operation":"table.list","payload":{}})).unwrap();Box::pin(worker.queue_command(&root,None,QueueCommand::Submit{body:bytes,context:Some(ctx.clone())})).await.unwrap();let key=RequestKey::new(id(99),Some(ctx)).unwrap();Box::pin(worker.run_queued(root.clone(),worker.queue.record(key.clone()).unwrap())).await.unwrap();let record=worker.queue.record(key).unwrap();assert_eq!(record.response_status,Some(200));let value:Value=serde_json::from_slice(record.response_body.as_ref().unwrap()).unwrap();assert_eq!(value["data"][0]["name"],"same");
 }
 // Seed equal table UUIDs into the two empty catalogs, then exercise actual signed
 // partition jobs with equal operation IDs and row keys but different values.
 for ctx in [&a,&b] {
   let grant=Box::pin(worker.open_database(ctx.clone())).await.unwrap();let data=grant.storage();
   let loaded=data.load_cluster_catalog().await.unwrap();let mut catalog=loaded.catalog().clone();catalog.tables.get_mut("same").unwrap().id=id(200);
   data.put(&ObjectKey::catalog(),catalog.encode().unwrap(),WriteCondition::Match(loaded.etag())).await.unwrap();
 }
 let arrived=Arc::new(Semaphore::new(0));let release=Arc::new(Semaphore::new(0));
 *worker.test_job_barrier.lock().unwrap()=Some((arrived.clone(),release.clone()));
 if let Some((w,..))=&second {*w.test_job_barrier.lock().unwrap()=Some((arrived.clone(),release.clone()));}
 let mut tasks=Vec::new();
 for (ctx,value) in [(&a,"one"),(&b,"two")] {
   let bytes=serde_json::to_vec(&json!({"request_id":id(101),"operation":"table.insert","payload":{"table_id":id(200),"row":{"id":1,"value":value}}})).unwrap();
   Box::pin(worker.queue_command(&root,None,QueueCommand::Submit{body:bytes,context:Some(ctx.clone())})).await.unwrap();let key=RequestKey::new(id(101),Some(ctx)).unwrap();let record=worker.queue.record(key.clone()).unwrap();let w=worker.clone();let r=root.clone();
   tasks.push(nabedra_rt::task::spawn(async move {Box::pin(w.run_queued(r,record)).await}));
 }
 arrived.acquire_many(2).await.unwrap().forget();
 for ctx in [&a,&b] {assert_eq!(worker.queue.record(RequestKey::new(id(101),Some(ctx)).unwrap()).unwrap().state,crate::request_queue::RequestState::Running);}
 let usage=pool.scheduler().usage()[0];assert!(usage.1<=resources.max_threads);assert!(usage.2<=resources.working_bytes());
 *worker.test_job_barrier.lock().unwrap()=None;if let Some((w,..))=&second {*w.test_job_barrier.lock().unwrap()=None;}release.add_permits(2);
 for task in tasks {task.await.unwrap().unwrap();}
 for ctx in [&a,&b] {let result=worker.queue.record(RequestKey::new(id(101),Some(ctx)).unwrap()).unwrap();assert_eq!(result.response_status,Some(200),"{:?}",result.response_body);}
 let get=serde_json::to_vec(&json!({"request_id":id(102),"operation":"table.get","payload":{"table_id":id(200),"key":1}})).unwrap();
 for (ctx,value) in [(&a,"one"),(&b,"two")] {
   Box::pin(worker.queue_command(&root,None,QueueCommand::Submit{body:get.clone(),context:Some(ctx.clone())})).await.unwrap();let key=RequestKey::new(id(102),Some(ctx)).unwrap();Box::pin(worker.run_queued(root.clone(),worker.queue.record(key.clone()).unwrap())).await.unwrap();
   let record=worker.queue.record(key).unwrap();assert_eq!(record.response_status,Some(200));let result:Value=serde_json::from_slice(record.response_body.as_ref().unwrap()).unwrap();assert_eq!(result["data"]["value"],value);
 }
 let rotated=context("tenant-one","rotated");Box::pin(worker.queue_command(&root,None,QueueCommand::Submit{body:body.clone(),context:Some(rotated.clone())})).await.unwrap();
 let original=worker.queue.record(RequestKey::new(request_id,Some(&a)).unwrap()).unwrap();assert_eq!(original.context.as_ref(),Some(&a));
 let mut changed:Value=serde_json::from_slice(&body).unwrap();changed["payload"]["name"]=json!("different");assert!(Box::pin(worker.queue_command(&root,None,QueueCommand::Submit{body:serde_json::to_vec(&changed).unwrap(),context:Some(a.clone())})).await.is_err());
 assert!(matches!(Box::pin(worker.queue_command(&root,None,QueueCommand::Lookup{run_id:fence.run_id,request_id,context:Some(rotated.clone())})).await.unwrap(),QueueReply::Receipt{..}));
 allowed.store(false,Ordering::SeqCst);assert!(Box::pin(worker.queue_command(&root,None,QueueCommand::Lookup{run_id:fence.run_id,request_id,context:Some(rotated)})).await.is_err());
 assert!(storage.load_cluster_catalog().await.unwrap().catalog().tables.is_empty());
 if let Some((w,o,t,p,s))=second {s.abort();let _=s.await;drop(w);o.close_and_drain().await.unwrap();drop(o);t.shutdown().await.unwrap();assert_eq!(p.scheduler().usage()[0].2,0);}
 server.abort();let _=server.await;drop(worker);owners.close_and_drain().await.unwrap();drop(owners);threads.shutdown().await.unwrap();assert_eq!(pool.scheduler().usage()[0].2,0);
 }).await.unwrap();
}
