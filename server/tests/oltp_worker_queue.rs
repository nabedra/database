#![cfg(all(feature = "oltp", unix))]
//! Real OS-process RAM queue/mTLS failures, with fixture-only authority and
//! deterministic pinned reads. No Control-Raft, transaction, production remote
//! API, native/RSS capacity or performance claim is made by these assertions.
#[path = "support/queue_child.rs"]
mod child;
#[path = "support/tls.rs"]
mod tls;

use child::{Admin, Config, Stage};
use nabedra_database_server::{
    oltp_replication::{ReadySetHasher, types::*},
    oltp_runtime::replica::{JoinPhase, JoinSnapshot},
};
use serde_json::Value;
use std::{
    fs,
    net::TcpListener,
    os::fd::AsRawFd,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tokio::time::{sleep, timeout};
use uuid::Uuid;

#[test]
#[ignore = "fixture worker, started only by this integration test"]
fn worker_child() {
    let path = std::env::var(child::CONFIG_ENV).expect("worker config is required");
    let config: Config = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    nabedra_database_server::oltp_runtime::standalone_network_runtime()
        .unwrap()
        .block_on(child::run(config));
}

struct Process {
    child: Child,
    config: Config,
    reaped: bool,
}
impl Process {
    fn kill(&mut self) {
        use std::os::unix::process::ExitStatusExt;
        if self.reaped {
            return;
        }
        let running = self.child.try_wait().unwrap().is_none();
        if running {
            let _ = self.child.kill();
        }
        let status = self.child.wait().unwrap();
        self.reaped = true;
        println!(
            "queue_child_reaped pid={} signal={:?} exit={:?}",
            self.child.id(),
            status.signal(),
            status.code()
        );
        if running {
            assert_eq!(status.signal(), Some(libc::SIGKILL));
        }
    }
    async fn call(&self, command: Admin) -> Value {
        timeout(
            Duration::from_secs(8),
            child::command(
                &self.config.admin_endpoint,
                self.config.admin_token,
                command,
            ),
        )
        .await
        .expect("bounded admin observation")
        .expect("fixture admin transport")
    }
    async fn ok(&self, command: Admin) -> Value {
        let value = self.call(command).await;
        assert!(value.get("error").is_none(), "fixture error: {value}");
        value
    }
    fn pending(&self, command: Admin) -> tokio::task::JoinHandle<Result<Value, String>> {
        let endpoint = self.config.admin_endpoint.clone();
        let token = self.config.admin_token;
        tokio::spawn(async move {
            timeout(
                Duration::from_secs(18),
                child::command(&endpoint, token, command),
            )
            .await
            .map_err(|_| "fixture observer deadline".to_owned())?
        })
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        self.kill();
    }
}

struct Cluster {
    directory: PathBuf,
    processes: Vec<Process>,
    ticket: Option<JoinTicket>,
}
impl Cluster {
    async fn new(joining: bool) -> Self {
        Self::with_memory(joining, None).await
    }
    async fn with_memory(joining: bool, memory_limit_bytes: Option<u64>) -> Self {
        let directory =
            std::env::temp_dir().join(format!("nabedra-queue-process-{}", Uuid::new_v4()));
        fs::create_dir(&directory).unwrap();
        let count = if joining { 3 } else { 2 };
        let materials = tls::certificates(&directory, count);
        let sockets: Vec<_> = (0..count)
            .map(|_| {
                (
                    TcpListener::bind("127.0.0.1:0").unwrap(),
                    TcpListener::bind("127.0.0.1:0").unwrap(),
                )
            })
            .collect();
        let base = (Uuid::new_v4().as_u128() >> 64) << 64;
        let enrollments: Vec<_> = (0..count as usize)
            .map(|index| WorkerEnrollment {
                worker_id: Uuid::from_u128(base + index as u128 + 1),
                incarnation: Uuid::new_v4(),
                certificate_sha256: Digest::parse(materials[index].certificate_sha256()).unwrap(),
                endpoint: Text::new(sockets[index].0.local_addr().unwrap().to_string()).unwrap(),
                server_name: Text::try_from("localhost").unwrap(),
                enrollment_revision: 1,
            })
            .collect();
        let members: Vec<_> = enrollments[..2].iter().map(WorkerIdentity::from).collect();
        let ready = ready(Uuid::new_v4(), 1, 1, &members);
        let group = GroupIdentity {
            cluster_id: Uuid::new_v4(),
            database: Text::try_from("default").unwrap(),
            group_id: Uuid::new_v4(),
            kind: GroupKind::Control,
            group_format_version: GROUP_FORMAT_VERSION,
        };
        let ticket = joining.then(|| JoinTicket {
            ticket_id: Uuid::new_v4(),
            target: WorkerIdentity::from(&enrollments[2]),
            source_ready: ready,
        });
        let mut processes = Vec::new();
        for (index, (network, admin)) in sockets.into_iter().enumerate() {
            let config = Config {
                local: enrollments[index].clone(),
                members: members.clone(),
                enrollments: enrollments.clone(),
                ready,
                owner: members[0],
                group: group.clone(),
                ticket: if index == 2 { ticket } else { None },
                fixture_cold_authorization: Uuid::new_v4(),
                directory: directory.clone(),
                tls_index: index as u64 + 1,
                admin_endpoint: admin.local_addr().unwrap().to_string(),
                admin_token: Uuid::new_v4(),
                proxy_upstream: None,
                memory_limit_bytes,
            };
            processes.push(spawn(&directory, index, config, network, admin));
        }
        let cluster = Self {
            directory,
            processes,
            ticket,
        };
        for process in &cluster.processes {
            wait_ready(process).await;
        }
        cluster
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        self.processes.clear();
        if std::thread::panicking() {
            eprintln!("retained queue fixture: {}", self.directory.display());
        } else {
            fs::remove_dir_all(&self.directory).unwrap();
        }
    }
}
fn ready(run: Uuid, term: u64, revision: u64, members: &[WorkerIdentity]) -> ReadyMembership {
    let mut hash = ReadySetHasher::default();
    for member in members {
        hash.push(*member).unwrap();
    }
    ReadyMembership {
        view: AuthorityView {
            run_id: run,
            owner_term: term,
            membership_revision: revision,
        },
        ready_root: hash.finish(),
        ready_count: hash.count(),
    }
}
fn spawn(
    directory: &std::path::Path,
    index: usize,
    config: Config,
    network: TcpListener,
    admin: TcpListener,
) -> Process {
    let path = directory.join(format!("worker{index}.json"));
    fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let log = fs::File::create(directory.join(format!("worker{index}.log"))).unwrap();
    // Only these two owned listeners survive exec. Other fixture descriptors
    // retain CLOEXEC, so another child cannot keep a killed worker's ports alive.
    for listener in [&network, &admin] {
        assert_ne!(
            unsafe { libc::fcntl(listener.as_raw_fd(), libc::F_SETFD, 0) },
            -1
        );
    }
    let result = Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "worker_child", "--nocapture"])
        .env(child::CONFIG_ENV, path)
        .env(child::TLS_LISTENER_ENV, network.as_raw_fd().to_string())
        .env(child::ADMIN_LISTENER_ENV, admin.as_raw_fd().to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log))
        .spawn();
    for listener in [&network, &admin] {
        assert_ne!(
            unsafe { libc::fcntl(listener.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) },
            -1
        );
    }
    let child = result.unwrap();
    println!(
        "queue_child_started pid={} fixture={}",
        child.id(),
        directory.display()
    );
    Process {
        child,
        config,
        reaped: false,
    }
}
async fn wait_ready(process: &Process) {
    let until = Instant::now() + Duration::from_secs(12);
    loop {
        if timeout(
            Duration::from_millis(250),
            child::command(
                &process.config.admin_endpoint,
                process.config.admin_token,
                Admin::GateState,
            ),
        )
        .await
        .is_ok_and(|r| r.is_ok())
        {
            return;
        }
        assert!(Instant::now() < until, "worker startup watchdog");
        sleep(Duration::from_millis(20)).await;
    }
}
async fn wait_gate(process: &Process) {
    let until = Instant::now() + Duration::from_secs(6);
    loop {
        if process.ok(Admin::GateState).await["entered"] == true {
            return;
        }
        assert!(Instant::now() < until, "gate watchdog");
        sleep(Duration::from_millis(20)).await;
    }
}
fn body(id: Uuid) -> String {
    format!(" {{\"request_id\":\"{id}\",\"operation\":\"table.list\", \"payload\":{{}}}}\n")
}
fn snapshot(value: Value) -> JoinSnapshot {
    serde_json::from_value(value["snapshot"].clone()).unwrap()
}
async fn proxy(cluster: &Cluster, index: usize) -> Process {
    let network = TcpListener::bind("127.0.0.1:0").unwrap();
    let admin = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut config = cluster.processes[0].config.clone();
    config.proxy_upstream = Some((config.admin_endpoint.clone(), config.admin_token));
    config.admin_endpoint = admin.local_addr().unwrap().to_string();
    config.admin_token = Uuid::new_v4();
    let process = spawn(&cluster.directory, index, config, network, admin);
    wait_ready(&process).await;
    process
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn q04_real_api_process_restart_and_lost_submit_reply_keep_worker_identity_once() {
    timeout(Duration::from_secs(45),async {
        let cluster=Cluster::new(false).await;
        let mut api=proxy(&cluster,5).await;
        let acknowledged=Uuid::new_v4();
        assert_eq!(api.ok(Admin::Admit{body:body(acknowledged)}).await["status"],202);
        let before=api.ok(Admin::Inspect{request_id:acknowledged,run_id:None}).await;
        api.kill();
        let mut api=proxy(&cluster,6).await;
        assert_eq!(api.ok(Admin::Inspect{request_id:acknowledged,run_id:None}).await,before);
        let uncertain=Uuid::new_v4();
        cluster.processes[1].ok(Admin::Gate{stage:Some(Stage::Acceptance)}).await;
        let pending=api.pending(Admin::Admit{body:body(uncertain)});
        wait_gate(&cluster.processes[1]).await;
        let retained=cluster.processes[0].ok(Admin::Inspect{request_id:uncertain,run_id:None}).await;
        api.kill();
        assert!(timeout(Duration::from_secs(3),pending).await.unwrap().unwrap().is_err());
        cluster.processes[1].ok(Admin::Gate{stage:None}).await;
        let api=proxy(&cluster,7).await;
        let until=Instant::now()+Duration::from_secs(6);
        loop {
            let recovered=api.ok(Admin::Inspect{request_id:uncertain,run_id:None}).await;
            assert_eq!(recovered["sequence"],retained["sequence"]);
            assert_eq!(recovered["context"],retained["context"]);
            if recovered["copies_complete"]==true {break;}
            assert!(Instant::now()<until,"worker acceptance after API loss watchdog"); sleep(Duration::from_millis(20)).await;
        }
        for id in [acknowledged,uncertain] {
            let terminal=api.ok(Admin::Execute{request_id:id}).await;
            assert_eq!(terminal["response"]["request_id"],id.to_string());
            assert_eq!(terminal["response"]["data"]["snapshot"],1);
            let after=api.ok(Admin::Inspect{request_id:id,run_id:None}).await;
            assert_eq!(after["accepted"],true); assert_eq!(after["state"],"succeeded");
        }
        println!("queue_process: Q04 separate API-only process restarts and lost-reply worker retention passed; fixture API protocol only");
    }).await.expect("bounded API process fixture");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn q03_r01_q06_real_process_ack_boundaries_takeover_terminal_race_and_cold_abandonment() {
    timeout(Duration::from_secs(100),async {
        for stage in [Stage::Payload,Stage::Acceptance,Stage::Terminal] {
            let mut cluster=Cluster::new(false).await;
            let id=Uuid::new_v4(); let raw=body(id); let run=cluster.processes[0].config.ready.view.run_id;
            let pending=if stage==Stage::Terminal {
                assert_eq!(cluster.processes[0].ok(Admin::Admit{body:raw.clone()}).await["status"],202);
                cluster.processes[1].ok(Admin::Gate{stage:Some(stage)}).await;
                cluster.processes[0].pending(Admin::Execute{request_id:id})
            } else {
                cluster.processes[1].ok(Admin::Gate{stage:Some(stage)}).await;
                cluster.processes[0].pending(Admin::Admit{body:raw.clone()})
            };
            wait_gate(&cluster.processes[1]).await;
            for process in &cluster.processes {
                assert_eq!(process.ok(Admin::GateState).await["resources"][0][1],0,
                    "network copy waits cannot own compute grants");
            }
            assert!(!pending.is_finished(),"copy barrier must precede observer completion");
            let retained=cluster.processes[1].ok(Admin::Inspect{request_id:id,run_id:Some(run)}).await;
            assert_eq!(retained["body"],raw);
            assert_eq!(retained["accepted"],stage!=Stage::Payload);
            assert_eq!(retained["run_id"],run.to_string());
            let context=retained["context"].clone();
            if stage==Stage::Terminal {
                // Actual mTLS QueueControl traffic progresses while the other
                // direction's QueueData terminal ACK remains blocked.
                let state=cluster.processes[0].ok(Admin::PeerState{target:WorkerIdentity::from(&cluster.processes[1].config.local),request_id:id}).await;
                assert_eq!(state["state"]["request_id"],id.to_string());
                assert_eq!(state["state"]["state"],"succeeded");
            }
            cluster.processes[0].kill();
            assert!(timeout(Duration::from_secs(3),pending).await.unwrap().unwrap().is_err(),"killed owner cannot send 202/result");
            cluster.processes[1].ok(Admin::Gate{stage:None}).await;
            let survivor=WorkerIdentity::from(&cluster.processes[1].config.local);
            let next=ready(run,2,2,&[survivor]);
            cluster.processes[1].ok(Admin::Fence{ready:next,owner:survivor,members:vec![survivor]}).await;
            cluster.processes[1].ok(Admin::Resume{request_id:id}).await;
            let recovered=cluster.processes[1].ok(Admin::Inspect{request_id:id,run_id:Some(run)}).await;
            assert_eq!(recovered["context"],context);
            assert_eq!(recovered["body"],raw);
            assert_eq!(recovered["copies_complete"],true);
            let terminal=if stage==Stage::Terminal {recovered} else {cluster.processes[1].ok(Admin::Execute{request_id:id}).await;
                cluster.processes[1].ok(Admin::Inspect{request_id:id,run_id:Some(run)}).await};
            let version=terminal["version"].as_u64().unwrap();
            let exact=cluster.processes[1].ok(Admin::PeerResult{target:survivor,request_id:id,version}).await;
            let response:Value=serde_json::from_str(exact["body"].as_str().unwrap()).unwrap();
            assert_eq!(response,serde_json::json!({"request_id":id,"data":{"snapshot":1,"value":"fixture-read"}}));
            // R04 must observe genuinely outstanding accepted work, separately
            // from the completed query whose terminal response was verified.
            let outstanding=Uuid::new_v4();
            let admission=cluster.processes[1].ok(Admin::Admit{body:body(outstanding)}).await;
            assert_eq!(admission["status"],202); assert_eq!(admission["run_id"],run.to_string());
            let queued=cluster.processes[1].ok(Admin::Inspect{request_id:outstanding,run_id:Some(run)}).await;
            assert_eq!(queued["accepted"],true); assert_eq!(queued["copies_complete"],true);
            assert_eq!(queued["state"],"queued"); assert!(queued["response"].is_null());
            // A whole-worker stop loses every RAM record. A separately supplied
            // explicit fixture cold authorization creates a different run.
            cluster.processes[1].kill();
            let mut config=cluster.processes[1].config.clone();
            config.local.incarnation=Uuid::new_v4(); config.local.enrollment_revision+=1;
            let network=TcpListener::bind("127.0.0.1:0").unwrap(); let admin=TcpListener::bind("127.0.0.1:0").unwrap();
            config.local.endpoint=Text::new(network.local_addr().unwrap().to_string()).unwrap(); config.admin_endpoint=admin.local_addr().unwrap().to_string();
            let local=WorkerIdentity::from(&config.local); config.members=vec![local]; config.enrollments=vec![config.local.clone()];
            config.ready=ready(Uuid::new_v4(),3,3,&[local]); config.owner=local; config.fixture_cold_authorization=Uuid::new_v4();
            let restarted=spawn(&cluster.directory,4,config,network,admin); wait_ready(&restarted).await;
            let old=restarted.call(Admin::Inspect{request_id:outstanding,run_id:Some(run)}).await;
            assert_eq!(old["error"]["outcome"],"unknown");
            assert!(old["error"]["message"].as_str().unwrap().contains("abandoned"));
            assert!(restarted.ok(Admin::Inspect{request_id:outstanding,run_id:None}).await["record"].is_null());
            drop(restarted);
        }
        println!("queue_process: Q03/R01/Q06/C07 ACK phases and R04 outstanding-read abandonment passed; fixture authority and read backend only");
    }).await.expect("bounded process failure fixture");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn r03_q03_real_process_paged_join_delta_before_readiness_and_surviving_last_holder() {
    timeout(Duration::from_secs(120),async {
        for count in [0,1,3] {
            let mut cluster=Cluster::new(true).await; let mut ids=Vec::new();
            for _ in 0..count {let id=Uuid::new_v4(); ids.push(id); assert_eq!(cluster.processes[0].ok(Admin::Admit{body:body(id)}).await["status"],202);}
            let ticket=cluster.ticket.unwrap(); let run=ticket.source_ready.view.run_id;
            assert_eq!(cluster.processes[2].ok(Admin::GateState).await["queue_ready"],false);
            let snapshot=snapshot(cluster.processes[0].ok(Admin::BeginJoin{ticket}).await);
            let target=WorkerIdentity::from(&cluster.processes[2].config.local);
            let members:Vec<_>=cluster.processes.iter().map(|p|WorkerIdentity::from(&p.config.local)).collect();
            let next=ready(run,1,2,&members); let owner=members[0];
            assert!(cluster.processes[2].call(Admin::Fence{ready:next,owner,members:members.clone()}).await.get("error").is_some());
            let mut after=None; let mut pages=0;
            loop {
                let page=cluster.processes[0].ok(Admin::Page{snapshot,phase:JoinPhase::Snapshot,after,target}).await;
                let replay=cluster.processes[0].ok(Admin::Page{snapshot,phase:JoinPhase::Snapshot,after,target}).await;
                assert_eq!(replay,page,"lost page reply must be safely repeatable");
                pages+=usize::from(page["has_record"]==true);
                assert_eq!(page["done"],pages==count as usize);
                after=page["next"].as_u64(); if page["done"]==true {break;}
            }
            let flags=cluster.processes[0].ok(Admin::PreviewPage{snapshot,phase:JoinPhase::Snapshot,after}).await;
            assert_eq!(flags["done"],true); assert_eq!(flags["has_record"],false);
            let exhausted=cluster.processes[0].ok(Admin::Page{snapshot,phase:JoinPhase::Snapshot,after,target}).await;
            assert_eq!(exhausted["done"],true); assert_eq!(exhausted["has_record"],false);
            if count==3 {
                cluster.processes[0].ok(Admin::Execute{request_id:ids[0]}).await;
                let id=Uuid::new_v4(); ids.push(id); cluster.processes[0].ok(Admin::Admit{body:body(id)}).await;
            }
            assert!(cluster.processes[2].call(Admin::JoinStatement).await.get("error").is_some());
            cluster.processes[0].ok(Admin::Seal{snapshot}).await;
            let mut after=None; let mut deltas=0;
            loop {
                let page=cluster.processes[0].ok(Admin::Page{snapshot,phase:JoinPhase::Delta,after,target}).await;
                deltas+=usize::from(page["has_record"]==true); after=page["next"].as_u64(); if page["done"]==true {break;}
            }
            assert_eq!(deltas,if count==3 {2} else {0});
            let source=cluster.processes[0].ok(Admin::JoinStatement).await;
            let copied=cluster.processes[2].ok(Admin::JoinStatement).await;
            assert_eq!(source,copied);
            assert_eq!(cluster.processes[2].ok(Admin::GateState).await["queue_ready"],false);
            for process in &cluster.processes {process.ok(Admin::Fence{ready:next,owner,members:members.clone()}).await;}
            assert_eq!(cluster.processes[2].ok(Admin::GateState).await["queue_ready"],true);
            for id in &ids {cluster.processes[0].ok(Admin::Resume{request_id:*id}).await;}
            // Both old holders stop only after the new worker has every record.
            cluster.processes[0].kill(); cluster.processes[1].kill();
            let next=ready(run,2,3,&[target]); cluster.processes[2].ok(Admin::Fence{ready:next,owner:target,members:vec![target]}).await;
            for id in &ids {
                cluster.processes[2].ok(Admin::Resume{request_id:*id}).await;
                let record=cluster.processes[2].ok(Admin::Inspect{request_id:*id,run_id:Some(run)}).await;
                assert_eq!(record["body"],body(*id)); assert_eq!(record["accepted"],true);
                if record["state"]!="succeeded" {cluster.processes[2].ok(Admin::Execute{request_id:*id}).await;}
                let terminal=cluster.processes[2].ok(Admin::Inspect{request_id:*id,run_id:Some(run)}).await;
                let result=cluster.processes[2].ok(Admin::PeerResult{target,request_id:*id,version:terminal["version"].as_u64().unwrap()}).await;
                let response:Value=serde_json::from_str(result["body"].as_str().unwrap()).unwrap(); assert_eq!(response["data"]["snapshot"],1);
            }
        }
        println!("queue_process: R03/Q03 empty/single/multiple snapshot+delta joins and last-holder takeover passed; fixture authority only");
    }).await.expect("bounded process join fixture");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn u02_m02_real_process_full_protocol_body_copies_and_joins_with_default_task_budget() {
    timeout(Duration::from_secs(60),async {
        // The default task limit stays512MiB, but fixed worker/native/queue
        // reservations leave less than another512MiB free. A duplicate full
        // task grant would deadlock instead of reaching the payload ACK gate.
        let cluster=Cluster::with_memory(true,Some(768<<20)).await;
        let small=Uuid::new_v4();
        assert_eq!(cluster.processes[0].ok(Admin::Admit{body:body(small)}).await["status"],202);
        cluster.processes[1].ok(Admin::Gate{stage:Some(Stage::Payload)}).await;
        let wide=Uuid::new_v4();
        let bytes=nabedra_database_server::protocol::MAX_BODY_BYTES;
        let pending=cluster.processes[0].pending(Admin::AdmitWide{request_id:wide,body_bytes:bytes});
        wait_gate(&cluster.processes[1]).await;
        assert!(!pending.is_finished());
        let copied=cluster.processes[1].ok(Admin::Fingerprint{request_id:wide}).await;
        assert_eq!(copied["body_bytes"],bytes);
        assert_eq!(copied["digest"],copied["actual_digest"]);
        let target=WorkerIdentity::from(&cluster.processes[1].config.local);
        let state=cluster.processes[0].ok(Admin::PeerState{target,request_id:small}).await;
        assert_eq!(state["state"]["request_id"],small.to_string());
        assert_eq!(state["state"]["state"],"queued");
        for process in &cluster.processes {
            assert_eq!(process.ok(Admin::GateState).await["resources"][0][1],0);
        }
        cluster.processes[1].ok(Admin::Gate{stage:None}).await;
        let accepted=timeout(Duration::from_secs(8),pending).await.unwrap().unwrap().unwrap();
        assert_eq!(accepted["status"],202);
        assert_eq!(accepted["body_bytes"],bytes);
        assert_eq!(accepted["digest"],copied["actual_digest"]);
        let snapshot=snapshot(cluster.processes[0].ok(Admin::BeginJoin{ticket:cluster.ticket.unwrap()}).await);
        let target=WorkerIdentity::from(&cluster.processes[2].config.local);
        let mut after=None; let mut count=0;
        loop {
            let page=cluster.processes[0].ok(Admin::Page{snapshot,phase:JoinPhase::Snapshot,after,target}).await;
            count+=usize::from(page["has_record"]==true); after=page["next"].as_u64();
            if page["done"]==true {break;}
        }
        assert_eq!(count,2);
        cluster.processes[0].ok(Admin::Seal{snapshot}).await;
        let delta=cluster.processes[0].ok(Admin::Page{snapshot,phase:JoinPhase::Delta,after:None,target}).await;
        assert_eq!(delta["done"],true); assert_eq!(delta["has_record"],false);
        assert_eq!(cluster.processes[0].ok(Admin::JoinStatement).await,cluster.processes[2].ok(Admin::JoinStatement).await);
        let joined=cluster.processes[2].ok(Admin::Fingerprint{request_id:wide}).await;
        assert_eq!(joined["body_bytes"],bytes); assert_eq!(joined["actual_digest"],accepted["digest"]);
        assert_eq!(joined["accepted"],true); assert_eq!(joined["state"],"queued");
        assert_eq!(cluster.processes[2].ok(Admin::GateState).await["queue_ready"],false);
        println!("queue_process: U02/M02 exact8MiB valid request copied/joined with default512MiB task budget and protected receipt progress; admission only, no data write");
    }).await.expect("bounded wide queue process fixture");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn u02_real_transport_preserves_maximum_logical_predicate_depth_and_rejects_one_over() {
    timeout(Duration::from_secs(35),async {
        let cluster=Cluster::new(false).await;
        let make=|id:Uuid,depth:usize| {
            let mut predicate=serde_json::json!({"op":"eq","column":"id","value":"needle"});
            for _ in 1..depth {predicate=serde_json::json!({"op":"and","args":[predicate]});}
            serde_json::json!({"request_id":id,"operation":"query.count","payload":{
                "table_id":Uuid::from_u128(99),"query":{"select":null,"filter":predicate,"order_by":null,"limit":null}}}).to_string()
        };
        let valid=Uuid::new_v4();
        let raw=make(valid,nabedra_database_server::protocol::MAX_PREDICATE_DEPTH);
        assert_eq!(cluster.processes[0].ok(Admin::Admit{body:raw.clone()}).await["status"],202);
        let copied=cluster.processes[1].ok(Admin::Inspect{request_id:valid,run_id:None}).await;
        assert_eq!(copied["body"],raw); assert_eq!(copied["accepted"],true);
        let target=WorkerIdentity::from(&cluster.processes[1].config.local);
        assert_eq!(cluster.processes[0].ok(Admin::PeerState{target,request_id:valid}).await["state"]["request_id"],valid.to_string());
        let excessive=Uuid::new_v4();
        let rejected=cluster.processes[0].call(Admin::Admit{body:make(excessive,nabedra_database_server::protocol::MAX_PREDICATE_DEPTH+1)}).await;
        assert_eq!(rejected["error"]["code"],"validation_error");
        for process in &cluster.processes {
            assert!(process.ok(Admin::Inspect{request_id:excessive,run_id:None}).await["record"].is_null());
        }
        println!("queue_process: U02 logical predicate depth32 copied over real mTLS; depth33 rejected before queue installation");
    }).await.expect("bounded predicate process fixture");
}
