//! Rust counterpart of the RF3 fixture in tests/oltp_direct_cluster.py.
//! All paths refer to the calling node's own volume.

use std::{
    io::{self, Write},
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
};

use nabedra_database_server::oltp::pool::POOL_FORMAT_VERSION;
use nabedra_oltp_types::types::Digest;
use nabedra_rt::{fs::os, ids::uuid_v4, sim::net::node_ip};
use nabedra_transport::tls::TlsMaterial;
use serde_json::{Value, json};
use uuid::Uuid;

pub(crate) const WORKERS: [u64; 3] = [1, 2, 3];
pub(crate) const GATEWAYS: [u64; 2] = [101, 102];
pub(crate) const BOOTSTRAP_CLIENT: u64 = 1001;
pub(crate) const CLIENTS: [u64; 2] = [1002, 1003];
pub(crate) const DATABASE: &str = "test";

/// The Data group the registered planner derives for a new table's first range, as
/// `initial_range` in `tests/oltp_recovery_operations.py` computes it.
pub(crate) fn initial_range(cluster_id: Uuid, table: Uuid) -> Value {
    let derive = |data: &[u8]| {
        let digest = ring::digest::digest(&ring::digest::SHA256, data);
        Uuid::from_slice(&digest.as_ref()[..16]).expect("16 bytes")
    };
    let shard = derive(&[table.as_bytes().as_slice(), b"initial-shard"].concat());
    let group = derive(&[shard.as_bytes().as_slice(), b"data-group"].concat());
    json!({
        "cluster_id": cluster_id, "database": DATABASE, "group_id": group,
        "kind": {"data": {"table_id": table, "shard_id": shard}}, "group_format_version": 1,
    })
}
pub(crate) const ROOT: &str = "/sim-oltp";
pub(crate) const CONFIG: &str = "/sim-oltp/bootstrap.json";
const AUTH: &str = "/sim-oltp/auth.json";
const CERT: &str = "/sim-oltp/node.pem";
const KEY: &str = "/sim-oltp/node.key";
const CA: &str = "/sim-oltp/ca.pem";
const MARKER: &str = "/sim-oltp/.nabedra-docker-volume.json";

pub(crate) fn http_addr(node: u64) -> SocketAddr {
    SocketAddr::new(node_ip(node), 20_000 + node as u16)
}

fn peer_addr(node: u64) -> SocketAddr {
    SocketAddr::new(node_ip(node), 21_000 + node as u16)
}

fn material(node: u64) -> (&'static [u8], &'static [u8]) {
    macro_rules! pem {
        ($id:literal) => {
            (
                include_bytes!(concat!("../fixtures/tls/", $id, ".pem")),
                include_bytes!(concat!("../fixtures/tls/", $id, ".key")),
            )
        };
    }
    match node {
        1 => pem!("1"),
        2 => pem!("2"),
        3 => pem!("3"),
        101 => pem!("101"),
        102 => pem!("102"),
        _ => unreachable!("no certificate for node {node}"),
    }
}

fn certificate_authority() -> &'static [u8] {
    include_bytes!("../fixtures/tls/ca.pem")
}

fn write(path: &str, data: &[u8]) -> io::Result<()> {
    let mut file = os::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(data)?;
    file.sync_all()?;
    os::sync_dir(Path::new(ROOT))
}

fn write_json(path: &str, value: &Value) -> io::Result<()> {
    write(path, &serde_json::to_vec(value)?)
}

fn resources() -> Value {
    json!({
        "max_threads": 2,
        "memory_limit_bytes": 2048_u64 << 20,
        "task_memory_limit_bytes": 512 << 20,
        "control_memory_reserve_bytes": 128 << 20,
    })
}

fn execution() -> Value {
    json!({
        "max_workers_per_request": 1,
        "max_pending_requests": 128,
        "max_queue_bytes_per_replica": 8 << 20,
        "max_retained_result_bytes_per_worker": 8 << 20,
        "result_retention_ms": 600_000,
        "queue_replication": "all_ready_workers",
        "queue_lifetime": "live_cluster",
        "partition_size": "64 MiB",
        "direct_oltp": true,
    })
}

fn enrollment(node: u64) -> io::Result<Value> {
    let (cert, key) = material(node);
    let tls = TlsMaterial::from_pem(cert, key, certificate_authority())?;
    Ok(json!({
        "worker_id": uuid_v4(),
        "incarnation": uuid_v4(),
        "enrollment_revision": 1,
        "certificate_sha256": tls.certificate_sha256(),
        "endpoint": peer_addr(node),
        "server_name": "localhost",
    }))
}

pub(crate) struct Bootstrap {
    pub cluster_id: Uuid,
    pub run_id: Uuid,
    pub data_token: String,
    pub admin_token: String,
    enrollment_token: String,
    worker_configs: [Value; 3],
    api_enrollments: [Value; 2],
    /// The workers' identities and secondary pools, shared with the operator's tasks.
    pub mounts: Mounts,
}

/// What the workers' bootstrap files say about identities and secondary pools. The operator's
/// tasks change it while the cluster runs.
#[derive(Clone)]
pub(crate) struct Mounts(Arc<Mutex<MountState>>);

struct MountState {
    cluster_id: Uuid,
    identities: [Value; 3],
    pools: [Vec<Value>; 3],
}

impl Mounts {
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self(Arc::new(Mutex::new(MountState {
            cluster_id: Uuid::nil(),
            identities: Default::default(),
            pools: Default::default(),
        })))
    }

    fn state(&self) -> MutexGuard<'_, MountState> {
        self.0.lock().unwrap()
    }

    /// The current mounted identity of worker `index` (its `mount.worker` field).
    pub fn identity(&self, index: usize) -> Value {
        self.state().identities[index].clone()
    }

    /// A Docker-profile mount record for a secondary pool on worker `index` at `path`,
    /// as `mount_pool` in `tests/oltp_direct_cluster.py` builds it. Writes the volume
    /// marker on the calling node's volume.
    pub fn new_pool(&self, index: usize, path: &str, pool_id: Uuid) -> io::Result<Value> {
        let node = WORKERS[index];
        let volume = format!("sim-oltp-{node}{}", path.replace('/', "-"));
        let marker = pool_id;
        let file = format!("{path}/.nabedra-docker-volume.json");
        let mut out = os::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(&file)?;
        out.write_all(&serde_json::to_vec(&json!({
            "format_version": 1, "volume_name": volume, "volume_marker": marker,
        }))?)?;
        out.sync_all()?;
        os::sync_dir(Path::new(path))?;
        let state = self.state();
        let worker = &state.identities[index];
        Ok(json!({
            "profile": {"docker": {"volume_marker": marker}},
            "worker": worker,
            "generation": 1,
            "identity": {
                "engine": "oltp", "pool_format_version": POOL_FORMAT_VERSION,
                "cluster_id": state.cluster_id, "worker_id": worker["worker_id"], "pool_id": pool_id,
            },
            "mount_path": path,
            "provider_volume_id": format!("docker-volume:{volume}"),
            "filesystem_uuid": "",
        }))
    }

    /// List a mounted secondary pool in the worker's bootstrap file. A restarted worker opens
    /// only the pools its bootstrap lists, so a restart without this entry can never host the
    /// replicas that live there.
    pub fn list(&self, index: usize, mount: &Value, claim: Uuid) {
        let pool = json!({"mount": mount, "growth_claim": claim});
        self.state().pools[index].push(pool);
    }

    /// The worker no longer opens this secondary pool.
    pub fn unlist(&self, index: usize, pool: &Value) {
        self.state().pools[index].retain(|entry| entry["mount"]["identity"]["pool_id"] != *pool);
    }

    /// The worker's next process has another identity, and its mounts follow it.
    fn advance(&self, index: usize, worker: &Value) {
        let mut state = self.state();
        state.identities[index] = worker.clone();
        for pool in &mut state.pools[index] {
            let mount = &mut pool["mount"];
            let generation = mount["generation"].as_u64().unwrap();
            mount["worker"] = worker.clone();
            mount["generation"] = json!(generation + 1);
        }
    }
}

impl Bootstrap {
    /// Generate all identities in the bootstrap client's own seeded node context.
    pub fn new() -> io::Result<Self> {
        let cluster_id = uuid_v4();
        let run_id = uuid_v4();
        let bootstrap_id = uuid_v4();
        let data_token = format!("data-{}", uuid_v4().to_string().repeat(2));
        let admin_token = format!("admin-{}", uuid_v4().to_string().repeat(2));
        let enrollment_token = format!("enroll-{}", uuid_v4().to_string().repeat(2));
        let enrollments = [enrollment(1)?, enrollment(2)?, enrollment(3)?];
        let pools = WORKERS.map(|_| uuid_v4());
        let place = |offset: usize, kind: Value| {
            let voters: Vec<_> = (0..WORKERS.len())
                .map(|voter| {
                    let local = (voter + offset) % WORKERS.len();
                    json!({"node_id": voter + 1, "worker_id": enrollments[local]["worker_id"], "pool_id": pools[local]})
                })
                .collect();
            json!({
                "identity": {
                    "cluster_id": cluster_id, "database": DATABASE, "group_id": uuid_v4(),
                    "group_format_version": 1, "kind": kind,
                },
                "voters": voters,
            })
        };
        let placements = json!({
            "control": place(0, json!("control")),
            "timestamps": place(1, json!("timestamps")),
            "status": place(2, json!({"status": {"bucket": 0}})),
            "catalog": place(3, json!("catalog")),
        });
        let (peers, pins) = Self::peer_inventory(enrollments.to_vec());
        let worker_configs: [Value; 3] = std::array::from_fn(|local| {
            let node = WORKERS[local];
            let enrollment = &enrollments[local];
            let marker = uuid_v4();
            json!({
                "version": 1,
                "bind": http_addr(node),
                "auth_file": AUTH,
                "resources": resources(),
                "execution": execution(),
                "node": {
                    "format": 1,
                    "directory": ROOT,
                    "pool_id": pools[local],
                    "scheduler_node_id": node,
                    "enrollment": enrollment,
                    "bind": peer_addr(node),
                    "certificate": CERT,
                    "private_key": KEY,
                    "authority_ca": CA,
                    "bootstrap_id": bootstrap_id,
                    "initial_run": run_id,
                    "required_voters": WORKERS.len(),
                    "placements": placements,
                    "control_enrollments": peers,
                    "operator_pins": pins,
                    "mount": {
                        "profile": {"docker": {"volume_marker": marker}},
                        "worker": Self::identity(enrollment),
                        "generation": 1,
                        "identity": {
                            "engine": "oltp", "pool_format_version": POOL_FORMAT_VERSION,
                            "cluster_id": cluster_id, "worker_id": enrollment["worker_id"], "pool_id": pools[local],
                        },
                        "mount_path": ROOT,
                        "provider_volume_id": format!("docker-volume:sim-oltp-{node}"),
                        "filesystem_uuid": "",
                    },
                },
            })
        });
        let api_enrollments = [
            json!({"database": DATABASE, "enrollment": enrollment(GATEWAYS[0])?}),
            json!({"database": DATABASE, "enrollment": enrollment(GATEWAYS[1])?}),
        ];
        let mounts = Mounts(Arc::new(Mutex::new(MountState {
            cluster_id,
            identities: enrollments.each_ref().map(Self::identity),
            pools: Default::default(),
        })));
        Ok(Self {
            cluster_id,
            run_id,
            data_token,
            admin_token,
            enrollment_token,
            worker_configs,
            api_enrollments,
            mounts,
        })
    }

    fn identity(enrollment: &Value) -> Value {
        json!({
            "worker_id": enrollment["worker_id"],
            "incarnation": enrollment["incarnation"],
            "enrollment_revision": enrollment["enrollment_revision"],
            "certificate_sha256": enrollment["certificate_sha256"],
        })
    }

    fn peer_inventory(mut peers: Vec<Value>) -> (Vec<Value>, Vec<String>) {
        peers.sort_by(|a, b| a["worker_id"].as_str().cmp(&b["worker_id"].as_str()));
        let mut pins: Vec<_> = peers
            .iter()
            .map(|peer| peer["certificate_sha256"].as_str().unwrap().to_owned())
            .collect();
        pins.sort_unstable();
        (peers, pins)
    }

    pub fn certificate_digest(&self, node: u64) -> [u8; 32] {
        let enrollment = if let Some(index) = WORKERS.iter().position(|id| *id == node) {
            &self.worker_configs[index]["node"]["enrollment"]
        } else {
            let index = GATEWAYS
                .iter()
                .position(|id| *id == node)
                .expect("known process");
            &self.api_enrollments[index]["enrollment"]
        };
        *Digest::parse(enrollment["certificate_sha256"].as_str().unwrap())
            .expect("generated SHA-256 fingerprint")
            .as_bytes()
    }

    fn write_material(&self, node: u64) -> io::Result<()> {
        let (cert, key) = material(node);
        write(CERT, cert)?;
        write(KEY, key)?;
        write(CA, certificate_authority())?;
        write_json(
            AUTH,
            &json!({
                "data_token": self.data_token,
                "admin_token": self.admin_token,
                "enrollment_token": self.enrollment_token,
            }),
        )
    }

    pub fn write_worker(&self, index: usize) -> io::Result<()> {
        let node = WORKERS[index];
        self.write_material(node)?;
        let mut config = self.worker_configs[index].clone();
        let pools = self.mounts.state().pools[index].clone();
        if !pools.is_empty() {
            config["node"]["secondary_pools"] = json!(pools);
        }
        write_json(
            MARKER,
            &json!({
                "format_version": 1,
                "volume_name": format!("sim-oltp-{node}"),
                "volume_marker": config["node"]["mount"]["profile"]["docker"]["volume_marker"],
            }),
        )?;
        write_json(CONFIG, &config)
    }

    pub fn api_enrollment(&self, index: usize) -> &Value {
        &self.api_enrollments[index]
    }

    pub fn write_gateway(&self, index: usize, connection: &Value) -> io::Result<()> {
        let node = GATEWAYS[index];
        self.write_material(node)?;
        write_json(
            CONFIG,
            &json!({
                "version": 1,
                "engine": "oltp",
                "bind": SocketAddr::from(([127, 0, 0, 1], http_addr(node).port())),
                "database": DATABASE,
                "auth_file": AUTH,
                "topology": {
                    "mode": "remote",
                    "cluster_id": self.cluster_id,
                    "control_group_id": connection["control_group_id"],
                    "control_peers": connection["control_peers"],
                    "enrollment": self.api_enrollment(index),
                    "worker_tls": {"cert_file": CERT, "key_file": KEY, "ca_file": CA},
                    "direct_oltp": true,
                },
                "worker": {"resources": resources(), "execution": execution(), "queue_size": 128},
            }),
        )
    }

    /// The TLS material of one process.
    pub fn tls(&self, node: u64) -> io::Result<TlsMaterial> {
        let (cert, key) = material(node);
        TlsMaterial::from_pem(cert, key, certificate_authority())
    }

    /// The primary pool in worker `index`'s bootstrap file.
    pub fn pool(&self, index: usize) -> Value {
        self.worker_configs[index]["node"]["pool_id"].clone()
    }

    /// Make worker `index`'s bootstrap file claim another primary pool on the same mount.
    pub fn claim_pool(&mut self, index: usize, pool: &Value) {
        let node = &mut self.worker_configs[index]["node"];
        node["pool_id"] = pool.clone();
        node["mount"]["identity"]["pool_id"] = pool.clone();
    }

    /// The enrollment the worker's bootstrap file presents.
    pub fn worker_enrollment(&self, index: usize) -> &Value {
        &self.worker_configs[index]["node"]["enrollment"]
    }

    /// A retained same-run restart of one worker, as `retained_config` in
    /// `tests/oltp_recovery_operations.py` prepares it: a fresh incarnation and
    /// enrollment revision on the same certificate, pool and mounted volume.
    /// Returns the plan id, the `worker.restart.prepare` payload and the next enrollment.
    pub fn rolling_plan(&self, index: usize) -> (Uuid, Value, Value) {
        let node = &self.worker_configs[index]["node"];
        let previous = &node["enrollment"];
        let mut next = previous.clone();
        next["incarnation"] = json!(uuid_v4());
        next["enrollment_revision"] = json!(previous["enrollment_revision"].as_u64().unwrap() + 1);
        let id = uuid_v4();
        let plan = json!({
            "id": id,
            "run_id": self.run_id,
            "previous": Self::identity(previous),
            "next": next,
            "pool_id": node["pool_id"],
            "mount_generation": node["mount"]["generation"].as_u64().unwrap() + 1,
        });
        (id, plan, next)
    }

    /// Record a prepared retained restart in the worker's next bootstrap file.
    pub fn apply_rolling(
        &mut self,
        index: usize,
        plan: Uuid,
        next: Value,
        identity: Value,
        control_peers: &Value,
    ) {
        let run_id = self.run_id;
        let node = &mut self.worker_configs[index]["node"];
        node["enrollment"] = next;
        node["control_enrollments"] = control_peers.clone();
        node["rolling"] = json!({"preparation_id": plan, "run_id": run_id});
        if let Some(fields) = node.as_object_mut() {
            fields.remove("cold");
        }
        self.mounts.advance(index, &identity);
        let mount = &mut node["mount"];
        let generation = mount["generation"].as_u64().unwrap();
        mount["worker"] = identity;
        mount["generation"] = json!(generation + 1);
    }

    /// Update the original topology through the same explicit cold-start profile as RF3.
    /// The durable volumes and peer certificate pins stay in place.
    pub fn prepare_cold_start(&mut self) {
        let next_run = uuid_v4();
        let profile = json!({
            "previous_run": self.run_id,
            "next_run": next_run,
            "transition_id": uuid_v4(),
        });
        for (index, config) in self.worker_configs.iter_mut().enumerate() {
            let node = &mut config["node"];
            let enrollment = &mut node["enrollment"];
            let revision = enrollment["enrollment_revision"].as_u64().unwrap();
            enrollment["enrollment_revision"] = json!(revision + 1);
            enrollment["incarnation"] = json!(uuid_v4());
            let worker = Self::identity(enrollment);
            self.mounts.advance(index, &worker);
            let mount = &mut node["mount"];
            let generation = mount["generation"].as_u64().unwrap();
            mount["worker"] = worker;
            mount["generation"] = json!(generation + 1);
            node["cold"] = profile.clone();
            if let Some(fields) = node.as_object_mut() {
                fields.remove("rolling");
            }
        }
        let (peers, pins) = Self::peer_inventory(
            self.worker_configs
                .iter()
                .map(|config| config["node"]["enrollment"].clone())
                .collect(),
        );
        for config in &mut self.worker_configs {
            config["node"]["control_enrollments"] = json!(peers);
            config["node"]["operator_pins"] = json!(pins);
        }
        self.run_id = next_run;
    }
}
