//! Rust counterpart of the RF3 fixture in tests/oltp_direct_cluster.py.
//! All paths refer to the calling node's own volume.

use std::{
    io::{self, Write},
    net::SocketAddr,
    path::Path,
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
/// Workers that a run may add. They have certificates, but no process starts unless the
/// workload asks for it.
pub(crate) const SPARES: [u64; 4] = [4, 5, 6, 7];
pub(crate) const ROOT: &str = "/sim-oltp";
/// The key volume of a worker whose volumes are split (see [`Bootstrap::split_volumes`]).
pub(crate) const KEYS: &str = "/sim-oltp-keys";
pub(crate) const CONFIG: &str = "/sim-oltp/bootstrap.json";
const AUTH: &str = "/sim-oltp/auth.json";
const CERT: &str = "/sim-oltp/node.pem";
const KEY: &str = "/sim-oltp/node.key";
const CA: &str = "/sim-oltp/ca.pem";
const RESTART_KEY: &str = "NABEDRA_RESTART_KEY";
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
        4 => pem!("4"),
        5 => pem!("5"),
        6 => pem!("6"),
        7 => pem!("7"),
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
    os::sync_dir(Path::new(path).parent().expect("absolute path"))
}

/// `path`'s file name in `dir`.
fn placed(dir: &str, path: &str) -> String {
    let (_, name) = path.rsplit_once('/').expect("absolute path");
    format!("{dir}/{name}")
}

fn write_json(path: &str, value: &Value) -> io::Result<()> {
    write(path, &serde_json::to_vec(value)?)
}

/// The Docker volume marker on the calling node's data volume.
fn write_marker(node: u64, marker: &Value) -> io::Result<()> {
    write_json(
        MARKER,
        &json!({
            "format_version": 1,
            "volume_name": format!("sim-oltp-{node}"),
            "volume_marker": marker,
        }),
    )
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

/// A provisioned replacement of a lost worker.
pub(crate) struct Candidate {
    pub spare: u64,
    /// The slot of the worker it replaces.
    pub index: usize,
    /// The `worker.system_replacement.prepare` payload.
    pub intent: Value,
    config: Value,
}

pub(crate) struct Bootstrap {
    pub cluster_id: Uuid,
    pub run_id: Uuid,
    pub data_token: String,
    pub admin_token: String,
    enrollment_token: String,
    worker_configs: [Value; 3],
    api_enrollments: [Value; 2],
    /// The process that runs each of `worker_configs`. A replacement takes a slot over.
    nodes: [u64; 3],
    /// Workers keep their keys and their bootstrap file on a volume of their own.
    split: bool,
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
        let worker_configs = std::array::from_fn(|local| {
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
        Ok(Self {
            cluster_id,
            run_id,
            data_token,
            admin_token,
            enrollment_token,
            worker_configs,
            api_enrollments,
            nodes: WORKERS,
            split: false,
        })
    }

    /// The workers that form the cluster, in slot order.
    pub fn nodes(&self) -> [u64; 3] {
        self.nodes
    }

    pub fn node(&self, index: usize) -> u64 {
        self.nodes[index]
    }

    pub fn index_of(&self, node: u64) -> Option<usize> {
        self.nodes.iter().position(|worker| *worker == node)
    }

    /// Opt in, before any worker starts: every worker's certificate, private key, CA, tokens
    /// and bootstrap file live on the volume at [`KEYS`], as a deployment mounts its secrets
    /// apart from its data volume. Only the pool and its volume marker stay at [`ROOT`].
    pub fn split_volumes(&mut self) {
        self.split = true;
        for config in &mut self.worker_configs {
            config["auth_file"] = json!(placed(KEYS, AUTH));
            for (field, path) in [
                ("certificate", CERT),
                ("private_key", KEY),
                ("authority_ca", CA),
            ] {
                config["node"][field] = json!(placed(KEYS, path));
            }
        }
    }

    pub fn is_split(&self) -> bool {
        self.split
    }

    /// The volume that holds `node`'s keys and bootstrap file.
    fn keys(&self, node: u64) -> &'static str {
        if self.split && !GATEWAYS.contains(&node) {
            KEYS
        } else {
            ROOT
        }
    }

    /// The bootstrap file of `node`.
    pub fn config_path(&self, node: u64) -> String {
        placed(self.keys(node), CONFIG)
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
        let enrollment = if let Some(index) = self.index_of(node) {
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
        let dir = self.keys(node);
        write(&placed(dir, CERT), cert)?;
        write(&placed(dir, KEY), key)?;
        write(&placed(dir, CA), certificate_authority())?;
        write_json(
            &placed(dir, AUTH),
            &json!({
                "data_token": self.data_token,
                "admin_token": self.admin_token,
                "enrollment_token": self.enrollment_token,
            }),
        )
    }

    pub fn write_worker(&self, index: usize) -> io::Result<()> {
        let node = self.nodes[index];
        self.write_material(node)?;
        let config = &self.worker_configs[index];
        write_marker(
            node,
            &config["node"]["mount"]["profile"]["docker"]["volume_marker"],
        )?;
        write_json(&self.config_path(node), config)
    }

    /// Write only worker `index`'s bootstrap file, as a restart of a worker whose key volume
    /// survived does. The data volume is not touched.
    pub fn write_bootstrap_file(&self, index: usize) -> io::Result<()> {
        write_json(
            &self.config_path(self.nodes[index]),
            &self.worker_configs[index],
        )
    }

    /// Write the marker that worker `index`'s bootstrap file names on its data volume.
    pub fn write_kept_marker(&self, index: usize) -> io::Result<()> {
        write_marker(
            self.nodes[index],
            &self.worker_configs[index]["node"]["mount"]["profile"]["docker"]["volume_marker"],
        )
    }

    /// Write the marker of a newly provisioned data volume.
    pub fn write_fresh_marker(&self, index: usize) -> io::Result<()> {
        write_marker(self.nodes[index], &json!(uuid_v4()))
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

    /// The certificate fingerprint of a spare worker that no slot names yet.
    pub fn spare_digest(spare: u64) -> io::Result<[u8; 32]> {
        let (cert, key) = material(spare);
        let tls = TlsMaterial::from_pem(cert, key, certificate_authority())?;
        Ok(*Digest::parse(tls.certificate_sha256())
            .expect("generated SHA-256 fingerprint")
            .as_bytes())
    }

    /// Provision `spare` as the replacement of the permanently lost worker `index`, as step 3
    /// of "Replace a permanently lost system voter" in `docs/oltp-recovery.md` says: a fresh
    /// worker ID, incarnation, certificate and pool at revision 1 and generation 1, with its
    /// own signing key on the mount. Runs on the spare and writes to its volumes.
    pub fn provision_replacement(
        &self,
        index: usize,
        spare: u64,
        id: Uuid,
        control_peers: &Value,
    ) -> io::Result<Candidate> {
        // A preparation that never enrolled does not change who was lost.
        let lost = &self.worker_enrollment(index)["worker_id"];
        let previous = control_peers
            .as_array()
            .into_iter()
            .flatten()
            .find(|peer| peer["worker_id"] == *lost)
            .ok_or_else(|| io::Error::other("Control does not list the lost worker"))?;
        let next = enrollment(spare)?;
        let (pool_id, marker) = (uuid_v4(), uuid_v4());
        let mut seed = [0; 32];
        seed[..16].copy_from_slice(uuid_v4().as_bytes());
        seed[16..].copy_from_slice(uuid_v4().as_bytes());
        let key = ring::signature::Ed25519KeyPair::from_seed_unchecked(&seed)
            .map_err(|_| io::Error::other("replacement mount key"))?;
        let public_key: String = ring::signature::KeyPair::public_key(&key)
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let mount = json!({
            "profile": {"docker": {"volume_marker": marker}},
            "worker": Self::identity(&next),
            "generation": 1,
            "identity": {
                "engine": "oltp", "pool_format_version": POOL_FORMAT_VERSION,
                "cluster_id": self.cluster_id, "worker_id": next["worker_id"], "pool_id": pool_id,
            },
            "mount_path": ROOT,
            "provider_volume_id": format!("docker-volume:sim-oltp-{spare}"),
            "filesystem_uuid": "",
        });
        self.write_material(spare)?;
        write_marker(spare, &json!(marker))?;
        write(&format!("{ROOT}/{RESTART_KEY}"), &seed)?;
        let intent = json!({
            "id": id,
            "run_id": self.run_id,
            "previous": Self::identity(previous),
            "next": next,
            "mount": mount,
            "mount_public_key": public_key,
        });
        let mut config = self.worker_configs[index].clone();
        config["bind"] = json!(http_addr(spare));
        let node = &mut config["node"];
        let fields = node.as_object_mut().expect("node configuration");
        for field in ["cold", "rolling", "secondary_pools", "system_replacement"] {
            fields.remove(field);
        }
        node["pool_id"] = json!(pool_id);
        node["scheduler_node_id"] = json!(spare);
        node["enrollment"] = next;
        node["bind"] = json!(peer_addr(spare));
        node["mount"] = mount;
        node["control_enrollments"] = control_peers.clone();
        node["initial_run"] = json!(self.run_id);
        Ok(Candidate {
            spare,
            index,
            intent,
            config,
        })
    }

    /// Write the bootstrap file of a replacement whose intent Control prepared as `record`.
    /// Runs on the spare.
    pub fn write_candidate(&self, candidate: &mut Candidate, record: &Value) -> io::Result<()> {
        candidate.config["node"]["system_replacement"] = record.clone();
        write_json(&self.config_path(candidate.spare), &candidate.config)
    }

    /// The replacement is complete: it takes over the lost worker's slot, and every worker's
    /// next bootstrap file names the current voters (step 7).
    pub fn adopt(&mut self, candidate: Candidate, groups: &Value) {
        let Candidate {
            spare,
            index,
            mut config,
            ..
        } = candidate;
        config["node"]
            .as_object_mut()
            .expect("node configuration")
            .remove("system_replacement");
        self.worker_configs[index] = config;
        self.nodes[index] = spare;
        let mut pins: Vec<String> = self
            .worker_configs
            .iter()
            .filter_map(|config| {
                let enrollment = &config["node"]["enrollment"];
                Some(enrollment["certificate_sha256"].as_str()?.to_owned())
            })
            .collect();
        pins.sort_unstable();
        for config in &mut self.worker_configs {
            config["node"]["operator_pins"] = json!(pins);
            let placements = config["node"]["placements"]
                .as_object_mut()
                .expect("system placements");
            for placement in placements.values_mut() {
                for group in groups.as_array().into_iter().flatten() {
                    if group["record"]["identity"] == placement["identity"] {
                        let mut voters = group["members"].as_array().cloned().unwrap_or_default();
                        voters.sort_by_key(|voter| voter["node_id"].as_u64());
                        placement["voters"] = json!(voters);
                    }
                }
            }
        }
    }

    /// The current mounted identity of worker `index` (its `mount.worker` field).
    pub fn worker_identity(&self, index: usize) -> Value {
        self.worker_configs[index]["node"]["mount"]["worker"].clone()
    }

    /// A Docker-profile mount record for a secondary pool on worker `index` at `path`,
    /// as `mount_pool` in `tests/oltp_direct_cluster.py` builds it. Writes the volume
    /// marker on the calling node's volume.
    pub fn secondary_pool(&self, index: usize, path: &str, pool_id: Uuid) -> io::Result<Value> {
        let node = self.nodes[index];
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
        let worker = &self.worker_configs[index]["node"]["mount"]["worker"];
        Ok(json!({
            "profile": {"docker": {"volume_marker": marker}},
            "worker": worker,
            "generation": 1,
            "identity": {
                "engine": "oltp", "pool_format_version": POOL_FORMAT_VERSION,
                "cluster_id": self.cluster_id, "worker_id": worker["worker_id"], "pool_id": pool_id,
            },
            "mount_path": path,
            "provider_volume_id": format!("docker-volume:{volume}"),
            "filesystem_uuid": "",
        }))
    }

    /// List a mounted secondary pool in the worker's bootstrap file. A restarted worker opens
    /// only the pools its bootstrap lists, so a restart without this entry can never host the
    /// replicas that live there.
    pub fn list_secondary_pool(&mut self, index: usize, mount: &Value, claim: Uuid) {
        self.worker_configs[index]["node"]["secondary_pools"] =
            json!([{"mount": mount, "growth_claim": claim}]);
    }

    /// Move a node's secondary pool mounts to the worker identity of its next process.
    fn advance_secondary(node: &mut Value, worker: &Value) {
        let pools = node
            .get_mut("secondary_pools")
            .and_then(Value::as_array_mut);
        for pool in pools.into_iter().flatten() {
            let mount = &mut pool["mount"];
            let generation = mount["generation"].as_u64().unwrap();
            mount["worker"] = worker.clone();
            mount["generation"] = json!(generation + 1);
        }
    }

    /// The TLS material of one process.
    pub fn tls(&self, node: u64) -> io::Result<TlsMaterial> {
        let (cert, key) = material(node);
        TlsMaterial::from_pem(cert, key, certificate_authority())
    }

    /// The system groups as the bootstrap files place them: the label, the group and its voters.
    pub fn system_groups(&self) -> Vec<(String, Uuid, Vec<Value>)> {
        self.worker_configs[0]["node"]["placements"]
            .as_object()
            .into_iter()
            .flatten()
            .filter_map(|(label, placement)| {
                let group = placement["identity"]["group_id"].as_str()?;
                Some((
                    label.clone(),
                    Uuid::parse_str(group).ok()?,
                    placement["voters"].as_array()?.clone(),
                ))
            })
            .collect()
    }

    /// The secondary pools that worker `index` opens when it starts.
    pub fn secondary_pools(&self, index: usize) -> Vec<Value> {
        self.worker_configs[index]["node"]["secondary_pools"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    }

    /// List `pools` before the ones worker `index` already opens.
    pub fn list_more_pools(&mut self, index: usize, mut pools: Vec<Value>) {
        pools.extend(self.secondary_pools(index));
        self.worker_configs[index]["node"]["secondary_pools"] = json!(pools);
    }

    /// The worker no longer has a secondary pool to open.
    pub fn unlist_secondary_pools(&mut self, index: usize) {
        if let Some(node) = self.worker_configs[index]["node"].as_object_mut() {
            node.remove("secondary_pools");
        }
    }

    /// The generation of worker `index`'s primary mount.
    pub fn mount_generation(&self, index: usize) -> Value {
        self.worker_configs[index]["node"]["mount"]["generation"].clone()
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
        Self::advance_secondary(node, &identity);
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
        for config in &mut self.worker_configs {
            let node = &mut config["node"];
            let enrollment = &mut node["enrollment"];
            let revision = enrollment["enrollment_revision"].as_u64().unwrap();
            enrollment["enrollment_revision"] = json!(revision + 1);
            enrollment["incarnation"] = json!(uuid_v4());
            let worker = Self::identity(enrollment);
            Self::advance_secondary(node, &worker);
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
