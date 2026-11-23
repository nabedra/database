//! Controller renewals and copy synchronization run outside client turns.
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use nabedra_rt::time::{self, MissedTickBehavior, interval, timeout};
use serde::Serialize;
use tokio::sync::oneshot;

use crate::cache::command::SLOT_COUNT;

use super::super::protocol::{
    CONTROL_FRAME, Heartbeat, Member, Progress, RENEW_MS, Reply, Request, Topology, Update,
    VERSION, WorkerId,
};
use super::super::transport::{Endpoint, PeerConnection, PeerTls, bounded_json};
use super::actor::{ActorReport, Control, Transfer};
use super::peer::{self, Data, Identity, PROBES, Probe};
use super::{OUTBOUND_SESSIONS, PEER_FRAME, Worker, invalid};

const LOCAL_DEADLINE: Duration = Duration::from_millis(250);
const REPLICATION_POLL: Duration = Duration::from_millis(25);

pub(super) async fn control(
    worker: Worker,
    tls: PeerTls,
    member: Member,
    controllers: BTreeMap<u64, Endpoint>,
    metrics_file: Option<PathBuf>,
) -> io::Result<()> {
    let mut client = Controller::new(tls, controllers);
    let mut renewals = interval(Duration::from_millis(RENEW_MS));
    renewals.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        renewals.tick().await;
        if matches!(
            client.request(&Request::Enroll(member.clone())).await,
            Some(Reply::Applied { .. })
        ) {
            break;
        }
    }
    loop {
        if let Err(error) = renew(&worker, &mut client, metrics_file.as_deref()).await {
            eprintln!(
                "nabedra-db: cache worker {} rejected update: {error}",
                member.id.node
            );
        }
        renewals.tick().await;
    }
}

async fn renew(
    worker: &Worker,
    client: &mut Controller,
    metrics_file: Option<&Path>,
) -> io::Result<()> {
    let topology = worker.topology();
    let mut reports = progress(worker, metrics_file.is_some()).await;
    reports.retain(|report| {
        report.generation == topology.generation && report.revision == topology.revision
    });
    if let Some(path) = metrics_file {
        let _ = publish_metrics(path, worker, &topology, &reports);
    }
    let request = Request::Heartbeat(Heartbeat {
        version: VERSION,
        generation: topology.generation,
        revision: topology.revision,
        worker: worker.shared.id,
        progress: reports
            .into_iter()
            .flat_map(|report| report.slots)
            .collect(),
    });
    drop(topology);
    if let Some(Reply::Update(update)) = client.request(&request).await {
        install(worker, update).await?;
    }
    Ok(())
}

#[cfg(all(test, feature = "sim"))]
pub(in crate::cache::cluster) async fn sim_renew_once(
    worker: &Worker,
    tls: PeerTls,
    endpoint: Endpoint,
) -> io::Result<()> {
    let mut client = Controller::new(tls, BTreeMap::from([(0, endpoint)]));
    renew(worker, &mut client, None).await
}

struct Controller {
    tls: PeerTls,
    peers: Vec<Endpoint>,
    preferred: usize,
    session: Option<PeerConnection>,
}

impl Controller {
    fn new(tls: PeerTls, peers: BTreeMap<u64, Endpoint>) -> Self {
        Self {
            tls,
            peers: peers.into_values().collect(),
            preferred: 0,
            session: None,
        }
    }

    async fn request(&mut self, request: &Request) -> Option<Reply> {
        for distance in 0..self.peers.len() {
            let index = (self.preferred + distance) % self.peers.len();
            let session = if distance == 0 {
                self.session.take()
            } else {
                None
            };
            let mut session = match session {
                Some(session) => session,
                None => match self.tls.connect(&self.peers[index]).await {
                    Ok(session) => session,
                    Err(_) => continue,
                },
            };
            match session.json::<_, Reply>(request, CONTROL_FRAME).await {
                Ok(response @ (Reply::Applied { .. } | Reply::Update(_))) => {
                    self.preferred = index;
                    self.session = Some(session);
                    return Some(response);
                }
                _ => {}
            }
        }
        None
    }
}

pub(super) async fn progress(worker: &Worker, telemetry: bool) -> Vec<ActorReport> {
    let mut receivers = Vec::with_capacity(worker.shared.actors.len());
    for actor in &worker.shared.actors {
        let (send, recv) = oneshot::channel();
        if actor
            .control
            .try_send(Control::Progress {
                telemetry,
                reply: send,
            })
            .is_ok()
        {
            receivers.push(recv);
        }
    }
    let mut reports = Vec::with_capacity(receivers.len());
    let due = time::now() + LOCAL_DEADLINE;
    for receiver in receivers {
        if let Ok(Ok(report)) = time::timeout_at(due, receiver).await {
            reports.push(report);
        }
    }
    reports
}

async fn install(worker: &Worker, update: Update) -> io::Result<()> {
    let previous = worker.topology();
    if update.version != VERSION
        || update.worker != worker.shared.id
        || update.generation < previous.generation
        || update.revision < previous.revision
    {
        return Err(invalid("incompatible controller update"));
    }
    let topology = match update.topology {
        Some(topology)
            if topology.generation == update.generation
                && topology.revision == update.revision
                && topology.slots.len() == SLOT_COUNT
                && topology.workers.len() <= super::MAX_WORKERS
                && topology.member(worker.shared.id).is_some() =>
        {
            Arc::new(topology)
        }
        None if previous.generation == update.generation
            && previous.revision == update.revision =>
        {
            previous
        }
        _ => return Err(invalid("controller update omitted a valid topology")),
    };
    let mut leases = vec![None; SLOT_COUNT];
    for grant in update.grants {
        if !grant.range.valid() || grant.lease.not_before_ms >= grant.lease.expires_ms {
            return Err(invalid("invalid controller lease grant"));
        }
        for slot in grant.range.first..=grant.range.last {
            let slot = usize::from(slot);
            let placement = topology.slots[slot];
            if placement.primary != Some(worker.shared.id)
                || placement.epoch != grant.range.epoch
                || leases[slot].replace(grant.lease).is_some()
            {
                return Err(invalid("lease recipient, epoch or range mismatch"));
            }
        }
    }
    let leases = Arc::new(leases);
    timeout(LOCAL_DEADLINE, async {
        let mut receivers = Vec::with_capacity(worker.shared.actors.len());
        for actor in &worker.shared.actors {
            let (done, finished) = oneshot::channel();
            actor
                .control
                .send(Control::Install {
                    topology: Arc::clone(&topology),
                    grants: Arc::clone(&leases),
                    done,
                })
                .await
                .map_err(|_| io::Error::other("cache actor exited"))?;
            receivers.push(finished);
        }
        for receiver in receivers {
            receiver.await.map_err(io::Error::other)?;
        }
        Ok::<(), io::Error>(())
    })
    .await??;
    worker.set_topology(topology);
    Ok(())
}

#[derive(Serialize)]
struct Metrics<'a> {
    worker: WorkerId,
    generation: u64,
    revision: u64,
    slots: Vec<&'a Progress>,
    actor_count: usize,
    actors: &'a [ActorReport],
    rss_bytes: Option<u64>,
}

fn publish_metrics(
    path: &Path,
    worker: &Worker,
    topology: &Topology,
    reports: &[ActorReport],
) -> io::Result<()> {
    let metrics = Metrics {
        worker: worker.shared.id,
        generation: topology.generation,
        revision: topology.revision,
        slots: reports.iter().flat_map(|report| &report.slots).collect(),
        actor_count: worker.shared.actors.len(),
        actors: reports,
        rss_bytes: rss_bytes(),
    };
    let payload = bounded_json(&metrics, CONTROL_FRAME)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, payload)?;
    std::fs::rename(tmp, path)
}

fn rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))?;
    line.split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?
        .checked_mul(1024)
}

#[derive(Default)]
struct Lane {
    cursor: usize,
    session: Option<PeerConnection>,
    next_attempt: Option<Instant>,
    last_used: Option<Instant>,
    failures: u32,
}

pub(super) async fn replicate(worker: Worker, tls: PeerTls) -> io::Result<()> {
    let mut lanes: BTreeMap<WorkerId, Lane> = BTreeMap::new();
    let mut topology = worker.topology();
    let mut groups = groups(&topology, worker.shared.id);
    let mut polls = interval(REPLICATION_POLL);
    polls.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        polls.tick().await;
        let current = worker.topology();
        if !Arc::ptr_eq(&current, &topology) {
            topology = current;
            groups = self::groups(&topology, worker.shared.id);
            lanes.retain(|source, _| groups.contains_key(source));
        }
        for (&source, group) in &groups {
            let Some(member) = topology.member(source) else {
                continue;
            };
            if lanes
                .get(&source)
                .and_then(|lane| lane.next_attempt)
                .is_some_and(|next| next > time::now_std())
            {
                continue;
            }
            cap_connections(&mut lanes, source);
            let lane = lanes.entry(source).or_default();
            let count = group.len().min(PROBES);
            let cursor = lane.cursor;
            let selected = (0..count).map(|index| group[(index + cursor) % group.len()]);
            lane.cursor = (cursor + count) % group.len();
            let probes = sample(&worker, source, topology.generation, selected).await;
            if probes.is_empty() {
                continue;
            }
            let identity = Identity {
                generation: topology.generation,
                source: worker.shared.id,
                destination: source,
            };
            match copy(&worker, &tls, &member.peer, identity, &probes, lane).await {
                Ok(()) => {
                    lane.last_used = Some(time::now_std());
                    lane.failures = 0;
                    lane.next_attempt = None;
                }
                Err(_) => {
                    lane.failures = (lane.failures + 1).min(4);
                    lane.session = None;
                    lane.next_attempt =
                        Some(time::now_std() + Duration::from_millis(100 << lane.failures));
                }
            }
        }
    }
}

fn cap_connections(lanes: &mut BTreeMap<WorkerId, Lane>, source: WorkerId) {
    if lanes
        .get(&source)
        .is_some_and(|lane| lane.session.is_some())
    {
        return;
    }
    let mut open = 0;
    let oldest = lanes
        .values_mut()
        .filter(|lane| {
            if lane.session.is_none() {
                false
            } else {
                open += 1;
                true
            }
        })
        .min_by_key(|lane| lane.last_used);
    if open >= OUTBOUND_SESSIONS
        && let Some(oldest) = oldest
    {
        oldest.session = None;
    }
}

fn groups(topology: &Topology, worker: WorkerId) -> BTreeMap<WorkerId, Vec<(u16, u64)>> {
    let mut groups = BTreeMap::<_, Vec<_>>::new();
    for (index, slot) in topology.slots.iter().enumerate() {
        if (slot.replica == Some(worker)
            || slot.moving.is_some_and(|intent| intent.target == worker))
            && let Some(owner) = slot.primary
            && owner != worker
        {
            groups
                .entry(owner)
                .or_default()
                .push((index as u16, slot.epoch));
        }
    }
    groups
}

async fn sample(
    worker: &Worker,
    source: WorkerId,
    generation: u64,
    slots: impl Iterator<Item = (u16, u64)>,
) -> Vec<Probe> {
    let mut groups = vec![Vec::new(); worker.shared.actors.len()];
    for point in slots {
        groups[usize::from(point.0) % worker.shared.actors.len()].push(point);
    }
    let mut pending = Vec::with_capacity(groups.len());
    for (index, slots) in groups.into_iter().enumerate() {
        let (reply, result) = oneshot::channel();
        if worker.shared.actors[index]
            .transfer
            .try_send(Transfer::Sample {
                source,
                generation,
                slots,
                reply,
            })
            .is_ok()
        {
            pending.push(result);
        }
    }
    let until = time::now() + LOCAL_DEADLINE;
    let mut probes = Vec::new();
    for result in pending {
        if let Ok(Ok(group)) = time::timeout_at(until, result).await {
            probes.extend(group);
        }
    }
    probes.sort_unstable_by_key(|probe| (probe.resume.is_none(), probe.slot));
    probes.truncate(PROBES);
    probes.sort_unstable_by_key(|probe| probe.slot);
    probes
}

async fn copy(
    worker: &Worker,
    tls: &PeerTls,
    endpoint: &Endpoint,
    identity: Identity,
    probes: &[Probe],
    lane: &mut Lane,
) -> io::Result<()> {
    let mut connection = match lane.session.take() {
        Some(connection) => connection,
        None => tls.connect(endpoint).await?,
    };
    let response = connection
        .exchange(&peer::encode_fetch(identity, probes), PEER_FRAME)
        .await?;
    lane.session = Some(connection);
    #[derive(Clone, Copy)]
    enum Received {
        Unseen,
        Sequenced(u64),
        Snapshot,
    }
    let mut responded = [Received::Unseen; PROBES];
    let mut packets = peer::replies(&response, identity.swapped())?;
    loop {
        let Ok(permit) = Arc::clone(&worker.shared.copy_handoffs).try_acquire_owned() else {
            break;
        };
        let Some(packet) = packets.next() else {
            break;
        };
        let packet = packet?;
        let index = probes
            .binary_search_by_key(&packet.slot, |probe| probe.slot)
            .map_err(|_| invalid("unsolicited replica payload"))?;
        let probe = probes[index];
        if packet.epoch != probe.epoch {
            return Err(invalid("stale replica payload"));
        }
        responded[index] = match (&packet.data, responded[index]) {
            (Data::Delta { sequence, .. }, Received::Unseen)
                if probe.baseline && probe.sequence.checked_add(1) == Some(*sequence) =>
            {
                Received::Sequenced(*sequence)
            }
            (Data::Delta { sequence, .. }, Received::Sequenced(previous))
                if previous.checked_add(1) == Some(*sequence) =>
            {
                Received::Sequenced(*sequence)
            }
            (Data::Empty(_) | Data::Snapshot(_), Received::Unseen) => Received::Snapshot,
            _ => return Err(invalid("duplicate or noncontiguous replica payload")),
        };
        let (reply, result) = oneshot::channel();
        let actor = worker.actor(packet.slot);
        if actor
            .transfer
            .try_send(Transfer::Apply {
                permit,
                source: identity.destination,
                generation: identity.generation,
                packet,
                reply,
            })
            .is_ok()
        {
            let _ = timeout(LOCAL_DEADLINE, result).await;
        }
    }
    Ok(())
}
