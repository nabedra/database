//! Bounded worker-to-worker replication bound to the slot epoch and both incarnations.
mod codec;

use std::io;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::cache::store::Effects;

use super::super::protocol::WorkerId;
use super::super::transport::Fingerprint;
use super::actor::Transfer;
use super::{PEER_FRAME, Worker};

#[cfg(test)]
pub(super) use codec::single_record_for_test;

use codec::{decode_fetch, extend, header};
pub(super) use codec::{
    encode_delta, encode_empty, encode_fetch, encode_snapshot, hash_effects, replies,
};

pub(in crate::cache::cluster) const PROBES: usize = 256;
pub(super) const DELTAS_PER_PROBE: usize = 8;
pub(super) const CHUNK_BYTES: usize = (1 << 20) + (64 << 10);
pub(super) const MAX_ITEMS: usize = 1024;
const HEADER: usize = 2 + 1 + 8 + 2 * (8 + 16);
const PROBE_LENGTH: usize = 2 + 8 + 8 + 1 + 16 + 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Identity {
    pub generation: u64,
    pub source: WorkerId,
    pub destination: WorkerId,
}

impl Identity {
    pub fn swapped(self) -> Self {
        Self {
            source: self.destination,
            destination: self.source,
            ..self
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Resume {
    pub id: Uuid,
    pub offset: u32,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Probe {
    pub slot: u16,
    pub epoch: u64,
    pub sequence: u64,
    pub baseline: bool,
    pub resume: Option<Resume>,
}

pub(super) struct Packet {
    pub slot: u16,
    pub epoch: u64,
    pub data: Data,
}

pub(super) enum Data {
    Empty(u64),
    Delta {
        sequence: u64,
        effects: Effects,
        digest: [u8; 32],
    },
    Snapshot(Chunk),
}

pub(super) struct Chunk {
    pub id: Uuid,
    pub cut: u64,
    pub offset: u32,
    pub effects: Effects,
    pub checksum: Option<[u8; 32]>,
}

pub(super) async fn handle(
    worker: &Worker,
    certificate: Fingerprint,
    request: &[u8],
) -> io::Result<Vec<u8>> {
    let (identity, probes) = decode_fetch(request)?;
    let topology = worker.topology();
    if identity.destination != worker.shared.id
        || identity.generation != topology.generation
        || !topology
            .member(identity.source)
            .is_some_and(|member| member.authenticates(certificate))
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "invalid cache replication peer identity",
        ));
    }
    drop(topology);
    let mut groups = vec![Vec::new(); worker.shared.actors.len()];
    for probe in probes {
        groups[usize::from(probe.slot) % worker.shared.actors.len()].push(probe);
    }
    let mut output = Vec::with_capacity(HEADER);
    header(&mut output, 2, identity.swapped());
    let mut groups: Vec<_> = groups.into_iter().enumerate().collect();
    groups.sort_by_key(|(_, probes)| !probes.iter().any(|probe| probe.resume.is_some()));
    for (index, mut probes) in groups {
        if probes.is_empty() {
            continue;
        }
        probes.sort_by_key(|probe| probe.resume.is_none());
        let limit = PEER_FRAME - output.len();
        if limit < 128 {
            break;
        }
        let (reply, result) = oneshot::channel();
        let message = Transfer::Fetch {
            source: identity.source,
            generation: identity.generation,
            probes,
            limit,
            reply,
        };
        if worker.shared.actors[index]
            .transfer
            .try_send(message)
            .is_ok()
            && let Ok(bytes) = result.await
        {
            extend(&mut output, &bytes, PEER_FRAME)?;
        }
    }
    Ok(output)
}
