use bytes::{Buf, Bytes};
use sha2::{Digest, Sha256};

use crate::cache::command::{SLOT_COUNT, key_slot};
use crate::cache::store::{Effect, MAX_KEY_BYTES, MAX_VALUE_BYTES, StoreError};

use super::super::super::protocol::VERSION;
use super::super::invalid;
use super::*;

pub(in crate::cache::cluster::worker) fn encode_fetch(
    identity: Identity,
    probes: &[Probe],
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(HEADER + PROBE_LENGTH * probes.len());
    header(&mut bytes, 1, identity);
    for probe in probes {
        bytes.extend_from_slice(&probe.slot.to_be_bytes());
        bytes.extend_from_slice(&probe.epoch.to_be_bytes());
        bytes.extend_from_slice(&probe.sequence.to_be_bytes());
        bytes.push(u8::from(probe.baseline));
        bytes.extend_from_slice(
            probe
                .resume
                .map_or(Uuid::nil(), |resume| resume.id)
                .as_bytes(),
        );
        bytes.extend_from_slice(&probe.resume.map_or(0, |resume| resume.offset).to_be_bytes());
    }
    bytes
}

pub(super) fn decode_fetch(mut frame: &[u8]) -> io::Result<(Identity, Vec<Probe>)> {
    let identity = read_header(&mut frame, 1)?;
    if frame.is_empty() || frame.len() % PROBE_LENGTH != 0 || frame.len() > PROBES * PROBE_LENGTH {
        return Err(invalid("invalid cache peer probe count"));
    }
    let mut probes = Vec::with_capacity(frame.len() / PROBE_LENGTH);
    for mut body in frame.chunks_exact(PROBE_LENGTH) {
        let slot = body.get_u16();
        let epoch = body.get_u64();
        let sequence = body.get_u64();
        let flags = body.get_u8();
        let id = Uuid::from_bytes(take(&mut body)?);
        let offset = body.get_u32();
        if usize::from(slot) >= SLOT_COUNT
            || epoch == 0
            || flags > 1
            || (id.is_nil() && offset != 0)
        {
            return Err(invalid("invalid cache peer probe"));
        }
        probes.push(Probe {
            slot,
            epoch,
            sequence,
            baseline: flags == 1,
            resume: (!id.is_nil()).then_some(Resume { id, offset }),
        });
    }
    Ok((identity, probes))
}

pub(in crate::cache::cluster::worker) fn replies(
    mut bytes: &[u8],
    expected: Identity,
) -> io::Result<impl Iterator<Item = io::Result<Packet>>> {
    if read_header(&mut bytes, 2)? != expected {
        return Err(invalid("replication reply identity mismatch"));
    }
    let mut packets = 0;
    Ok(std::iter::from_fn(move || {
        if bytes.is_empty() {
            return None;
        }
        packets += 1;
        if packets > PROBES * DELTAS_PER_PROBE {
            bytes = &[];
            return Some(Err(invalid("too many replication replies")));
        }
        let result = decode_packet(&mut bytes);
        if result.is_err() {
            bytes = &[];
        }
        Some(result)
    }))
}

pub(super) fn header(output: &mut Vec<u8>, kind: u8, identity: Identity) {
    output.extend_from_slice(&VERSION.to_be_bytes());
    output.push(kind);
    output.extend_from_slice(&identity.generation.to_be_bytes());
    for worker in [identity.source, identity.destination] {
        output.extend_from_slice(&worker.node.to_be_bytes());
        output.extend_from_slice(worker.incarnation.as_bytes());
    }
}

fn read_header(bytes: &mut &[u8], kind: u8) -> io::Result<Identity> {
    if bytes.len() < HEADER || bytes.get_u16() != VERSION || bytes.get_u8() != kind {
        return Err(invalid("invalid cache peer header"));
    }
    Ok(Identity {
        generation: bytes.get_u64(),
        source: read_worker(bytes)?,
        destination: read_worker(bytes)?,
    })
}

fn read_worker(bytes: &mut &[u8]) -> io::Result<WorkerId> {
    Ok(WorkerId {
        node: bytes.get_u64(),
        incarnation: Uuid::from_bytes(take(bytes)?),
    })
}

fn take<const LEN: usize>(bytes: &mut &[u8]) -> io::Result<[u8; LEN]> {
    take_slice(bytes, LEN)?
        .try_into()
        .map_err(|_| invalid("truncated cache peer packet"))
}

fn take_slice<'a>(bytes: &mut &'a [u8], len: usize) -> io::Result<&'a [u8]> {
    let (value, remaining) = bytes
        .split_at_checked(len)
        .ok_or_else(|| invalid("truncated cache peer packet"))?;
    *bytes = remaining;
    Ok(value)
}

pub(super) fn extend(output: &mut Vec<u8>, bytes: &[u8], limit: usize) -> io::Result<()> {
    reserve(output, bytes.len(), limit).map_err(|_| invalid("cache peer frame exceeds limit"))?;
    output.extend_from_slice(bytes);
    Ok(())
}

fn reserve(output: &mut Vec<u8>, additional: usize, limit: usize) -> Result<(), StoreError> {
    if additional > limit - output.len() {
        return Err(StoreError::SnapshotLimit);
    }
    output
        .try_reserve_exact(additional)
        .map_err(|_| StoreError::OutOfMemory)
}

pub(in crate::cache::cluster::worker) fn encode_empty(
    probe: &Probe,
    cut: u64,
    out: &mut Vec<u8>,
    limit: usize,
) -> Result<(), StoreError> {
    record_header(probe, out, limit, 0, 8)?;
    out.extend_from_slice(&cut.to_be_bytes());
    Ok(())
}

pub(in crate::cache::cluster::worker) fn encode_delta(
    probe: &Probe,
    sequence: u64,
    effects: &[Effect],
    out: &mut Vec<u8>,
    limit: usize,
) -> Result<(), StoreError> {
    record_header(probe, out, limit, 1, 8 + effects_length(effects))?;
    out.extend_from_slice(&sequence.to_be_bytes());
    write_effects(effects, out);
    Ok(())
}

pub(in crate::cache::cluster::worker) fn encode_snapshot(
    probe: &Probe,
    id: Uuid,
    cut: u64,
    offset: u32,
    effects: &[Effect],
    checksum: Option<[u8; 32]>,
    out: &mut Vec<u8>,
    limit: usize,
) -> Result<(), StoreError> {
    let length = 16 + 8 + 4 + 1 + checksum.map_or(0, |_| 32) + effects_length(effects);
    record_header(probe, out, limit, 2, length)?;
    out.extend_from_slice(id.as_bytes());
    out.extend_from_slice(&cut.to_be_bytes());
    out.extend_from_slice(&offset.to_be_bytes());
    out.push(u8::from(checksum.is_some()));
    if let Some(checksum) = checksum {
        out.extend_from_slice(&checksum);
    }
    write_effects(effects, out);
    Ok(())
}

fn record_header(
    probe: &Probe,
    out: &mut Vec<u8>,
    limit: usize,
    kind: u8,
    body: usize,
) -> Result<(), StoreError> {
    let packet_bytes = 1 + 2 + 8 + body;
    reserve(out, 4 + packet_bytes, limit)?;
    out.extend_from_slice(&(packet_bytes as u32).to_be_bytes());
    out.push(kind);
    out.extend_from_slice(&probe.slot.to_be_bytes());
    out.extend_from_slice(&probe.epoch.to_be_bytes());
    Ok(())
}

fn decode_packet(bytes: &mut &[u8]) -> io::Result<Packet> {
    let length = u32::from_be_bytes(take(bytes)?) as usize;
    let original = take_slice(bytes, length)?;
    let mut packet = original;
    if length < 11 {
        return Err(invalid("truncated cache replication record"));
    }
    let kind = packet.get_u8();
    let slot = packet.get_u16();
    let epoch = packet.get_u64();
    if usize::from(slot) >= SLOT_COUNT || epoch == 0 {
        return Err(invalid("invalid replication slot or epoch"));
    }
    let data = match kind {
        0 => Data::Empty(u64::from_be_bytes(take(&mut packet)?)),
        1 => {
            let sequence = u64::from_be_bytes(take(&mut packet)?);
            let effects = read_effects(slot, &mut packet)?;
            if sequence == 0 || effects.is_empty() {
                return Err(invalid("invalid replication delta"));
            }
            Data::Delta {
                sequence,
                effects,
                digest: Sha256::digest(original).into(),
            }
        }
        2 => {
            let id = Uuid::from_bytes(take(&mut packet)?);
            let cut = u64::from_be_bytes(take(&mut packet)?);
            let offset = u32::from_be_bytes(take(&mut packet)?);
            let checksum = match take::<1>(&mut packet)?[0] {
                0 => None,
                1 => Some(take(&mut packet)?),
                _ => return Err(invalid("invalid replica snapshot flag")),
            };
            let effects = read_effects(slot, &mut packet)?;
            if id.is_nil()
                || effects.is_empty()
                || effects
                    .iter()
                    .any(|effect| matches!(effect, Effect::Delete(_)))
            {
                return Err(invalid("invalid replica snapshot chunk"));
            }
            Data::Snapshot(Chunk {
                id,
                cut,
                offset,
                checksum,
                effects,
            })
        }
        _ => return Err(invalid("unknown cache replication record")),
    };
    if !packet.is_empty() {
        return Err(invalid("trailing cache replication bytes"));
    }
    Ok(Packet { slot, epoch, data })
}

fn effects_length(effects: &[Effect]) -> usize {
    2 + effects
        .iter()
        .map(|effect| {
            let key = 3 + effect.key().len();
            match effect {
                Effect::Delete(_) => key,
                Effect::Upsert { value, .. } => key + 12 + value.len(),
            }
        })
        .sum::<usize>()
}

fn write_effects(effects: &[Effect], bytes: &mut Vec<u8>) {
    bytes.extend_from_slice(&(effects.len() as u16).to_be_bytes());
    for effect in effects {
        write_effect(effect, |part| bytes.extend_from_slice(part));
    }
}

pub(in crate::cache::cluster::worker) fn hash_effects(effects: &[Effect], digest: &mut Sha256) {
    for effect in effects {
        write_effect(effect, |part| digest.update(part));
    }
}

fn write_effect(effect: &Effect, mut write: impl FnMut(&[u8])) {
    let key = effect.key();
    write(&[u8::from(matches!(effect, Effect::Upsert { .. }))]);
    write(&(key.len() as u16).to_be_bytes());
    write(key);
    if let Effect::Upsert {
        value, deadline_ms, ..
    } = effect
    {
        write(&deadline_ms.unwrap_or_default().to_be_bytes());
        write(&(value.len() as u32).to_be_bytes());
        write(value);
    }
}

fn read_effects(slot: u16, bytes: &mut &[u8]) -> io::Result<Effects> {
    let count = u16::from_be_bytes(take(bytes)?) as usize;
    if count > MAX_ITEMS || count > bytes.len() / 3 {
        return Err(invalid("invalid cache effect count"));
    }
    let mut effects = Effects::new();
    effects
        .try_reserve_exact(count)
        .map_err(|_| io::Error::other("cache peer effect allocation failed"))?;
    for _ in 0..count {
        let kind = take::<1>(bytes)?[0];
        let key_size = u16::from_be_bytes(take(bytes)?) as usize;
        if key_size > MAX_KEY_BYTES {
            return Err(invalid("cache peer key exceeds limit"));
        }
        let source = take_slice(bytes, key_size)?;
        if key_slot(source) != slot {
            return Err(invalid("cache peer key does not belong to slot"));
        }
        let key = Bytes::copy_from_slice(source);
        let item = match kind {
            0 => Effect::Delete(key),
            1 => {
                let deadline = u64::from_be_bytes(take(bytes)?);
                let size = u32::from_be_bytes(take(bytes)?) as usize;
                if size > MAX_VALUE_BYTES || deadline > i64::MAX as u64 {
                    return Err(invalid("cache peer value or TTL exceeds limit"));
                }
                let value = Bytes::copy_from_slice(take_slice(bytes, size)?);
                Effect::Upsert {
                    key,
                    value,
                    deadline_ms: (deadline != 0).then_some(deadline),
                }
            }
            _ => return Err(invalid("unknown cache effect")),
        };
        effects.push(item);
    }
    Ok(effects)
}

#[cfg(test)]
pub(in crate::cache::cluster::worker) fn single_record_for_test(
    mut bytes: &[u8],
) -> io::Result<Packet> {
    let packet = decode_packet(&mut bytes)?;
    assert!(bytes.is_empty());
    Ok(packet)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn k03_peer_frames_bind_generation_and_incarnations_and_reject_foreign_keys() {
        let sender = WorkerId {
            node: 1,
            incarnation: Uuid::new_v4(),
        };
        let receiver = WorkerId {
            node: 2,
            incarnation: Uuid::new_v4(),
        };
        let identity = Identity {
            generation: 3,
            source: sender,
            destination: receiver,
        };
        let key = Bytes::from_static(b"{known}:one");
        let probe = Probe {
            slot: key_slot(&key),
            epoch: 2,
            sequence: 4,
            baseline: true,
            resume: None,
        };
        let request = encode_fetch(identity, &[probe]);
        let (parsed, requested) = decode_fetch(&request).unwrap();
        assert_eq!(parsed, identity);
        assert_eq!(requested[0].slot, probe.slot);
        assert_eq!(requested[0].sequence, 4);
        assert!(decode_fetch(&request[..request.len() - 1]).is_err());

        let mut response = Vec::new();
        header(&mut response, 2, identity.swapped());
        let wrong = Identity {
            generation: 4,
            ..identity.swapped()
        };
        assert!(replies(&response, wrong).is_err());
        assert!(replies(&response, identity).is_err());

        let foreign = Bytes::from_static(b"{foreign}:two");
        assert_ne!(probe.slot, key_slot(&foreign));
        assert!(
            encode_delta(
                &probe,
                5,
                &[Effect::Delete(foreign)],
                &mut response,
                4 << 10
            )
            .is_ok()
        );
        assert!(
            replies(&response, identity.swapped())
                .unwrap()
                .next()
                .unwrap()
                .is_err()
        );
    }

    #[test]
    fn k02_peer_output_reservation_cannot_emit_partial_record() {
        let key = Bytes::from_static(b"{limit}");
        let probe = Probe {
            slot: key_slot(&key),
            epoch: 1,
            sequence: 0,
            baseline: false,
            resume: None,
        };
        let mut output = vec![3; 20];
        let effect = Effect::Upsert {
            key,
            value: Bytes::from(vec![42; 512]),
            deadline_ms: None,
        };
        assert_eq!(
            encode_delta(&probe, 1, &[effect], &mut output, 128),
            Err(StoreError::SnapshotLimit)
        );
        assert_eq!(output, vec![3; 20]);
    }
}
