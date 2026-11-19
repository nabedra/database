use std::collections::BTreeMap;

use bytes::Bytes;

use super::protocol::{Member, Topology, WorkerId};
use crate::cache::wire::Response;

pub(super) fn moved(slot: u16, member: &Member) -> Response {
    let host = &member.client_host;
    let port = member.client_port;
    let endpoint = if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    Response::Error(Bytes::from(format!("MOVED {slot} {endpoint}")))
}

pub(super) fn slots(topology: &Topology, limit: usize) -> Response {
    bounded(
        ranges(topology).filter_map(|(start, end, primary, replica)| {
            let mut row = vec![int(start), int(end), address(topology.member(primary)?)];
            if let Some(replica) = replica.and_then(|id| topology.member(id)) {
                row.push(address(replica));
            }
            Some(Response::Array(row))
        }),
        limit,
    )
}

pub(super) fn shards(topology: &Topology, limit: usize) -> Response {
    let mut groups: BTreeMap<_, Vec<Response>> = BTreeMap::new();
    let mut range_bytes = 0;
    for (start, end, primary, replica) in ranges(topology) {
        range_bytes += int(start).encoded_len().unwrap() + int(end).encoded_len().unwrap();
        if range_bytes > limit {
            return too_large();
        }
        groups
            .entry((primary, replica))
            .or_default()
            .extend([int(start), int(end)]);
    }
    bounded(
        groups
            .into_iter()
            .filter_map(|((primary, replica), slots)| {
                let mut nodes = vec![node(topology.member(primary)?, true)];
                if let Some(replica) = replica.and_then(|id| topology.member(id)) {
                    nodes.push(node(replica, false));
                }
                Some(Response::Array(vec![
                    Response::bulk(b"slots"),
                    Response::Array(slots),
                    Response::bulk(b"nodes"),
                    Response::Array(nodes),
                ]))
            }),
        limit,
    )
}

fn ranges(topology: &Topology) -> impl Iterator<Item = (u16, u16, WorkerId, Option<WorkerId>)> {
    let mut position = 0;
    std::iter::from_fn(move || {
        while position < topology.slots.len() {
            let start = position;
            let owner = topology.slots[start];
            position += 1;
            while position < topology.slots.len()
                && topology.slots[position].primary == owner.primary
                && topology.slots[position].replica == owner.replica
            {
                position += 1;
            }
            if let Some(primary) = owner.primary {
                return Some((start as u16, (position - 1) as u16, primary, owner.replica));
            }
        }
        None
    })
}

fn address(member: &Member) -> Response {
    Response::Array(vec![
        bulk(member.client_host.clone()),
        int(member.client_port),
        node_id(member),
    ])
}

fn node(member: &Member, primary: bool) -> Response {
    let role = if primary {
        b"master".as_slice()
    } else {
        b"replica"
    };
    Response::Array(vec![
        Response::bulk(b"id"),
        node_id(member),
        Response::bulk(b"port"),
        int(member.client_port),
        Response::bulk(b"tls-port"),
        int(member.client_port),
        Response::bulk(b"endpoint"),
        bulk(member.client_host.clone()),
        Response::bulk(b"hostname"),
        bulk(member.client_host.clone()),
        Response::bulk(b"role"),
        Response::bulk(role),
        Response::bulk(b"replication-offset"),
        Response::Integer(0),
        Response::bulk(b"health"),
        Response::bulk(b"online"),
    ])
}

fn int(value: u16) -> Response {
    Response::Integer(value.into())
}

fn node_id(member: &Member) -> Response {
    bulk(format!(
        "{:08x}{}",
        member.id.node as u32,
        member.id.incarnation.simple()
    ))
}

fn bulk(text: String) -> Response {
    Response::Bulk(Some(Bytes::from(text)))
}

fn bounded(items: impl Iterator<Item = Response>, limit: usize) -> Response {
    let mut size = 8usize;
    let mut array = Vec::new();
    for item in items {
        size = size.saturating_add(item.encoded_len().unwrap_or(usize::MAX));
        if size > limit {
            return too_large();
        }
        array.push(item);
    }
    Response::Array(array)
}

fn too_large() -> Response {
    Response::error(b"ERR topology exceeds cache output limit")
}
