mod adversary;
mod adversary_ck;
mod adversary_moves;
mod adversary_multi;
mod adversary_rules;
mod codes;
mod crash;
mod diff;
mod model;
mod rv_dense;
mod unit;

use std::sync::Arc;

use super::*;
use crate::types::*;

const RANGE: RangeId = RangeId(7);

fn schema(dim: usize, columns: &[ColumnType], partition_by: Option<usize>) -> Schema {
    Schema {
        dim,
        columns: columns.to_vec(),
        partition_by,
    }
}

fn key(n: u64) -> Key {
    Arc::new(n.to_be_bytes())
}

fn row(vec: &[f32], cols: Vec<Value>, image: u64) -> Row {
    Row {
        vec: Some(vec.into()),
        cols,
        image: Arc::new(image.to_be_bytes()),
    }
}

/// A page of one change per position, after `after`.
fn page(after: Pos, changes: Vec<(u64, Option<Row>)>) -> Page {
    let through = after + changes.len() as Pos;
    let record = |(i, (k, row))| {
        let at = after + 1 + i as Pos;
        let change = Change::Row {
            key: key(k),
            commit_ts: at * 10,
            row,
        };
        Record { at, change }
    };
    Page {
        after,
        through,
        records: changes.into_iter().enumerate().map(record).collect(),
    }
}

/// An index that serves `RANGE`, empty.
fn serving<C: Codec>(cfg: Config, codec: C, store: Option<Store>) -> Index<C> {
    let mut ix = Index::new(cfg, codec, store).unwrap();
    ix.begin_load(RANGE, 0, 0).unwrap();
    ix.finish_load(RANGE).unwrap();
    ix.caught_up(RANGE, 0).unwrap();
    ix
}

/// Applies a page whole.
fn feed<C: Codec>(ix: &mut Index<C>, range: RangeId, page: &Page) {
    let mut page = page.clone();
    while page.after < page.through {
        match ix.apply(range, &page) {
            Ok(at) => page.after = at,
            Err(Error::Busy) => {}
            Err(e) => panic!("{e}"),
        }
        page.records.retain(|r| r.at > page.after);
        ix.run();
    }
}

/// Maintenance until there is nothing left to do.
fn settle<C: Codec>(ix: &mut Index<C>) {
    for _ in 0..10_000 {
        ix.run();
        if !ix.maintain(64) && ix.shared.dir.values().all(|m| m.state == State::Ready) {
            return;
        }
    }
    panic!("maintenance does not end");
}

fn latest<C: Codec>(ix: &mut Index<C>, filter: Option<&Filter>) -> Pin {
    ix.pin(&[(RANGE, ReadAt::Latest { barrier: 0 })], filter)
        .unwrap()
}

fn query(op: Op, vec: &[f32], limit: usize) -> Query {
    Query {
        op,
        vec: vec.to_vec(),
        limit,
        filter: None,
        exact: true,
        recall: 0.95,
    }
}

fn images(hits: &[Hit]) -> Vec<u64> {
    let image = |h: &Hit| u64::from_be_bytes((&*h.row.image).try_into().unwrap());
    hits.iter().map(image).collect()
}
