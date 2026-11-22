use super::*;
use crate::protocol::{Comparison, ComparisonOp};
use nabedra_check::{Gen, prop};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Truth {
    False,
    Unknown,
    True,
}

fn compare(op: ComparisonOp, column: &str, value: Scalar) -> Predicate {
    Predicate::Comparison(Comparison {
        op,
        column: column.into(),
        value,
    })
}

fn present() -> Predicate {
    compare(ComparisonOp::Eq, "active", Scalar::Bool(true))
}

fn not(arg: Predicate) -> Predicate {
    Predicate::Logical(LogicalPredicate::Not { arg: Box::new(arg) })
}

fn combine(and: bool, args: Vec<Predicate>) -> Predicate {
    Predicate::Logical(if and {
        LogicalPredicate::And { args }
    } else {
        LogicalPredicate::Or { args }
    })
}

fn nullable_predicate(g: &mut Gen, depth: u8) -> Predicate {
    if depth > 0 && g.bool() {
        return match g.below(3) {
            0 => not(nullable_predicate(g, depth - 1)),
            op => combine(
                op == 1,
                vec![
                    nullable_predicate(g, depth - 1),
                    nullable_predicate(g, depth - 1),
                ],
            ),
        };
    }
    let op = *g.pick(&[
        ComparisonOp::Eq,
        ComparisonOp::Ne,
        ComparisonOp::Gt,
        ComparisonOp::Ge,
        ComparisonOp::Lt,
        ComparisonOp::Le,
    ]);
    match g.below(5) {
        0 => compare(op, "id", Scalar::Int64(g.range_i64(0, 28))),
        1 => compare(
            op,
            "text",
            Scalar::Utf8(g.pick(&["", "same", "é"]).to_string()),
        ),
        2 => compare(
            *g.pick(&[ComparisonOp::Eq, ComparisonOp::Ne]),
            "active",
            Scalar::Bool(g.bool()),
        ),
        _ => compare(
            op,
            "score",
            Scalar::Float64(g.range_i64(-4, 4) as f64 / 2.0),
        ),
    }
}

// The protocol has no NULL scalar. The fixture stores nullable score as a
// presence bit and a float payload. Lower each expression into two disjoint
// predicates for its true and false rows. The remaining rows are unknown.
fn lower(p: &Predicate) -> (Predicate, Predicate) {
    match p {
        Predicate::Comparison(c) if c.column == "score" => (
            combine(true, vec![present(), p.clone()]),
            combine(true, vec![present(), not(p.clone())]),
        ),
        Predicate::Comparison(_) => (p.clone(), not(p.clone())),
        Predicate::Logical(LogicalPredicate::Not { arg }) => {
            let (yes, no) = lower(arg);
            (no, yes)
        }
        Predicate::Logical(LogicalPredicate::And { args } | LogicalPredicate::Or { args }) => {
            let and = matches!(p, Predicate::Logical(LogicalPredicate::And { .. }));
            let (yes, no) = args.iter().map(lower).unzip();
            (combine(and, yes), combine(!and, no))
        }
    }
}

// Compare a present value with a literal without the engine's
// `query_semantics`, so a wrong operator or ordering there is visible.
fn holds(op: ComparisonOp, value: &Scalar, literal: &Scalar) -> bool {
    use std::cmp::Ordering::{Equal, Greater, Less};
    let order = match (value, literal) {
        (Scalar::Int64(a), Scalar::Int64(b)) => a.cmp(b),
        (Scalar::Utf8(a), Scalar::Utf8(b)) => a.as_bytes().cmp(b.as_bytes()),
        (Scalar::Bool(a), Scalar::Bool(b)) => u8::from(*a).cmp(&u8::from(*b)),
        (Scalar::Float64(a), Scalar::Float64(b)) if a == b => Equal,
        (Scalar::Float64(a), Scalar::Float64(b)) if a < b => Less,
        (Scalar::Float64(_), Scalar::Float64(_)) => Greater,
        other => panic!("generated comparison has mismatched types: {other:?}"),
    };
    match op {
        ComparisonOp::Eq => order == Equal,
        ComparisonOp::Ne => order != Equal,
        ComparisonOp::Gt => order == Greater,
        ComparisonOp::Ge => order != Less,
        ComparisonOp::Lt => order == Less,
        ComparisonOp::Le => order != Greater,
    }
}

// Interpret SQL three-valued logic over the original logical rows. The pure
// scalar evaluator handles only comparisons whose operands are present.
fn evaluate(p: &Predicate, row: &Row) -> Truth {
    match p {
        Predicate::Comparison(c) => {
            if c.column == "score" && row["active"] == Scalar::Bool(false) {
                Truth::Unknown
            } else if holds(c.op, &row[&c.column], &c.value) {
                Truth::True
            } else {
                Truth::False
            }
        }
        Predicate::Logical(LogicalPredicate::Not { arg }) => match evaluate(arg, row) {
            Truth::False => Truth::True,
            Truth::Unknown => Truth::Unknown,
            Truth::True => Truth::False,
        },
        Predicate::Logical(LogicalPredicate::And { args }) => {
            args.iter().map(|p| evaluate(p, row)).min().unwrap()
        }
        Predicate::Logical(LogicalPredicate::Or { args }) => {
            args.iter().map(|p| evaluate(p, row)).max().unwrap()
        }
    }
}

fn data(g: &mut Gen, len: i64) -> Vec<Row> {
    (0..len)
        .map(|id| {
            let present = id > 1 && g.chance(2, 3);
            let score = if present {
                g.range_i64(-4, 4) as f64 / 2.0
            } else {
                0.0
            };
            // The first two rows are identical once projected past the key.
            let text = if id <= 1 {
                "same"
            } else {
                *g.pick(&["", "same", "é"])
            };
            Row::from([
                ("id".into(), Scalar::Int64(id)),
                ("active".into(), Scalar::Bool(present)),
                ("score".into(), Scalar::Float64(score)),
                ("text".into(), Scalar::Utf8(text.into())),
            ])
        })
        .collect()
}

fn multiset(rows: impl IntoIterator<Item = Row>) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for row in rows {
        *counts
            .entry(serde_json::to_string(&row).unwrap())
            .or_default() += 1;
    }
    counts
}

#[derive(Clone, Copy)]
struct ScanWindow {
    shard: u32,
    shards: u32,
    batch_rows: usize,
}

async fn scan_rows(
    fixture: &ScanFixture,
    catalog: &Catalog,
    table: &TableDescriptor,
    query: Query,
    window: ScanWindow,
    force_full_scan: bool,
) -> Result<Vec<Row>, String> {
    let mode = if force_full_scan { "full" } else { "pruned" };
    let failed = |error| format!("{mode} scan of {query:?} failed: {error:?}");
    let mut scan = PinnedScan::capture(catalog, table.id, query.clone()).map_err(failed)?;
    scan.force_full_scan = force_full_scan;
    let scan = Arc::new(scan);
    let mut cursor = scan.cursor(window.shard, window.shards).map_err(failed)?;
    let mut reader = ScanReader::default();
    let mut rows = Vec::new();
    // An engine error is an oracle failure with a replay seed, not a panic.
    loop {
        let batch = reader
            .next_batch(
                &fixture.storage,
                &fixture.pool,
                scan.clone(),
                cursor,
                options(BatchFormat::Rows, window.batch_rows),
            )
            .await
            .map_err(failed)?;
        let Some(data) = batch.data else {
            return Ok(rows);
        };
        rows.extend(decode_rows(data.bytes()));
        cursor = batch.cursor;
    }
}

async fn check_oracles(
    fixture: &ScanFixture,
    source: &[Row],
    split: i64,
    predicate: &Predicate,
    select: &[String],
    window: ScanWindow,
    prune_null_bug: bool,
) -> Result<[Result<(), String>; 3], String> {
    let table = table();
    let catalog = fixture.partitioned(&table, source, split).await;
    let (yes, no) = lower(predicate);
    let unknown = not(combine(false, vec![yes.clone(), no.clone()]));
    let boundary = |shard| source.len() * shard as usize / window.shards as usize;
    let source = &source[boundary(window.shard)..boundary(window.shard + 1)];
    let mut partitioned = Vec::new();
    let mut norec = Ok(());
    let mut reference = Ok(());
    let mut unfiltered = Vec::new();
    for (truth, filter) in [
        (Some(Truth::True), Some(yes)),
        (Some(Truth::False), Some(no)),
        (Some(Truth::Unknown), Some(unknown)),
        (None, None),
    ] {
        let mut query = Query {
            select: Some(select.to_vec()),
            filter,
            ..all()
        };
        // NoREC: every partition and the unfiltered scan also run without
        // row-group selection or column projection.
        let full = scan_rows(fixture, &catalog, &table, query.clone(), window, true).await?;
        if prune_null_bug && truth == Some(Truth::True) {
            // Omitting the presence guard compares the placeholder float
            // even when the logical score is NULL.
            query.filter = Some(predicate.clone());
        }
        let pruned = scan_rows(fixture, &catalog, &table, query, window, false).await?;
        let expected: Vec<Row> = source
            .iter()
            .filter(|row| truth.is_none_or(|want| evaluate(predicate, row) == want))
            .map(|row| {
                select
                    .iter()
                    .map(|name| (name.clone(), row[name].clone()))
                    .collect()
            })
            .collect();
        if full != pruned && norec.is_ok() {
            norec = Err(format!(
                "NoREC {truth:?}, {predicate:?}: pruned {pruned:?}, full {full:?}"
            ));
        }
        if pruned != expected && reference.is_ok() {
            reference = Err(format!(
                "pure evaluator {truth:?}, {predicate:?}: expected {expected:?}, pruned {pruned:?}"
            ));
        }
        if truth.is_some() {
            partitioned.extend(pruned);
        } else {
            unfiltered = pruned;
        }
    }
    let tlp = if partitioned.len() == unfiltered.len()
        && multiset(partitioned.iter().cloned()) == multiset(unfiltered.iter().cloned())
    {
        Ok(())
    } else {
        Err(format!(
            "TLP {predicate:?}: partitioned {} rows {:?}, expected {} rows {:?}",
            partitioned.len(),
            multiset(partitioned.iter().cloned()),
            unfiltered.len(),
            multiset(unfiltered.iter().cloned()),
        ))
    };
    Ok([tlp, norec, reference])
}

#[test]
fn generated_typed_queries_agree_with_all_three_oracles() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (fixture, workers) = runtime.block_on(async { ScanFixture::new() });
    let projections = [
        vec!["text".into()],
        vec!["text".into(), "active".into()],
        vec!["id".into(), "text".into()],
    ];
    prop::check("OLAP-G06 OLAP-G08 typed query oracles", |g| {
        let split = g.range_i64(9, 14);
        let len = split + g.range_i64(9, 14);
        let source = data(g, len);
        let predicate = nullable_predicate(g, 2);
        let select = g.pick(&projections);
        // Cover both complete snapshots and nonaligned shard boundaries.
        let shards = if g.chance(1, 4) { 1 } else { 3 };
        let window = ScanWindow {
            shard: g.below(shards) as u32,
            shards: shards as u32,
            batch_rows: g.range_i64(5, 10) as usize,
        };
        let [tlp, norec, reference] = runtime.block_on(check_oracles(
            &fixture, &source, split, &predicate, select, window, false,
        ))?;
        tlp.and(norec).and(reference)
    });
    runtime.block_on(workers.shutdown()).unwrap();
}

#[tokio::test]
async fn force_full_scan_uses_the_unprojected_file_reader() {
    let (fixture, workers) = ScanFixture::new();
    let source = data(&mut Gen::from_seed(1), 20);
    let table = table();
    let catalog = fixture.partitioned(&table, &source, 10).await;
    for full in [false, true] {
        let mut scan = PinnedScan::capture(&catalog, table.id, all()).unwrap();
        scan.force_full_scan = full;
        let cursor = scan.cursor(1, 3).unwrap();
        let mut reader = ScanReader::default();
        let batch = reader
            .next_batch(
                &fixture.storage,
                &fixture.pool,
                Arc::new(scan),
                cursor,
                options(BatchFormat::Rows, 1),
            )
            .await
            .unwrap();
        assert!(batch.data.is_some());
        assert_eq!(reader.file.as_ref().unwrap().is_full_scan(), full);
    }
    workers.shutdown().await.unwrap();
}

#[tokio::test]
async fn pruned_null_bug_is_caught_by_tlp_and_norec() {
    let (fixture, workers) = ScanFixture::new();
    let source = data(&mut Gen::from_seed(1), 20);
    let predicate = compare(ComparisonOp::Ge, "score", Scalar::Float64(0.0));
    let select = ["text".into()];
    let window = ScanWindow {
        shard: 0,
        shards: 1,
        batch_rows: 5,
    };
    let baseline = check_oracles(&fixture, &source, 10, &predicate, &select, window, false).await;
    assert_eq!(baseline, Ok([Ok(()), Ok(()), Ok(())]));

    let [tlp, norec, _] = check_oracles(&fixture, &source, 10, &predicate, &select, window, true)
        .await
        .unwrap();
    let tlp = tlp.unwrap_err();
    let norec = norec.unwrap_err();
    assert!(tlp.starts_with("TLP "), "{tlp}");
    assert!(norec.starts_with("NoREC Some(True)"), "{norec}");
    eprintln!("Planted NULL regression caught by TLP and NoREC");
    workers.shutdown().await.unwrap();
}
