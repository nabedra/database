//! Filters: column blocks inside partitions, and summaries that say which partitions may hold a
//! match and how many rows match. A filter decides before a row is ranked, never after.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::Arc;

use crate::types::{ColumnType, Error, PartId, Result, Schema, Value};

/// A condition on filter columns, which it names by their place in the schema.
#[derive(Clone, PartialEq, Debug)]
pub enum Filter {
    Eq(usize, Value),
    /// Int, float or text.
    Range(usize, Bound<Value>, Bound<Value>),
    StartsWith(usize, Arc<str>),
    Contains(usize, Arc<str>),
    ContainsAny(usize, Vec<Arc<str>>),
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
}

impl Filter {
    /// Checks columns, types and depth.
    pub fn check(&self, schema: &Schema) -> Result<()> {
        const MAX_DEPTH: usize = 64;
        self.check_at(schema, MAX_DEPTH)
    }

    fn check_at(&self, schema: &Schema, depth: usize) -> Result<()> {
        const BAD: Error = Error::InvalidQuery("a filter does not fit the column's type");
        let depth = depth
            .checked_sub(1)
            .ok_or(Error::InvalidQuery("the filter is too deep"))?;
        let column = |c: &usize| schema.columns.get(*c).copied().ok_or(BAD);
        let fits = |ok: bool| if ok { Ok(()) } else { Err(BAD) };
        match self {
            Filter::Eq(c, v) => {
                fits(column(c)? == v.kind() && v.kind() != ColumnType::Tags && finite(v))
            }
            Filter::Range(c, lo, hi) => {
                let kind = column(c)?;
                let ordered =
                    matches!(kind, ColumnType::Int | ColumnType::Float | ColumnType::Text);
                let end = |b: &Bound<Value>| match b {
                    Bound::Included(v) | Bound::Excluded(v) => v.kind() == kind && finite(v),
                    Bound::Unbounded => true,
                };
                fits(ordered && end(lo) && end(hi))
            }
            Filter::StartsWith(c, _) => fits(column(c)? == ColumnType::Text),
            Filter::Contains(c, _) | Filter::ContainsAny(c, _) => {
                fits(column(c)? == ColumnType::Tags)
            }
            Filter::And(all) | Filter::Or(all) => {
                all.iter().try_for_each(|f| f.check_at(schema, depth))
            }
            Filter::Not(f) => f.check_at(schema, depth),
        }
    }

    /// Whether a row with these column values matches.
    pub fn matches(&self, cols: &[Value]) -> bool {
        match self {
            Filter::And(all) => all.iter().all(|f| f.matches(cols)),
            Filter::Or(all) => all.iter().any(|f| f.matches(cols)),
            Filter::Not(f) => !f.matches(cols),
            Filter::Eq(c, _)
            | Filter::Range(c, ..)
            | Filter::StartsWith(c, _)
            | Filter::Contains(c, _)
            | Filter::ContainsAny(c, _) => cols.get(*c).is_some_and(|v| self.accepts(v)),
        }
    }

    /// A leaf condition on its column's value.
    fn accepts(&self, v: &Value) -> bool {
        match (self, v) {
            (Filter::Eq(_, want), v) => want == v,
            (Filter::Range(_, lo, hi), v) => within(v, lo, hi),
            (Filter::StartsWith(_, prefix), Value::Text(t)) => t.starts_with(&**prefix),
            (Filter::Contains(_, tag), Value::Tags(tags)) => tags.contains(tag),
            (Filter::ContainsAny(_, any), Value::Tags(tags)) => {
                any.iter().any(|t| tags.contains(t))
            }
            _ => false,
        }
    }

    /// Conditions and connectives: the work units of evaluating it once.
    pub fn nodes(&self) -> u64 {
        let mut todo = vec![self];
        let mut n = 0u64;
        while let Some(f) = todo.pop() {
            n = n.saturating_add(1);
            match f {
                Filter::And(all) | Filter::Or(all) => todo.extend(all),
                Filter::Not(f) => todo.push(f),
                Filter::ContainsAny(_, any) => n = n.saturating_add(any.len() as u64),
                _ => {}
            }
        }
        n
    }

    /// The one value of the column that this filter allows.
    pub fn only(&self, column: usize) -> Option<&Value> {
        match self {
            Filter::Eq(c, v) if *c == column => Some(v),
            Filter::And(all) => all.iter().find_map(|f| f.only(column)),
            _ => None,
        }
    }
}

pub fn finite(v: &Value) -> bool {
    !matches!(v, Value::Float(x) if !x.is_finite())
}

fn within(v: &Value, lo: &Bound<Value>, hi: &Bound<Value>) -> bool {
    use std::cmp::Ordering::{Equal, Greater, Less};
    let is = |other: &Value, side| {
        let order = compare(v, other);
        order == Some(side) || order == Some(Equal)
    };
    (match lo {
        Bound::Included(b) => is(b, Greater),
        Bound::Excluded(b) => compare(v, b) == Some(Greater),
        Bound::Unbounded => true,
    }) && (match hi {
        Bound::Included(b) => is(b, Less),
        Bound::Excluded(b) => compare(v, b) == Some(Less),
        Bound::Unbounded => true,
    })
}

fn compare(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
        (Value::Text(a), Value::Text(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

/// A row bitmap of one block.
pub struct Bits(Vec<u64>);

impl Bits {
    fn collect(n: usize, f: impl Fn(usize) -> bool) -> Bits {
        let mut words = vec![0u64; n.div_ceil(64)];
        for i in 0..n {
            words[i / 64] |= u64::from(f(i)) << (i % 64);
        }
        Bits(words)
    }

    pub fn get(&self, i: usize) -> bool {
        self.0.get(i / 64).is_some_and(|w| w >> (i % 64) & 1 == 1)
    }

    fn join(mut self, other: &Bits, f: impl Fn(u64, u64) -> u64) -> Bits {
        for (a, b) in self.0.iter_mut().zip(&other.0) {
            *a = f(*a, *b);
        }
        self
    }
}

/// The values of one filter column in a block, in entry order.
pub type Column = Vec<Value>;

/// The rows of a block that match. `n` is the block's number of entries.
pub fn evaluate(filter: &Filter, cols: &[Column], n: usize) -> Bits {
    let fold = |all: &[Filter], start: bool, f: fn(u64, u64) -> u64| {
        let start = Bits::collect(n, |_| start);
        all.iter()
            .fold(start, |acc, x| acc.join(&evaluate(x, cols, n), f))
    };
    match filter {
        Filter::And(all) => fold(all, true, |a, b| a & b),
        Filter::Or(all) => fold(all, false, |a, b| a | b),
        Filter::Not(f) => Bits::collect(n, |_| true).join(&evaluate(f, cols, n), |a, b| a & !b),
        Filter::Eq(c, _)
        | Filter::Range(c, ..)
        | Filter::StartsWith(c, _)
        | Filter::Contains(c, _)
        | Filter::ContainsAny(c, _) => {
            let column = cols.get(*c).map_or(&[][..], |c| c);
            Bits::collect(n, |i| column.get(i).is_some_and(|v| filter.accepts(v)))
        }
    }
}

/// A value that a summary counts and that names a tree.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Scalar {
    Bool(bool),
    Int(i64),
    Float(u64),
    Text(Arc<str>),
}

impl Scalar {
    /// `None` for tags.
    pub fn of(v: &Value) -> Option<Scalar> {
        Some(match v {
            Value::Bool(b) => Scalar::Bool(*b),
            Value::Int(i) => Scalar::Int(*i),
            // Adding zero turns -0.0 into 0.0, which it equals.
            Value::Float(f) => Scalar::Float((f + 0.0).to_bits()),
            Value::Text(t) => Scalar::Text(t.clone()),
            Value::Tags(_) => return None,
        })
    }

    fn all(v: &Value) -> Vec<Scalar> {
        match v {
            Value::Tags(tags) => {
                let mut tags: Vec<_> = tags.iter().cloned().map(Scalar::Text).collect();
                tags.sort();
                tags.dedup();
                tags
            }
            v => Scalar::of(v).into_iter().collect(),
        }
    }
}

/// What one column holds: where each value is, and the smallest and largest value per partition.
#[derive(Clone)]
struct ColumnSummary {
    /// Value → entries per partition. `None` once the column has too many values to count.
    values: Option<BTreeMap<Scalar, BTreeMap<PartId, u32>>>,
    pairs: usize,
    spans: HashMap<PartId, (Value, Value)>,
}

/// Counts of physical entries, so copies and superseded versions count too. A partition that holds a
/// match is never excluded.
#[derive(Clone)]
pub struct Summary {
    cols: Vec<ColumnSummary>,
    /// The (value, partition) pairs that one column may use.
    max_pairs: usize,
    sizes: BTreeMap<PartId, u32>,
}

impl Summary {
    pub fn new(columns: usize, max_pairs: usize) -> Summary {
        let col = ColumnSummary {
            values: Some(BTreeMap::new()),
            pairs: 0,
            spans: HashMap::new(),
        };
        Summary {
            cols: vec![col; columns],
            max_pairs,
            sizes: BTreeMap::new(),
        }
    }

    pub fn add(&mut self, part: PartId, row: &[Value]) {
        *self.sizes.entry(part).or_default() += 1;
        for (col, v) in self.cols.iter_mut().zip(row) {
            if let Some(values) = &mut col.values {
                for s in Scalar::all(v) {
                    let n = values.entry(s).or_default().entry(part).or_default();
                    col.pairs += usize::from(*n == 0);
                    *n += 1;
                }
                if col.pairs > self.max_pairs {
                    col.values = None;
                }
            }
            if compare(v, v).is_some() {
                let span = col
                    .spans
                    .entry(part)
                    .or_insert_with(|| (v.clone(), v.clone()));
                if compare(v, &span.0) == Some(std::cmp::Ordering::Less) {
                    span.0 = v.clone();
                }
                if compare(v, &span.1) == Some(std::cmp::Ordering::Greater) {
                    span.1 = v.clone();
                }
            }
        }
    }

    /// An entry left. Spans only shrink with the partition's last entry.
    pub fn remove(&mut self, part: PartId, row: &[Value]) {
        let left = self.sizes.get_mut(&part).map_or(0, |n| {
            *n = n.saturating_sub(1);
            *n
        });
        if left == 0 {
            self.sizes.remove(&part);
        }
        for (col, v) in self.cols.iter_mut().zip(row) {
            if left == 0 {
                col.spans.remove(&part);
            }
            let Some(values) = &mut col.values else {
                continue;
            };
            for s in Scalar::all(v) {
                let Some(parts) = values.get_mut(&s) else {
                    continue;
                };
                if let Some(n) = parts.get_mut(&part) {
                    *n = n.saturating_sub(1);
                    if *n == 0 {
                        parts.remove(&part);
                        col.pairs = col.pairs.saturating_sub(1);
                    }
                }
                if parts.is_empty() {
                    values.remove(&s);
                }
            }
        }
    }

    /// False only if no entry of the partition matches.
    pub fn may_match(&self, filter: &Filter, part: PartId) -> bool {
        let has = |c: &usize, s: &Scalar| match self.cols.get(*c).map(|c| &c.values) {
            Some(Some(values)) => values.get(s).is_some_and(|p| p.contains_key(&part)),
            _ => true,
        };
        match filter {
            Filter::Eq(c, v) => Scalar::of(v).is_none_or(|s| has(c, &s)),
            Filter::Contains(c, t) => has(c, &Scalar::Text(t.clone())),
            Filter::ContainsAny(c, any) => any.iter().any(|t| has(c, &Scalar::Text(t.clone()))),
            Filter::Range(c, lo, hi) => self.share(*c, part, lo, hi) > 0.0,
            Filter::StartsWith(..) | Filter::Not(_) => self.sizes.contains_key(&part),
            Filter::And(all) => all.iter().all(|f| self.may_match(f, part)),
            Filter::Or(all) => all.iter().any(|f| self.may_match(f, part)),
        }
    }

    /// The share of a partition's span that a range covers, as if values were spread evenly.
    fn share(&self, c: usize, part: PartId, lo: &Bound<Value>, hi: &Bound<Value>) -> f64 {
        let Some((min, max)) = self.cols.get(c).and_then(|c| c.spans.get(&part)) else {
            return 0.0;
        };
        let overlaps = within(max, lo, &Bound::Unbounded) && within(min, &Bound::Unbounded, hi);
        if !overlaps {
            return 0.0;
        }
        let number = |v: &Value| match v {
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            _ => None,
        };
        let end = |b: &Bound<Value>, open: f64| match b {
            Bound::Included(v) | Bound::Excluded(v) => number(v).unwrap_or(open),
            Bound::Unbounded => open,
        };
        let (Some(min), Some(max)) = (number(min), number(max)) else {
            return 1.0;
        };
        let covered = end(hi, max).min(max) - end(lo, min).max(min);
        let share = if max > min {
            covered / (max - min)
        } else {
            1.0
        };
        // Values bunch up, so a range that overlaps counts for something. Not a number: the span is
        // too wide to divide.
        if share.is_nan() {
            1.0
        } else {
            share.clamp(0.01, 1.0)
        }
    }

    /// Counts matching text values when the column is still within the summary budget.
    fn count_text(
        &self,
        column: usize,
        lower: Bound<Scalar>,
        accept: impl Fn(&str) -> bool,
    ) -> Option<f64> {
        let values = self.cols.get(column)?.values.as_ref()?;
        Some(
            values
                .range((lower, Bound::Unbounded))
                .map_while(|(value, parts)| match value {
                    Scalar::Text(text) if accept(text) => Some(parts),
                    _ => None,
                })
                .flat_map(BTreeMap::values)
                .map(|&n| f64::from(n))
                .sum(),
        )
    }

    /// About how many entries match.
    pub fn estimate(&self, filter: &Filter) -> f64 {
        let total = || self.sizes.values().map(|&n| f64::from(n)).sum::<f64>();
        let count = |c: &usize, s: &Scalar| match self.cols.get(*c).map(|c| &c.values) {
            Some(Some(values)) => values
                .get(s)
                .map_or(0.0, |p| p.values().map(|&n| f64::from(n)).sum()),
            _ => total(),
        };
        match filter {
            Filter::Eq(c, v) => Scalar::of(v).map_or(0.0, |s| count(c, &s)),
            Filter::Contains(c, t) => count(c, &Scalar::Text(t.clone())),
            Filter::ContainsAny(c, any) => {
                let sum: f64 = any.iter().map(|t| count(c, &Scalar::Text(t.clone()))).sum();
                sum.min(total())
            }
            Filter::Range(c, lo, hi) => {
                let range = || {
                    let part = |(p, n): (&PartId, &u32)| self.share(*c, *p, lo, hi) * f64::from(*n);
                    self.sizes.iter().map(part).sum()
                };
                let text = |bound: &Bound<Value>| {
                    matches!(
                        bound,
                        Bound::Included(Value::Text(_)) | Bound::Excluded(Value::Text(_))
                    )
                };
                if text(lo) || text(hi) {
                    let lower = match lo {
                        Bound::Included(Value::Text(s)) => Bound::Included(Scalar::Text(s.clone())),
                        Bound::Excluded(Value::Text(s)) => Bound::Excluded(Scalar::Text(s.clone())),
                        _ => Bound::Unbounded,
                    };
                    self.count_text(*c, lower, |s| match hi {
                        Bound::Included(Value::Text(end)) => s <= end.as_ref(),
                        Bound::Excluded(Value::Text(end)) => s < end.as_ref(),
                        _ => true,
                    })
                    .unwrap_or_else(range)
                } else {
                    range()
                }
            }
            Filter::StartsWith(c, prefix) => self
                .count_text(*c, Bound::Included(Scalar::Text(prefix.clone())), |s| {
                    s.starts_with(prefix.as_ref())
                })
                .unwrap_or_else(total),
            Filter::And(all) => all.iter().map(|f| self.estimate(f)).fold(total(), f64::min),
            Filter::Or(all) => all
                .iter()
                .map(|f| self.estimate(f))
                .sum::<f64>()
                .min(total()),
            Filter::Not(f) => (total() - self.estimate(f)).max(0.0),
        }
    }
}
