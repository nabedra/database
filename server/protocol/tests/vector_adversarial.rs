//! Adversarial tests of the vector wire types (task C1, VEC-G11, OLTP-G12).
//! Black box: only `parse_oltp_request`, `parse_request` and the public
//! decoders. Every test takes `LOCK`, because the allocation tests count the
//! whole process.
use nabedra_protocol::protocol::*;
use serde_json::{Value, json};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Counting;
static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
// SAFETY: forwards every call to `System` unchanged and only counts bytes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let now = CURRENT.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(p, layout) }
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, new: usize) -> *mut u8 {
        let q = unsafe { System.realloc(p, layout, new) };
        if !q.is_null() {
            if new >= layout.size() {
                let now =
                    CURRENT.fetch_add(new - layout.size(), Ordering::Relaxed) + new - layout.size();
                PEAK.fetch_max(now, Ordering::Relaxed);
            } else {
                CURRENT.fetch_sub(layout.size() - new, Ordering::Relaxed);
            }
        }
        q
    }
}
#[global_allocator]
static ALLOC: Counting = Counting;

static LOCK: Mutex<()> = Mutex::new(());
fn lock() -> std::sync::MutexGuard<'static, ()> {
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}
/// Peak bytes allocated while `f` runs, beyond what was live before.
fn peak<T>(f: impl FnOnce() -> T) -> (T, usize) {
    let base = CURRENT.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    let out = f();
    let top = PEAK.load(Ordering::Relaxed);
    (out, top.saturating_sub(base))
}

const ID: &str = "67e55044-10b1-426f-9247-bb680e5fe0c8";

fn envelope(op: &str, payload: &str) -> String {
    format!(r#"{{"request_id":"{ID}","operation":"{op}","payload":{payload}}}"#)
}
fn parse(op: &str, payload: &str) -> Result<Request, ProtocolError> {
    parse_oltp_request(envelope(op, payload).as_bytes())
}
fn table() -> TableDescriptor {
    serde_json::from_str(&format!(
        r#"{{"id":"{ID}","name":"t","schema":{{"id":"utf8","body":"utf8","tags":{{"type":"list","item":"utf8"}},"v":{{"type":"vector","dim":2}},"s":{{"type":"vector","sparse":true}},"m":{{"type":"list","item":{{"type":"vector","dim":2}}}},"n":"int64","f":"float64"}},"key":"id","format":"parquet","full_text":{{"body":"english"}}}}"#
    ))
    .unwrap()
}
/// Decoded and checked against the table: `Ok` only when both pass.
fn accepted(op: &str, payload: &str) -> Result<(), ProtocolError> {
    let request = parse(op, payload)?;
    request.operation.validate_for_table(&table())
}
fn b64(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = u32::from(b[0]) << 16 | u32::from(b[1]) << 8 | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(A[(n >> (18 - 6 * i)) as usize & 63] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}
fn floats(values: &[f32]) -> String {
    b64(&values
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect::<Vec<u8>>())
}
fn upsert(row: &str) -> String {
    format!(r#"{{"table_id":"{ID}","rows":[{row}]}}"#)
}
fn row_with(extra: &str) -> String {
    let mut fields = String::from(r#"{"id":"a","body":"b","tags":[],"n":1,"f":1.5"#);
    for column in ["v", "s", "m"] {
        if !extra.contains(&format!("\"{column}\":")) {
            fields.push_str(&format!(r#","{column}":null"#));
        }
    }
    if !extra.is_empty() {
        fields.push(',');
        fields.push_str(extra);
    }
    fields.push('}');
    fields
}
/// `request_decode_budget` of nabedra-resources: 2 x bytes + 256 x structural
/// bytes + 4096, charged before a body is parsed.
fn budget(body: &str) -> usize {
    let (mut in_string, mut escaped, mut structural) = (false, false, 0usize);
    for byte in body.bytes() {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
        } else if byte == b'"' {
            in_string = true;
        } else if b"{}[],:".contains(&byte) {
            structural += 1;
        }
    }
    body.len() * 2 + structural * 256 + 4096
}

// ---- 1. duplicate keys ----

#[test]
fn adv_duplicate_json_keys_are_refused_everywhere_a_vector_type_has_a_key() {
    let _g = lock();
    let two = floats(&[1.0, 2.0]);
    let cases: Vec<(&str, String)> = vec![
        (
            "dense twice",
            upsert(&row_with(&format!(
                r#""v":{{"f32":"{two}","f32":"{two}"}}"#
            ))),
        ),
        (
            "sparse i twice",
            upsert(&row_with(r#""s":{"sparse":{"i":"","i":"","f32":""}}"#)),
        ),
        (
            "sparse twice",
            upsert(&row_with(
                r#""s":{"sparse":{"i":"","f32":""},"sparse":{"i":"","f32":""}}"#,
            )),
        ),
        (
            "multi n twice",
            upsert(&row_with(&format!(r#""m":{{"f32s":"{two}","n":1,"n":1}}"#))),
        ),
        (
            "column twice",
            upsert(&row_with(&format!(
                r#""v":{{"f32":"{two}"}},"v":{{"f32":"{two}"}}"#
            ))),
        ),
        (
            "id twice",
            upsert(r#"{"id":"a","id":"b","body":"b","tags":[],"n":1,"f":1.5}"#),
        ),
    ];
    for (name, payload) in cases {
        assert!(parse("table.upsert_many", &payload).is_err(), "{name}");
    }
    let create = |extra: &str| {
        format!(
            r#"{{"name":"t","schema":{{"id":"utf8","v":{{"type":"vector","dim":2{extra}}}}},"key":"id","format":"parquet"}}"#
        )
    };
    assert!(parse("table.create", &create("")).is_ok());
    assert!(
        parse("table.create", &create(r#","dim":2"#)).is_err(),
        "dim twice"
    );
    assert!(
        parse("table.create", &create(r#","type":"vector""#)).is_err(),
        "type twice"
    );
    for (name, extra) in [
        (
            "partition_by twice",
            r#","partition_by":"id","partition_by":"id""#,
        ),
        (
            "full_text key twice",
            r#","full_text":{"id":"english","id":"english"}"#,
        ),
        (
            "embed key twice",
            r#","embed":{"v":{"source":"id","model":"m"},"v":{"source":"id","model":"m"}}"#,
        ),
        (
            "embed field twice",
            r#","embed":{"v":{"source":"id","source":"id","model":"m"}}"#,
        ),
        (
            "full_text twice",
            r#","full_text":{"id":"english"},"full_text":{"id":"english"}"#,
        ),
    ] {
        let payload = format!(
            r#"{{"name":"t","schema":{{"id":"utf8","v":{{"type":"vector","dim":2}}}},"key":"id","format":"parquet"{extra}}}"#
        );
        assert!(parse("table.create", &payload).is_err(), "{name}");
    }
    // The envelope.
    for body in [
        format!(
            r#"{{"request_id":"{ID}","operation":"table.list","operation":"table.drop","payload":{{}}}}"#
        ),
        format!(
            r#"{{"request_id":"{ID}","operation":"table.upsert_many","payload":{},"payload":{}}}"#,
            upsert(""),
            upsert("")
        ),
    ] {
        assert!(parse_oltp_request(body.as_bytes()).is_err(), "{body}");
    }
    // Ranked queries.
    let rank = |extra: &str| {
        format!(
            r#"{{"table_id":"{ID}","query":{{"select":null,"filter":null,"limit":5{extra}}},"rank":{{"rankings":[{{"column":"v","op":"dot","value":{{"f32":"{two}"}}}}]}}}}"#
        )
    };
    assert!(parse("query.rank", &rank("")).is_ok());
    assert!(
        parse("query.rank", &rank(r#","limit":6"#)).is_err(),
        "limit twice"
    );
    assert!(
        parse("query.rank", &rank(r#","select":null"#)).is_err(),
        "select twice"
    );
}

// ---- 2. numbers at the edges ----

#[test]
fn adv_dimensions_accept_exactly_one_to_4096_integers() {
    let _g = lock();
    for (dim, ok) in [
        ("1", true),
        ("4096", true),
        ("0", false),
        ("4097", false),
        ("65535", false),
        ("65536", false),
        ("4294967296", false),
        ("18446744073709551616", false),
        ("-1", false),
        ("-0", false),
        ("2.0", false),
        ("2e0", false),
        ("1e3", false),
        (r#""2""#, false),
        ("null", false),
        ("true", false),
        ("[]", false),
    ] {
        for shape in [
            format!(r#"{{"type":"vector","dim":{dim}}}"#),
            format!(r#"{{"type":"list","item":{{"type":"vector","dim":{dim}}}}}"#),
        ] {
            let payload = format!(
                r#"{{"name":"t","schema":{{"id":"utf8","v":{shape}}},"key":"id","format":"parquet"}}"#
            );
            let result = parse("table.create", &payload);
            assert_eq!(result.is_ok(), ok, "{dim} in {shape}: {:?}", result.err());
        }
    }
}

#[test]
fn adv_multi_count_must_be_an_exact_integer_that_matches_the_data() {
    let _g = lock();
    let two = floats(&[1.0, 2.0]);
    for (n, ok) in [
        ("1", true),
        ("2", false),
        ("0", false),
        ("1024", false),
        ("1025", false),
        ("65535", false),
        ("65536", false),
        ("-1", false),
        ("-0", false),
        ("1.0", false),
        ("1e0", false),
        ("1.5", false),
        (r#""1""#, false),
        ("null", false),
        ("18446744073709551616", false),
    ] {
        let row = row_with(&format!(r#""m":{{"f32s":"{two}","n":{n}}}"#));
        let result = accepted("table.upsert_many", &upsert(&row));
        assert_eq!(result.is_ok(), ok, "n={n}: {:?}", result.err());
    }
    // n = 0 with no data is the empty list, and only that.
    assert!(
        accepted(
            "table.upsert_many",
            &upsert(&row_with(r#""m":{"f32s":"","n":0}"#))
        )
        .is_ok()
    );
    assert!(
        accepted(
            "table.upsert_many",
            &upsert(&row_with(&format!(r#""m":{{"f32s":"{two}","n":0}}"#)))
        )
        .is_err()
    );
    // k vectors of the wrong dimension: 2 vectors of 1 float, 3 floats in 1.
    let three = floats(&[1.0, 2.0, 3.0]);
    assert!(
        accepted(
            "table.upsert_many",
            &upsert(&row_with(&format!(r#""m":{{"f32s":"{three}","n":1}}"#)))
        )
        .is_err()
    );
    assert!(
        accepted(
            "table.upsert_many",
            &upsert(&row_with(&format!(r#""m":{{"f32s":"{three}","n":3}}"#)))
        )
        .is_err()
    );
    let four = floats(&[1.0, 2.0, 3.0, 4.0]);
    assert!(
        accepted(
            "table.upsert_many",
            &upsert(&row_with(&format!(r#""m":{{"f32s":"{four}","n":2}}"#)))
        )
        .is_ok()
    );
    assert!(
        accepted(
            "table.upsert_many",
            &upsert(&row_with(&format!(r#""m":{{"f32s":"{four}","n":4}}"#)))
        )
        .is_err()
    );
}

#[test]
fn adv_floats_must_be_finite_in_every_bit_pattern() {
    let _g = lock();
    for bits in [
        0x7f80_0000u32, // +inf
        0xff80_0000,    // -inf
        0x7fc0_0000,    // qNaN
        0x7f80_0001,    // sNaN
        0xffff_ffff,
    ] {
        let bad = b64(&[bits.to_le_bytes(), 1.0f32.to_le_bytes()].concat());
        let sparse = format!(
            r#""s":{{"sparse":{{"i":"{}","f32":"{}"}}}}"#,
            b64(&0u32.to_le_bytes()),
            b64(&bits.to_le_bytes())
        );
        for row in [
            row_with(&format!(r#""v":{{"f32":"{bad}"}}"#)),
            row_with(&format!(r#""m":{{"f32s":"{bad}","n":1}}"#)),
            row_with(&sparse),
        ] {
            assert!(
                parse("table.upsert_many", &upsert(&row)).is_err(),
                "{bits:#x}"
            );
        }
    }
    // The largest, smallest, subnormal and signed-zero values are values.
    for v in [
        f32::MAX,
        f32::MIN,
        f32::MIN_POSITIVE,
        f32::from_bits(1),
        -0.0,
        0.0,
    ] {
        let row = row_with(&format!(r#""v":{{"f32":"{}"}}"#, floats(&[v, 1.0])));
        assert!(
            accepted("table.upsert_many", &upsert(&row)).is_ok(),
            "{v:e}"
        );
    }
}

#[test]
fn adv_rank_numbers_at_the_edges() {
    let _g = lock();
    let two = floats(&[1.0, 2.0]);
    let rank = |limit: &str, rank_extra: &str| {
        format!(
            r#"{{"table_id":"{ID}","query":{{"select":null,"filter":null,"limit":{limit}}},"rank":{{"rankings":[{{"column":"v","op":"dot","value":{{"f32":"{two}"}}}}]{rank_extra}}}}}"#
        )
    };
    for (limit, ok) in [
        ("1", true),
        ("0", true),
        ("1000", true),
        ("1001", false),
        ("18446744073709551615", false),
        ("18446744073709551616", false),
        ("-1", false),
        ("1.0", false),
        ("1e3", false),
        ("null", false),
        (r#""10""#, false),
    ] {
        let result = accepted("query.rank", &rank(limit, ""));
        assert_eq!(result.is_ok(), ok, "limit {limit}: {:?}", result.err());
    }
    for (extra, ok) in [
        (r#","candidates":1"#, true),
        (r#","candidates":0"#, false),
        (r#","candidates":4294967295"#, true),
        (r#","candidates":4294967296"#, false),
        (r#","candidates":-1"#, false),
        (r#","candidates":1.5"#, false),
        (r#","recall":1"#, true),
        (r#","recall":1.0"#, true),
        (r#","recall":0.5"#, true),
        (r#","recall":0"#, false),
        (r#","recall":-0.0"#, false),
        (r#","recall":1.0000000000000002"#, false),
        (r#","recall":5e-324"#, true),
        (r#","recall":1e-400"#, false),
        (r#","recall":1e400"#, false),
        (r#","recall":"0.9""#, false),
        (r#","recall":null"#, false),
        (r#","exact":1"#, false),
        (r#","exact":null"#, false),
        (r#","consistency":"Latest""#, false),
        (r#","fusion":[1.0]"#, true),
        (r#","fusion":[]"#, false),
        (r#","fusion":[1.0,1.0]"#, false),
        (r#","fusion":[-1.0]"#, false),
        (r#","fusion":[-0.0]"#, true),
        (r#","fusion":[1e400]"#, false),
        (r#","fusion":[null]"#, false),
        (r#","fusion":"RRF""#, false),
        (r#","fusion":"rrf ""#, false),
        (r#","rerank":null"#, true),
        (
            r#","rerank":{"ranking":{"column":"v","op":"dot","value":{"f32":"AAAAAAAAAAA="}},"candidates":0}"#,
            false,
        ),
    ] {
        let result = accepted("query.rank", &rank("1", extra));
        assert_eq!(result.is_ok(), ok, "{extra}: {:?}", result.err());
    }
}

#[test]
fn adv_keys_of_delete_many_at_the_integer_edges() {
    let _g = lock();
    let del = |keys: &str| format!(r#"{{"table_id":"{ID}","keys":{keys}}}"#);
    // The test table has a utf8 key; use an int64-keyed one for the edges.
    let int_table: TableDescriptor = serde_json::from_str(&format!(
        r#"{{"id":"{ID}","name":"t","schema":{{"id":"int64"}},"key":"id","format":"parquet"}}"#
    ))
    .unwrap();
    for (keys, ok) in [
        ("[-9223372036854775808,9223372036854775807]", true),
        ("[9223372036854775808]", false),
        ("[-9223372036854775809]", false),
        ("[1,1]", false),
        ("[1,1.0]", false),
        (r#"[1,"1"]"#, false),
        ("[-0,0]", false),
        ("[1e0]", false),
        ("[null]", false),
        ("[true]", false),
        ("[[1]]", false),
        ("[{}]", false),
        ("{}", false),
        ("null", false),
    ] {
        let result = parse("table.delete_many", &del(keys))
            .and_then(|r| r.operation.validate_for_table(&int_table));
        assert_eq!(result.is_ok(), ok, "{keys}: {:?}", result.err());
    }
}

// ---- 3. base64 ----

#[test]
fn adv_base64_has_one_spelling() {
    let _g = lock();
    let canonical = floats(&[1.0, 2.0]);
    assert_eq!(canonical, "AACAPwAAAEA=");
    let dense = |text: &str| row_with(&format!(r#""v":{{"f32":"{text}"}}"#));
    assert!(accepted("table.upsert_many", &upsert(&dense(&canonical))).is_ok());
    for bad in [
        "AACAPwAAAEA",         // no padding
        "AACAPwAAAEA==",       // too much padding
        "AACAPwAAAEA=\\n",     // newline
        "AACA PwAAAEA=",       // space
        "AACAPw\\nAAAEA=",     // newline inside
        " AACAPwAAAEA=",       // leading space
        "AACAPwAAAEA= ",       // trailing space
        "AACAPwAAAEA=\\u0000", // NUL
        "AACAPwAAAEB=",        // non-zero trailing bits
        "AACAPwAAAEC=",        // non-zero trailing bits
        "AACAPwAAAE_=",        // url alphabet
        "AACAPwAAAE-=",
        "AACAPwAAAEA\\u00e9", // non-ascii
        "AACAPw=AAEA=",       // padding in the middle
        "=ACAPwAAAEA=",
        "====",
        "AACAPwAA",            // 6 bytes: not whole floats
        "AACAPwAAAEAAAA==",    // 9 bytes
        "AAAA",                // 3 bytes
        "",                    // dimension 0
        "AACAPwAAAEA\u{ff21}", // fullwidth A, which a lossy decoder maps
        "AA\\uD800CAPwAAAEA=", // lone surrogate escape
    ] {
        let result = accepted("table.upsert_many", &upsert(&dense(bad)));
        assert!(result.is_err(), "{bad:?}");
    }
    // A JSON escape of a legal character is the same string.
    let escaped = canonical.replacen('A', "\\u0041", 1);
    assert!(accepted("table.upsert_many", &upsert(&dense(&escaped))).is_ok());
    // Many of the same sort inside a sparse object.
    for (i, f) in [
        ("AAAAAA==", "AACAPw=="),
        ("AAAAAA", "AACAPw=="),
        ("AAAAAA==", "AACAPw"),
        ("AAAAAA==", "AACAPw== "),
    ] {
        let row = row_with(&format!(r#""s":{{"sparse":{{"i":"{i}","f32":"{f}"}}}}"#));
        let ok = (i, f) == ("AAAAAA==", "AACAPw==");
        assert_eq!(
            accepted("table.upsert_many", &upsert(&row)).is_ok(),
            ok,
            "{i} {f}"
        );
    }
}

// ---- 4. sparse and multi shapes ----

#[test]
fn adv_sparse_vectors_need_strictly_ascending_matched_indices() {
    let _g = lock();
    let idx = |v: &[u32]| b64(&v.iter().flat_map(|i| i.to_le_bytes()).collect::<Vec<u8>>());
    let sparse = |i: &[u32], f: &[f32]| {
        row_with(&format!(
            r#""s":{{"sparse":{{"i":"{}","f32":"{}"}}}}"#,
            idx(i),
            floats(f)
        ))
    };
    let ok = |i: &[u32], f: &[f32]| accepted("table.upsert_many", &upsert(&sparse(i, f)));
    assert!(ok(&[], &[]).is_ok());
    assert!(ok(&[0], &[1.0]).is_ok());
    assert!(ok(&[0, 1, u32::MAX], &[1.0, 2.0, 3.0]).is_ok());
    assert!(
        ok(&[u32::MAX], &[0.0]).is_ok(),
        "an explicit zero weight is a value"
    );
    assert!(ok(&[1, 0], &[1.0, 2.0]).is_err(), "descending");
    assert!(ok(&[1, 1], &[1.0, 2.0]).is_err(), "equal");
    assert!(
        ok(&[0, 2, 1], &[1.0, 2.0, 3.0]).is_err(),
        "out of order late"
    );
    assert!(ok(&[u32::MAX, 0], &[1.0, 2.0]).is_err());
    assert!(ok(&[0, 1], &[1.0]).is_err(), "fewer weights");
    assert!(ok(&[0], &[1.0, 2.0]).is_err(), "more weights");
    assert!(ok(&[], &[1.0]).is_err());
    assert!(ok(&[0], &[]).is_err());
    // The ranking form of the same rules.
    let rank = |i: &[u32], f: &[f32]| {
        format!(
            r#"{{"table_id":"{ID}","query":{{"select":null,"filter":null,"limit":1}},"rank":{{"rankings":[{{"column":"s","op":"dot","value":{{"sparse":{{"i":"{}","f32":"{}"}}}}}}]}}}}"#,
            idx(i),
            floats(f)
        )
    };
    assert!(accepted("query.rank", &rank(&[1, 2], &[1.0, 1.0])).is_ok());
    assert!(accepted("query.rank", &rank(&[2, 1], &[1.0, 1.0])).is_err());
    assert!(accepted("query.rank", &rank(&[1, 1], &[1.0, 1.0])).is_err());
    assert!(
        accepted("query.rank", &rank(&[1], &[0.0])).is_err()
            || accepted("query.rank", &rank(&[1], &[0.0])).is_ok()
    );
    // cosine of an all-zero sparse query is undefined.
    let cosine = rank(&[1], &[0.0]).replace(r#""op":"dot""#, r#""op":"cosine""#);
    assert!(
        accepted("query.rank", &cosine).is_err(),
        "zero query under cosine"
    );
    let empty = rank(&[], &[]).replace(r#""op":"dot""#, r#""op":"cosine""#);
    assert!(
        accepted("query.rank", &empty).is_err(),
        "empty sparse query under cosine"
    );
}

#[test]
fn adv_vector_object_shapes() {
    let _g = lock();
    let two = floats(&[1.0, 2.0]);
    for (name, value) in [
        ("f32 with n", format!(r#"{{"f32":"{two}","n":1}}"#)),
        (
            "f32 with f32s",
            format!(r#"{{"f32":"{two}","f32s":"{two}"}}"#),
        ),
        (
            "f32 with sparse",
            format!(r#"{{"f32":"{two}","sparse":{{"i":"","f32":""}}}}"#),
        ),
        ("f32s without n", format!(r#"{{"f32s":"{two}"}}"#)),
        ("n without f32s", r#"{"n":1}"#.into()),
        ("empty", "{}".into()),
        ("unknown field", format!(r#"{{"f32":"{two}","x":1}}"#)),
        ("f32 is a number", r#"{"f32":1}"#.into()),
        ("f32 is an array", r#"{"f32":[1.0,2.0]}"#.into()),
        ("f32 is null", r#"{"f32":null}"#.into()),
        ("sparse is an array", r#"{"sparse":[]}"#.into()),
        ("sparse missing i", r#"{"sparse":{"f32":""}}"#.into()),
        (
            "sparse extra",
            r#"{"sparse":{"i":"","f32":"","x":0}}"#.into(),
        ),
        ("positional", format!(r#"["{two}"]"#)),
        ("string", format!(r#""{two}""#)),
    ] {
        for column in ["v", "s", "m"] {
            let row = row_with(&format!(r#""{column}":{value}"#));
            assert!(
                accepted("table.upsert_many", &upsert(&row)).is_err(),
                "{name} in {column}"
            );
        }
    }
    // A value of the wrong kind for its column.
    let dense = format!(r#"{{"f32":"{two}"}}"#);
    for column in ["s", "m", "tags", "body", "n", "f"] {
        let row = row_with(&format!(r#""{column}":{dense}"#));
        // The `row_with` already names n and f; the later key wins in a map, so
        // only the upsert's own duplicate check can refuse this.
        assert!(
            accepted("table.upsert_many", &upsert(&row)).is_err(),
            "dense in {column}"
        );
    }
    // null is for vectors only.
    assert!(
        accepted(
            "table.upsert_many",
            &upsert(&row_with(r#""v":null,"s":null,"m":null"#))
        )
        .is_ok()
    );
    for column in ["id", "body", "tags", "n", "f"] {
        let mut row: Value = serde_json::from_str(&row_with(r#""v":null"#)).unwrap();
        row[column] = Value::Null;
        assert!(
            accepted("table.upsert_many", &upsert(&row.to_string())).is_err(),
            "null in {column}"
        );
    }
    // A list column holds strings only.
    for tags in [
        r#"[1]"#,
        r#"[null]"#,
        r#"[["a"]]"#,
        r#"[{"a":1}]"#,
        r#""a""#,
        r#"{}"#,
        r#"[true]"#,
    ] {
        let row = format!(r#"{{"id":"a","body":"b","tags":{tags},"n":1,"f":1.5,"v":null}}"#);
        assert!(
            accepted("table.upsert_many", &upsert(&row)).is_err(),
            "{tags}"
        );
    }
}

// ---- 5. filters ----

fn nested_not(depth: usize) -> String {
    let mut s = String::new();
    for _ in 0..depth {
        s.push_str(r#"{"op":"not","arg":"#);
    }
    s.push_str(r#"{"op":"contains","column":"tags","value":"x"}"#);
    for _ in 0..depth {
        s.push('}');
    }
    s
}
fn collect(filter: &str) -> String {
    format!(
        r#"{{"table_id":"{ID}","query":{{"select":null,"filter":{filter},"order_by":null,"limit":null}}}}"#
    )
}

#[test]
fn adv_filters_stop_at_depth_32_without_recursing_further() {
    let _g = lock();
    // Depth counts the text node: 31 nots around it is depth 32.
    for (depth, ok) in [(0, true), (31, true), (32, false), (33, false)] {
        let result = accepted("query.collect", &collect(&nested_not(depth)));
        assert_eq!(result.is_ok(), ok, "depth {depth}: {:?}", result.err());
    }
    // A hostile body: the largest one that fits, on a small stack.
    let handle = std::thread::Builder::new()
        .stack_size(1 << 20)
        .spawn(|| {
            for depth in [1_000, 50_000, 400_000] {
                let body = envelope("query.collect", &collect(&nested_not(depth)));
                assert!(body.len() <= 8 << 20 || depth == 400_000);
                let result = parse_oltp_request(body.as_bytes());
                assert!(result.is_err(), "depth {depth}");
            }
            // The same shape in `and`/`or` and inside a ranked query's filter.
            for depth in [1_000, 100_000] {
                let mut f = String::new();
                for _ in 0..depth {
                    f.push_str(r#"{"op":"and","args":[{"op":"starts_with","column":"id","value":"a"},"#);
                }
                f.push_str(r#"{"op":"contains","column":"tags","value":"x"}"#);
                for _ in 0..depth {
                    f.push_str("]}");
                }
                assert!(parse_oltp_request(envelope("query.collect", &collect(&f)).as_bytes()).is_err());
                let rank = format!(
                    r#"{{"table_id":"{ID}","query":{{"select":null,"filter":{f},"limit":1}},"rank":{{"rankings":[{{"column":"body","op":"match","value":"x"}}]}}}}"#
                );
                assert!(parse_oltp_request(envelope("query.rank", &rank).as_bytes()).is_err());
            }
        })
        .unwrap();
    handle.join().expect("deep filters overflowed the stack");
}

#[test]
fn adv_deep_json_in_vector_values_does_not_recurse() {
    let _g = lock();
    let handle = std::thread::Builder::new()
        .stack_size(1 << 20)
        .spawn(|| {
            let deep = "[".repeat(200_000) + &"]".repeat(200_000);
            for column in ["v", "s", "m", "tags", "body"] {
                let row = row_with(&format!(r#""{column}":{deep}"#));
                let _ = parse_oltp_request(envelope("table.upsert_many", &upsert(&row)).as_bytes());
                let obj = r#"{"a":"#.repeat(100_000) + "1" + &"}".repeat(100_000);
                let row = row_with(&format!(r#""{column}":{obj}"#));
                let _ = parse_oltp_request(envelope("table.upsert_many", &upsert(&row)).as_bytes());
                let _ = parse_oltp_request(
                    envelope(
                        "table.upsert",
                        &format!(r#"{{"table_id":"{ID}","row":{row}}}"#),
                    )
                    .as_bytes(),
                );
            }
            let _ = decode_row(&format!(r#"{{"v":{deep}}}"#));
            let _ = decode_upsert_row(&format!(r#"{{"v":{deep}}}"#));
        })
        .unwrap();
    handle.join().expect("deep values overflowed the stack");
}

/// Known to fail. `contains_any` has no bound on its list but the 8 MiB body,
/// and each row compares every one of its tags with every value, so one
/// request of 50,000 values costs 50 times that of 1,000 per scanned row
/// (VEC-G11: CPU limits hold). A set lookup would make it linear in the tags.
#[test]
#[ignore = "open bug 2026-11-26-contains-any-quadratic-cost"]
fn adv_contains_any_evaluation_cost_is_not_quadratic_in_the_list() {
    let _g = lock();
    let rows = |tags: usize| -> Row {
        let mut row = Row::new();
        row.insert(
            "tags".into(),
            Scalar::Utf8List(
                (0..tags)
                    .map(|i| format!("t{i}"))
                    .collect::<Vec<_>>()
                    .into(),
            ),
        );
        row
    };
    let query = |values: usize| -> Query {
        let list: Vec<String> = (0..values).map(|i| format!("v{i}")).collect();
        let filter = json!({"op":"contains_any","column":"tags","values":list});
        let body = envelope(
            "query.collect",
            &format!(
                r#"{{"table_id":"{ID}","query":{{"select":null,"filter":{filter},"order_by":null,"limit":null}}}}"#
            ),
        );
        match parse_oltp_request(body.as_bytes()) {
            Ok(Request {
                operation: Operation::Collect(p),
                ..
            }) => p.query,
            other => panic!("{other:?}"),
        }
    };
    let row = rows(2_000);
    let (small, large) = (query(1_000), query(50_000));
    let time = |q: &Query| {
        let t = std::time::Instant::now();
        for _ in 0..3 {
            assert!(!nabedra_protocol::query_semantics::matches_filter(q, &row));
        }
        t.elapsed()
    };
    let (a, b) = (time(&small), time(&large));
    // 50x the values, a row with 2,000 tags: a hash set would stay within a
    // small factor of the first; a list scan costs 50x.
    assert!(
        b < a * 10 + std::time::Duration::from_millis(50),
        "1,000 values {a:?}, 50,000 values {b:?} for one row of 2,000 tags"
    );
}

// ---- 6. allocation before the bound ----

fn heavy(op: &str, payload: String) -> (usize, usize, usize, bool) {
    let body = envelope(op, &payload);
    let len = body.len();
    assert!(len <= 8 << 20, "{len}");
    let allowed = budget(&body);
    let (result, used) = peak(|| parse_oltp_request(body.as_bytes()).is_ok());
    (len, used, allowed, result)
}

#[test]
fn adv_nothing_allocates_more_than_a_small_multiple_of_its_input_before_a_bound() {
    let _g = lock();
    let mb = 7 << 20;
    let cases: Vec<(&str, String, String)> = vec![
        (
            "dense of 7 MiB of base64",
            "table.upsert_many".into(),
            upsert(&row_with(&format!(r#""v":{{"f32":"{}"}}"#, "A".repeat(mb)))),
        ),
        (
            "sparse i of 7 MiB",
            "table.upsert_many".into(),
            upsert(&row_with(&format!(
                r#""s":{{"sparse":{{"i":"{}","f32":""}}}}"#,
                "A".repeat(mb)
            ))),
        ),
        (
            "multi of 7 MiB",
            "table.upsert_many".into(),
            upsert(&row_with(&format!(
                r#""m":{{"f32s":"{}","n":1}}"#,
                "A".repeat(mb)
            ))),
        ),
        (
            "dense with escapes",
            "table.upsert_many".into(),
            upsert(&row_with(&format!(
                r#""v":{{"f32":"{}"}}"#,
                "\\u0041".repeat(mb / 6)
            ))),
        ),
        (
            "delete_many of 3.5M keys",
            "table.delete_many".into(),
            format!(
                r#"{{"table_id":"{ID}","keys":[{}]}}"#,
                "1,".repeat(mb / 2) + "1"
            ),
        ),
        (
            "upsert_many of 3.5M empty rows",
            "table.upsert_many".into(),
            format!(
                r#"{{"table_id":"{ID}","rows":[{}]}}"#,
                "{},".repeat(mb / 3) + "{}"
            ),
        ),
        (
            "contains_any of 2.3M empty strings",
            "query.collect".into(),
            collect(&format!(
                r#"{{"op":"contains_any","column":"tags","values":[{}]}}"#,
                "\"\",".repeat(mb / 3) + "\"\""
            )),
        ),
        (
            "tags of 2.3M empty strings",
            "table.upsert_many".into(),
            upsert(&format!(
                r#"{{"id":"a","body":"b","tags":[{}],"n":1,"f":1.5}}"#,
                "\"\",".repeat(mb / 3) + "\"\""
            )),
        ),
        (
            "fusion of 3.5M weights",
            "query.rank".into(),
            format!(
                r#"{{"table_id":"{ID}","query":{{"select":null,"filter":null,"limit":1}},"rank":{{"rankings":[{{"column":"body","op":"match","value":"x"}}],"fusion":[{}]}}}}"#,
                "0,".repeat(mb / 2) + "0"
            ),
        ),
        (
            "rankings of 100k",
            "query.rank".into(),
            format!(
                r#"{{"table_id":"{ID}","query":{{"select":null,"filter":null,"limit":1}},"rank":{{"rankings":[{}]}}}}"#,
                r#"{"column":"body","op":"match","value":"x"},"#.repeat(100_000)
                    + r#"{"column":"body","op":"match","value":"x"}"#
            ),
        ),
        (
            "batch of 100k",
            "query.rank_many".into(),
            format!(
                r#"{{"table_id":"{ID}","query":{{"select":null,"filter":null,"limit":1}},"rank":{{"rankings":[]}},"batch":[{}]}}"#,
                r#"{"column":"body","op":"match","value":"x"},"#.repeat(100_000)
                    + r#"{"column":"body","op":"match","value":"x"}"#
            ),
        ),
        (
            "full_text of 1M entries",
            "table.create".into(),
            format!(
                r#"{{"name":"t","schema":{{"id":"utf8"}},"key":"id","format":"parquet","full_text":{{{}}}}}"#,
                (0..200_000)
                    .map(|i| format!(r#""c{i}":"english""#))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        ),
        (
            "embed of 200k entries",
            "table.create".into(),
            format!(
                r#"{{"name":"t","schema":{{"id":"utf8"}},"key":"id","format":"parquet","embed":{{{}}}}}"#,
                (0..100_000)
                    .map(|i| format!(r#""c{i}":{{"source":"id","model":"m"}}"#))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        ),
        (
            "schema of 200k columns",
            "table.create".into(),
            format!(
                r#"{{"name":"t","schema":{{{}}},"key":"id","format":"parquet"}}"#,
                (0..200_000)
                    .map(|i| format!(r#""c{i}":"utf8""#))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        ),
    ];
    let mut report = Vec::new();
    for (name, op, payload) in cases {
        let (len, used, allowed, ok) = heavy(&op, payload);
        println!(
            "{name}: input {len} B, peak {used} B ({:.1}x), budget {allowed} B, accepted={ok}",
            used as f64 / len as f64
        );
        // The gateway charges `allowed` before it parses; decoding must stay
        // inside it, and a refused body must stay near its own size.
        if used > allowed || (!ok && used > 3 * len + (1 << 20) && name != "schema of 200k columns")
        {
            report.push(format!("{name}: {used} B for {len} B, budget {allowed}"));
        }
    }
    assert!(report.is_empty(), "{report:#?}");
}

// ---- 7. the unchanged grammar ----

#[test]
fn adv_the_scalar_grammar_refuses_every_new_operation_and_type_like_an_unknown_one() {
    let _g = lock();
    let unknown = parse_request(envelope("table.nosuch", "{}").as_bytes()).unwrap_err();
    let two = floats(&[1.0, 2.0]);
    for op in [
        "table.upsert_many",
        "table.delete_many",
        "table.add_column",
        "table.drop_column",
        "table.add_full_text",
        "table.drop_full_text",
        "table.search_status",
        "query.rank",
        "query.rank_many",
    ] {
        let error = parse_request(envelope(op, "{}").as_bytes()).unwrap_err();
        assert_eq!(error.code, unknown.code, "{op}");
        assert_eq!(error.outcome, unknown.outcome, "{op}");
        assert_eq!(
            error.message.replace(op, "X"),
            unknown.message.replace("table.nosuch", "X"),
            "{op}"
        );
        // And with a payload that the full grammar accepts.
        let full = match op {
            "table.upsert_many" => upsert(&row_with(&format!(r#""v":{{"f32":"{two}"}}"#))),
            "table.delete_many" => format!(r#"{{"table_id":"{ID}","keys":["a"]}}"#),
            _ => "{}".into(),
        };
        let error = parse_request(envelope(op, &full).as_bytes()).unwrap_err();
        assert_eq!(
            (error.code, error.outcome),
            (unknown.code, unknown.outcome),
            "{op}"
        );
    }
    let plain = |x: &str| {
        format!(r#"{{"name":"t","schema":{{"id":"utf8"{x}}},"key":"id","format":"parquet"}}"#)
    };
    let malformed =
        parse_request(envelope("table.create", r#"{"name":1}"#).as_bytes()).unwrap_err();
    for ty in [
        r#","v":{"type":"vector","dim":2}"#,
        r#","v":{"type":"vector","sparse":true}"#,
        r#","v":{"type":"list","item":"utf8"}"#,
    ] {
        let error = parse_request(envelope("table.create", &plain(ty)).as_bytes()).unwrap_err();
        assert_eq!(error.message, malformed.message, "{ty}");
        assert_eq!(
            (error.code, error.outcome),
            (malformed.code, malformed.outcome)
        );
    }
    for extra in [
        r#","partition_by":"id""#,
        r#","full_text":{"id":"english"}"#,
        r#","embed":{"id":{"source":"id","model":"m"}}"#,
    ] {
        let payload = plain("").replace(
            r#""format":"parquet""#,
            &format!(r#""format":"parquet"{extra}"#),
        );
        let error = parse_request(envelope("table.create", &payload).as_bytes()).unwrap_err();
        assert_eq!(error.message, malformed.message, "{extra}");
    }
    // Rows and filters.
    for (op, payload) in [
        (
            "table.insert",
            format!(r#"{{"table_id":"{ID}","row":{{"id":"a","v":null}}}}"#),
        ),
        (
            "table.insert",
            format!(r#"{{"table_id":"{ID}","row":{{"id":"a","v":["x"]}}}}"#),
        ),
        (
            "table.insert",
            format!(r#"{{"table_id":"{ID}","row":{{"id":"a","v":{{"f32":"{two}"}}}}}}"#),
        ),
        (
            "table.update",
            format!(r#"{{"table_id":"{ID}","key":"a","values":{{"v":null}}}}"#),
        ),
        (
            "query.collect",
            collect(r#"{"op":"contains","column":"tags","value":"x"}"#),
        ),
        (
            "query.collect",
            collect(r#"{"op":"starts_with","column":"id","value":"x"}"#),
        ),
        (
            "query.count",
            collect(r#"{"op":"contains_any","column":"tags","values":[]}"#),
        ),
    ] {
        let scalar = parse_request(envelope(op, &payload).as_bytes());
        let error = scalar.expect_err(&format!("{op} {payload}"));
        assert_eq!(error.message, malformed.message, "{op} {payload}");
    }
}
