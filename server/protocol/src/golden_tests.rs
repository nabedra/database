//! Bytes captured at 265a111d, before vector columns existed. Requests and
//! descriptors without search options keep them exactly, in both grammars.
use crate::protocol::*;

macro_rules! id {
    () => {
        "67e55044-10b1-426f-9247-bb680e5fe0c8"
    };
}
macro_rules! t {
    () => {
        concat!(r#""table_id":""#, id!(), r#"""#)
    };
}
macro_rules! row_in {
    () => {
        r#"{"active":true,"age":24,"id":"a","score":1.50}"#
    };
}
macro_rules! row_out {
    () => {
        r#"{"active":true,"age":24,"id":"a","score":1.5}"#
    };
}
macro_rules! query_in {
    () => {
        concat!("{", t!(), r#","query":{"select":["id","age"],"filter":{"op":"and","args":[{"op":"or","args":[{"op":"gt","column":"age","value":18},{"op":"not","arg":{"op":"eq","column":"score","value":1e2}}]},{"op":"ne","column":"active","value":false}]},"order_by":{"column":"age","desc":true},"limit":10}}"#)
    };
}
macro_rules! query_out {
    () => {
        concat!("{", t!(), r#","query":{"select":["id","age"],"filter":{"op":"and","args":[{"op":"or","args":[{"op":"gt","column":"age","value":18},{"op":"not","arg":{"op":"eq","column":"score","value":100.0}}]},{"op":"ne","column":"active","value":false}]},"order_by":{"column":"age","desc":true},"limit":10}}"#)
    };
}
macro_rules! all {
    () => {
        concat!(
            "{",
            t!(),
            r#","query":{"select":null,"filter":null,"order_by":null,"limit":null}}"#
        )
    };
}

/// (operation, payload sent, payload encoded by the old build)
#[rustfmt::skip]
const CASES: [(&str, &str, &str); 14] = [
    ("table.create",
     r#"{"name":"users","schema":{"id":"utf8","age":"int64","score":"float64","active":"bool"},"key":"id","format":"parquet"}"#,
     r#"{"name":"users","schema":{"active":"bool","age":"int64","id":"utf8","score":"float64"},"key":"id","format":"parquet"}"#),
    ("table.describe", r#"{"name":"users"}"#, r#"{"name":"users"}"#),
    ("table.list", "{}", "{}"),
    ("table.drop", concat!("{", t!(), "}"), concat!("{", t!(), "}")),
    ("table.insert",
     concat!("{", t!(), r#","row":"#, row_in!(), "}"),
     concat!("{", t!(), r#","row":"#, row_out!(), "}")),
    ("table.insert_many",
     concat!("{", t!(), r#","rows":["#, row_in!(), ",", row_in!(), "]}"),
     concat!("{", t!(), r#","rows":["#, row_out!(), ",", row_out!(), "]}")),
    ("table.get", concat!("{", t!(), r#","key":"a"}"#), concat!("{", t!(), r#","key":"a"}"#)),
    ("table.get", concat!("{", t!(), r#","key":-7}"#), concat!("{", t!(), r#","key":-7}"#)),
    ("table.update",
     concat!("{", t!(), r#","key":"a","values":{"age":25}}"#),
     concat!("{", t!(), r#","key":"a","values":{"age":25}}"#)),
    ("table.upsert",
     concat!("{", t!(), r#","row":"#, row_in!(), "}"),
     concat!("{", t!(), r#","row":"#, row_out!(), "}")),
    ("table.delete", concat!("{", t!(), r#","key":"a"}"#), concat!("{", t!(), r#","key":"a"}"#)),
    ("query.collect", query_in!(), query_out!()),
    ("query.count", all!(), all!()),
    ("query.explain", query_in!(), query_out!()),
];
const DESCRIPTOR: &str = concat!(
    r#"{"id":""#,
    id!(),
    r#"","name":"users","schema":{"active":"bool","age":"int64","id":"utf8","score":"float64"},"key":"id","format":"parquet"}"#
);

fn descriptor() -> TableDescriptor {
    serde_json::from_str(DESCRIPTOR).unwrap()
}

#[test]
fn vec_g14_scalar_requests_keep_their_bytes_in_both_grammars() {
    for (operation, sent, encoded) in CASES {
        let raw = format!(
            r#"{{"payload":{sent},"operation":"{operation}","request_id":"{}"}}"#,
            id!()
        );
        let golden = format!(
            r#"{{"request_id":"{}","operation":"{operation}","payload":{encoded}}}"#,
            id!()
        );
        for parse in [parse_request, parse_oltp_request] {
            let request = parse(raw.as_bytes()).unwrap();
            assert_eq!(serde_json::to_string(&request).unwrap(), golden);
            assert_eq!(request.operation.name(), operation);
            if !sent.contains(":-7") {
                request.operation.validate_for_table(&descriptor()).unwrap();
            }
        }
        assert_eq!(
            serde_json::to_string(&serde_json::from_str::<Request>(&raw).unwrap()).unwrap(),
            golden
        );
    }
}

#[test]
fn vec_g14_scalar_descriptors_keep_their_bytes_and_values_their_size() {
    assert_eq!(serde_json::to_string(&descriptor()).unwrap(), DESCRIPTOR);
    assert!(!descriptor().uses_search());
    assert_eq!(std::mem::size_of::<Scalar>(), 24);
}
