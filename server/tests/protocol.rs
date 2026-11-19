#![cfg(any(feature = "olap", feature = "oltp"))]

use nabedra_database_server::protocol::*;
use serde_json::{Value, json};

const ID: &str = "67e55044-10b1-426f-9247-bb680e5fe0c8";
fn request(op: &str, payload: Value) -> Result<Request, ProtocolError> {
    parse_request(
        &serde_json::to_vec(&json!({"request_id":ID,"operation":op,"payload":payload})).unwrap(),
    )
}
fn table() -> TableDescriptor {
    serde_json::from_value(json!({"id":ID,"name":"users","schema":{"id":"utf8","age":"int64","score":"float64","active":"bool"},"key":"id","format":"parquet"})).unwrap()
}
fn query(filter: Value, limit: Value) -> Value {
    json!({"select":null,"filter":filter,"order_by":null,"limit":limit})
}
#[test]
fn operation_roundtrips_and_error_envelopes() {
    let row = json!({"id":"a","age":24,"score":1.5,"active":true});
    for (op, payload) in [
        (
            "table.create",
            json!({"name":"users","schema":{"id":"utf8"},"key":"id","format":"parquet"}),
        ),
        ("table.describe", json!({"name":"users"})),
        ("table.list", json!({})),
        ("table.drop", json!({"table_id":ID})),
        ("table.insert", json!({"table_id":ID,"row":row})),
        ("table.insert_many", json!({"table_id":ID,"rows":[row]})),
        ("table.get", json!({"table_id":ID,"key":"a"})),
        (
            "table.update",
            json!({"table_id":ID,"key":"a","values":{"age":25}}),
        ),
        ("table.upsert", json!({"table_id":ID,"row":row})),
        ("table.delete", json!({"table_id":ID,"key":"a"})),
        (
            "query.collect",
            json!({"table_id":ID,"query":query(Value::Null,Value::Null)}),
        ),
        (
            "query.count",
            json!({"table_id":ID,"query":query(Value::Null,json!(0))}),
        ),
        (
            "query.explain",
            json!({"table_id":ID,"query":query(json!({"op":"gt","column":"age","value":18}),json!(10))}),
        ),
    ] {
        let req = request(op, payload.clone()).unwrap();
        req.operation.validate_for_table(&table()).unwrap();
        assert_eq!(
            serde_json::to_value(&req).unwrap(),
            json!({"request_id":ID,"operation":op,"payload":payload})
        );
        assert_eq!(
            serde_json::from_slice::<Request>(&serde_json::to_vec(&req).unwrap()).unwrap(),
            req
        );
    }
    let error = ProtocolError::new(
        ErrorCode::OutcomeUnknown,
        "Publication response lost",
        Outcome::Unknown,
    );
    assert_eq!(error.status_code(), 503);
    assert_eq!(
        serde_json::to_value(ErrorResponse {
            request_id: None,
            error
        })
        .unwrap(),
        json!({"request_id":null,"error":{"code":"outcome_unknown","message":"Publication response lost","outcome":"unknown"}})
    );
    assert_eq!(
        serde_json::to_value(SuccessResponse {
            request_id: uuid::Uuid::parse_str(ID).unwrap(),
            data: ()
        })
        .unwrap(),
        json!({"request_id":ID,"data":null})
    );
}
#[test]
fn strict_shapes_and_operation_classification() {
    for bad_query in [
        json!({"select":null,"filter":null,"order_by":[],"limit":null}),
        json!({"select":null,"filter":null,"order_by":null,"limit":null,"extra":0}),
        query(
            json!({"op":"and","args":[{"op":"eq","column":"id","value":"a"}],"extra":0}),
            Value::Null,
        ),
        query(
            json!({"op":"eq","column":"age","value":{"op":"eval","code":"1+1"}}),
            Value::Null,
        ),
    ] {
        assert_eq!(
            request("query.collect", json!({"table_id":ID,"query":bad_query}))
                .unwrap_err()
                .code,
            ErrorCode::ValidationError
        );
    }

    assert_eq!(
        request("sql.execute", json!({"code":"anything"}))
            .unwrap_err()
            .code,
        ErrorCode::UnsupportedOperation
    );
    for (op, payload) in [
        ("table.list", json!({"extra":1})),
        ("table.list", json!([])),
        (
            "query.collect",
            json!({"table_id":ID,"query":[null,null,null,null]}),
        ),
        ("table.get", json!({"table_id":"bad","key":"a"})),
        (
            "table.create",
            json!({"name":"x","schema":{"id":"utf8"},"key":"id","format":"csv"}),
        ),
        (
            "query.collect",
            json!({"table_id":ID,"query":{"select":null,"filter":null,"order_by":null}}),
        ),
        (
            "query.collect",
            json!({"table_id":ID,"query":query(json!({"op":"eval","code":"True"}),Value::Null)}),
        ),
        (
            "query.collect",
            json!({"table_id":ID,"query":query(json!({"op":"eq","column":"id","value":"a","extra":true}),Value::Null)}),
        ),
        ("table.insert", json!({"table_id":ID,"row":{"id":null}})),
        ("table.get", json!({"table_id":ID,"key":true})),
    ] {
        assert_eq!(
            request(op, payload).unwrap_err().code,
            ErrorCode::ValidationError,
            "{op}"
        );
    }
    for raw in [
        r#"{"request_id":"bad","operation":"table.list","payload":{}}"#.to_string(),
        format!(r#"{{"request_id":"{ID}","operation":"table.list","payload":{{}},"extra":1}}"#),
    ] {
        assert_eq!(
            parse_request(raw.as_bytes()).unwrap_err().code,
            ErrorCode::ValidationError
        );
    }
}
#[test]
fn schema_rows_and_exact_scalars() {
    let t = table();
    t.validate().unwrap();
    let mut edge = t.clone();
    edge.schema = (0..128)
        .map(|i| (format!("c{i}"), ColumnType::Int64))
        .collect();
    edge.key = "c0".into();
    edge.validate().unwrap();
    edge.schema.insert("c128".into(), ColumnType::Int64);
    assert!(edge.validate().is_err());
    for name in ["", "1bad", "é", &"x".repeat(65)] {
        assert!(validate_identifier(name).is_err());
    }
    validate_identifier(&"x".repeat(64)).unwrap();
    for schema in [
        json!({}),
        json!({"id":"bool"}),
        json!({"bad-name":"utf8"}),
        json!({"x":"utf8"}),
    ] {
        let mut bad = t.clone();
        bad.schema = serde_json::from_value(schema).unwrap();
        assert!(bad.validate().is_err());
    }
    for row in [
        json!({"id":"a","age":1.0,"score":1.0,"active":true}),
        json!({"id":"a","age":true,"score":1.0,"active":true}),
        json!({"id":"a","age":1,"score":1,"active":true}),
        json!({"id":"a"}),
        json!({"id":"a","extra":1}),
    ] {
        let req = request("table.insert", json!({"table_id":ID,"row":row})).unwrap();
        assert!(req.operation.validate_for_table(&t).is_err());
    }
    for values in [
        json!({"id":"b"}),
        json!({"unknown":1}),
        json!({"age":false}),
    ] {
        assert!(
            request(
                "table.update",
                json!({"table_id":ID,"key":"a","values":values})
            )
            .unwrap()
            .operation
            .validate_for_table(&t)
            .is_err()
        );
    }
    assert!(
        request("table.update", json!({"table_id":ID,"key":"a","values":{}}))
            .unwrap()
            .operation
            .validate_for_table(&t)
            .is_ok()
    );
    for raw in [
        "9223372036854775808",
        "-9223372036854775809",
        "1e400",
        "null",
        "{}",
        "[]",
    ] {
        assert!(serde_json::from_str::<Scalar>(raw).is_err(), "{raw}");
    }
    for raw in [
        "-9223372036854775808",
        "9223372036854775807",
        "1.0",
        "true",
        "\"literal code is only a string\"",
    ] {
        assert!(serde_json::from_str::<Scalar>(raw).is_ok(), "{raw}");
    }
    assert!(serde_json::to_string(&Scalar::Float64(f64::INFINITY)).is_err());
    assert!(validate_scalar(&Scalar::Float64(f64::NAN), ColumnType::Float64).is_err());
}
#[test]
fn query_depth_and_limits_are_enforced() {
    let t = table();
    for q in [
        query(
            json!({"op":"gt","column":"active","value":true}),
            Value::Null,
        ),
        query(json!({"op":"and","args":[]}), Value::Null),
        query(json!({"op":"eq","column":"age","value":1.0}), Value::Null),
        query(Value::Null, json!(-1)),
        query(Value::Null, json!(true)),
        query(Value::Null, json!(1.0)),
        json!({"select":[],"filter":null,"order_by":null,"limit":null}),
        json!({"select":["id","id"],"filter":null,"order_by":null,"limit":null}),
        json!({"select":["absent"],"filter":null,"order_by":null,"limit":null}),
        json!({"select":null,"filter":null,"order_by":{"column":"absent","desc":false},"limit":null}),
    ] {
        assert!(
            request("query.collect", json!({"table_id":ID,"query":q}))
                .and_then(|r| r.operation.validate_for_table(&t))
                .is_err()
        );
    }
    let mut pred = json!({"op":"eq","column":"id","value":"a"});
    for _ in 1..32 {
        pred = json!({"op":"not","arg":pred});
    }
    assert!(
        request(
            "query.collect",
            json!({"table_id":ID,"query":query(pred.clone(),json!(10001))})
        )
        .unwrap()
        .operation
        .validate_for_table(&t)
        .is_ok()
    );
    pred = json!({"op":"not","arg":pred});
    assert!(
        request(
            "query.collect",
            json!({"table_id":ID,"query":query(pred,Value::Null)})
        )
        .is_err()
    );
    let rows = vec![json!({"id":"a"}); MAX_BATCH_ROWS];
    assert!(request("table.insert_many", json!({"table_id":ID,"rows":rows})).is_ok());
    let rows = vec![json!({"id":"a"}); MAX_BATCH_ROWS + 1];
    assert_eq!(
        request("table.insert_many", json!({"table_id":ID,"rows":rows}))
            .unwrap_err()
            .code,
        ErrorCode::LimitExceeded
    );
    assert_eq!(
        parse_request(&vec![b' '; MAX_BODY_BYTES + 1])
            .unwrap_err()
            .code,
        ErrorCode::LimitExceeded
    );
}

#[test]
fn query_limit_accepts_unbounded_integer_wire_syntax() {
    let long_integer = "9".repeat(4096);
    let accepted = [
        ("null", None),
        ("0", Some(0)),
        ("1", Some(1)),
        ("100001", Some(100001)),
        ("9223372036854775808", Some(1_u64 << 63)),
        ("18446744073709551614", Some(u64::MAX - 1)),
        ("18446744073709551615", Some(u64::MAX)),
        ("18446744073709551616", Some(u64::MAX)),
        (long_integer.as_str(), Some(u64::MAX)),
    ];
    let rejected = [
        "-1",
        "-0",
        "1.0",
        "1.5",
        "1e0",
        "1E+2",
        "1e400",
        "true",
        "false",
        r#""1""#,
        "{}",
        "[]",
        "[1]",
        r#"{"$serde_json::private::Number":"1"}"#,
        r#"{"$serde_json::private::RawValue":"1"}"#,
        r#"{"$serde_json::private::Number":"18446744073709551616"}"#,
        r#"{"$serde_json::private::RawValue":"18446744073709551616"}"#,
    ];
    for op in ["query.collect", "query.count", "query.explain"] {
        let parse_limit = |literal: &str| {
            let raw = format!(
                r#"{{"request_id":"{ID}","operation":"{op}","payload":{{"table_id":"{ID}","query":{{"select":null,"filter":null,"order_by":null,"limit":{literal}}}}}}}"#
            );
            parse_request(raw.as_bytes())
        };
        for (literal, expected) in accepted {
            let parsed = parse_limit(literal).unwrap_or_else(|e| panic!("{op} {literal}: {e}"));
            let (Operation::Collect(p) | Operation::Count(p) | Operation::Explain(p)) =
                parsed.operation
            else {
                panic!("expected query operation");
            };
            assert_eq!(p.query.limit, expected, "{op} {literal}");
        }
        for literal in rejected {
            assert_eq!(
                parse_limit(literal).unwrap_err().code,
                ErrorCode::ValidationError,
                "{op} {literal}"
            );
        }
    }
}

#[test]
fn scalar_syntax_survives_rows_and_nested_predicates() {
    let cases = [
        ("-9223372036854775808", Some(Scalar::Int64(i64::MIN))),
        ("9223372036854775807", Some(Scalar::Int64(i64::MAX))),
        ("1", Some(Scalar::Int64(1))),
        ("1.0", Some(Scalar::Float64(1.0))),
        ("1e0", Some(Scalar::Float64(1.0))),
        ("1E+2", Some(Scalar::Float64(100.0))),
        ("true", Some(Scalar::Bool(true))),
        (r#""literal""#, Some(Scalar::Utf8("literal".into()))),
        ("9223372036854775808", None),
        ("-9223372036854775809", None),
        ("1e400", None),
        (r#"{"$serde_json::private::Number":"1"}"#, None),
        (r#"{"$serde_json::private::RawValue":"1"}"#, None),
        ("{}", None),
        ("[]", None),
        ("null", None),
    ];
    for (literal, expected) in cases {
        let scalar = serde_json::from_str::<Scalar>(literal);
        assert_eq!(scalar.ok(), expected, "direct scalar {literal}");
        let row = format!(r#"{{"table_id":"{ID}","row":{{"id":{literal}}}}}"#);
        let filter = format!(
            r#"{{"op":"and","args":[{{"op":"or","args":[{{"op":"not","arg":{{"op":"eq","column":"id","value":{literal}}}}}]}}]}}"#
        );
        let q = format!(
            r#"{{"table_id":"{ID}","query":{{"select":null,"filter":{filter},"order_by":null,"limit":null}}}}"#
        );
        for (op, payload) in [("table.insert", row), ("query.collect", q)] {
            let raw = format!(r#"{{"request_id":"{ID}","operation":"{op}","payload":{payload}}}"#);
            let parsed = parse_request(raw.as_bytes());
            // Adjacent enum tags may appear after their payload on the wire.
            let operation_json = format!(r#"{{"payload":{payload},"operation":"{op}"}}"#);
            assert_eq!(
                serde_json::from_str::<Operation>(&operation_json).is_ok(),
                expected.is_some(),
                "operation {literal}"
            );
            assert_eq!(
                serde_json::from_str::<Request>(&raw).is_ok(),
                expected.is_some(),
                "request {literal}"
            );
            if let Some(expected) = &expected {
                let parsed = parsed.unwrap_or_else(|e| panic!("{op} {literal}: {e}"));
                let mut t = table();
                t.schema = [(
                    "id".into(),
                    match expected {
                        Scalar::Int64(_) => ColumnType::Int64,
                        Scalar::Float64(_) => ColumnType::Float64,
                        Scalar::Utf8(_) => ColumnType::Utf8,
                        Scalar::Bool(_) => ColumnType::Bool,
                    },
                )]
                .into();
                // Float/bool keys are not valid descriptors; compare decoded values instead.
                match parsed.operation {
                    Operation::Insert(p) => assert_eq!(&p.row["id"], expected),
                    Operation::Collect(p) => {
                        let predicate = p.query.filter.unwrap();
                        predicate.validate(&t.schema).unwrap();
                        let mut leaf = &predicate;
                        loop {
                            leaf = match leaf {
                                Predicate::Comparison(c) => {
                                    assert_eq!(&c.value, expected);
                                    break;
                                }
                                Predicate::Logical(
                                    LogicalPredicate::And { args } | LogicalPredicate::Or { args },
                                ) => &args[0],
                                Predicate::Logical(LogicalPredicate::Not { arg }) => arg,
                            };
                        }
                    }
                    _ => unreachable!(),
                }
            } else {
                assert_eq!(
                    parsed.unwrap_err().code,
                    ErrorCode::ValidationError,
                    "{op} {literal}"
                );
            }
        }
    }
    for marker in [
        "$serde_json::private::Number",
        "$serde_json::private::RawValue",
    ] {
        assert!(serde_json::from_value::<Scalar>(json!({marker:"1"})).is_err());
    }
}

#[test]
fn wire_enums_require_strings() {
    for (op, payload) in [
        (
            "table.create",
            json!({"name":"t","schema":{"id":{"int64":null}},"key":"id","format":"parquet"}),
        ),
        (
            "table.create",
            json!({"name":"t","schema":{"id":"int64"},"key":"id","format":{"parquet":null}}),
        ),
        (
            "query.collect",
            json!({"table_id":ID,"query":query(json!({"op":{"eq":null},"column":"id","value":1}),Value::Null)}),
        ),
        (
            "query.collect",
            json!({"table_id":ID,"query":query(json!({"op":{"not":null},"arg":{"op":"eq","column":"id","value":1}}),Value::Null)}),
        ),
    ] {
        assert_eq!(
            request(op, payload).unwrap_err().code,
            ErrorCode::ValidationError
        );
    }
    for (name, ty) in [
        ("utf8", ColumnType::Utf8),
        ("int64", ColumnType::Int64),
        ("float64", ColumnType::Float64),
        ("bool", ColumnType::Bool),
    ] {
        assert_eq!(
            serde_json::from_value::<ColumnType>(json!(name)).unwrap(),
            ty
        );
        assert_eq!(serde_json::to_value(ty).unwrap(), json!(name));
        assert!(serde_json::from_value::<ColumnType>(json!({name:null})).is_err());
    }
    assert_eq!(
        serde_json::from_value::<TableFormat>(json!("parquet")).unwrap(),
        TableFormat::Parquet
    );
    assert_eq!(
        serde_json::to_value(TableFormat::Parquet).unwrap(),
        json!("parquet")
    );
    assert!(serde_json::from_value::<TableFormat>(json!({"parquet":null})).is_err());
    for (name, op) in [
        ("eq", ComparisonOp::Eq),
        ("ne", ComparisonOp::Ne),
        ("gt", ComparisonOp::Gt),
        ("ge", ComparisonOp::Ge),
        ("lt", ComparisonOp::Lt),
        ("le", ComparisonOp::Le),
    ] {
        assert_eq!(
            serde_json::from_value::<ComparisonOp>(json!(name)).unwrap(),
            op
        );
        assert_eq!(serde_json::to_value(op).unwrap(), json!(name));
        assert!(serde_json::from_value::<ComparisonOp>(json!({name:null})).is_err());
    }
}

/// O01/O21: request limits and scalar counts do not inherit OLAP table caps.
#[test]
fn count_requests_and_results_have_no_legacy_table_ceiling() {
    for n in [100_001_u64, 1 << 40, u64::MAX] {
        let request = request(
            "query.count",
            json!({"table_id": ID, "query": query(Value::Null, json!(n))}),
        )
        .unwrap();
        request.operation.validate_for_table(&table()).unwrap();
        let Operation::Count(payload) = request.operation else {
            panic!("count operation")
        };
        assert_eq!(payload.query.limit, Some(n));
        let response = SuccessResponse {
            request_id: uuid::Uuid::parse_str(ID).unwrap(),
            data: n,
        };
        let bytes = serde_json::to_vec(&response).unwrap();
        let decoded: SuccessResponse<u64> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.data, n);
    }
}
