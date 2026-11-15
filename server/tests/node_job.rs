use meshdb_server::node::job::{
    PartMeta, PreparationValidator, encode_job, encode_part_meta, encode_result, parse_get_rows,
    parse_job, parse_part_meta, parse_result, validate_get_rows,
};
use serde_json::{Value, json};

fn job(task: Value) -> Value {
    json!({"version":1,"cluster_id":"00000000-0000-0000-0000-000000000001",
        "operation_id":"00000000-0000-0000-0000-000000000002",
        "job_id":"00000000-0000-0000-0000-000000000003",
        "attempt_id":"00000000-0000-0000-0000-000000000004",
        "request_digest":"a".repeat(64),"snapshot":{"catalog_revision":2,
        "catalog_commit_id":"00000000-0000-0000-0000-000000000005","catalog_etag":"\"token\""},
        "ordinal":0,"table":{"id":"00000000-0000-0000-0000-000000000006","name":"items",
        "schema":{"id":"int64","score":"float64"},"key":"id","format":"parquet"},"task":task})
}
fn file() -> Value {
    json!({"kind":"legacy","payload":{"object":"tables/00000000-0000-0000-0000-000000000006/00000000-0000-0000-0000-000000000007.parquet","row_count":1}})
}
fn query() -> Value {
    json!({"select":null,"filter":null,"order_by":null,"limit":null})
}
fn result(j: &Value, result: Value) -> Value {
    let mut r = j.clone();
    r.as_object_mut().unwrap().remove("table");
    r.as_object_mut().unwrap().remove("task");
    r["result"] = result;
    r
}
#[test]
fn task_and_result_round_trips_preserve_scalar_types() {
    let cases = [
        (
            json!({"kind":"get","payload":{"file":file(),"key":1}}),
            json!({"kind":"get","payload":{"source_rows":1,"source_payload_bytes":64,"found":true}}),
        ),
        (
            json!({"kind":"collect","payload":{"file":file(),"query":query()}}),
            json!({"kind":"collect","payload":{"source_rows":1,"source_payload_bytes":64,"matched_rows":1}}),
        ),
        (
            json!({"kind":"count","payload":{"file":file(),"query":query()}}),
            json!({"kind":"count","payload":{"source_rows":1,"source_payload_bytes":64,"matched_rows":1}}),
        ),
        (
            json!({"kind":"prepare","payload":{"file":null,"range":{"lower":null,"upper":null},"mutation":{"kind":"insert","payload":{"table_id":"00000000-0000-0000-0000-000000000006","row":{"id":1,"score":-0.0}}},"target_bytes":1024}}),
            json!({"kind":"prepared","payload":{"effect":{"kind":"null","payload":{}},"replacement_count":0}}),
        ),
    ];
    for (t, r) in cases {
        let jv = job(t);
        let j = parse_job(&serde_json::to_vec(&jv).unwrap()).unwrap();
        let encoded = encode_job(&j).unwrap();
        assert_eq!(parse_job(&encoded).unwrap(), j);
        if jv["task"]["kind"] == "prepare" {
            assert!(String::from_utf8(encoded).unwrap().contains("-0.0"));
        }
        let r = parse_result(&serde_json::to_vec(&result(&jv, r)).unwrap(), &j).unwrap();
        let encoded = encode_result(&r, &j).unwrap();
        assert_eq!(parse_result(&encoded, &j).unwrap(), r);
        if jv["task"]["kind"] == "get" {
            let rows = parse_get_rows(br#"[{"id":1,"score":-0.0}]"#, &r, &j).unwrap();
            assert!(serde_json::to_string(&rows).unwrap().contains("-0.0"));
        }
    }
}
#[test]
fn rejects_mismatch_and_invalid_control_records() {
    let jv = job(json!({"kind":"count","payload":{"file":file(),"query":query()}}));
    let j = parse_job(&serde_json::to_vec(&jv).unwrap()).unwrap();
    let good = result(
        &jv,
        json!({"kind":"count","payload":{"source_rows":1,"source_payload_bytes":64,"matched_rows":1}}),
    );
    for (field, value) in [
        ("ordinal", json!(1)),
        ("request_digest", json!("b".repeat(64))),
        ("job_id", json!("00000000-0000-0000-0000-000000000009")),
        (
            "result",
            json!({"kind":"get","payload":{"source_rows":1,"source_payload_bytes":64,"found":false}}),
        ),
    ] {
        let mut bad = good.clone();
        bad[field] = value;
        assert!(parse_result(&serde_json::to_vec(&bad).unwrap(), &j).is_err());
    }
    let text = serde_json::to_string(&jv).unwrap();
    for bad in [
        text.replacen("\"ordinal\":0", "\"ordinal\":0,\"ordinal\":0", 1),
        text.replacen("\"id\":\"int64\"", "\"id\":\"int64\",\"id\":\"int64\"", 1),
        text.replacen("\"row_count\":1", "\"row_count\":100001", 1),
        text.replacen("\"version\":1", "\"version\":1,\"extra\":true", 1),
        text.replacen("\"ordinal\":0", "\"ordinal\":0.0", 1),
        text.replacen("\"lower\":null", "\"lower\":[]", 1),
    ] {
        if bad != text {
            assert!(parse_job(bad.as_bytes()).is_err(), "{bad}");
        }
    }
    assert!(parse_job(&vec![b' '; 16 * 1024 * 1024 + 1]).is_err());
}

#[test]
fn preparation_metadata_and_mutation_contracts() {
    let table_id = "00000000-0000-0000-0000-000000000006";
    let mutations = [
        (
            "insert",
            json!({"table_id":table_id,"row":{"id":1,"score":-0.0}}),
            json!({"kind":"null","payload":{}}),
        ),
        (
            "insert_many",
            json!({"table_id":table_id,"rows":[{"id":1,"score":-0.0}]}),
            json!({"kind":"count","payload":{"value":1}}),
        ),
        (
            "update",
            json!({"table_id":table_id,"key":1,"values":{"score":-0.0}}),
            json!({"kind":"found","payload":{"value":true}}),
        ),
        (
            "upsert",
            json!({"table_id":table_id,"row":{"id":1,"score":-0.0}}),
            json!({"kind":"null","payload":{}}),
        ),
        (
            "delete",
            json!({"table_id":table_id,"key":1}),
            json!({"kind":"found","payload":{"value":false}}),
        ),
    ];
    for (kind, payload, effect) in mutations {
        let jv = job(
            json!({"kind":"prepare","payload":{"file":{"kind":"partition","payload":{"object":format!("tables/{table_id}/00000000-0000-0000-0000-000000000007/0.parquet"),"row_count":1,"range":{"lower":null,"upper":null},"byte_size":1024,"payload_bytes":64}},"range":{"lower":null,"upper":null},"mutation":{"kind":kind,"payload":payload},"target_bytes":67108864}}),
        );
        let j = parse_job(&serde_json::to_vec(&jv).unwrap()).unwrap();
        assert_eq!(parse_job(&encode_job(&j).unwrap()).unwrap(), j);
        let part = json!({"range":{"lower":null,"upper":null},"row_count":1,"byte_size":1024,"payload_bytes":64,"content_digest":"b".repeat(64)});
        for count in [Value::Null, json!(0), json!(1)] {
            let rv = result(
                &jv,
                json!({"kind":"prepared","payload":{"effect":effect,"replacement_count":count}}),
            );
            let r = parse_result(&serde_json::to_vec(&rv).unwrap(), &j).unwrap();
            assert_eq!(
                parse_result(&encode_result(&r, &j).unwrap(), &j).unwrap(),
                r
            );
            let mut validator = PreparationValidator::new(&r, &j).unwrap();
            if count == json!(1) {
                let p = parse_part_meta(&serde_json::to_vec(&part).unwrap(), &j.table).unwrap();
                assert_eq!(
                    parse_part_meta(&encode_part_meta(&p, &j.table).unwrap(), &j.table).unwrap(),
                    p
                );
                validator.push(&p).unwrap();
            }
            validator.finish().unwrap();
        }
        // Individual outputs must remain readable; the coordinator checks the
        // final table-wide row/payload limits after combining preparations.
        if kind == "insert_many" {
            let mut left = part.clone();
            left["row_count"] = json!(55000);
            left["payload_bytes"] = json!(4_000_000);
            left["range"]["upper"] = json!(0);
            let mut right = left.clone();
            right["range"] = json!({"lower":0,"upper":null});
            let rv = result(
                &jv,
                json!({"kind":"prepared","payload":{"effect":effect,"replacement_count":2}}),
            );
            let r = parse_result(&serde_json::to_vec(&rv).unwrap(), &j).unwrap();
            let l: PartMeta = serde_json::from_value(left).unwrap();
            let mut right: PartMeta = serde_json::from_value(right).unwrap();
            let mut v = PreparationValidator::new(&r, &j).unwrap();
            v.push(&l).unwrap();
            v.push(&right).unwrap();
            v.finish().unwrap();
            right.row_count = 55001;
            let mut v = PreparationValidator::new(&r, &j).unwrap();
            v.push(&l).unwrap();
            assert!(v.push(&right).is_err());
        }
        let mut bad_part = part;
        bad_part["byte_size"] = json!(67108865_u64);
        assert!(parse_part_meta(&serde_json::to_vec(&bad_part).unwrap(), &j.table).is_err());
        for pointer in [
            "/task/payload/mutation/payload/table_id",
            "/task/payload/target_bytes",
            "/task/payload/range",
        ] {
            let mut bad = jv.clone();
            *bad.pointer_mut(pointer).unwrap() = match pointer {
                p if p.ends_with("table_id") => json!("00000000-0000-0000-0000-000000000009"),
                p if p.ends_with("target_bytes") => json!(0),
                _ => json!([null, null]),
            };
            assert!(parse_job(&serde_json::to_vec(&bad).unwrap()).is_err());
        }
    }
}

#[test]
fn nested_wire_objects_and_error_records_are_strict() {
    let jv = job(json!({"kind":"get","payload":{"file":file(),"key":1}}));
    let j = parse_job(&serde_json::to_vec(&jv).unwrap()).unwrap();
    let rv = result(
        &jv,
        json!({"kind":"error","payload":{"code":"storage_unavailable","message":"worker input unavailable"}}),
    );
    let r = parse_result(&serde_json::to_vec(&rv).unwrap(), &j).unwrap();
    assert_eq!(
        parse_result(&encode_result(&r, &j).unwrap(), &j).unwrap(),
        r
    );
    for message in ["\nunsafe".into(), "x".repeat(1025)] {
        let mut bad = rv.clone();
        bad["result"]["payload"]["message"] = json!(message);
        assert!(parse_result(&serde_json::to_vec(&bad).unwrap(), &j).is_err());
    }
    let get = result(
        &jv,
        json!({"kind":"get","payload":{"source_rows":1,"source_payload_bytes":64,"found":true}}),
    );
    let get = parse_result(&serde_json::to_vec(&get).unwrap(), &j).unwrap();
    let mut missing = get.clone();
    if let meshdb_server::node::job::JobResult::Get { found, .. } = &mut missing.result {
        *found = false;
    }
    assert!(parse_get_rows(b"[]", &missing, &j).is_ok());
    assert!(parse_get_rows(br#"[{"id":1,"score":-0.0}]"#, &missing, &j).is_err());
    for field in ["row", "extra"] {
        let mut bad = serde_json::to_value(&get).unwrap();
        bad["result"]["payload"][field] = Value::Null;
        assert!(parse_result(&serde_json::to_vec(&bad).unwrap(), &j).is_err());
    }

    for bad in [
        r#"[{"id":1,"score":-0.0,"score":-0.0}]"#,
        r#"[[1,-0.0]]"#,
        r#"[{"id":1,"score":1e999}]"#,
        r#"[{"id":1,"score":0}]"#,
        r#"[{"id":2,"score":-0.0}]"#,
        r#"[{"id":1,"score":-0.0,"extra":1}]"#,
        "[]",
        r#"[{"id":1,"score":-0.0},{"id":1,"score":-0.0}]"#,
    ] {
        assert!(parse_get_rows(bad.as_bytes(), &get, &j).is_err(), "{bad}");
    }
    for field in [
        "cluster_id",
        "operation_id",
        "attempt_id",
        "snapshot",
        "version",
    ] {
        let mut bad = rv.clone();
        bad[field] = if field == "snapshot" {
            json!({"catalog_revision":3,"catalog_commit_id":"00000000-0000-0000-0000-000000000005","catalog_etag":"token"})
        } else if field == "version" {
            json!(2)
        } else {
            json!("00000000-0000-0000-0000-000000000099")
        };
        assert!(parse_result(&serde_json::to_vec(&bad).unwrap(), &j).is_err());
    }
    for (pointer, value) in [
        ("/table", json!([])),
        ("/task/payload/file/payload", json!([])),
        ("/snapshot", json!([])),
        ("/request_digest", json!("A".repeat(64))),
        ("/task/payload/key", json!(1.0)),
        (
            "/task/payload/file/payload/object",
            json!("tables/other/object.parquet"),
        ),
    ] {
        let mut bad = jv.clone();
        *bad.pointer_mut(pointer).unwrap() = value;
        assert!(parse_job(&serde_json::to_vec(&bad).unwrap()).is_err());
    }
    assert!(parse_result(&vec![b' '; 16 * 1024 * 1024 + 1], &j).is_err());
}

#[test]
fn internal_get_row_can_exceed_public_response_cap() {
    let mut jv = job(json!({"kind":"get","payload":{"file":file(),"key":1}}));
    jv["table"]["schema"]["score"] = json!("utf8");
    let j = parse_job(&serde_json::to_vec(&jv).unwrap()).unwrap();
    let rv = result(
        &jv,
        json!({"kind":"get","payload":{"source_rows":1,"source_payload_bytes":10*1024*1024,"found":true}}),
    );
    let r = parse_result(&serde_json::to_vec(&rv).unwrap(), &j).unwrap();
    assert!(encode_result(&r, &j).unwrap().len() < 1024);
    let rows =
        vec![serde_json::from_value(json!({"id":1,"score":"\0".repeat(3*1024*1024)})).unwrap()];
    assert!(validate_get_rows(&r, &j, &rows).unwrap() > 16 * 1024 * 1024);
    let mut too_small = r.clone();
    if let meshdb_server::node::job::JobResult::Get {
        source_payload_bytes,
        ..
    } = &mut too_small.result
    {
        *source_payload_bytes = 64;
    }
    assert!(validate_get_rows(&too_small, &j, &rows).is_err());
}

#[test]
fn preparation_metadata_stream_scales_and_requires_complete_cover() {
    let jv = job(
        json!({"kind":"prepare","payload":{"file":null,"range":{"lower":null,"upper":null},"mutation":{"kind":"insert","payload":{"table_id":"00000000-0000-0000-0000-000000000006","row":{"id":1,"score":-0.0}}},"target_bytes":1024}}),
    );
    let j = parse_job(&serde_json::to_vec(&jv).unwrap()).unwrap();
    let control = |count| {
        parse_result(&serde_json::to_vec(&result(&jv, json!({"kind":"prepared","payload":{"effect":{"kind":"null","payload":{}},"replacement_count":count}}))).unwrap(), &j)
    };
    for count in [100_000u32, 110_000] {
        let r = control(count).unwrap();
        assert!(encode_result(&r, &j).unwrap().len() < 1024);
        let mut v = PreparationValidator::new(&r, &j).unwrap();
        for i in 0..count {
            let p = PartMeta {
                range: meshdb_server::storage::KeyRange {
                    lower: (i > 0).then_some(meshdb_server::protocol::Key::Int64(i as i64)),
                    upper: (i + 1 < count)
                        .then_some(meshdb_server::protocol::Key::Int64((i + 1) as i64)),
                },
                row_count: 1,
                payload_bytes: 64,
                byte_size: 1024,
                content_digest: "a".repeat(64),
            };
            v.push(&p).unwrap();
        }
        v.finish().unwrap(); // Encoded aggregate exceeds 80 MiB deliberately.
    }
    assert!(control(110_001).is_err());
    let p: PartMeta = serde_json::from_value(json!({"range":{"lower":null,"upper":null},"row_count":1,"payload_bytes":64,"byte_size":1024,"content_digest":"a".repeat(64)})).unwrap();
    let wire = serde_json::to_string(&p).unwrap();
    for bad in [
        wire.replace("\"row_count\":1", "\"row_count\":1,\"row_count\":1"),
        wire.replace("\"row_count\":1", "\"row_count\":0"),
        wire.replace("\"payload_bytes\":64", "\"payload_bytes\":67108865"),
        wire.replace("\"lower\":null,", ""),
        wire.replace("\"lower\":null", "\"lower\":0.0"),
        wire.replace(&"a".repeat(64), &"A".repeat(64)),
        wire.replace("\"row_count\":1", "\"extra\":1,\"row_count\":1"),
        "[]".into(),
    ] {
        assert!(parse_part_meta(bad.as_bytes(), &j.table).is_err(), "{bad}");
    }
    assert!(parse_part_meta(&vec![b' '; 16 * 1024 * 1024 + 1], &j.table).is_err());
    let mut old = serde_json::to_value(control(1).unwrap()).unwrap();
    old["result"]["payload"]["replacements"] = json!([]);
    assert!(parse_result(&serde_json::to_vec(&old).unwrap(), &j).is_err());
    let r = control(1).unwrap();
    assert!(PreparationValidator::new(&r, &j).unwrap().finish().is_err());
    let mut v = PreparationValidator::new(&r, &j).unwrap();
    v.push(&p).unwrap();
    assert!(v.push(&p).is_err());
    assert!(v.finish().is_err());
    let r = control(2).unwrap();
    let mut left = p.clone();
    left.range.upper = Some(meshdb_server::protocol::Key::Int64(10));
    let mut right = p.clone();
    right.range.lower = Some(meshdb_server::protocol::Key::Int64(11));
    let mut v = PreparationValidator::new(&r, &j).unwrap();
    v.push(&left).unwrap();
    assert!(v.finish().is_err());
    let mut v = PreparationValidator::new(&r, &j).unwrap();
    v.push(&left).unwrap();
    assert!(v.push(&right).is_err());
    right.range.lower = Some(meshdb_server::protocol::Key::Int64(10));
    right.range.upper = Some(meshdb_server::protocol::Key::Int64(20));
    let mut v = PreparationValidator::new(&r, &j).unwrap();
    v.push(&left).unwrap();
    assert!(v.push(&right).is_err());
}
