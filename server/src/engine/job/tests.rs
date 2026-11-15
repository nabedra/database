use super::*;
#[tokio::test]
async fn captured_reads_complete_input_and_validate_before_io() {
    let f = LocalFixture::new().await;
    for field in ["cluster", "operation", "table", "object", "schema", "range"] {
        let mut j = f.job.clone();
        match field {
            "cluster" => j.cluster_id = uuid::Uuid::new_v4(),
            "operation" => j.operation_id = uuid::Uuid::new_v4(),
            "table" => j.table.id = uuid::Uuid::new_v4(),
            "schema" => j.table.schema.clear(),
            "range" => {
                j.task = Task::Get {
                    file: crate::node::job::FileInput::Partition {
                        object: crate::storage::ObjectKey::partition(
                            j.table.id,
                            uuid::Uuid::new_v4(),
                            0,
                        )
                        .as_str()
                        .into(),
                        range: crate::storage::KeyRange {
                            lower: Some(crate::protocol::Key::Int64(5)),
                            upper: Some(crate::protocol::Key::Int64(1)),
                        },
                        row_count: 1,
                        byte_size: 1,
                        payload_bytes: 8,
                    },
                    key: crate::protocol::Key::Int64(1),
                };
            }
            _ => {
                let Task::Get {
                    file: crate::node::job::FileInput::Legacy { object, .. },
                    ..
                } = &mut j.task
                else {
                    unreachable!()
                };
                *object = "../invalid".into();
            }
        }
        assert!(f.start(j).is_err());
    }
    assert_eq!(f.store.backend().state.lock().unwrap().gets, 0);
    let mut j = f.job.clone();
    let Task::Get { key, .. } = &mut j.task else {
        unreachable!()
    };
    *key = crate::protocol::Key::Int64(99);
    let (r, mut lease) = f.run(j).await.unwrap();
    assert!(matches!(
        r.result,
        JobResult::Get {
            found: false,
            source_rows: 3,
            ..
        }
    ));
    assert!(
        matches!(lease.command(Command::TakeRows).await.unwrap(), Reply::Rows(rows) if rows.is_empty())
    );
    lease.dispose().await.unwrap();
    let mut j = f.job.clone();
    j.task = Task::Collect {
        file: file(&j),
        query: serde_json::from_value(
            serde_json::json!({"select":["v"],"filter":null,"order_by":{"column":"v","desc":true},"limit":0}),
        )
        .unwrap(),
    };
    let (r, mut lease) = f.run(j).await.unwrap();
    assert_eq!(r.snapshot, f.job.snapshot);
    assert!(matches!(
        r.result,
        JobResult::Collect {
            matched_rows: 3,
            source_rows: 3,
            ..
        }
    ));
    let Reply::Rows(rows) = lease.command(Command::TakeRows).await.unwrap() else {
        panic!()
    };
    assert_eq!(
        rows.iter().map(|r| r["id"].clone()).collect::<Vec<_>>(),
        [1, 2, 3].map(crate::protocol::Scalar::Int64)
    );
    lease.dispose().await.unwrap();
    let mut j = f.job.clone();
    let Task::Get {
        file: crate::node::job::FileInput::Legacy { row_count, .. },
        ..
    } = &mut j.task
    else {
        unreachable!()
    };
    *row_count = 2;
    assert!(f.run(j).await.is_err());
    f.finish().await;
}
#[tokio::test]
async fn captured_prepare_null_empty_split_and_range_atomicity() {
    use serde_json::json;
    let f = LocalFixture::new().await;
    for (mutation, input, count, effect) in [
        (
            json!({"kind":"update","payload":{"table_id":f.job.table.id,"key":1,"values":{"v":true}}}),
            true,
            None,
            crate::node::job::Effect::Found { value: true },
        ),
        (
            json!({"kind":"delete","payload":{"table_id":f.job.table.id,"key":1}}),
            false,
            None,
            crate::node::job::Effect::Found { value: false },
        ),
        (
            json!({"kind":"insert_many","payload":{"table_id":f.job.table.id,"rows":[{"id":1,"v":true},{"id":2,"v":true},{"id":3,"v":true}]}}),
            false,
            Some(3),
            crate::node::job::Effect::Count { value: 3 },
        ),
    ] {
        let (r, lease) = f.run(prepare(&f.job, mutation, input)).await.unwrap();
        assert!(
            matches!(&r.result, JobResult::Prepared { effect:e, replacement_count } if *replacement_count == count && e == &effect)
        );
        lease.dispose().await.unwrap();
    }
    let mut empty = prepare(
        &f.job,
        json!({"kind":"delete","payload":{"table_id":f.job.table.id,"key":3}}),
        true,
    );
    let Task::Prepare {
        file: Some(crate::node::job::FileInput::Legacy { object, row_count }),
        ..
    } = &mut empty.task
    else {
        unreachable!()
    };
    *object = crate::storage::ObjectKey::snapshot(f.job.table.id, uuid::Uuid::from_u128(8))
        .as_str()
        .into();
    *row_count = 1;
    let (r, lease) = f.run(empty).await.unwrap();
    assert!(matches!(
        r.result,
        JobResult::Prepared {
            effect: crate::node::job::Effect::Found { value: true },
            replacement_count: Some(0)
        }
    ));
    lease.dispose().await.unwrap();
    let mut j = prepare(
        &f.job,
        json!({"kind":"insert_many","payload":{"table_id":f.job.table.id,"rows":[{"id":1,"v":true},{"id":9,"v":true}]}}),
        false,
    );
    let Task::Prepare { range, .. } = &mut j.task else {
        unreachable!()
    };
    range.upper = Some(crate::protocol::Key::Int64(5));
    let gets = f.store.backend().state.lock().unwrap().gets;
    assert!(f.run(j).await.is_err());
    assert_eq!(f.store.backend().state.lock().unwrap().gets, gets);
    f.finish().await;
    let f = LocalFixture::with_files(2).await;
    let j = prepare(
        &f.job,
        json!({"kind":"insert_many","payload":{"table_id":f.job.table.id,"rows":[{"id":1,"v":true},{"id":2,"v":true},{"id":3,"v":true}]}}),
        false,
    );
    assert!(f.run(j).await.is_err());
    assert_eq!(quota_charged(&f).await, Ok(false));
    f.finish().await;
}

#[tokio::test]
async fn captured_partition_accounting_and_deposit_identity() {
    use crate::{
        node::job::FileInput,
        protocol::{Key, Scalar},
        storage::KeyRange,
    };
    let f = LocalFixture::new().await;
    let rows = [1, 2, 3]
        .into_iter()
        .map(|id| {
            [
                ("id".into(), Scalar::Int64(id)),
                ("v".into(), Scalar::Bool(true)),
            ]
            .into()
        })
        .collect::<Vec<Row>>();
    let bytes = crate::snapshot::encode(&f.job.table, &rows).unwrap();
    let object = crate::storage::ObjectKey::partition(f.job.table.id, uuid::Uuid::new_v4(), 0);
    let byte_size = bytes.len() as u64;
    let payload_bytes = crate::snapshot::validate_rows(&f.job.table, &rows).unwrap() as u64;
    // Direct Fake seeding, with write counters fixed before handler execution.
    f.store.backend().state.lock().unwrap().objects.insert(
        format!("test/{}", object.as_str()),
        (bytes, "captured".into()),
    );
    let valid = FileInput::Partition {
        object: object.as_str().into(),
        range: KeyRange {
            lower: None,
            upper: None,
        },
        row_count: 3,
        byte_size,
        payload_bytes,
    };
    for mode in [
        "valid", "rows", "bytes", "payload", "range", "schema", "missing",
    ] {
        let mut j = f.job.clone();
        let mut input = valid.clone();
        let FileInput::Partition {
            object,
            row_count,
            byte_size,
            payload_bytes,
            range,
        } = &mut input
        else {
            unreachable!()
        };
        match mode {
            "rows" => *row_count += 1,
            "bytes" => *byte_size += 1,
            "payload" => *payload_bytes += 1,
            "range" => range.upper = Some(Key::Int64(2)),
            "schema" => {
                j.table
                    .schema
                    .insert("missing".into(), crate::protocol::ColumnType::Bool);
            }
            "missing" => {
                *object = crate::storage::ObjectKey::partition(j.table.id, uuid::Uuid::new_v4(), 0)
                    .as_str()
                    .into()
            }
            _ => (),
        }
        j.task = Task::Get {
            file: input,
            key: Key::Int64(1),
        };
        match f.run(j).await {
            Ok((r, lease)) => {
                assert_eq!(mode, "valid");
                assert!(matches!(
                    r.result,
                    JobResult::Get {
                        found: true,
                        source_rows: 3,
                        ..
                    }
                ));
                lease.dispose().await.unwrap();
            }
            Err(_) => assert_ne!(mode, "valid"),
        }
    }
    // Exercise the real registered cell borrow; a different exact job cannot deposit.
    for mode in ["job", "attempt", "snapshot"] {
        let mut lease = f
            .owners
            .try_admit(&f.operation, Role::Operation, f.guard.clone())
            .unwrap();
        lease.ready().await.unwrap();
        let slot = lease.slot.clone();
        let expected = f.job.clone();
        let mut different = expected.clone();
        match mode {
            "job" => different.job_id = uuid::Uuid::new_v4(),
            "attempt" => different.attempt_id = uuid::Uuid::new_v4(),
            _ => different.snapshot.catalog_revision += 1,
        }
        f.owners
            .shared
            .pool
            .submit(0, move |_, _| {
                let mut disk = slot.disk.lock().unwrap();
                let mut output = LocalOutput {
                    slot: &slot,
                    disk: disk.as_mut().unwrap(),
                    expected: &expected,
                };
                assert!(
                    output
                        .accept(
                            &different,
                            JobResult::Get {
                                source_rows: 3,
                                source_payload_bytes: 48,
                                found: false
                            },
                            Vec::new()
                        )
                        .is_err()
                );
                assert!(matches!(output.disk.processor, Processor::Empty));
                Ok(())
            })
            .await
            .unwrap()
            .receive()
            .await
            .unwrap();
        lease.dispose().await.unwrap();
    }
    f.finish().await;
}
