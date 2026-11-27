//! Crash and replay on `FaultFs`: an entry's rows, vectors, records, floor and
//! change record are one batch, and a log replayed after any crash leaves
//! exactly the state and the feed of one that never crashed.
use super::*;

/// One random log with every kind of entry, its replies, and the state after it.
async fn history(seed: u64, log: &mut Vec<ParticipantCommand>) -> (S, Vec<ParticipantReply>) {
    let mut rng = Rng(seed);
    let mut s = S::plain(Some(&FaultFs::new())).await;
    let mut replies = Vec::new();
    let fence = s.fence_command(1, columns());
    s.search_epoch = 1;
    s.epoch = 2;
    log.push(fence);
    for step in 0..24u64 {
        let key = rng.below(2) as i64;
        let got = match log.is_empty() {
            true => Got::default(),
            false => s.get_after(log, &mut replies, key).await,
        };
        let at = s.tick();
        let body = rng.below(2).to_string();
        let value = serde_json::json!({"id": key, "body": body, "v": step});
        let vectors = match rng.below(3) {
            0 => vec![s.set(PLAIN, s.dense(step as u8))],
            1 => vec![s.set(SPARSE, s.value(VectorKind::Sparse, &rng.bytes(5000)))],
            _ => Vec::new(),
        };
        match rng.below(7) {
            0 | 1 => {
                let write = s.write(key, got.version, Some(value), vectors);
                log.push(ParticipantCommand::Direct(s.commit(at, vec![write])));
            }
            2 => {
                let members = vec![
                    s.commit(at, vec![s.write(key, got.version, Some(value), vectors)]),
                    s.commit(at, vec![s.write(7, Some(1), Some(serde_json::json!({})), Vec::new())]),
                ];
                log.push(ParticipantCommand::DirectBatch(DirectBatch { members }));
            }
            3 => {
                let writes = vec![s.put_row(key, got.version, value, vectors)];
                let plan = s.f.queued_plan(at - 1, &writes);
                s.queued(log, &plan, writes, at, rng.one_in(3));
            }
            4 => {
                if let (Some(version), Some(source)) = (got.version, got.source.get(&EMBEDDED)) {
                    log.push(ParticipantCommand::SetVector(s.set_vector(key, version, *source, at)));
                }
            }
            5 => {
                if let Some(record) = got.records.get(&EMBEDDED) {
                    log.push(ParticipantCommand::EmbedFailed(EmbedFailed {
                        authority: s.f.authority,
                        table_id: s.f.table,
                        primary_key: Key::Int64(key),
                        column: EMBEDDED,
                        source_version: record.source_version,
                        id: record.id,
                        attempt: record.attempts,
                        reason: Text::try_from("no").unwrap(),
                    }));
                }
            }
            _ => log.push(ParticipantCommand::Closed(Closed {
                authority: s.f.authority,
                ts: ts(at),
                unix_ms: at,
            })),
        }
    }
    s.get_after(log, &mut replies, 0).await;
    (s, replies)
}
impl S {
    /// Applies what `log` holds beyond the applied entries, then reads a row.
    async fn get_after(
        &mut self,
        log: &[ParticipantCommand],
        replies: &mut Vec<ParticipantReply>,
        key: i64,
    ) -> Got {
        for command in &log[replies.len()..] {
            replies.push(self.f.submit(command.clone()).await);
        }
        self.get(key).await
    }
    fn queued(
        &self,
        log: &mut Vec<ParticipantCommand>,
        plan: &QueuedPlan,
        writes: Vec<PreparedWrite>,
        at: u64,
        abort: bool,
    ) {
        let guard = lifecycle::VerifiedLifecycleGuard::model_for_test(
            lifecycle::LifecycleBinding {
                guard_id: nabedra_rt::ids::uuid_v4(),
                catalog_group_id: self.f.catalog_group_id,
                table_id: self.f.table,
                lifecycle_version: None,
                schema_epoch: 1,
                txn: plan.txn,
                request: plan.request,
                read_ts: plan.plan.read_ts,
                authority: self.f.authority,
            },
            lifecycle::GuardMode::Shared,
        )
        .into_sealed();
        log.push(ParticipantCommand::PreparePage {
            authority: self.f.authority,
            txn: plan.txn,
            request: plan.request,
            manifest: plan.manifest,
            plan: plan.plan.clone(),
            page_index: 0,
            previous_root: plan.root,
            guards: Page::new(vec![guard]).unwrap(),
            writes: Page::new(writes).unwrap(),
        });
        log.push(ParticipantCommand::Seal {
            authority: self.f.authority,
            txn: plan.txn,
            manifest: plan.manifest,
            result_digest: digest(4),
        });
        let decision = match abort {
            true => TxnDecision::Abort {
                reason: nabedra_oltp_types::types::AbortReason::Conflict,
            },
            false => TxnDecision::Commit { commit_ts: ts(at) },
        };
        log.push(ParticipantCommand::Resolve {
            authority: self.f.authority,
            proof: SealedStatusDecision::seal(DecisionStatement {
                txn: plan.txn,
                request: plan.request,
                manifest: plan.manifest,
                decided_at: position(1),
                decision,
            })
            .unwrap(),
            max_items: 128,
        });
    }
}

async fn apply(
    machine: &ParticipantMachine,
    shared: &WorkerSharedResources,
    index: u64,
    command: &ParticipantCommand,
) -> Result<ParticipantReply, ProtocolError> {
    let command = OwnedCommand::new_owned(command.clone(), shared, ByteClass::Data).unwrap();
    let meta = ApplyMeta {
        position: position(index),
        command_digest: nabedra_oltp_types::owned::serialized_digest(&command).unwrap(),
        membership: None,
    };
    let raw = machine.apply(meta, command).await?;
    Ok(serde_json::from_slice(raw.as_slice()).unwrap())
}

#[tokio::test]
async fn vs15_vs16_replay_after_any_crash_leaves_the_state_and_feed_of_no_crash() {
    let mut crashes = 0;
    let mut lost = 0;
    for seed in 0..60u64 {
        let mut log = Vec::new();
        let (reference, replies) = history(seed % 12, &mut log).await;
        let expected = stored(&reference.f.pool, reference.f.group.group_id);
        let feed = seen(&reference.changes(2, 64).await);
        assert!(!feed.is_empty());

        let mut rng = Rng(seed);
        let fs = FaultFs::new();
        let dir = Dir(std::env::temp_dir().join(format!("search-crash-{}", nabedra_rt::ids::uuid_v4())));
        std::fs::create_dir_all(&dir.0).unwrap();
        fs.create_dir_all_durable(&dir.0);
        let mut fs = fs;
        // The first two entries of the reference: its fence and activation.
        let mut entries = vec![reference.f.first_fence.clone(), reference.f.activation()];
        entries.extend(log);
        // Another worker's replica of the same group.
        let mut identity = reference.f.pool_identity.clone();
        identity.worker_id = nabedra_rt::ids::uuid_v4();
        identity.pool_id = nabedra_rt::ids::uuid_v4();
        let deferred = seed % 2 == 0;
        let mut highest = 0;
        let mut done = false;
        let mut round = 0;
        while !done {
            round += 1;
            let shared = shared();
            let pool = open_on(&dir, &fs, &identity, &shared);
            let store = pool.group(reference.f.group.clone()).await.unwrap();
            let machine = ParticipantMachine::new(store.clone()).unwrap();
            let token = deferred.then(|| store.defer_apply_sync(&store).unwrap());
            let applied = store.applied_state(BUDGET).await.unwrap();
            let next = applied.map_or(0, |applied| applied.position.index + 1);
            lost += usize::from(next < highest);
            // The last applied entry may be delivered again.
            let first = if next > 0 && rng.one_in(2) { next - 1 } else { next };
            if round < 4 {
                fs.crash_after(1 + rng.next() % 40);
            }
            done = true;
            for index in first..entries.len() as u64 {
                match apply(&machine, &shared, index, &entries[index as usize]).await {
                    Ok(reply) => {
                        highest = highest.max(index + 1);
                        if index >= 2 {
                            assert_eq!(reply, replies[index as usize - 2], "seed {seed} entry {index}");
                        }
                    }
                    Err(_) => {
                        crashes += 1;
                        done = false;
                        break;
                    }
                }
            }
            if done {
                let state = stored(&pool, reference.f.group.group_id);
                assert!(state == expected, "seed {seed}");
                assert_eq!(seen(&changes(&machine, 2, 64).await.unwrap()), feed, "seed {seed}");
            }
            drop((token, machine, store, pool));
            fs = if rng.one_in(2) { fs.power_loss(rng.next()) } else { fs.crash() };
        }
    }
    assert!(crashes > 20 && lost > 5, "{crashes} crashes, {lost} lost applied entries");
}
