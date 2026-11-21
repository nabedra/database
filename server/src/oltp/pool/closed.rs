//! A close proof certifies one physical DB handle is gone. It is not a shard
//! quorum, queue handoff, provider detach, or permission to reopen the pool.
use super::*;
use crate::oltp_replication::types::WorkerIdentity;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct PoolClosedStatement {
    pub close_id: Uuid,
    pub identity: PoolIdentity,
    pub registered: Option<mount::MountedPoolObservation>,
}
pub(crate) struct VerifiedPoolClosed {
    statement: PoolClosedStatement,
}
impl VerifiedPoolClosed {
    pub(crate) fn statement(&self) -> &PoolClosedStatement {
        &self.statement
    }
    pub(crate) fn registered_owner(&self) -> Option<(WorkerIdentity, u64)> {
        self.statement
            .registered
            .as_ref()
            .map(|mounted| (mounted.expected.worker, mounted.expected.generation))
    }
}
pub(crate) enum PoolCloseOutcome {
    Closed(VerifiedPoolClosed),
    StillOwned(OltpPool),
    Failed {
        pool: OltpPool,
        error: ProtocolError,
    },
}
impl OltpPool {
    /// Capacity and CPU are prepared before transferring the last handle here.
    /// Posting native work is the first await: cancellation after dispatch keeps
    /// DB/mount ownership and native charges alive through flush and Drop.
    pub(crate) async fn close_with_native(
        self,
        close_id: Uuid,
        prepared: PreparedNative,
        compute: NativeComputeGuard,
    ) -> Result<PoolCloseOutcome, ProtocolError> {
        let (result, _compute) = prepared
            .run_with_guard(compute, move || {
                if close_id.is_nil() {
                    return Ok(PoolCloseOutcome::Failed {
                        pool: self,
                        error: storage_error_message(),
                    });
                }
                // Stats briefly hold pool references under this lock; holding it
                // here makes the last-owner check exact.
                let registry = self.inner.shared.clone();
                let inner = {
                    let _pools = registry
                        .storage
                        .pools
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner());
                    match Arc::try_unwrap(self.inner) {
                        Ok(inner) => inner,
                        Err(inner) => {
                            return Ok(PoolCloseOutcome::StillOwned(OltpPool { inner }));
                        }
                    }
                };
                let registered = match &inner._mount {
                    Some(mount) => match mount.reobserve_for_close(close_id) {
                        Ok(observed) if observed.expected.identity == inner.identity => {
                            Some(observed)
                        }
                        _ => {
                            return Ok(PoolCloseOutcome::Failed {
                                pool: OltpPool {
                                    inner: Arc::new(inner),
                                },
                                error: storage_error_message(),
                            });
                        }
                    },
                    None => None,
                };
                if inner.recovery.load(Ordering::Acquire) || inner.store.requires_recovery() {
                    return Ok(PoolCloseOutcome::Failed {
                        pool: OltpPool {
                            inner: Arc::new(inner),
                        },
                        error: recovery_unknown(),
                    });
                }
                if inner.store.sync().and_then(wait_commit).is_err() {
                    inner.recovery.store(true, Ordering::Release);
                    return Ok(PoolCloseOutcome::Failed {
                        pool: OltpPool {
                            inner: Arc::new(inner),
                        },
                        error: recovery_unknown(),
                    });
                }
                if let Some(mount) = &inner._mount {
                    if let Err(error) = mount.seal_closed_generation(close_id) {
                        return Ok(PoolCloseOutcome::Failed {
                            pool: OltpPool {
                                inner: Arc::new(inner),
                            },
                            error,
                        });
                    }
                }
                let statement = PoolClosedStatement {
                    close_id,
                    identity: inner.identity.clone(),
                    registered,
                };
                // PoolInner closes the store first and drops the verified mount last. Every
                // exported snapshot/transfer owns a pool guard and prevented the
                // unique unwrap above while it could still use this filesystem.
                drop(inner);
                Ok(PoolCloseOutcome::Closed(VerifiedPoolClosed { statement }))
            })
            .await?;
        result
    }
}

impl VerifiedPoolClosed {
    /// Recover a reply lost after fsync/DB drop. The marker is useful only
    /// under an exclusive lock on the same verified mount instance.
    pub(crate) fn recover_registered(
        close_id: Uuid,
        mount: &crate::oltp_replication::authority::control::DrainMount,
    ) -> Result<Self, ProtocolError> {
        #[cfg(target_os = "linux")]
        {
            let registered = mount::prove_closed(mount, close_id)?;
            Ok(Self {
                statement: PoolClosedStatement {
                    close_id,
                    identity: registered.expected.identity.clone(),
                    registered: Some(registered),
                },
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (close_id, mount);
            Err(storage_error_message())
        }
    }
}

impl OltpPool {
    pub(crate) async fn check_retired_groups<F, Check>(&self, check: Check) -> Result<(), ProtocolError>
    where
        F: std::future::Future<Output = Result<bool, ProtocolError>>,
        Check: Fn(crate::oltp_replication::authority::control::ReplicaRetiredStamp) -> F,
    {
        #[derive(serde::Deserialize)]
        struct LocalGroupPage {
            items: Vec<GroupIdentity>,
            after: Option<Uuid>,
            exhausted: bool,
        }
        let budget = TransactionBudget {
            scratch_bytes: 256 << 10,
            batch_bytes: 0,
            output_bytes: 64 << 10,
        };
        let mut after = None;
        loop {
            let bytes = self.list_groups(after, 32, budget).await?;
            let page: LocalGroupPage = serde_json::from_slice(bytes.as_ref())
                .map_err(|_| storage_error_message())?;
            for group in page.items {
                let proof = self.drain_group_retirement(group).await?;
                if !check(proof).await? {
                    return Err(failure(ErrorCode::Conflict, "Control has not acknowledged exact replica retirement"));
                }
            }
            if page.exhausted { return Ok(()); }
            after = Some(page.after.filter(|next| after.is_none_or(|old| *next > old))
                .ok_or_else(storage_error_message)?);
        }
    }

    pub(crate) fn exclusively_owned(&self) -> bool {
        // Stats and the damage reporter keep Weak observers for the lifetime
        // of an open pool. The final try_unwrap still arbitrates any racing
        // upgrades before the close marker is written.
        Arc::strong_count(&self.inner) == 1
    }

    /// Inspect retired data without opening a group or recovering a staged
    /// snapshot. An unresolved local write or activation blocks close.
    pub(crate) async fn drain_group_retirement(
        &self,
        group: GroupIdentity,
    ) -> Result<crate::oltp_replication::authority::control::ReplicaRetiredStamp, ProtocolError>
    {
        use crate::oltp_replication::{authority::control::ReplicaRetiredStamp, types::GroupKind};
        let pool = self.clone();
        self.resources()
            .execute(ExecutionClass::Control, 0, 0, move || {
                pool.healthy()?;
                let blocked = || {
                    failure(
                        ErrorCode::Conflict,
                        "pool retains an unretired group, transaction or snapshot activation",
                    )
                };
                if !matches!(group.kind, GroupKind::Data { .. }) {
                    return Err(blocked());
                }
                let id = group.group_id;
                let db = pool.snapshot();
                if db
                    .get(&group_key(id, b't', &[]))
                    .map_err(storage_error)?
                    .is_some()
                {
                    return Err(blocked());
                }
                let active = db
                    .get(&group_key(id, b'a', &[]))
                    .map_err(storage_error)?
                    .ok_or_else(recovery_unknown)?;
                if active.len() != 8 {
                    return Err(recovery_unknown());
                }

                let prefix = group_key(id, b's', active.as_ref());
                let scope = group_scope(id);
                let mut iter = RawIter::bounded(&db, &scope, &prefix_end(&scope), true);
                let mut intent = prefix.clone();
                intent.push(crate::oltp::mvcc::store::INTENT);
                iter.seek(&intent);
                if iter.key().is_some_and(|key| key.starts_with(&intent)) {
                    return Err(blocked());
                }
                // Transaction metadata survives settlement. Its corresponding
                // durable resolution marker distinguishes it from live work.
                let mut transactions = prefix;
                transactions.push(b't');
                iter.seek(&transactions);
                while let Some(key) = iter.key().filter(|key| key.starts_with(&transactions)) {
                    if key.len() != transactions.len() + 24 {
                        return Err(recovery_unknown());
                    }
                    let mut resolution = key.to_vec();
                    resolution[transactions.len() - 1] = b'd';
                    if db.get(&resolution).map_err(storage_error)?.is_none() {
                        return Err(blocked());
                    }
                    iter.next();
                }
                iter.status()?;

                let incoming = pool
                    .inner
                    .path
                    .join("snapshots")
                    .join(id.to_string())
                    .join("incoming");
                match fs::read_dir(incoming) {
                    Ok(mut items) => {
                        if items.next().is_some() {
                            return Err(blocked());
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(_) => return Err(storage_error_message()),
                }
                let retired_prefix = group_key(id, b'x', &[]);
                iter.seek(&retired_prefix);
                let key = iter
                    .key()
                    .filter(|key| key.starts_with(&retired_prefix))
                    .ok_or_else(blocked)?;
                let bytes = iter.value()?;
                if bytes.len() > 4096 || key.len() != retired_prefix.len() + 16 {
                    return Err(recovery_unknown());
                }
                let proof: ReplicaRetiredStamp =
                    serde_json::from_slice(&bytes).map_err(|_| recovery_unknown())?;
                if proof.group != group
                    || proof.generation.as_bytes() != &key[retired_prefix.len()..]
                    || proof.replica.pool_id != pool.identity().pool_id
                    || proof.replica.worker_id != pool.identity().worker_id
                {
                    return Err(recovery_unknown());
                }
                iter.next();
                if iter
                    .key()
                    .is_some_and(|key| key.starts_with(&retired_prefix))
                {
                    return Err(blocked());
                }
                iter.status()?;
                Ok(proof)
            })
            .await
    }
}
