use super::*;
use crate::oltp_runtime::coordinator::QueueCoordinator;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use std::path::PathBuf;
use uuid::Uuid;

tokio::task_local! {
    pub(super) static GRANT: ReadGrant;
}

fn fixture(scope: ReadScope, group: Uuid) -> Option<PathBuf> {
    let dir = PathBuf::from(std::env::var_os("NABEDRA_OLTP_TEST_BARRIER_DIR")?);
    let path = dir.join(format!(
        "cursor-{}-{group}-enabled",
        scope.request.request_id
    ));
    path.is_file().then_some(path)
}

fn write(base: &PathBuf, suffix: &str, value: Value) {
    let path = base.with_file_name(format!(
        "{}-{suffix}.json",
        base.file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .trim_end_matches("-enabled")
    ));
    let temporary = path.with_extension(format!("{}.tmp", nabedra_rt::ids::uuid_v4()));
    std::fs::write(&temporary, serde_json::to_vec(&value).unwrap()).unwrap();
    std::fs::rename(temporary, path).unwrap();
}

fn input(payload: &ChargedBytes) -> &str {
    std::str::from_utf8(payload.as_slice()).unwrap()
}

fn digest(payload: &ChargedBytes) -> Digest {
    Digest::from_bytes(Sha256::digest(payload.as_slice()).into())
}

pub(crate) fn applied(position: AppliedPosition) {
    let _ = GRANT.try_with(|grant| {
        if let Some(base) = fixture(grant.scope, grant.group) {
            write(
                &base,
                &format!("applied-{}", grant.ticket),
                json!({
                    "grant": grant,
                    "applied": position,
                }),
            );
        }
    });
}

pub(super) fn page(grant: ReadGrant, payload: &ChargedBytes, page: &ScanPage) {
    let Some(base) = fixture(grant.scope, grant.group) else {
        return;
    };
    if let ScanPage::Records { records, exhausted } = page {
        let keys = records
            .iter()
            .filter(|record| record.value.is_some())
            .map(|record| &record.key)
            .collect::<Vec<_>>();
        write(
            &base,
            &format!("page-{}", grant.ticket),
            json!({
                "grant": grant,
                "input": input(payload),
                "keys": keys,
                "exhausted": exhausted,
            }),
        );
    }
}

pub(super) struct HeldContinuation(PathBuf);

pub(super) async fn checkpoint(
    scope: ReadScope,
    client: &Client,
    payload: &ChargedBytes,
    hash: Digest,
) -> Option<HeldContinuation> {
    let base = fixture(scope, client.group.group_id)?;
    let ScanPayload(request) = serde_json::from_slice(payload.as_slice()).unwrap();
    request.after.as_ref()?;
    let release = base.with_extension("release");
    write(
        &base,
        "waiting",
        json!({"scope": scope, "input": input(payload), "digest": hash}),
    );
    let deadline = nabedra_rt::time::now() + Duration::from_secs(240);
    while !release.is_file() {
        assert!(
            nabedra_rt::time::now() < deadline,
            "scan continuation fixture timed out"
        );
        sleep_until(nabedra_rt::time::now() + Duration::from_millis(25)).await;
    }
    Some(HeldContinuation(base))
}

fn rejected(result: Result<ServiceResponse, ProtocolError>, expected: &str) -> ProtocolError {
    let error = result.err().expect("fixture probe unexpectedly succeeded");
    assert_eq!(error.code, ErrorCode::ValidationError, "{error:?}");
    assert_eq!(error.outcome, Outcome::NotApplied, "{error:?}");
    assert_eq!(error.message, expected, "{error:?}");
    error
}

impl HeldContinuation {
    pub(super) async fn probe(
        self,
        client: &Client,
        queue: &Arc<QueueCoordinator>,
        scope: ReadScope,
        payload: &ChargedBytes,
        hash: Digest,
    ) -> Result<(), ProtocolError> {
        let target = std::fs::read_to_string(self.0.with_extension("target")).unwrap();
        let target = Uuid::parse_str(target.trim()).unwrap();
        let routes = client.control.replica_routes(&client.group).await?;
        let node = routes
            .members
            .iter()
            .find(|member| member.worker_id == target)
            .ok_or_else(unavailable)?
            .node_id;
        let replica = client.shared.read_replica()?;
        let (owner, worker) = {
            let replica = replica.lock().map_err(|_| unavailable())?;
            (
                replica.local_identity(),
                replica
                    .read_member(scope.authority, target)
                    .ok_or_else(unavailable)?,
            )
        };
        let wrong_scope = ReadScope {
            read_ts: ReadTs::new(scope.read_ts.get().checked_add(1).unwrap()).unwrap(),
            ..scope
        };
        let wrong_reply = client
            .call_member(
                ServiceAction::Owner,
                Method::ParticipantScanOpen {
                    scope: wrong_scope,
                    input: hash,
                },
                &None,
                node,
                worker.into(),
            )
            .await
            .map_err(ProtocolError::from)?;
        let wrong_error = rejected(
            wrong_reply,
            "authenticated participant proof context differs",
        );

        let opened = client
            .call_member(
                ServiceAction::Owner,
                Method::ParticipantScanOpen { scope, input: hash },
                &None,
                node,
                worker.into(),
            )
            .await
            .map_err(ProtocolError::from)??;
        assert_eq!(opened.reply.remote_identity(), worker);
        let grant = ReadGrant {
            scope,
            owner,
            worker,
            group: client.group.group_id,
            node,
            ticket: *opened.reply.decode_section::<Uuid>(0)?,
            input: hash,
        };
        drop(opened);
        let mut forged: ScanPayload = serde_json::from_slice(payload.as_slice()).unwrap();
        assert!(forged.0.after.take().is_some());
        let forged = client
            .shared
            .encode_in(ByteClass::Data, &forged, MAX_FIXED_COMMAND_BYTES)?;
        let forged_digest = digest(&forged);
        assert_ne!(forged_digest, hash);
        let forged = Some(forged);
        let attempted = async {
            queue.open_read_grant(grant).await?;
            client
                .call_member(
                    ServiceAction::Owner,
                    Method::ParticipantScanRun {
                        ticket: grant.ticket,
                    },
                    &forged,
                    node,
                    worker.into(),
                )
                .await
                .map_err(ProtocolError::from)
        }
        .await;
        queue.settle_read_grant(grant).await?;
        let forged_error = rejected(
            attempted?,
            "authenticated participant proof context differs",
        );
        write(
            &self.0,
            "probes",
            json!({
                "wrong_scope": wrong_scope, "wrong_scope_error": wrong_error,
                "grant": grant, "forged_input": input(forged.as_ref().unwrap()),
                "forged_digest": forged_digest, "forged_error": forged_error,
            }),
        );
        Ok(())
    }
}
