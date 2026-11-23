pub(crate) use crate::oltp::read_attempt::{ScheduledReadError, UnsettledRead};
pub use nabedra_oltp_types::authority::{ReadGrant, ReadScope};

#[cfg(test)]
pub(crate) mod fixture;

#[cfg(test)]
mod tests {
    use super::*;
    use nabedra_resources::config::WorkerResources;
    use crate::oltp::{
        pool::WorkerSharedResources,
        read_attempt::{ATTEMPT_BYTES, UNUSED_LIFETIME},
    };
    use nabedra_oltp_types::types::{Authority, Digest, ReadTs, RequestBinding, WorkerIdentity};
    use uuid::Uuid;

    #[tokio::test(start_paused = true)]
    async fn o22_f01_f03_running_tickets_survive_timeout_and_unused_tickets_are_fenced() {
        let shared =
            WorkerSharedResources::new(1, WorkerResources::default(), Default::default()).unwrap();
        let book = &shared.read_attempts;
        let base = shared.scheduler().usage();
        let hash = Digest::from_bytes([7; 32]);
        let worker = WorkerIdentity {
            worker_id: nabedra_rt::ids::uuid_v4(),
            incarnation: nabedra_rt::ids::uuid_v4(),
            certificate_sha256: hash,
            enrollment_revision: 1,
        };
        let run_id = nabedra_rt::ids::uuid_v4();
        let scope = ReadScope {
            request: RequestBinding {
                run_id,
                request_id: nabedra_rt::ids::uuid_v4(),
                digest: hash,
                accepted_context: hash,
            },
            authority: Authority {
                run_id,
                owner_term: 1,
            },
            read_ts: ReadTs::new(13).unwrap(),
        };
        let group = nabedra_rt::ids::uuid_v4();
        let grant = ReadGrant {
            scope,
            owner: worker,
            worker,
            group,
            node: 7,
            ticket: Uuid::nil(),
            input: hash,
        };
        let unused = book.open(&shared, grant).unwrap();
        book.settle(unused).unwrap();
        assert!(
            book.claim(unused.ticket, worker, group, grant.node)
                .is_err()
        );
        let live = book.open(&shared, grant).unwrap();
        let stale = WorkerIdentity {
            incarnation: nabedra_rt::ids::uuid_v4(),
            ..worker
        };
        assert!(book.claim(live.ticket, stale, group, grant.node).is_err());
        assert!(
            book.claim(live.ticket, worker, group, grant.node + 1)
                .is_err()
        );
        assert!(
            book.settle(ReadGrant {
                owner: stale,
                ..live
            })
            .is_err()
        );
        let (original, work) = book.claim(live.ticket, worker, group, grant.node).unwrap();
        assert_eq!(original, live);
        assert!(book.claim(live.ticket, worker, group, grant.node).is_err());
        let aged = book.open(&shared, grant).unwrap();
        tokio::time::advance(UNUSED_LIFETIME * 2).await;
        let another = book.open(&shared, grant).unwrap();
        assert!(book.claim(aged.ticket, worker, group, grant.node).is_err());
        book.settle(another).unwrap();
        assert!(book.settle(live).is_err());
        assert_eq!(shared.scheduler().usage()[0].1, 0);
        assert_eq!(shared.scheduler().usage()[0].2, base[0].2 + ATTEMPT_BYTES);
        drop(work);
        book.settle(live).unwrap();
        assert!(book.claim(live.ticket, worker, group, grant.node).is_err());
        assert_eq!(shared.scheduler().usage(), base);
    }
}
