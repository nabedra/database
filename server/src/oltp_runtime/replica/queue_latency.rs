use crate::oltp_replication::types::{AuthorityView, WorkerIdentity};
use serde::Serialize;
use std::time::Instant;

const UPPER_US: [u64; 18] = [
    50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000, 2_500_000, 5_000_000, 10_000_000, 30_000_000,
];

#[derive(Default)]
pub(super) enum AcceptanceTiming {
    #[default]
    Untracked,
    Copying,
    Waiting(Instant),
}

#[derive(Default)]
pub(super) struct QueueLatency {
    bucket_counts: [u64; UPPER_US.len() + 1],
    waiting: u64,
    discarded_before_dispatch: u64,
    discarded_on_authority_change_total: u64,
}

#[derive(Serialize)]
pub(crate) struct QueueLatencySnapshot {
    worker: WorkerIdentity,
    owner: WorkerIdentity,
    view: AuthorityView,
    upper_us: &'static [u64],
    bucket_counts: [u64; UPPER_US.len() + 1],
    waiting: u64,
    discarded_before_dispatch: u64,
    discarded_on_authority_change_total: u64,
}

impl QueueLatency {
    pub(super) fn accepted(&mut self, timing: &mut AcceptanceTiming, now: Instant) {
        if matches!(timing, AcceptanceTiming::Copying) {
            *timing = AcceptanceTiming::Waiting(now);
            self.waiting += 1;
        }
    }

    pub(super) fn dispatch(&mut self, timing: &mut AcceptanceTiming, now: Instant) {
        if let AcceptanceTiming::Waiting(accepted) = std::mem::take(timing) {
            self.waiting -= 1;
            let micros = now.duration_since(accepted).as_nanos().div_ceil(1_000);
            let bucket = UPPER_US.partition_point(|&limit| u128::from(limit) < micros);
            self.bucket_counts[bucket] = self.bucket_counts[bucket].saturating_add(1);
        }
    }

    pub(super) fn discard(&mut self, timing: &mut AcceptanceTiming) {
        if let AcceptanceTiming::Waiting(_) = std::mem::take(timing) {
            self.waiting -= 1;
            self.discarded_before_dispatch = self.discarded_before_dispatch.saturating_add(1);
        }
    }

    pub(super) fn change_authority(&mut self) {
        *self = Self {
            discarded_on_authority_change_total: self
                .discarded_on_authority_change_total
                .saturating_add(self.waiting),
            ..Self::default()
        };
    }

    pub(super) fn snapshot(
        &self,
        worker: WorkerIdentity,
        owner: WorkerIdentity,
        view: AuthorityView,
    ) -> QueueLatencySnapshot {
        QueueLatencySnapshot {
            worker,
            owner,
            view,
            upper_us: &UPPER_US,
            bucket_counts: self.bucket_counts,
            waiting: self.waiting,
            discarded_before_dispatch: self.discarded_before_dispatch,
            discarded_on_authority_change_total: self.discarded_on_authority_change_total,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn b02_first_dispatch_records_one_bucket_and_does_not_measure_recovery_replays() {
        let mut latency = QueueLatency::default();
        let mut timing = AcceptanceTiming::Copying;
        let now = Instant::now();
        latency.accepted(&mut timing, now);
        latency.accepted(&mut timing, now + Duration::from_micros(50));
        latency.dispatch(&mut timing, now + Duration::from_nanos(50_001));
        latency.dispatch(&mut timing, now + Duration::from_secs(40));
        assert_eq!(latency.waiting, 0);
        assert_eq!(latency.bucket_counts[1], 1);
        assert_eq!(latency.bucket_counts.iter().sum::<u64>(), 1);
    }

    #[test]
    fn b02_r01_terminal_and_fenced_requests_discard_local_timings() {
        let mut latency = QueueLatency::default();
        let mut terminal = AcceptanceTiming::Copying;
        let mut fenced = AcceptanceTiming::Copying;
        let mut tentative = AcceptanceTiming::Copying;
        let now = Instant::now();
        latency.accepted(&mut terminal, now);
        latency.accepted(&mut fenced, now);
        latency.discard(&mut tentative);
        latency.discard(&mut terminal);
        latency.discard(&mut terminal);
        assert_eq!(latency.discarded_before_dispatch, 1);
        assert_eq!(latency.waiting, 1);
        latency.change_authority();
        latency.change_authority();
        assert_eq!(latency.discarded_on_authority_change_total, 1);
        assert_eq!(latency.discarded_before_dispatch, 0);
        assert_eq!(latency.waiting, 0);
        assert_eq!(latency.bucket_counts.iter().sum::<u64>(), 0);
    }
}
