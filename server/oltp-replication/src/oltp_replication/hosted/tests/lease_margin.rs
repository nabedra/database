//! RANGES-3: the lease read window stays inside every voter's vote-refusal
//! window under the stated clock-rate bound.
use super::super::{LEASE_DRIFT_PPM, LEASE_EPSILON, lease_read_window};
use std::time::Duration;

const MILLION: u128 = 1_000_000;

/// True-time end of the leader's window when its clock runs `ppm` slow, and of
/// a voter's refusal (openraft: the full `election_timeout_max`) when the
/// voter's clock runs `ppm` fast, both from the same instant: the leader's send
/// time, which is no later than the voter's receive time. Microseconds.
fn true_ends(window: Duration, lease: Duration, ppm: u64) -> (u128, u128) {
    let ppm = u128::from(ppm);
    (
        window.as_micros() * MILLION / (MILLION - ppm),
        lease.as_micros() * MILLION / (MILLION + ppm),
    )
}

fn holds(election_timeout_max_ms: u64, ppm: u64) -> bool {
    let window = lease_read_window(election_timeout_max_ms);
    let (leader, voter) = true_ends(window, Duration::from_millis(election_timeout_max_ms), ppm);
    leader < voter
}

/// The production timeouts (`registered_raft_config`: 1,500 to 3,000 ms) and
/// the test ones: the relation holds at the stated bound and fails beyond it,
/// so a change to the margin, the bound or the timeouts that breaks it fails
/// here. The server crate pins its registered configuration to the same check.
#[test]
fn o18_ranges3_lease_window_is_inside_the_voter_refusal_at_the_drift_bound() {
    assert_eq!(LEASE_DRIFT_PPM, 1_000);
    assert_eq!(LEASE_EPSILON, Duration::from_millis(1));
    assert_eq!(lease_read_window(3_000), Duration::from_millis(2_993));
    // The simulator's 400 ms timeouts: 400 x 0.998 - 1 = 398.2 ms.
    assert_eq!(lease_read_window(400), Duration::from_micros(398_200));
    for timeout in [300, 400, 600, 950, 3_000, 4_000, 60_000] {
        assert!(holds(timeout, LEASE_DRIFT_PPM), "{timeout} ms at the bound");
        assert!(holds(timeout, 0), "{timeout} ms without drift");
        // Four times the bound overruns the margin: the check is not vacuous.
        assert!(!holds(timeout, 4 * LEASE_DRIFT_PPM), "{timeout} ms beyond the bound");
    }
    // The bare lease (the shipped rule before RANGES-3) fails at the bound.
    let lease = Duration::from_millis(3_000);
    let (leader, voter) = true_ends(lease, lease, LEASE_DRIFT_PPM);
    assert!(leader >= voter, "the bare lease overruns the voters' refusal");
}

/// RANGES-3: the read path's option (what `linearized` passes openraft)
/// carries the margin. The bare `ReadPolicy::LeaseRead` would show
/// `max_quorum_ack_age: None` (openraft then uses the full lease).
#[test]
fn o18_ranges3_lease_reads_use_the_margin_option() {
    use openraft::ReadPolicy;
    let lease = super::super::lease_read_option(ReadPolicy::LeaseRead, 3_000).to_string();
    assert!(lease.contains("max_quorum_ack_age: Some(2.993s)"), "{lease}");
    assert!(lease.contains("heartbeat_if_quorum_ack_stale: false"), "{lease}");
    assert!(lease.contains("wait_timeout: Some(0ns)"), "{lease}");
    let bare = openraft::LinearizerOption::from(ReadPolicy::LeaseRead).to_string();
    assert_ne!(lease, bare);
    let index = super::super::lease_read_option(ReadPolicy::ReadIndex, 3_000).to_string();
    assert_eq!(index, openraft::LinearizerOption::from(ReadPolicy::ReadIndex).to_string());
}
