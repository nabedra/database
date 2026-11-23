use super::*;

#[test]
fn o06_o18_direct_catalog_reads_use_bounded_replica_failover() {
    let read = Method::ParticipantRead {
        read_ts: ReadTs::new(u64::MAX).unwrap(),
    };
    assert!(read.uses_bounded_read_failover(ServiceAction::DirectLeader));
    assert_eq!(
        read.peer_timeout(ServiceAction::DirectLeader, false),
        Duration::from_millis(500)
    );
    assert!(Method::ParticipantGuard.uses_bounded_read_failover(ServiceAction::DirectLeader));
    assert!(
        !Method::ParticipantPropose { progress: false }
            .uses_bounded_read_failover(ServiceAction::DirectLeader)
    );
    assert!(!read.uses_bounded_read_failover(ServiceAction::Owner));
}

#[test]
fn o06_o18_allocate_direct_leaves_time_to_find_the_new_leader() {
    let allocate = Method::TimestampCall {
        operation: 41,
        mutation: true,
    };
    let peer = allocate.peer_timeout(ServiceAction::DirectLeader, false);
    let stalled = allocate.peer_timeout(ServiceAction::DirectLeader, true);
    assert!(peer <= Duration::from_millis(750));
    assert!(stalled <= Duration::from_millis(200));
    assert!(stalled < peer);
    let other = Method::TimestampCall {
        operation: 1,
        mutation: true,
    };
    assert_eq!(
        other.peer_timeout(ServiceAction::Owner, false),
        SCOPED_MUTATION_TIMEOUT
    );
}
