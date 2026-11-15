use super::*;

#[test]
fn control_open_rollback_keeps_ordered_barrier_and_generation() {
    let mut state = distributed_fixture(1, 1, 0);
    let (_, local) = state.parts();
    let local = local.unwrap();
    local.init = InitState::Initialized {
        identity: Uuid::from_u128(30),
    };
    let binding = Binding {
        coordinator: local.coordinator.clone(),
        operation: Uuid::from_u128(6),
        admission: Uuid::from_u128(7),
        digest: [10; 32],
    };
    let before = local.generation;
    let reservation = LocalControlReservation::open(local, binding, Uuid::from_u128(8)).unwrap();
    drop(reservation);
    assert_eq!(local.snapshot().dependencies, 0);
    assert_eq!(local.generation, before);
}
