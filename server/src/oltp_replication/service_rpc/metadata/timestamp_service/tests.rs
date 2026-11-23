use super::*;

#[test]
fn d01_t9_allocate_direct_admits_the_direct_leader_role() {
    assert!(allocate_direct_permitted(&ServiceAction::DirectLeader));
    assert!(allocate_direct_permitted(&ServiceAction::Owner));
    assert!(allocate_direct_permitted(
        &ServiceAction::ParticipantProof {
            source_group: nabedra_rt::ids::uuid_v4(),
        }
    ));
    assert!(!allocate_direct_permitted(&ServiceAction::ApiIngress));
}
