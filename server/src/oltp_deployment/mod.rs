//! Stateful deployment decisions. Provider acknowledgements never constitute
//! mounted capacity, voting membership, queue transfer, or storage closure.
pub(crate) mod controller;
pub(crate) mod inventory;
pub mod live;
pub mod planner;
