//! Stateful deployment decisions. Provider acknowledgements never constitute
//! mounted capacity, voting membership, queue transfer, or storage closure.
#[doc(hidden)]
pub mod controller;
#[doc(hidden)]
pub mod inventory;
pub mod live;
pub mod planner;
