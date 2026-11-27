//! The journal's protocol steps, for TLA+ trace validation against `specs/lsm/Lsm.tla`
//! (`specs/lsm/LsmTrace.tla`). Simulation builds only.
//!
//! The journal writer reports each step at the point where it takes effect, through a
//! [`JournalProbe`] that a test puts in the World's extension slot. Without one, or outside
//! a World, a report does nothing. The probe runs on the thread that steps the journal,
//! under none of the journal's locks, and must not call back into the store.
//!
//! | Step | Spec action | Where |
//! | --- | --- | --- |
//! | [`JournalStep::Written`] | `JournalWriteGroup` | `Writer::process`, after the group's bytes are written (`flush_buf` or `write_aligned`) |
//! | [`JournalStep::Published`] | `JournalPublish` | after `visible_seq` is stored without a sync: the unsynced head of a syncing group, or a group without a sync |
//! | [`JournalStep::Synced`] | `JournalSync` | after `sync_written` and the publication of the rest of the group |
//!
//! A `Submit` is logged by the caller of `Store::submit`, which knows the writer, the key
//! and the operation; the sequence is `Commit::seq`.

use crate::types::Seq;

/// One step of the journal writer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalStep {
    /// The group's records are written through `last_seq`.
    Written { last_seq: Seq },
    /// Records are visible through `visible`; durable through `durable`, unchanged.
    Published { visible: Seq, durable: Seq },
    /// Every record written is synced through `synced` and the group is visible through
    /// `visible`, durable through `durable`.
    Synced {
        synced: Seq,
        visible: Seq,
        durable: Seq,
    },
}

/// The receiver of [`JournalStep`]s, in the World's extension slot.
pub struct JournalProbe(pub Box<dyn FnMut(JournalStep) + Send>);

/// Reports `step` to the probe, if a test installed one.
pub(crate) fn report(step: JournalStep) {
    if let Some(world) = nabedra_rt::sim::world() {
        world.with_ext::<JournalProbe, _>(|probe| (probe.0)(step));
    }
}
