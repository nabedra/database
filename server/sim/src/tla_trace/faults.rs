//! The shared fault vocabulary, and the subscriber that logs the faults the simulation
//! reports: [`SimEvent`] from the World and [`NetEvent`] from a `NetPlan`. A World event it
//! does not know fails the trace.
//!
//! | Action | Node | `msg` | Reported by |
//! | --- | --- | --- | --- |
//! | `Crash` | the node | `kind`: `kill` or `power_off` | World |
//! | `Restart` | the node | `cut`: `none`, `kill`, `crash`, `power512` or `power4k`; `incarnation`: restarts after a crash the trace saw, 0 when the node was not down | World |
//! | `TornWrite` | the node | `mount`, `unit`, and the counts `kept`, `lost`, `torn`, `destroyed`, `lengths_kept`, `lengths_lost`, `dir_ops_kept`, `dir_ops_lost`; one line per volume, after the `Restart` of a power-loss cut | World |
//! | `DiskLoss` | the node | `mount`; `how`: `reformat` or `replace`; `mkfs`: inode numbers start over | World |
//! | `StaleRestore` | the node | `mount`, `taken_ns`: when the image was taken | World |
//! | `Pause` | `world` | `nanos` | World |
//! | `DropMessage` | the sender | `to`; `cause`: `plan` or `blocked` | `NetPlan` |
//! | `DelayMessage` | the sender | `to`, `delay_ns` | `NetPlan`, when asked for |
//! | `DuplicateMessage` | the sender | `to` | `NetPlan` |
//! | `Partition` | the link's `from` | `to` | `NetPlan` |
//! | `Heal` | the link's `from`, or `world` | `to`, or `all` | `NetPlan` |
//! | `HoldMessages`, `ReleaseMessages` | the node | none | `NetPlan` pause and resume |
//! | `StreamDrop` | the sender | `to`, `conn` | stream network (`rt::sim::net`) |
//! | `ResetConnection` | the dialing side | `to`, `conn`; `cause`: `planner`, `world`, `reboot` or `killed` | stream network |
//! | `StreamFlush` | the killed node | `conn` | stream network |
//! | `LinkDown`, `LinkUp` | the link's `from`, or `world` | `to`, or `all` | stream network black holes |
//! | `DialRefused` | the dialer | `to` | stream network planner |
//! | `StoragePaused` | the node | `op`, `path`; `side`: `before` or `after` the call | World |
//! | `StorageResumed` | the node | none | World |
//! | `ClockFrozen` | the node | none | World |
//! | `Freeze`, `Thaw` | the node | none | World |
//! | `ClockDrift` | the node | `ppm` | World |
//! | `WallJump` | the node | `nanos`, signed | World |
//! | `MonoAdvance` | the node | `nanos` | World |
//! | `SyncError`, `WriteError`, `NoSpace` | the node | `mount`, `path`, `op` | World, from an injected fault or a full volume |
//!
//! `ClockFrozen` is the freeze that lands right after a clock read; the `Thaw` that ends it
//! is logged like any other. A crash while a job is paused ends it without a
//! `StorageResumed`. A node outside the World (code outside every node) is `external`.
//!
//! **Not in a trace today**, so a spec that models one of these is not checked on it:
//!
//! - `crash_volume_after` and `flip_bit`, which act on a `FaultFs` directly;
//! - faults a harness injects into a `FaultFs` it does not mount on the World, and the cuts
//!   it makes of one: log those with [`TlaSteps::fault`];
//! - the inside of a blocking job or a service's step that no storage pause stops.
//!
//! `SimEvent::Fs` becomes a line only when the call returned an injected error
//! (`FsCall::fault`). Other failed calls are not faults: a call on a halted or wiped image
//! by a destructor of an earlier incarnation fails too, and can run after the node is up
//! again.
//!
//! `nanos`, `delay_ns`, `taken_ns` and `conn` can outgrow the 32-bit integers TLC reads, so
//! they are decimal strings on every line. A `NetPlan` pause holds a node's messages while
//! its tasks run, which is neither a `Pause` of the world nor a freeze of the node.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use nabedra_rt::fs::{FaultError, FsOp, PowerLossReport};
use nabedra_rt::sim::net::{NetFault, ResetCause};
use nabedra_rt::sim::{Cut, PausedCall, Side, SimEvent, StepOutcome, Wipe};
use serde_json::{Value, json};

use super::{Inner, NodeRef, Stage, TlaSteps, lock};
use crate::net::{BASE_DELAY_MS, NetEvent, Plan};

/// The node of a world-wide fault.
pub(super) const WORLD: &str = "world";

/// The node of code outside every World node.
pub(super) const EXTERNAL: &str = "external";

/// One fault, in the shared vocabulary.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Fault {
    Crash {
        node: u64,
        /// [`nabedra_rt::sim::World::power_off`], not a process kill.
        power_off: bool,
    },
    Restart {
        node: u64,
        cut: Option<Cut>,
    },
    TornWrite {
        node: u64,
        mount: String,
        report: PowerLossReport,
    },
    DiskLoss {
        node: u64,
        mount: String,
        how: Wipe,
        mkfs: bool,
    },
    StaleRestore {
        node: u64,
        mount: String,
        taken: Duration,
    },
    Pause {
        by: Duration,
    },
    DropMessage {
        from: u64,
        to: u64,
        blocked: bool,
    },
    DelayMessage {
        from: u64,
        to: u64,
        delay_ms: u64,
    },
    DuplicateMessage {
        from: u64,
        to: u64,
    },
    Partition {
        from: u64,
        to: u64,
    },
    Heal {
        from: u64,
        to: u64,
    },
    HealAll,
    HoldMessages {
        node: u64,
    },
    ReleaseMessages {
        node: u64,
    },
    StreamDrop {
        from: Option<u64>,
        to: Option<u64>,
        conn: u64,
    },
    /// `from` is the dialing side, `to` the accepting side.
    ResetConnection {
        from: Option<u64>,
        to: Option<u64>,
        conn: u64,
        cause: String,
    },
    StreamFlush {
        node: u64,
        conn: u64,
    },
    LinkDown {
        from: Option<u64>,
        to: Option<u64>,
    },
    LinkUp {
        from: Option<u64>,
        to: Option<u64>,
    },
    LinkUpAll,
    DialRefused {
        from: Option<u64>,
        to: Option<u64>,
    },
    StoragePaused {
        node: u64,
        call: PausedCall,
    },
    StorageResumed {
        node: u64,
    },
    ClockFrozen {
        node: u64,
    },
    SyncError {
        node: u64,
        mount: String,
        path: String,
        op: String,
    },
    WriteError {
        node: u64,
        mount: String,
        path: String,
        op: String,
    },
    NoSpace {
        node: u64,
        mount: String,
        path: String,
        op: String,
    },
    Freeze {
        node: u64,
    },
    Thaw {
        node: u64,
    },
    ClockDrift {
        node: u64,
        ppm: i64,
    },
    WallJump {
        node: u64,
        nanos: i128,
    },
    MonoAdvance {
        node: u64,
        by: Duration,
    },
}

/// A fault's line before the workload's mapping.
#[derive(Clone, Debug, PartialEq)]
pub struct FaultLine {
    pub fault: Fault,
    /// The vocabulary name; a mapping may put the spec's action here.
    pub action: String,
    pub node: NodeRef,
    /// Empty; a mapping fills what its action declares.
    pub vars: Value,
    pub msg: Option<Value>,
    /// What the router said the message was ([`crate::net::NetPlan::plan_about`]).
    pub about: Option<Value>,
}

/// What the subscriber remembers about a run.
#[derive(Debug, Default)]
pub(super) struct Tracker {
    down: BTreeSet<u64>,
    restarts: BTreeMap<u64, u64>,
    /// The cut image a node restarts from, until its `Restart` event.
    cuts: BTreeMap<u64, Cut>,
    /// Nodes whose next `Crash` is a power-off.
    powering_off: BTreeSet<u64>,
}

impl Tracker {
    /// The incarnation `node` starts with at this restart, 0 when it was not down.
    fn restart(&mut self, node: u64) -> u64 {
        if !self.down.remove(&node) {
            return 0;
        }
        let count = self.restarts.entry(node).or_default();
        *count += 1;
        *count
    }
}

fn cut_name(cut: Option<Cut>) -> &'static str {
    match cut {
        None => "none",
        Some(Cut::Kill) => "kill",
        Some(Cut::Crash) => "crash",
        Some(Cut::PowerLoss { .. }) => "power512",
        Some(Cut::PowerLoss4k { .. }) => "power4k",
    }
}

fn reset_cause(cause: ResetCause) -> &'static str {
    match cause {
        ResetCause::Planner => "planner",
        ResetCause::World => "world",
        ResetCause::Reboot => "reboot",
        ResetCause::Killed => "killed",
    }
}

/// A duration as a message: always a decimal string.
fn nanos(by: Duration) -> Value {
    json!({"nanos": by.as_nanos().to_string()})
}

/// The fault an injected filesystem error is.
fn fs_fault(node: u64, mount: String, path: String, op: FsOp, error: FaultError) -> Fault {
    let op_name = format!("{op:?}");
    match error {
        FaultError::Enospc => Fault::NoSpace {
            node,
            mount,
            path,
            op: op_name,
        },
        FaultError::Eio if matches!(op, FsOp::SyncData | FsOp::SyncAll | FsOp::SyncDir) => {
            Fault::SyncError {
                node,
                mount,
                path,
                op: op_name,
            }
        }
        FaultError::Eio => Fault::WriteError {
            node,
            mount,
            path,
            op: op_name,
        },
    }
}

fn net_fault(fault: NetFault) -> Option<Fault> {
    Some(match fault {
        NetFault::Drop { conn, from, to } => Fault::StreamDrop {
            from,
            to,
            conn: conn.0,
        },
        NetFault::Reset {
            conn,
            from,
            to,
            cause,
        } => Fault::ResetConnection {
            from,
            to,
            conn: conn.0,
            cause: reset_cause(cause).to_owned(),
        },
        NetFault::Flush { conn, node } => Fault::StreamFlush { node, conn: conn.0 },
        NetFault::BlackHole { from, to } => Fault::LinkDown { from, to },
        NetFault::Heal { from, to } => Fault::LinkUp { from, to },
        NetFault::HealAll => Fault::LinkUpAll,
        NetFault::Refuse { from, to } => Fault::DialRefused { from, to },
        // A fault this writer does not know: the caller fails the trace.
        _ => return None,
    })
}

impl TlaSteps {
    /// Logs a fault the subscriber does not see, such as one the harness injects into a
    /// disk of its own. Never log one it does see: the trace would hold it twice.
    pub fn fault(&self, fault: Fault) {
        lock(&self.inner).fault(fault, None);
    }

    pub(super) fn observe(&self, event: &SimEvent<'_>) {
        let mut inner = lock(&self.inner);
        let fault = match *event {
            SimEvent::PowerOff { node } => {
                inner.tracker.powering_off.insert(node);
                return;
            }
            SimEvent::Crash { node } => Fault::Crash {
                node,
                power_off: inner.tracker.powering_off.remove(&node),
            },
            SimEvent::Cut { node, cut } => {
                // `World::restart_with` reports its `Restart` next.
                inner.tracker.cuts.insert(node, cut);
                return;
            }
            SimEvent::Restart { node } => Fault::Restart {
                node,
                cut: inner.tracker.cuts.remove(&node),
            },
            SimEvent::TornWrite {
                node,
                mount,
                report,
            } => Fault::TornWrite {
                node,
                mount: mount.to_string_lossy().into_owned(),
                report: *report,
            },
            SimEvent::Wipe {
                node,
                mount,
                how,
                mkfs,
            } => Fault::DiskLoss {
                node,
                mount: mount.to_string_lossy().into_owned(),
                how,
                mkfs,
            },
            SimEvent::StaleRestore { node, mount, taken } => Fault::StaleRestore {
                node,
                mount: mount.to_string_lossy().into_owned(),
                taken,
            },
            SimEvent::Pause { by } => Fault::Pause { by },
            SimEvent::StoragePaused { node, call } => Fault::StoragePaused {
                node,
                call: call.clone(),
            },
            SimEvent::StorageResumed { node } => Fault::StorageResumed { node },
            SimEvent::ClockFrozen { node } => Fault::ClockFrozen { node },
            SimEvent::Freeze { node } => Fault::Freeze { node },
            SimEvent::Thaw { node } => Fault::Thaw { node },
            SimEvent::Drift { node, ppm } => Fault::ClockDrift { node, ppm },
            SimEvent::WallJump { node, nanos } => Fault::WallJump { node, nanos },
            SimEvent::MonoAdvance { node, by } => Fault::MonoAdvance { node, by },
            SimEvent::Net(fault) => match net_fault(fault) {
                Some(fault) => fault,
                None => {
                    return inner
                        .fail(&format!("the network reported an unknown fault: {fault:?}"));
                }
            },
            SimEvent::Fs { node, mount, call } => match call.fault {
                Some(error) => fs_fault(
                    node,
                    mount.to_string_lossy().into_owned(),
                    call.path.to_string_lossy().into_owned(),
                    call.op,
                    error,
                ),
                // Not a fault; see the module documentation.
                None => return,
            },
            SimEvent::Step {
                service,
                outcome: StepOutcome::Panicked,
                ..
            } => return inner.fail(&format!("service {service} panicked")),
            SimEvent::Step { .. } => return,
            // A fault this writer does not know must not be left out in silence.
            ref unknown => {
                return inner.fail(&format!("the World reported an unknown event: {unknown:?}"));
            }
        };
        inner.fault(fault, None);
    }

    pub(super) fn observe_net(&self, event: &NetEvent) {
        let mut inner = lock(&self.inner);
        let (fault, about) = match *event {
            NetEvent::Message {
                from,
                to,
                plan,
                blocked,
                ref about,
            } => {
                let fault = match plan {
                    Plan::Drop => Fault::DropMessage { from, to, blocked },
                    Plan::Deliver {
                        delay_ms,
                        duplicate,
                    } => {
                        if inner.delays && delay_ms > BASE_DELAY_MS {
                            let delayed = Fault::DelayMessage { from, to, delay_ms };
                            inner.fault(delayed, Some(about.clone()));
                        }
                        if !duplicate {
                            return;
                        }
                        Fault::DuplicateMessage { from, to }
                    }
                };
                (fault, Some(about.clone()))
            }
            NetEvent::Blocked { from, to } => (Fault::Partition { from, to }, None),
            NetEvent::Unblocked { from, to } => (Fault::Heal { from, to }, None),
            NetEvent::Healed => (Fault::HealAll, None),
            NetEvent::Held { node } => (Fault::HoldMessages { node }, None),
            NetEvent::Released { node } => (Fault::ReleaseMessages { node }, None),
        };
        inner.fault(fault, about.filter(|about| !about.is_null()));
    }
}

impl Inner {
    /// The vocabulary line of `fault`.
    fn vocabulary(&mut self, fault: Fault, about: Option<Value>) -> FaultLine {
        let incarnation = match fault {
            Fault::Crash { node, .. } => {
                self.tracker.down.insert(node);
                0
            }
            Fault::Restart { node, .. } => self.tracker.restart(node),
            _ => 0,
        };
        let name = |id: u64| self.node_name(&NodeRef::World(id));
        let any_name = |id: Option<u64>| id.map_or_else(|| EXTERNAL.to_owned(), &name);
        let to = |id: u64| Some(json!({"to": name(id)}));
        let node = NodeRef::World;
        let any_node = |id: Option<u64>| id.map_or_else(|| NodeRef::from(EXTERNAL), NodeRef::World);
        let world = || NodeRef::from(WORLD);
        let fs_line = |id: &u64, mount: &String, path: &String, op: &String| {
            (
                node(*id),
                Some(json!({"mount": mount, "path": path, "op": op})),
            )
        };
        let (action, node, msg) = match &fault {
            Fault::Crash {
                node: id,
                power_off,
            } => {
                let kind = if *power_off { "power_off" } else { "kill" };
                ("Crash", node(*id), Some(json!({"kind": kind})))
            }
            Fault::Restart { node: id, cut } => (
                "Restart",
                node(*id),
                Some(json!({"cut": cut_name(*cut), "incarnation": incarnation})),
            ),
            Fault::TornWrite {
                node: id,
                mount,
                report,
            } => (
                "TornWrite",
                node(*id),
                Some(json!({
                    "mount": mount,
                    "unit": report.unit,
                    "kept": report.kept,
                    "lost": report.lost,
                    "torn": report.torn,
                    "destroyed": report.destroyed,
                    "lengths_kept": report.lengths_kept,
                    "lengths_lost": report.lengths_lost,
                    "dir_ops_kept": report.dir_ops_kept,
                    "dir_ops_lost": report.dir_ops_lost,
                })),
            ),
            Fault::DiskLoss {
                node: id,
                mount,
                how,
                mkfs,
            } => {
                let how = match how {
                    Wipe::Reformat => "reformat",
                    Wipe::Replace => "replace",
                };
                (
                    "DiskLoss",
                    node(*id),
                    Some(json!({"mount": mount, "how": how, "mkfs": mkfs})),
                )
            }
            Fault::StaleRestore {
                node: id,
                mount,
                taken,
            } => (
                "StaleRestore",
                node(*id),
                Some(json!({"mount": mount, "taken_ns": taken.as_nanos().to_string()})),
            ),
            Fault::Pause { by } => ("Pause", world(), Some(nanos(*by))),
            Fault::DropMessage { from, to, blocked } => {
                let cause = if *blocked { "blocked" } else { "plan" };
                (
                    "DropMessage",
                    node(*from),
                    Some(json!({"to": name(*to), "cause": cause})),
                )
            }
            Fault::DelayMessage { from, to, delay_ms } => (
                "DelayMessage",
                node(*from),
                Some(json!({
                    "to": name(*to),
                    "delay_ns": (u128::from(*delay_ms) * 1_000_000).to_string(),
                })),
            ),
            Fault::DuplicateMessage { from, to: id } => ("DuplicateMessage", node(*from), to(*id)),
            Fault::Partition { from, to: id } => ("Partition", node(*from), to(*id)),
            Fault::Heal { from, to: id } => ("Heal", node(*from), to(*id)),
            Fault::HealAll => ("Heal", world(), Some(json!({"all": true}))),
            Fault::HoldMessages { node: id } => ("HoldMessages", node(*id), None),
            Fault::ReleaseMessages { node: id } => ("ReleaseMessages", node(*id), None),
            Fault::StreamDrop { from, to, conn } => (
                "StreamDrop",
                any_node(*from),
                Some(json!({"to": any_name(*to), "conn": conn.to_string()})),
            ),
            Fault::ResetConnection {
                from,
                to,
                conn,
                cause,
            } => (
                "ResetConnection",
                any_node(*from),
                Some(json!({"to": any_name(*to), "conn": conn.to_string(), "cause": cause})),
            ),
            Fault::StreamFlush { node: id, conn } => (
                "StreamFlush",
                node(*id),
                Some(json!({"conn": conn.to_string()})),
            ),
            Fault::LinkDown { from, to } => (
                "LinkDown",
                any_node(*from),
                Some(json!({"to": any_name(*to)})),
            ),
            Fault::LinkUp { from, to } => (
                "LinkUp",
                any_node(*from),
                Some(json!({"to": any_name(*to)})),
            ),
            Fault::LinkUpAll => ("LinkUp", world(), Some(json!({"all": true}))),
            Fault::DialRefused { from, to } => (
                "DialRefused",
                any_node(*from),
                Some(json!({"to": any_name(*to)})),
            ),
            Fault::StoragePaused { node: id, call } => {
                let side = match call.side {
                    Side::Before => "before",
                    Side::After => "after",
                };
                let op = format!("{:?}", call.op);
                let msg = json!({"op": op, "path": call.path.to_string_lossy(), "side": side});
                ("StoragePaused", node(*id), Some(msg))
            }
            Fault::StorageResumed { node: id } => ("StorageResumed", node(*id), None),
            Fault::ClockFrozen { node: id } => ("ClockFrozen", node(*id), None),
            Fault::SyncError {
                node: id,
                mount,
                path,
                op,
            } => {
                let (node, msg) = fs_line(id, mount, path, op);
                ("SyncError", node, msg)
            }
            Fault::WriteError {
                node: id,
                mount,
                path,
                op,
            } => {
                let (node, msg) = fs_line(id, mount, path, op);
                ("WriteError", node, msg)
            }
            Fault::NoSpace {
                node: id,
                mount,
                path,
                op,
            } => {
                let (node, msg) = fs_line(id, mount, path, op);
                ("NoSpace", node, msg)
            }
            Fault::Freeze { node: id } => ("Freeze", node(*id), None),
            Fault::Thaw { node: id } => ("Thaw", node(*id), None),
            Fault::ClockDrift { node: id, ppm } => {
                ("ClockDrift", node(*id), Some(json!({"ppm": ppm})))
            }
            Fault::WallJump { node: id, nanos } => (
                "WallJump",
                node(*id),
                Some(json!({"nanos": nanos.to_string()})),
            ),
            Fault::MonoAdvance { node: id, by } => ("MonoAdvance", node(*id), Some(nanos(*by))),
        };
        FaultLine {
            action: action.to_owned(),
            node,
            vars: json!({}),
            msg,
            about,
            fault,
        }
    }

    fn fault(&mut self, fault: Fault, about: Option<Value>) {
        if !matches!(self.stage, Stage::Open { .. }) {
            return;
        }
        let line = self.vocabulary(fault, about);
        let name = line.action.clone();
        let Some(line) = (self.faults)(line) else {
            return;
        };
        let checked = match self.actions.get(&line.action) {
            Some(declared) => declared.check(&line.vars, line.msg.as_ref()),
            None if line.action == name && line.vars == json!({}) => Ok(()),
            None => Err(format!(
                "fault {name} became {} with {}, which is not declared",
                line.action, line.vars
            )),
        };
        match checked {
            Ok(()) => self.write(&line.node, &line.action, &line.vars, line.msg.as_ref()),
            Err(error) => self.fail(&error),
        }
    }
}
