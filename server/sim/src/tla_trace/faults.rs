//! The shared fault vocabulary, and the subscriber that logs the faults the simulation
//! reports today: [`SimEvent`] from the World and [`NetEvent`] from a `NetPlan`.
//!
//! | Action | Node | `msg` | Reported by |
//! | --- | --- | --- | --- |
//! | `Crash` | the node | `kind`: `unspecified` | World |
//! | `Restart` | the node | `cut`: `none`, `kill`, `crash`, `power512` or `power4k`; `incarnation`: restarts after a crash the trace saw, 0 when the node was not down | World |
//! | `DiskLoss` | the node | `mount`; `how`: `reformat` or `replace` | World |
//! | `Pause` | `world` | `nanos` | World |
//! | `DropMessage` | the sender | `to`; `cause`: `plan` or `blocked` | `NetPlan` |
//! | `DelayMessage` | the sender | `to`, `delay_ns` | `NetPlan`, when asked for |
//! | `DuplicateMessage` | the sender | `to` | `NetPlan` |
//! | `Partition` | the link's `from` | `to` | `NetPlan` |
//! | `Heal` | the link's `from`, or `world` | `to`, or `all` | `NetPlan` |
//! | `HoldMessages`, `ReleaseMessages` | the node | none | `NetPlan` pause and resume |
//! | `SyncError` | the node | `mount`, `path`, `op` | nobody yet |
//! | `Freeze`, `Thaw` | the node | none | nobody yet |
//! | `ClockDrift` | the node | `ppm` | nobody yet |
//! | `WallJump` | the node | `nanos`, signed | nobody yet |
//! | `MonoAdvance` | the node | `nanos` | nobody yet |
//! | `ResetConnection` | the side that reset, or `world` | `conn`, `cause` | nobody yet |
//!
//! Whoever injects a fault that nobody reports logs it with [`TlaSteps::fault`]. The World
//! reports a kill and a power-off as the same event, so `kind` is `unspecified` until it
//! tells them apart. `nanos`, `delay_ns` and `conn` can outgrow the 32-bit integers TLC reads,
//! so they are decimal strings on every line. A `NetPlan` pause holds a node's messages while its tasks run, which is neither a `Pause` of the
//! world nor a `Freeze` of the node.
//!
//! `SimEvent::Fs` is deliberately not turned into `SyncError`. A failed call there may be
//! an injected fault, but also a call on a halted or wiped image by a destructor of an
//! earlier incarnation, which can run after the node is up again.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use nabedra_rt::sim::{Cut, SimEvent, Wipe};
use serde_json::{Value, json};

use super::{Inner, NodeRef, Stage, TlaSteps, lock};
use crate::net::{BASE_DELAY_MS, NetEvent, Plan};

/// One fault, in the shared vocabulary.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Fault {
    Crash {
        node: u64,
    },
    Restart {
        node: u64,
        cut: Option<Cut>,
    },
    DiskLoss {
        node: u64,
        mount: String,
        how: Wipe,
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
    SyncError {
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
    /// `node` is `None` when the world reset the connection.
    ResetConnection {
        node: Option<u64>,
        conn: u64,
        cause: String,
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

/// A duration as a message: always a decimal string.
fn nanos(by: Duration) -> Value {
    json!({"nanos": by.as_nanos().to_string()})
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
            SimEvent::Crash { node } => Fault::Crash { node },
            SimEvent::Cut { node, cut } => {
                // `World::restart_with` reports its `Restart` next.
                inner.tracker.cuts.insert(node, cut);
                return;
            }
            SimEvent::Restart { node } => Fault::Restart {
                node,
                cut: inner.tracker.cuts.remove(&node),
            },
            SimEvent::Wipe { node, mount, how } => Fault::DiskLoss {
                node,
                mount: mount.to_string_lossy().into_owned(),
                how,
            },
            SimEvent::Pause { by } => Fault::Pause { by },
            // Not faults; see the module documentation for `Fs`.
            SimEvent::Step { .. } | SimEvent::Fs { .. } => return,
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
            Fault::Crash { node } => {
                self.tracker.down.insert(node);
                0
            }
            Fault::Restart { node, .. } => self.tracker.restart(node),
            _ => 0,
        };
        let name = |id: u64| self.node_name(&NodeRef::World(id));
        let to = |id: u64| Some(json!({"to": name(id)}));
        let node = NodeRef::World;
        let world = || NodeRef::from("world");
        let (action, node, msg) = match &fault {
            Fault::Crash { node: id } => ("Crash", node(*id), Some(json!({"kind": "unspecified"}))),
            Fault::Restart { node: id, cut } => (
                "Restart",
                node(*id),
                Some(json!({"cut": cut_name(*cut), "incarnation": incarnation})),
            ),
            Fault::DiskLoss {
                node: id,
                mount,
                how,
            } => {
                let how = match how {
                    Wipe::Reformat => "reformat",
                    Wipe::Replace => "replace",
                };
                (
                    "DiskLoss",
                    node(*id),
                    Some(json!({"mount": mount, "how": how})),
                )
            }
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
            Fault::SyncError {
                node: id,
                mount,
                path,
                op,
            } => (
                "SyncError",
                node(*id),
                Some(json!({"mount": mount, "path": path, "op": op})),
            ),
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
            Fault::ResetConnection {
                node: id,
                conn,
                cause,
            } => (
                "ResetConnection",
                id.map_or_else(world, node),
                Some(json!({"conn": conn.to_string(), "cause": cause})),
            ),
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
        if !matches!(self.stage, Stage::Open(_)) {
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
