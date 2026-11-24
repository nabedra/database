//! Nemesis profiles for the `direct`, `cold` and `move` workloads (design section 3.6).
//!
//! Every action is weighted in a generic profile, and every pick, target and duration is
//! drawn from the seed's workload tape. Network loss is modelled the way TCP shows it to an
//! endpoint: a lost frame or a lost HTTP response resets the session (finding F3 of the
//! design review). Per-frame drop, black-hole and reorder faults are a separate, opt-in
//! `beyond-tcp` profile. The T1 and nightly sweeps never select it, and a failure under it is
//! never counted as a product bug without a TCP-possible reproduction.

pub(crate) mod apply;
pub(crate) mod net;

use std::{collections::BTreeSet, fmt, time::Duration};

use nabedra_check::Gen;
use nabedra_sim::Profile;

pub(crate) use net::NetFaults;

/// One nemesis decision. The mildest action comes first: the shrinker moves picks toward it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Action {
    /// Drift (up to 200 ppm) and a wall jump (up to 2 s) on one process (O09 clocks).
    Clock,
    /// Freeze one Data follower, like SIGSTOP (D05, one paused).
    FreezeFollower,
    /// Freeze both followers (D05, two paused).
    FreezeTwo,
    /// Isolate a minority of workers (O06).
    Partition,
    /// Isolate the leader from the other workers until another one leads (D07).
    LeaderMove,
    /// Reset one worker session after a drawn number of frames (D10).
    SessionDrop,
    /// Reset the session that carries a leader's reply frames (lost reply, O10).
    LostReply,
    /// Reset a gateway's HTTP connection when it flushes a response (lost reply, D02).
    LostHttpReply,
    /// Delay the leader's frames so attempts end near the request deadline (FIXA).
    DeadlineEdge,
    /// A gateway presents a stale certificate to the workers for a while (BUG1).
    AuthFlap,
    /// Kill a gateway, then restart it (D02).
    KillGateway,
    /// Kill the Data leader, then restart it through the retained restart path (D04).
    CrashLeader,
    /// Power off the Data leader: unsynced writes are lost (D04, O06).
    PowerLeader,
    /// Stop the leader gracefully with work in flight, then restart it (D04 lost responder).
    DrainLeader,
    /// Burst reads at one gateway past its lanes (D10 overload).
    LaneExhaust,
    /// Split the table's range under load (D07).
    Split,
    /// Fail one sync on a worker volume (O29, O06).
    DiskError,
    /// Stop every worker and cold start them (O15, D09).
    Cold,
    /// Beyond TCP: stall one direction of a session at a random frame.
    FrameDrop,
    /// Beyond TCP: stall a single stream while the rest of its session lives.
    BlackHole,
    /// Beyond TCP: delay frames independently, so streams of one session reorder.
    Reorder,
}

impl Action {
    pub(crate) const ALL: [Action; 21] = [
        Action::Clock,
        Action::FreezeFollower,
        Action::FreezeTwo,
        Action::Partition,
        Action::LeaderMove,
        Action::SessionDrop,
        Action::LostReply,
        Action::LostHttpReply,
        Action::DeadlineEdge,
        Action::AuthFlap,
        Action::KillGateway,
        Action::CrashLeader,
        Action::PowerLeader,
        Action::DrainLeader,
        Action::LaneExhaust,
        Action::Split,
        Action::DiskError,
        Action::Cold,
        Action::FrameDrop,
        Action::BlackHole,
        Action::Reorder,
    ];

    pub(crate) fn name(self) -> &'static str {
        match self {
            Action::Clock => "clock",
            Action::FreezeFollower => "freeze-follower",
            Action::FreezeTwo => "freeze-two",
            Action::Partition => "partition",
            Action::LeaderMove => "leader-move",
            Action::SessionDrop => "session-drop",
            Action::LostReply => "lost-reply",
            Action::LostHttpReply => "lost-http-reply",
            Action::DeadlineEdge => "deadline-edge",
            Action::AuthFlap => "auth-flap",
            Action::KillGateway => "kill-gateway",
            Action::CrashLeader => "crash-leader",
            Action::PowerLeader => "power-leader",
            Action::DrainLeader => "drain-leader",
            Action::LaneExhaust => "lane-exhaust",
            Action::Split => "split",
            Action::DiskError => "disk-error",
            Action::Cold => "cold",
            Action::FrameDrop => "frame-drop",
            Action::BlackHole => "black-hole",
            Action::Reorder => "reorder",
        }
    }

    pub(crate) fn beyond_tcp(self) -> bool {
        matches!(self, Action::FrameDrop | Action::BlackHole | Action::Reorder)
    }

    /// Aliases a manifest may use for one action family.
    fn parse(name: &str) -> Option<Vec<Action>> {
        Some(match name {
            "lost-reply" => vec![Action::LostReply, Action::LostHttpReply],
            "freeze-follower" => vec![Action::FreezeFollower, Action::FreezeTwo],
            "crash-leader" => vec![Action::CrashLeader, Action::PowerLeader],
            name => vec![*Action::ALL.iter().find(|action| action.name() == name)?],
        })
    }
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// The generic profiles. Weights are per mille of the profile's total.
const MIXED: &[(Action, u64)] = &[
    (Action::Clock, 6),
    (Action::FreezeFollower, 8),
    (Action::FreezeTwo, 4),
    (Action::Partition, 6),
    (Action::LeaderMove, 6),
    (Action::SessionDrop, 8),
    (Action::LostReply, 10),
    (Action::LostHttpReply, 8),
    (Action::DeadlineEdge, 6),
    (Action::AuthFlap, 5),
    (Action::KillGateway, 5),
    (Action::CrashLeader, 6),
    (Action::PowerLeader, 3),
    (Action::DrainLeader, 6),
    (Action::LaneExhaust, 4),
    (Action::Split, 3),
    (Action::DiskError, 2),
    (Action::Cold, 1),
];
const PROCESS: &[(Action, u64)] = &[
    (Action::Clock, 2),
    (Action::KillGateway, 6),
    (Action::CrashLeader, 8),
    (Action::PowerLeader, 4),
    (Action::DrainLeader, 8),
    (Action::DiskError, 3),
    (Action::Cold, 1),
];
const NETWORK: &[(Action, u64)] = &[
    (Action::Partition, 6),
    (Action::LeaderMove, 6),
    (Action::SessionDrop, 8),
    (Action::LostReply, 10),
    (Action::LostHttpReply, 8),
    (Action::DeadlineEdge, 8),
    (Action::AuthFlap, 6),
];
const PAUSE: &[(Action, u64)] = &[
    (Action::Clock, 6),
    (Action::FreezeFollower, 8),
    (Action::FreezeTwo, 6),
    (Action::LaneExhaust, 6),
    (Action::Split, 4),
    (Action::DeadlineEdge, 4),
];
const BEYOND_TCP: &[(Action, u64)] = &[
    (Action::FrameDrop, 4),
    (Action::BlackHole, 4),
    (Action::Reorder, 4),
    (Action::LostReply, 2),
];

/// The profiles the regular sweep rotates through. `beyond-tcp` is opt-in only.
pub(crate) const SWEPT: [&str; 4] = ["mixed", "process", "network", "pause"];

fn profile_actions(name: &str) -> Option<&'static [(Action, u64)]> {
    Some(match name {
        "mixed" => MIXED,
        "process" => PROCESS,
        "network" => NETWORK,
        "pause" => PAUSE,
        "beyond-tcp" => BEYOND_TCP,
        _ => return None,
    })
}

/// Workload tokens a seed-search manifest may list beside profiles and actions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Focus {
    /// `batch`: force `data.direct.batch` on.
    pub batch: bool,
    /// `same-range-writers`: concurrent writers on a few keys of one range.
    pub contention: bool,
    /// `idle`: no clients, only the nemesis and the final reads (diagnostics).
    pub idle: bool,
}

/// What `NABEDRA_SIM_PROFILE` selected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Selection {
    /// Explicit profile names, or empty for the round robin over [`SWEPT`].
    pub profiles: Vec<&'static str>,
    /// Restrict the profile to these actions (from action names in the list).
    pub actions: BTreeSet<Action>,
    pub focus: Focus,
}

impl Selection {
    /// Parse a comma list of profile names, action names and focus tokens.
    pub(crate) fn parse(list: &[String]) -> Result<Self, String> {
        let mut selection = Selection {
            profiles: Vec::new(),
            actions: BTreeSet::new(),
            focus: Focus::default(),
        };
        for item in list {
            let item = item.trim();
            if let Some(name) = ["mixed", "process", "network", "pause", "beyond-tcp"]
                .into_iter()
                .find(|name| *name == item)
            {
                selection.profiles.push(name);
            } else if let Some(actions) = Action::parse(item) {
                selection.actions.extend(actions);
            } else {
                match item {
                    "batch" => selection.focus.batch = true,
                    "same-range-writers" => selection.focus.contention = true,
                    "idle" => selection.focus.idle = true,
                    // The cold and move workloads always run these schedules.
                    "staggered-reopen" | "delay-old-voters" => {}
                    "none" => {}
                    _ => return Err(format!("unknown NABEDRA_SIM_PROFILE entry {item:?}")),
                }
            }
        }
        let beyond = selection.profiles.contains(&"beyond-tcp")
            || selection.actions.iter().any(|action| action.beyond_tcp());
        if beyond && std::env::var("NABEDRA_SIM_BEYOND_TCP").as_deref() != Ok("1") {
            return Err(
                "beyond-TCP network faults are opt-in: set NABEDRA_SIM_BEYOND_TCP=1 as well".into(),
            );
        }
        Ok(selection)
    }

    /// Whether the selection disables the nemesis.
    pub(crate) fn quiet(list: &[String]) -> bool {
        list.iter().any(|item| item.trim() == "none")
    }

    /// The profile of `seed`: an explicit profile, the round robin, and then the action
    /// filter. With only actions listed, the actions keep their `mixed` weights, or a
    /// weight of one when `mixed` lacks them.
    pub(crate) fn profile(&self, seed: u64) -> Profile<Action> {
        let names: Vec<&'static str> = if self.profiles.is_empty() {
            SWEPT.to_vec()
        } else {
            self.profiles.clone()
        };
        let name = names[(seed % names.len() as u64) as usize];
        let base = profile_actions(name).expect("known profile");
        let mut profile = Profile::new(name, (400, 2_500));
        if self.actions.is_empty() {
            for &(action, weight) in base {
                profile = profile.with(action, weight);
            }
            return profile;
        }
        let mut profile = Profile::new("focus", (300, 2_000));
        for &action in &self.actions {
            let weight = MIXED
                .iter()
                .chain(BEYOND_TCP)
                .chain(base)
                .find(|(candidate, _)| *candidate == action)
                .map_or(1, |(_, weight)| *weight);
            profile = profile.with(action, weight);
        }
        profile
    }
}

/// A follow-up the nemesis applies later, or at the heal at the latest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FollowUp {
    Thaw(Vec<u64>),
    HealLinks,
    /// Restart a stopped worker; `true` after a power cut.
    RestartWorker(u64, bool),
    RestartGateway(u64),
    RestoreCertificate(u64),
    ClearNet(Action),
    ClearDisk(u64),
}

/// One scheduled nemesis step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Step {
    Act(Action),
    Follow(FollowUp),
}

/// Durations of a fault window, drawn from the tape.
pub(crate) fn window(g: &mut Gen, lo_ms: u64, hi_ms: u64) -> Duration {
    Duration::from_millis(lo_ms + g.below(hi_ms - lo_ms + 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selection_names_profiles_actions_and_focus() {
        let list = ["drain-leader", "crash-leader", "batch"].map(String::from);
        let selection = Selection::parse(&list).unwrap();
        assert!(selection.profiles.is_empty());
        assert_eq!(
            selection.actions,
            BTreeSet::from([Action::CrashLeader, Action::PowerLeader, Action::DrainLeader])
        );
        assert!(selection.focus.batch);
        let profile = selection.profile(7);
        assert_eq!(profile.name, "focus");
        assert_eq!(profile.actions().count(), 3);

        let swept = Selection::parse(&[]).unwrap();
        let names: Vec<_> = (0..4).map(|seed| swept.profile(seed).name).collect();
        assert_eq!(names, SWEPT);
        assert!(
            (0..8)
                .flat_map(|seed| swept
                    .profile(seed)
                    .actions()
                    .map(|(action, _)| *action)
                    .collect::<Vec<_>>())
                .all(|action| !action.beyond_tcp())
        );
        assert!(Selection::parse(&["nonsense".into()]).is_err());
        assert!(Selection::parse(&["frame-drop".into()]).is_err());
    }

    #[test]
    fn every_tcp_action_is_swept() {
        let swept: BTreeSet<Action> = SWEPT
            .into_iter()
            .flat_map(|name| profile_actions(name).unwrap().iter().map(|(a, _)| *a))
            .collect();
        for action in Action::ALL {
            assert_eq!(
                swept.contains(&action),
                !action.beyond_tcp(),
                "{action} sweep membership"
            );
        }
    }
}
