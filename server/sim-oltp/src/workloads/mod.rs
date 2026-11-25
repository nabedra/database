//! The phase 10 workloads: `direct`, `cold` and `move` (design section 3.6).
//!
//! Clients run real public HTTP operations through both gateways while the nemesis
//! drives the profile the seed selects. Every client and nemesis choice comes from the
//! seed's workload tape; clients draw their whole program before the run starts, so the
//! nemesis is the only consumer of the tape while the cluster runs.

pub(crate) mod clauses;
pub(crate) mod clients;
pub(crate) mod cold;
pub(crate) mod corrupt;
pub(crate) mod direct;
pub(crate) mod moves;
pub(crate) mod old_peer;
pub(crate) mod paused;
pub(crate) mod queued;
mod queued_clauses;

use std::{collections::BTreeSet, fmt};

/// One selectable workload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Workload {
    Smoke,
    Direct,
    Cold,
    Move,
    Queued,
}

/// The workloads `mixed` rotates through, in seed order.
pub(crate) const ROTATION: [Workload; 3] = [Workload::Direct, Workload::Cold, Workload::Move];

impl Workload {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Workload::Smoke => "smoke",
            Workload::Direct => "direct",
            Workload::Cold => "cold",
            Workload::Move => "move",
            Workload::Queued => "queued",
        }
    }

    fn parse(name: &str) -> Option<Self> {
        [
            Workload::Smoke,
            Workload::Direct,
            Workload::Cold,
            Workload::Move,
            Workload::Queued,
        ]
        .into_iter()
        .find(|workload| workload.name() == name)
    }
}

impl fmt::Display for Workload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// `NABEDRA_SIM_WORKLOAD`: one workload, a comma list, or `mixed` for [`ROTATION`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WorkloadChoice(Vec<Workload>);

impl WorkloadChoice {
    pub(crate) fn parse(value: &str) -> Result<Self, String> {
        let mut chosen = Vec::new();
        for name in value
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
        {
            if name == "mixed" {
                chosen.extend(ROTATION);
            } else {
                chosen.push(
                    Workload::parse(name)
                        .ok_or_else(|| format!("OLTP workload {name:?} is unavailable"))?,
                );
            }
        }
        if chosen.is_empty() {
            return Err("NABEDRA_SIM_WORKLOAD names no workload".into());
        }
        if chosen.contains(&Workload::Smoke) && chosen.len() > 1 {
            return Err("the smoke workload runs alone".into());
        }
        Ok(Self(chosen))
    }

    /// The workload of `seed`: seeds rotate through the list.
    pub(crate) fn of(&self, seed: u64) -> Workload {
        self.0[(seed % self.0.len() as u64) as usize]
    }

    pub(crate) fn is_smoke(&self) -> bool {
        self.0 == [Workload::Smoke]
    }

    pub(crate) fn label(&self) -> String {
        if self.0 == ROTATION {
            return "mixed".into();
        }
        self.0
            .iter()
            .map(|workload| workload.name())
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// The scenario clauses whose assertions executed in a seed.
pub(crate) type Scenarios = BTreeSet<&'static str>;

pub(crate) use clauses::SCENARIOS;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workload_choice_rotates() {
        let mixed = WorkloadChoice::parse("mixed").unwrap();
        assert_eq!(mixed.label(), "mixed");
        let order: Vec<_> = (0..6).map(|seed| mixed.of(seed)).collect();
        assert_eq!(order[..3], ROTATION);
        assert_eq!(order[3..], ROTATION);
        assert!(WorkloadChoice::parse("smoke,direct").is_err());
        assert!(WorkloadChoice::parse("bogus").is_err());
        assert_eq!(WorkloadChoice::parse("cold").unwrap().of(5), Workload::Cold);
    }
}
