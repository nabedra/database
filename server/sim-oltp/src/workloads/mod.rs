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
pub(crate) mod resources;

use std::{collections::BTreeSet, fmt};

/// One selectable workload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Workload {
    Smoke,
    Direct,
    Cold,
    Move,
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
        }
    }

    fn parse(name: &str) -> Option<Self> {
        [
            Workload::Smoke,
            Workload::Direct,
            Workload::Cold,
            Workload::Move,
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

/// The name that puts the [`resources`] overlay on the chosen workloads.
const OVERLAY: &str = "resources";

/// `NABEDRA_SIM_WORKLOAD`: one workload, a comma list, or `mixed` for [`ROTATION`]. With
/// `resources` in the list the overlay runs on the others, or on [`ROTATION`] if there are none.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WorkloadChoice {
    workloads: Vec<Workload>,
    overlay: bool,
}

impl WorkloadChoice {
    pub(crate) fn parse(value: &str) -> Result<Self, String> {
        let mut chosen = Vec::new();
        let mut overlay = false;
        for name in value
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
        {
            if name == "mixed" {
                chosen.extend(ROTATION);
            } else if name == OVERLAY {
                overlay = true;
            } else {
                chosen.push(
                    Workload::parse(name)
                        .ok_or_else(|| format!("OLTP workload {name:?} is unavailable"))?,
                );
            }
        }
        if chosen.is_empty() && overlay {
            chosen.extend(ROTATION);
        }
        if chosen.is_empty() {
            return Err("NABEDRA_SIM_WORKLOAD names no workload".into());
        }
        if chosen.contains(&Workload::Smoke) && (chosen.len() > 1 || overlay) {
            return Err("the smoke workload runs alone".into());
        }
        Ok(Self {
            workloads: chosen,
            overlay,
        })
    }

    /// Whether the `resources` overlay runs.
    pub(crate) fn overlay(&self) -> bool {
        self.overlay
    }

    /// The workload of `seed`: seeds rotate through the list.
    pub(crate) fn of(&self, seed: u64) -> Workload {
        self.workloads[(seed % self.workloads.len() as u64) as usize]
    }

    pub(crate) fn is_smoke(&self) -> bool {
        self.workloads == [Workload::Smoke]
    }

    pub(crate) fn label(&self) -> String {
        let mut names: Vec<_> = if self.workloads == ROTATION {
            vec!["mixed"]
        } else {
            self.workloads.iter().map(|chosen| chosen.name()).collect()
        };
        if self.overlay {
            names.push(OVERLAY);
        }
        names.join(",")
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
        assert!(!mixed.overlay());
        let overlaid = WorkloadChoice::parse("resources").unwrap();
        assert!(overlaid.overlay());
        assert_eq!(overlaid.label(), "mixed,resources");
        assert_eq!(overlaid.of(4), mixed.of(4));
        let cold = WorkloadChoice::parse("cold,resources").unwrap();
        assert!(cold.overlay() && cold.of(5) == Workload::Cold);
        assert_eq!(WorkloadChoice::parse(&cold.label()).unwrap(), cold);
        assert!(WorkloadChoice::parse("smoke,resources").is_err());
    }
}
