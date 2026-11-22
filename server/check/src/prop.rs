//! Property runner with choice-tape shrinking. (Skeleton: shrinking lands next.)

use crate::r#gen::Gen;

#[derive(Clone, Debug)]
pub struct Config {
    pub cases: u64,
    pub seed: Option<u64>,
    pub max_shrink_steps: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self { cases: 256, seed: None, max_shrink_steps: 5000 }
    }
}

pub fn check(name: &str, f: impl Fn(&mut Gen) -> Result<(), String>) {
    check_with(Config::default(), name, f)
}

pub fn check_with(cfg: Config, name: &str, f: impl Fn(&mut Gen) -> Result<(), String>) {
    let base = cfg.seed.unwrap_or(0x6e61_6265_6472_6121);
    for case in 0..cfg.cases {
        let seed = base.wrapping_add(case);
        if let Err(msg) = f(&mut Gen::from_seed(seed)) {
            panic!("property `{name}` failed\n  seed: {seed}\n  case: {case}\n  failure: {msg}\n  replay: NABEDRA_SEED={seed} NABEDRA_CASES=1");
        }
    }
}
