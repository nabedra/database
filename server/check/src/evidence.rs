//! Evidence records, schema `nabedra.evidence/1` (field reference: `server/check/README.md`).
//!
//! One JSON file per run and check. Rust tests build a [`Record`], fill in what they know, and
//! write it with [`Record::write_to`]. The Python runners write the same format through their own
//! stdlib helper, and `docs/testing/evidence/schema.json` validates both.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const SCHEMA: &str = "nabedra.evidence/1";
/// The plan caps a record at 20 KB; logs and histories belong in Actions artifacts instead.
pub const MAX_RECORD_BYTES: usize = 20 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Outcome {
    Pass,
    Fail,
    NotRun,
    Quarantined,
    /// The default, so a record whose runner never set a result can never read as a pass.
    #[default]
    Error,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Build {
    /// Cargo profile, such as `dev`, `release` or `ci`.
    pub profile: String,
    pub features: Vec<String>,
    /// SHA-256 of the binary under test, lowercase hex, when one exists.
    pub binary_sha256: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Run {
    /// Actions run URL; `None` for a local run.
    pub url: Option<String>,
    pub attempt: Option<u64>,
    pub request_id: Option<String>,
    pub runner: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotRun {
    pub name: String,
    pub reason: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Assertions {
    pub executed: u64,
    pub failed: u64,
    pub not_run: Vec<NotRun>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Seeds {
    /// Inclusive `[first, last]` of a contiguous seed range, when the run used one.
    pub range: Option<[u64; 2]>,
    pub count: u64,
    pub failing: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Artifact {
    pub path: String,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub schema: String,
    pub check: String,
    pub kind: String,
    pub guarantees: Vec<String>,
    pub scenarios: Vec<String>,
    /// Full 40-character commit SHA; empty when unknown.
    pub sha: String,
    pub tree_clean: bool,
    pub build: Build,
    /// Configuration the run used, such as `{"direct_oltp": true}`, often from `config::sample`.
    pub mode: BTreeMap<String, Value>,
    pub run: Run,
    pub duration_s: f64,
    pub result: Outcome,
    pub assertions: Assertions,
    pub seeds: Seeds,
    pub sim_core_hours: f64,
    pub virtual_hours: f64,
    pub artifacts: Vec<Artifact>,
}

impl Record {
    /// An empty record for `check` of check kind `kind` (one of `M U S H F D R P K`), with result
    /// `error` until the runner sets one.
    pub fn new(check: &str, kind: &str) -> Self {
        Self {
            schema: SCHEMA.into(),
            check: check.into(),
            kind: kind.into(),
            guarantees: Vec::new(),
            scenarios: Vec::new(),
            sha: String::new(),
            tree_clean: false,
            build: Build::default(),
            mode: BTreeMap::new(),
            run: Run::default(),
            duration_s: 0.0,
            result: Outcome::Error,
            assertions: Assertions::default(),
            seeds: Seeds::default(),
            sim_core_hours: 0.0,
            virtual_hours: 0.0,
            artifacts: Vec::new(),
        }
    }

    /// Fills `sha` and `run` from the GitHub Actions environment (`GITHUB_SHA`,
    /// `GITHUB_SERVER_URL`, `GITHUB_REPOSITORY`, `GITHUB_RUN_ID`, `GITHUB_RUN_ATTEMPT`,
    /// `RUNNER_NAME`) and `request_id` from `NABEDRA_REQUEST_ID`. Unset variables leave fields as
    /// they are.
    pub fn fill_from_env(&mut self) -> &mut Self {
        let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        if let Some(sha) = var("GITHUB_SHA") {
            self.sha = sha;
        }
        if let (Some(server), Some(repo), Some(id)) =
            (var("GITHUB_SERVER_URL"), var("GITHUB_REPOSITORY"), var("GITHUB_RUN_ID"))
        {
            self.run.url = Some(format!("{server}/{repo}/actions/runs/{id}"));
        }
        if let Some(a) = var("GITHUB_RUN_ATTEMPT").and_then(|a| a.parse().ok()) {
            self.run.attempt = Some(a);
        }
        if let Some(r) = var("RUNNER_NAME") {
            self.run.runner = Some(r);
        }
        if let Some(id) = var("NABEDRA_REQUEST_ID") {
            self.run.request_id = Some(id);
        }
        self
    }

    /// Counts one executed assertion.
    pub fn assertion(&mut self, passed: bool) -> &mut Self {
        self.assertions.executed += 1;
        if !passed {
            self.assertions.failed += 1;
        }
        self
    }

    /// Records an assertion that did not run, with its reason (such as `s3-unconfigured`).
    pub fn not_run(&mut self, name: &str, reason: &str) -> &mut Self {
        self.assertions.not_run.push(NotRun { name: name.into(), reason: reason.into() });
        self
    }

    /// Sets `result` from the assertion counts: `fail` if any failed, `pass` if any executed and
    /// none failed, otherwise `not-run`. A runner that saw an infrastructure error sets `error`
    /// itself instead.
    pub fn conclude(&mut self) -> Outcome {
        self.result = if self.assertions.failed > 0 {
            Outcome::Fail
        } else if self.assertions.executed > 0 {
            Outcome::Pass
        } else {
            Outcome::NotRun
        };
        self.result
    }

    /// The file name `write_to` uses: `<request_id or "local">--<check>.json`, with any character
    /// outside `[A-Za-z0-9._-]` replaced by `_`.
    pub fn file_name(&self) -> String {
        let clean = |s: &str| -> String {
            s.chars().map(|c| if c.is_ascii_alphanumeric() || "._-".contains(c) { c } else { '_' }).collect()
        };
        let id = self.run.request_id.as_deref().unwrap_or("local");
        format!("{}--{}.json", clean(id), clean(&self.check))
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("a Record always serializes")
    }

    pub fn from_json(s: &str) -> serde_json::Result<Self> {
        serde_json::from_str(s)
    }

    /// Writes the record as pretty JSON to `dir/<file_name>`, creating `dir`. Fails with
    /// `InvalidData` when the record exceeds [`MAX_RECORD_BYTES`].
    pub fn write_to(&self, dir: &Path) -> std::io::Result<PathBuf> {
        let mut json = self.to_json();
        json.push('\n');
        if json.len() > MAX_RECORD_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("evidence record is {} bytes, over the {MAX_RECORD_BYTES}-byte cap", json.len()),
            ));
        }
        std::fs::create_dir_all(dir)?;
        let path = dir.join(self.file_name());
        std::fs::write(&path, json)?;
        Ok(path)
    }
}
