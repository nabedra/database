//! Evidence records, schema `nabedra.evidence/1` (`docs/testing/evidence/schema.json`; field
//! reference in `server/check/README.md`).
//!
//! One JSON file per run and check, at `<records>/<yyyy>/<mm>/<request_id>--<check>.json`. Rust
//! tests build a [`Record`], fill in what they know, and write it with [`Record::write_to`], which
//! refuses a record the schema would reject. The Python runners write the same format through
//! their own stdlib helper.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

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
#[serde(deny_unknown_fields)]
pub struct Build {
    /// Cargo profile, such as `dev`, `release` or `ci`.
    #[serde(deserialize_with = "Deserialize::deserialize")]
    pub profile: Option<String>,
    pub features: Vec<String>,
    /// SHA-256 of the binary under test, lowercase hex, when one exists.
    #[serde(deserialize_with = "Deserialize::deserialize")]
    pub binary_sha256: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Run {
    /// Actions run URL (`https://...`); `None` for a local run.
    #[serde(deserialize_with = "Deserialize::deserialize")]
    pub url: Option<String>,
    /// Actions run attempt, from 1.
    #[serde(deserialize_with = "Deserialize::deserialize")]
    pub attempt: Option<u64>,
    /// The dispatch request ID, or `local`.
    pub request_id: String,
    #[serde(deserialize_with = "Deserialize::deserialize")]
    pub runner: Option<String>,
    /// When the run started, RFC 3339 UTC, such as `2026-11-22T17:05:09Z`.
    pub date: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotRun {
    pub id: String,
    pub reason: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Assertions {
    pub executed: u64,
    pub failed: u64,
    pub not_run: Vec<NotRun>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Seeds {
    /// Inclusive `[lo, hi]` of a contiguous seed range, when the run used one.
    #[serde(deserialize_with = "Deserialize::deserialize")]
    pub range: Option<[u64; 2]>,
    pub count: u64,
    pub failing: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub name: String,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub schema: String,
    pub check: String,
    /// One or more check-kind letters (`M U S H F D R P K`) separated by single spaces.
    pub kind: String,
    pub guarantees: Vec<String>,
    pub scenarios: Vec<String>,
    /// Full 40-character lowercase commit SHA.
    pub sha: String,
    pub tree_clean: bool,
    pub build: Build,
    /// Run mode and sampled configuration, such as `{"direct_oltp": true}` from `config::sample`.
    /// Values are scalars. `None` when the check has no mode.
    #[serde(deserialize_with = "Deserialize::deserialize")]
    pub mode: Option<BTreeMap<String, Value>>,
    pub run: Run,
    pub duration_s: f64,
    pub result: Outcome,
    pub assertions: Assertions,
    /// `None` for checks that use no seeds.
    #[serde(deserialize_with = "Deserialize::deserialize")]
    pub seeds: Option<Seeds>,
    #[serde(deserialize_with = "Deserialize::deserialize")]
    pub sim_core_hours: Option<f64>,
    #[serde(deserialize_with = "Deserialize::deserialize")]
    pub virtual_hours: Option<f64>,
    pub artifacts: Vec<Artifact>,
    /// Optional in the schema: S3 requests the check made, summed for the monthly budget.
    /// Omitted when `None`, so records without S3 keep the schema's required field set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3_requests: Option<u64>,
}

impl Record {
    /// An empty record for `check` of kind `kind`, dated now, with request ID `local` and result
    /// `error` until the runner sets one. `sha` is empty until set or filled by
    /// [`Record::fill_from_env`]; [`Record::validate`] rejects it until then.
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
            mode: None,
            run: Run {
                url: None,
                attempt: None,
                request_id: "local".into(),
                runner: None,
                date: rfc3339_utc(written_at()),
            },
            duration_s: 0.0,
            result: Outcome::Error,
            assertions: Assertions::default(),
            seeds: None,
            sim_core_hours: None,
            virtual_hours: None,
            artifacts: Vec::new(),
            s3_requests: None,
        }
    }

    /// Fills `sha` from `TESTED_SHA`, the checkout's `HEAD`, or `GITHUB_SHA`, in that order.
    /// Reads tree cleanliness from Git, run metadata from GitHub Actions, and the request ID
    /// from `NABEDRA_REQUEST_ID`. Unavailable values leave fields as they are.
    pub fn fill_from_env(&mut self) -> &mut Self {
        let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        if let Some(sha) = var("TESTED_SHA")
            .or_else(|| git(&["rev-parse", "HEAD"]))
            .or_else(|| var("GITHUB_SHA"))
        {
            self.sha = sha;
        }
        if let Some(status) = git(&["status", "--porcelain"]) {
            self.tree_clean = status.is_empty();
        }
        if let (Some(server), Some(repo), Some(id)) = (
            var("GITHUB_SERVER_URL"),
            var("GITHUB_REPOSITORY"),
            var("GITHUB_RUN_ID"),
        ) {
            self.run.url = Some(format!("{server}/{repo}/actions/runs/{id}"));
        }
        if let Some(a) = var("GITHUB_RUN_ATTEMPT").and_then(|a| a.parse().ok()) {
            self.run.attempt = Some(a);
        }
        if let Some(r) = var("RUNNER_NAME") {
            self.run.runner = Some(r);
        }
        if let Some(id) = var("NABEDRA_REQUEST_ID") {
            self.run.request_id = id;
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
    pub fn not_run(&mut self, id: &str, reason: &str) -> &mut Self {
        self.assertions.not_run.push(NotRun {
            id: id.into(),
            reason: reason.into(),
        });
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

    /// Checks the evidence schema and the matrix's assertion rules. `Err` names the first violation.
    pub fn validate(&self) -> Result<(), String> {
        let err =
            |what: &str, v: &str| Err(format!("evidence {what} {v:?} does not match the schema"));
        if self.schema != SCHEMA {
            return err("schema", &self.schema);
        }
        if !is_kebab(&self.check) || self.check.len() > 80 {
            return err("check", &self.check);
        }
        let mut kinds = BTreeSet::new();
        if !self
            .kind
            .split(' ')
            .all(|k| k.len() == 1 && "MUSHFDRPK".contains(k) && kinds.insert(k))
        {
            return err("kind", &self.kind);
        }
        if let Some(g) = self.guarantees.iter().find(|g| !is_guarantee(g)) {
            return err("guarantee", g);
        }
        if let Some(s) = self.scenarios.iter().find(|s| !is_id(s, 40, true)) {
            return err("scenario", s);
        }
        if !is_hex(&self.sha, 40) {
            return err("sha", &self.sha);
        }
        if let Some(h) = self
            .build
            .binary_sha256
            .as_deref()
            .filter(|h| !is_hex(h, 64))
        {
            return err("build.binary_sha256", h);
        }
        if self
            .build
            .profile
            .as_deref()
            .is_some_and(|p| p.chars().count() > 40)
        {
            return err("build.profile", self.build.profile.as_deref().unwrap_or(""));
        }
        if let Some(f) = self.build.features.iter().find(|f| f.chars().count() > 80) {
            return err("build feature", f);
        }
        if let Some((k, v)) = self
            .mode
            .iter()
            .flatten()
            .find(|(_, v)| v.is_null() || v.is_array() || v.is_object())
        {
            return err("mode value", &format!("{k}={v}"));
        }
        if let Some(u) = self
            .run
            .url
            .as_deref()
            .filter(|u| !u.starts_with("https://") || u.chars().count() > 300)
        {
            return err("run.url", u);
        }
        if self.run.attempt == Some(0) {
            return err("run.attempt", "0");
        }
        if !is_id(&self.run.request_id, 100, true) {
            return err("run.request_id", &self.run.request_id);
        }
        if self
            .run
            .runner
            .as_deref()
            .is_some_and(|r| r.chars().count() > 100)
        {
            return err("run.runner", self.run.runner.as_deref().unwrap_or(""));
        }
        if year_month(&self.run.date).is_none() {
            return Err(format!(
                "evidence run.date {:?} must be RFC 3339 UTC",
                self.run.date
            ));
        }
        let nonneg = |x: f64| x.is_finite() && x >= 0.0;
        if !nonneg(self.duration_s) {
            return err("duration_s", &self.duration_s.to_string());
        }
        for (name, h) in [
            ("sim_core_hours", self.sim_core_hours),
            ("virtual_hours", self.virtual_hours),
        ] {
            if let Some(h) = h.filter(|h| !nonneg(*h)) {
                return err(name, &h.to_string());
            }
        }
        let a = &self.assertions;
        if a.failed > a.executed {
            return Err("evidence assertions: failed exceeds executed".into());
        }
        match self.result {
            Outcome::Pass if a.executed == 0 => {
                return Err("evidence result: pass with zero executed assertions".into());
            }
            Outcome::Pass if a.failed > 0 => {
                return Err("evidence result: pass with failed assertions".into());
            }
            Outcome::NotRun if a.not_run.is_empty() => {
                return Err(
                    "evidence assertions.not_run: a not-run record needs at least one reason"
                        .into(),
                );
            }
            Outcome::Quarantined if a.executed == 0 => {
                return Err("evidence result: quarantined with zero executed runs".into());
            }
            _ => {}
        }
        if a.not_run.len() > 200 {
            return err("assertions.not_run length", &a.not_run.len().to_string());
        }
        if let Some(n) = a
            .not_run
            .iter()
            .find(|n| n.id.chars().count() > 200 || n.reason.chars().count() > 300)
        {
            return err("assertions.not_run entry", &n.id);
        }
        if self.seeds.as_ref().is_some_and(|s| s.failing.len() > 100) {
            return err("seeds.failing length", "over 100");
        }
        if self.artifacts.len() > 50 {
            return err("artifacts length", &self.artifacts.len().to_string());
        }
        if let Some(x) = self
            .artifacts
            .iter()
            .find(|x| !is_hex(&x.sha256, 64) || x.name.chars().count() > 200)
        {
            return err("artifact", &x.name);
        }
        Ok(())
    }

    /// The path `write_to` uses under a records root:
    /// `<dir>/<yyyy>/<mm>/<request_id>--<check>.json`, with the month taken from `run.date`.
    pub fn path_in(&self, dir: &Path) -> Result<PathBuf, String> {
        let (y, m) = year_month(&self.run.date)
            .ok_or_else(|| format!("bad run.date {:?}", self.run.date))?;
        Ok(dir
            .join(y)
            .join(m)
            .join(format!("{}--{}.json", self.run.request_id, self.check)))
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("a Record always serializes")
    }

    pub fn from_json(s: &str) -> serde_json::Result<Self> {
        serde_json::from_str(s)
    }

    /// Validates the record and writes it as pretty JSON to [`Record::path_in`]`(dir)`, creating
    /// directories. Fails with `InvalidData` when the record breaks the schema or exceeds
    /// [`MAX_RECORD_BYTES`].
    pub fn write_to(&self, dir: &Path) -> std::io::Result<PathBuf> {
        let invalid = |m: String| std::io::Error::new(std::io::ErrorKind::InvalidData, m);
        self.validate().map_err(invalid)?;
        let mut json = self.to_json();
        json.push('\n');
        if json.len() > MAX_RECORD_BYTES {
            return Err(invalid(format!(
                "evidence record is {} bytes, over the {MAX_RECORD_BYTES}-byte cap",
                json.len()
            )));
        }
        let path = self.path_in(dir).map_err(invalid)?;
        std::fs::create_dir_all(path.parent().expect("path_in has a parent"))?;
        std::fs::write(&path, json)?;
        Ok(path)
    }
}

fn git(args: &[&str]) -> Option<String> {
    let out = std::process::Command::new("git").args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn is_hex(s: &str, len: usize) -> bool {
    s.len() == len
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// `^[a-z0-9]+(-[a-z0-9]+)*$`
fn is_kebab(s: &str) -> bool {
    !s.is_empty()
        && s.split('-').all(|p| {
            !p.is_empty()
                && p.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        })
}

/// `^[A-Za-z0-9][A-Za-z0-9_.-]*$` (with `_` allowed only when `underscore`), at most `max` bytes.
fn is_id(s: &str, max: usize, underscore: bool) -> bool {
    let ok =
        |b: u8| b.is_ascii_alphanumeric() || b == b'.' || b == b'-' || (underscore && b == b'_');
    s.len() <= max
        && s.bytes().next().is_some_and(|b| b.is_ascii_alphanumeric())
        && s.bytes().all(ok)
}

/// `^[A-Z]+(-[A-Z]+)*-G[0-9]{2}$`, such as `OLAP-STR-G01`.
fn is_guarantee(s: &str) -> bool {
    let Some((prefix, num)) = s.rsplit_once("-G") else {
        return false;
    };
    num.len() == 2
        && num.bytes().all(|b| b.is_ascii_digit())
        && !prefix.is_empty()
        && prefix
            .split('-')
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_uppercase()))
}

/// Extracts the calendar month from an RFC 3339 UTC timestamp.
fn year_month(date: &str) -> Option<(&str, &str)> {
    let [
        y1,
        y2,
        y3,
        y4,
        b'-',
        m1,
        m2,
        b'-',
        d1,
        d2,
        b'T' | b't',
        h1,
        h2,
        b':',
        min1,
        min2,
        b':',
        s1,
        s2,
        suffix @ ..,
    ] = date.as_bytes()
    else {
        return None;
    };
    let year = u16::from(two_digits(*y1, *y2)?) * 100 + u16::from(two_digits(*y3, *y4)?);
    let month = two_digits(*m1, *m2)?;
    let leap_year =
        year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let days = match month {
        2 => 28 + u8::from(leap_year),
        4 | 6 | 9 | 11 => 30,
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        _ => return None,
    };
    if year == 0
        || !(1..=days).contains(&two_digits(*d1, *d2)?)
        || two_digits(*h1, *h2)? > 23
        || two_digits(*min1, *min2)? > 59
        || two_digits(*s1, *s2)? > 59
    {
        return None;
    }
    let zone = if let [b'.', fraction @ ..] = suffix {
        let digits = fraction.iter().take_while(|b| b.is_ascii_digit()).count();
        if digits == 0 {
            return None;
        }
        &fraction[digits..]
    } else {
        suffix
    };
    if !matches!(zone, [b'Z' | b'z'] | b"+00:00") {
        return None;
    }
    Some((&date[..4], &date[5..7]))
}

fn two_digits(a: u8, b: u8) -> Option<u8> {
    (a.is_ascii_digit() && b.is_ascii_digit()).then(|| (a - b'0') * 10 + (b - b'0'))
}

/// RFC 3339 UTC with second precision. Civil-date conversion after Howard Hinnant's
/// `civil_from_days`.
pub fn rfc3339_utc(t: SystemTime) -> String {
    let secs = t
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0) as i64;
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// The real time an evidence record is written at. Evidence is dated in wall time, never on
/// a simulated clock, so this is the one direct clock read of the crate (clippy.toml:
/// nabedra-check's records carry real dates).
#[allow(clippy::disallowed_methods)]
fn written_at() -> SystemTime {
    SystemTime::now()
}
