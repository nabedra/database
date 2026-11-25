//! The commit a trace names, and whether the sources that made it are that commit's.
//!
//! The SHA is HEAD of the repository that tracks this crate, asked without any `GIT_*`
//! variable. `NABEDRA_TRACE_SHA` can only confirm it. `GITHUB_SHA` is not used: a pinned
//! dispatch tests another commit than the one its workflow came from. Without git there is
//! no SHA and no trace.
//!
//! The sources are unclean when `server/` or `specs/` has uncommitted or untracked files, or
//! when HEAD has moved since the executable was built and `server/` differs between the two
//! commits, or git does not know where HEAD was then.
//!
//! **Not found:**
//!
//! - changes that were there at the build and are gone now. `cargo test` rebuilds after
//!   `git checkout` or `git stash`, but not when a file comes back with its old modification
//!   time, as `cp -p` does, and an executable that is run directly is never rebuilt;
//! - an executable whose own modification time was changed, or that was built in an earlier
//!   checkout at the same path and is younger than this one's first reflog entry;
//! - files that git is told to overlook (assume-unchanged, skip-worktree, a global ignore),
//!   and changes outside `server/` and `specs/`.
//!
//! A checker cannot tell either: TLC accepts any SHA and any workload name. Evidence comes
//! from a run whose record names its own fresh checkout.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::OnceLock;
use std::time::UNIX_EPOCH;

/// The commit under test, as whoever started the run knows it. It must agree with git.
pub const SHA_VAR: &str = "NABEDRA_TRACE_SHA";

/// `mark` publishes a trace of unclean sources, with that in its workload name. Without it,
/// [`super::Builder::install`] refuses.
pub const UNCLEAN_VAR: &str = "NABEDRA_TRACE_UNCLEAN";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Commit {
    pub sha: String,
    /// Why the sources may not be that commit's.
    pub unclean: Option<String>,
}

impl Commit {
    /// Refuses unclean sources, unless the environment asks to mark them.
    pub fn strict(self, unclean: Option<&str>) -> Result<Self, String> {
        match &self.unclean {
            Some(why) if unclean != Some("mark") => Err(format!(
                "the sources are not those of {}: {why}. Set {UNCLEAN_VAR}=mark for a trace \
                 that says so",
                self.sha
            )),
            _ => Ok(self),
        }
    }

    /// The workload name, marked when the sources are unclean.
    pub fn workload(&self, name: &str) -> String {
        match self.unclean {
            Some(_) => format!("{name} [unclean sources]"),
            None => name.to_owned(),
        }
    }
}

/// The commit of this process, asked once.
pub(super) fn current() -> Result<Commit, String> {
    static COMMIT: OnceLock<Result<Commit, String>> = OnceLock::new();
    COMMIT
        .get_or_init(|| {
            let built = std::env::current_exe()
                .and_then(|exe| exe.metadata()?.modified())
                .ok()
                .and_then(|at| at.duration_since(UNIX_EPOCH).ok())
                .map(|at| at.as_secs());
            let given = std::env::var(SHA_VAR).ok();
            of(
                Path::new(env!("CARGO_MANIFEST_DIR")),
                given.as_deref(),
                built,
            )
        })
        .clone()
}

/// The commit of the sources in `dir`, for an executable built at Unix time `built`.
pub(super) fn of(dir: &Path, given: Option<&str>, built: Option<u64>) -> Result<Commit, String> {
    let root = PathBuf::from(text(dir, &["rev-parse", "--show-toplevel"])?);
    if !run(dir, &["ls-files", "--error-unmatch", "Cargo.toml"])?
        .status
        .success()
    {
        return Err(format!(
            "{} is not tracked by the repository at {}",
            dir.display(),
            root.display()
        ));
    }
    let sha = text(dir, &["rev-parse", "--verify", "HEAD^{commit}"])?;
    let hex = |byte: u8| matches!(byte, b'0'..=b'9' | b'a'..=b'f');
    if sha.len() != 40 || !sha.bytes().all(hex) || sha.bytes().all(|byte| byte == b'0') {
        return Err(format!("git says HEAD is {sha:?}, which is no commit SHA"));
    }
    if let Some(given) = given
        && given != sha
    {
        return Err(format!(
            "{SHA_VAR} is {given:?}, but the source tree is at {sha}"
        ));
    }
    let unclean = match changed(dir)? {
        Some(files) => Some(files),
        None => moved(dir, &sha, built),
    };
    Ok(Commit { sha, unclean })
}

fn changed(dir: &Path) -> Result<Option<String>, String> {
    let status = [
        "status",
        "--porcelain",
        "--untracked-files=normal",
        "--",
        ":/server",
        ":/specs",
    ];
    let files = text(dir, &status)?;
    let first = files.lines().next().unwrap_or_default().trim();
    Ok((!files.is_empty()).then(|| {
        format!(
            "{} uncommitted files, such as {first}",
            files.lines().count()
        )
    }))
}

/// Why the executable may be of an earlier HEAD.
fn moved(dir: &Path, sha: &str, built: Option<u64>) -> Option<String> {
    let Some(built) = built else {
        return Some("the build time of the executable is unknown".into());
    };
    let then = format!("HEAD@{{{built}}}");
    let unknown = "git has no record of where HEAD was when the executable was built";
    let Ok(found) = run(dir, &["rev-parse", "--verify", &then]) else {
        return Some(unknown.into());
    };
    // For a time before its first entry git answers with that entry, and warns.
    if !found.status.success() || !found.stderr.is_empty() {
        return Some(unknown.into());
    }
    let then = String::from_utf8_lossy(&found.stdout).trim_end().to_owned();
    let same = then == sha
        || run(dir, &["diff", "--quiet", &then, sha, "--", ":/server"])
            .is_ok_and(|out| out.status.success());
    (!same).then(|| format!("the executable was built at {then}"))
}

fn run(dir: &Path, args: &[&str]) -> Result<Output, String> {
    let mut command = Command::new("git");
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("GIT_") {
            command.env_remove(name);
        }
    }
    command
        .args(args)
        .current_dir(dir)
        .output()
        .map_err(|error| format!("no commit SHA: git does not run: {error}"))
}

fn text(dir: &Path, args: &[&str]) -> Result<String, String> {
    let out = run(dir, args)?;
    if !out.status.success() {
        return Err(format!(
            "no commit SHA: git {} in {}: {}",
            args[0],
            dir.display(),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim_end().to_owned())
}
