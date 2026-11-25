# Releasing Nabedra

Use this page when the user asks to release, publish, tag or ship a version, or asks whether a
release is due.

> **Status: proposed, not usable yet.** `nabedra-release.yml` and `scripts/release/` do not exist.
> Until [the release plan](../../../../plans/releases.md) is done, do not publish anything. If the
> user asks for a release, tell them which plan tasks are still open. Remove this note in the PR
> that completes REL7.

## Rules

1. **The user starts every release.** An agent may propose one, but never starts one on its own.
2. **Only the workflow publishes.** Nobody runs `cargo publish`, `twine upload`, `npm publish` or
   `docker push` from a laptop.
3. **The user approves the publish job.** It waits in the `release` environment. Agents use the
   user's GitHub token, so GitHub would accept an approval from them. An agent never approves a
   deployment, changes the environment, or calls the `pending_deployments` API.
4. **A published version is final.** Registries never accept the same version twice. Fix a bad
   release with a new version.
5. **Release only a commit on `main`**, named by its full SHA. Evidence counts only for that SHA.

## What a release contains

Everything shares one version and one tag, `vX.Y.Z`.

| Artifact | Where | Platforms |
| --- | --- | --- |
| `nabedra-db` binaries, one archive per engine, with `SHA256SUMS` and attestations | GitHub Release | Linux x86_64 and arm64 (glibc 2.36 or newer), macOS arm64 and x86_64 |
| Server crates: `nabedra-database` and the crates it needs | crates.io | Source |
| `nabedra` for Python | PyPI | One pure Python wheel and an sdist |
| `nabedra` for TypeScript | npm | One package |
| Images `nabedra-olap`, `nabedra-oltp`, `nabedra-cache`, `nabedra-events` | `ghcr.io/zpy52` | `linux/amd64`, `linux/arm64` |

- The server has no native Windows build because it uses Unix-only APIs. Windows users run the
  image or WSL.
- Events ships only as an image.
- The two ORMs need no build per OS.

The GitHub Release is the changelog. The two tags made before this process,
`nabedra-database-v0.1.0` and `nabedra-database-v0.1.1`, stay as they are.

## Versions

Every part carries the same version, so an ORM fix also releases the server. This extends the
[ORM parity rule](../../../../docs/testing/orm-parity.md).

| Change since the last release | Before 1.0 | From 1.0 |
| --- | --- | --- |
| Breaks an API, the CLI, configuration, or the on-disk or peer wire format | Minor | Major |
| Adds a feature | Minor | Minor |
| Only fixes | Patch | Patch |

- A patch release never changes an on-disk or peer wire format. It must interoperate with every
  release of the same minor, in both directions.
- A release that changes a format ships an upgrade path tested from the last release, and its
  notes say whether downgrading works.
- The CLIs accept a `nabedra-db` with the same major and minor version.

There are two channels:

| Channel | Version | PyPI spelling | npm tag | Image tags | Release gate |
| --- | --- | --- | --- | --- | --- |
| Candidate | `0.3.0-rc.1` | `0.3.0rc1` | `next` | Exact version | May be red. The notes list every cell that is not green. |
| Stable | `0.3.0` | `0.3.0` | `latest` | Exact version, `0.3`, `latest` | Must pass |

By default, pip and Cargo skip candidates and npm installs `latest`. The GitHub Release for a
candidate is marked as a prerelease.

## When to release

Propose a release when one of these holds:

- **Now, as a patch:** a fix on `main` for data loss, a wrong result, a security problem or a
  crash that the published version also has.
- **As a candidate:** a user-visible feature is complete on `main`, or two weeks of user-visible
  changes have built up.
- **As stable:** the latest candidate has been out for a week with no new release blocker, and
  the gate passes on the SHA to be released.

Changes only to tests, docs, specs or tooling do not need a release. Anything pending on `main`
that should not ship blocks the release. There are no release branches before 1.0.

## What a change passes before it ships

```text
design review (TLA+) -> implementation -> code review with the simulator -> release gate
```

1. New features and revisions of existing ones start with a
   [design review](../../design-review/SKILL.md) where its rules require one.
2. [Code review](../../code-review/SKILL.md) checks the code against that design, with
   [simulation](../../code-review/references/simulation.md) where the module has it.
3. The release gate reads the evidence from both on the release SHA.

The simulator is unfinished, so a candidate does not wait for it. Its missing cells are red and
the notes list them. A stable release needs those cells green. Do not add simulation to a module
just to make a release pass.

## Steps

### 1. Propose

List the user-visible changes since the last release:

```sh
git fetch origin main --tags
LAST=$(git describe --tags --abbrev=0 --match 'v*' origin/main 2>/dev/null || echo nabedra-database-v0.1.1)
git log --first-parent --format='%h %s' "$LAST..origin/main" -- server orm Dockerfile
```

Send the user the proposed version, the channel, the changes in it, every open incident in
`docs/testing/bugs/`, and the quarantined tests. Wait for their yes.

### 2. Merge the version PR

On a `nabedra/release-<version>` branch:

```sh
python3 scripts/release/version.py set 0.3.0-rc.1   # every manifest, lock file and constant
python3 scripts/release/version.py check
```

Write `docs/releases/v<version>.md` with these sections: highlights, breaking changes, upgrade
steps, fixes, and known problems. Title the PR `[release] prepare <version>`. It changes only
versions and notes. [Review](../../code-review/SKILL.md) and merge it like any non-docs change.
Its merge commit on `main` is the release SHA. If anything else must go in, the SHA changes, and
steps 3 and 4 start again.

### 3. Verify the release SHA

Run T3 on that SHA with
[the usual dispatch](../../code-review/references/gh-actions.md#dispatch-pinned-to-your-commit).
Dispatch the base and opt-in jobs, and every registered check, not just the ones tagged T3.
Ingest each run with `nabedra-evidence.yml`. Then read the gate from current `main`, where the
evidence is:

```sh
git switch --detach origin/main
python3 scripts/testing/release_gate.py --sha "$SHA" --notes gate.md
```

If it fails, report the cells by name. Stop unless this is a candidate. Never use
`--allow-unattested` for a release, and never report a cell as green because it is deferred or
quarantined.

### 4. Rehearse

```sh
RID="release-0.3.0-rc.1-$(date +%H%M%S)"
gh workflow run nabedra-release.yml --ref main -f sha="$SHA" -f version=0.3.0-rc.1 \
  -f request_id="$RID" -f publish=false
```

A rehearsal builds, smoke-tests and packs every artifact, then stops before it publishes. Read
the run as [GitHub Actions for checks](../../code-review/references/gh-actions.md#read-the-results)
describes. Every job must succeed.

### 5. Publish

Dispatch again with `-f publish=true` and a new request ID. Send the user the run URL, the gate
result and the artifact list, and wait for them to approve the `release` environment. The
workflow then does this, in order:

1. Creates a draft GitHub Release with the archives, checksums and attested evidence.
2. Publishes to crates.io, PyPI and npm, then pushes the images under their exact version.
3. Installs each artifact from its registry on a clean runner and runs a smoke test. For crates,
   it runs `cargo install --locked`.
4. Attaches `release-record.json`: the SHA, run URL, gate result, and the digest of each artifact.
5. Moves the `latest` and `X.Y` image tags if the release is stable, and publishes the GitHub
   Release, which creates the tag. The release is immutable from here.

### 6. Report

Give the user the release URL and the registry links, and say what was not verified. If any
install command in the docs names a version, the version PR already updated it.

## If publishing fails partway

A multi-crate publish is not atomic, and neither is a release across four registries. Rerun the
failed jobs of the same run with `gh run rerun "$RUN" --failed`. Each step first checks whether
the registry already has the version. It skips an identical artifact and fails on a different
one. The user approves again if GitHub asks.

If the cause needs a code change, the version is spent. Delete the draft release, yank what was
published, and release the next patch or candidate number.

## Withdrawing a release

Get the user's decision first. Then have the user run these, because they need owner credentials:

| Where | Action |
| --- | --- |
| crates.io | `cargo yank --version <v> <crate>` for each crate |
| PyPI | Yank the release in the project settings, with the reason |
| npm | `npm deprecate nabedra@<v> "<reason>"`, and move `latest` back with `npm dist-tag add` |
| GHCR | Point `latest` and `X.Y` at the last good digest. Keep the exact version tag. |
| GitHub | Add a warning to the top of the release notes. Keep the tag. |

Record the bug in `docs/testing/bugs/` and release the fix as a patch.
