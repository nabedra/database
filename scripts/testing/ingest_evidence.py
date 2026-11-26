#!/usr/bin/env python3
"""Import evidence from a completed nabedra build run and rebuild the coverage matrix."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tempfile
import zipfile

import evidence
import matrix


DEFAULT_REPO = evidence.GITHUB_REPOSITORY
BUILD_WORKFLOW = ".github/workflows/nabedra-build.yml"
SHA = re.compile(r"[0-9a-f]{40}")


def gh(*args, stdout=subprocess.PIPE, timeout=120):
    return subprocess.run(
        ["gh", *args], check=True, stdout=stdout, stderr=subprocess.PIPE, timeout=timeout,
    ).stdout


def gh_error(exc):
    return exc.stderr.decode(errors="replace").strip() or str(exc)


def get_run(repo, run_id):
    run = json.loads(gh("api", f"repos/{repo}/actions/runs/{run_id}"))
    if (str(run["id"]) != run_id or run["path"] != BUILD_WORKFLOW
            or (run.get("head_repository") or {}).get("full_name", "").lower() != repo.lower()
            or run["event"] not in {"workflow_dispatch", "schedule"}
            or run["status"] != "completed"):
        raise ValueError("expected a completed nabedra build dispatch or schedule from this repository")
    if not SHA.fullmatch(run["head_sha"]):
        raise ValueError("invalid workflow revision in run metadata")
    if run["event"] == "schedule":
        return run, run["head_sha"], None
    # GitHub's head_sha describes the workflow checkout. The dispatch input sha can differ.
    dispatch = re.fullmatch(r"([A-Za-z0-9._-]{1,80}) @ ([0-9a-f]{40})", run["display_title"])
    if dispatch is None:
        raise ValueError("dispatch run name does not identify the tested revision")
    request_id, tested_sha = dispatch.groups()
    return run, tested_sha, request_id


def get_artifacts(repo, run_id):
    pages = json.loads(gh(
        "api", "--paginate", "--slurp", f"repos/{repo}/actions/runs/{run_id}/artifacts?per_page=100",
    ))
    return [artifact for page in pages for artifact in page["artifacts"]
            if artifact["name"] == "evidence" or artifact["name"].startswith("evidence-")]


def verify_attestation(data, repo, run, info, signer_repo):
    certificate = ".[].verificationResult.signature.certificate"
    with tempfile.TemporaryDirectory() as tmp:
        subject = Path(tmp) / "record.json"
        subject.write_bytes(data)
        try:
            bundle_arg = []
            if signer_repo != repo:
                # Download from the repository's current location and verify against the
                # identity in the historical signing certificate.
                bundle = Path(tmp) / "attestations.jsonl"
                digest = hashlib.sha256(data).hexdigest()
                bundle.write_bytes(gh(
                    "api", "--paginate", f"repos/{repo}/attestations/sha256:{digest}",
                    "--jq", ".attestations[].bundle | @json",
                ))
                bundle_arg = ["--bundle", str(bundle)]
                certificate += f' | select(.sourceRepositoryIdentifier == "{evidence.GITHUB_REPOSITORY_ID}")'
            invocations = json.loads(gh(
                "attestation", "verify", str(subject), "--repo", signer_repo, *bundle_arg,
                "--signer-workflow", f"{signer_repo}/{BUILD_WORKFLOW}", "--source-digest", run["head_sha"],
                "--format", "json",
                "--jq", f"[{certificate} | .runInvocationURI]",
            ))
        except subprocess.CalledProcessError as exc:
            raise ValueError(f"attestation verification failed: {gh_error(exc)}") from exc
        except subprocess.TimeoutExpired as exc:
            raise ValueError("attestation verification timed out") from exc
    if f"{info['url']}/attempts/{info['attempt']}" not in invocations:
        raise ValueError("attestation belongs to a different run or attempt")


def import_record(data, schema, dest, repo, run, tested_sha, request_id, verify):
    if len(data) > evidence.MAX_RECORD_BYTES:
        raise ValueError(f"record exceeds {evidence.MAX_RECORD_BYTES} bytes")
    record = json.loads(data.decode("utf-8"))
    errors = evidence.validate(record, schema)
    if errors:
        raise ValueError(f"invalid record: {errors[0]}")
    if record["sha"] != tested_sha:
        raise ValueError(f"SHA mismatch: record {record['sha']}, run tested {tested_sha}")
    info = record["run"]
    signer_repo = repo
    if (repo.lower() == DEFAULT_REPO
            and info["url"] == f"https://github.com/{evidence.PRE_TRANSFER_REPOSITORY}/actions/runs/{run['id']}"):
        # GitHub now reports the canonical URL even for runs whose signed bytes use the old URL.
        signer_repo = evidence.PRE_TRANSFER_REPOSITORY
    elif info["url"] != run["html_url"]:
        raise ValueError("record belongs to a different run")
    if request_id and info["request_id"] != request_id:
        raise ValueError("record belongs to a different run")
    if info["attempt"] is None or info["attempt"] > run["run_attempt"]:
        raise ValueError("record has an invalid run attempt")
    date = datetime.fromisoformat(info["date"].upper()).astimezone(timezone.utc)
    path = evidence.record_path(record, dest, now=date)
    if verify:
        verify_attestation(data, repo, run, info, signer_repo)
    else:
        # Readers of trusted records only scan *.json. Keep the original bytes for later verification.
        path = path.with_suffix(".json.unattested")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent) as pending:
        pending.write(data)
        pending.flush()
        try:
            os.link(pending.name, path)
        except FileExistsError:
            if path.is_symlink() or path.stat().st_size != len(data) or path.read_bytes() != data:
                raise ValueError(f"different record already exists at {path.relative_to(dest)}") from None
            return False
    return True


def ingest(repo, run_id, dest, verify):
    run, tested_sha, request_id = get_run(repo, run_id)
    artifacts = get_artifacts(repo, run_id)
    schema = evidence.load_schema()
    accepted = rejected = duplicates = 0

    def reject(label, reason):
        nonlocal rejected
        rejected += 1
        print(f"Rejected {label!r}: {str(reason)!r}")

    for artifact in artifacts:
        name = artifact["name"]
        if artifact["expired"]:
            reject(name, "artifact expired")
            continue
        try:
            with tempfile.TemporaryFile() as archive:
                gh("api", f"repos/{repo}/actions/artifacts/{artifact['id']}/zip", stdout=archive, timeout=300)
                archive.seek(0)
                with zipfile.ZipFile(archive) as bundle:
                    for member in bundle.infolist():
                        parts = PurePosixPath(member.filename).parts
                        if member.is_dir() or not member.filename.endswith(".json") or parts[:1] == ("target",):
                            continue
                        try:
                            # The archive is never extracted. Only record bytes enter the checkout.
                            with bundle.open(member) as source:
                                data = source.read(evidence.MAX_RECORD_BYTES + 1)
                            if import_record(data, schema, dest, repo, run, tested_sha, request_id, verify):
                                accepted += 1
                            else:
                                duplicates += 1
                        except (ValueError, OverflowError, RecursionError, zipfile.BadZipFile) as exc:
                            reject(f"{name}/{member.filename}", exc)
        except zipfile.BadZipFile as exc:
            reject(name, exc)

    status = "attested" if verify else "unattested"
    print(f"Accepted: {accepted} ({status}), rejected: {rejected}, duplicates: {duplicates}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True, help="GitHub Actions run ID")
    parser.add_argument("--repo", default=DEFAULT_REPO, help="GitHub repository as OWNER/REPO")
    parser.add_argument("--dest", type=Path, default=evidence.DEFAULT_RECORDS_DIR)
    parser.add_argument("--no-verify", action="store_true", help="save records with an .unattested suffix")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[1-9][0-9]*", args.run):
        parser.error("--run must be a positive numeric run ID")
    if not re.fullmatch(r"[A-Za-z0-9-]+/[A-Za-z0-9_.-]+", args.repo):
        parser.error("--repo must be OWNER/REPO")

    dest = args.dest.resolve()
    try:
        ingest(args.repo, args.run, dest, not args.no_verify)
    except subprocess.CalledProcessError as exc:
        print(f"ingest_evidence: {gh_error(exc)}", file=sys.stderr)
        return 1
    except (ValueError, KeyError, OSError, subprocess.TimeoutExpired) as exc:
        print(f"ingest_evidence: {exc}", file=sys.stderr)
        return 1

    matrix_args = ["build", "--evidence-dir", str(dest)]
    if dest != evidence.DEFAULT_RECORDS_DIR:
        matrix_args.extend(["--out-dir", str(dest.parent / f"{dest.name}-matrix")])
    if not args.no_verify:
        matrix_args.append("--verify-attestations")
    return matrix.main(matrix_args)


if __name__ == "__main__":
    sys.exit(main())
