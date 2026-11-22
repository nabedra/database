import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest
import zipfile

import evidence
import ingest_evidence


REPO = ingest_evidence.DEFAULT_REPO
RUN_ID = "1234"
RUN_URL = f"https://github.com/{REPO}/actions/runs/{RUN_ID}"
RUN_ATTEMPT = f"{RUN_URL}/attempts/1"
REQUEST_ID = "fixture"
WORKFLOW_SHA = "a" * 40


GH_STUB = '''
import hashlib
import json
import os
from pathlib import Path
import sys

fixture = Path(os.environ["EVIDENCE_FIXTURE"])
args = sys.argv[1:]
if args[0] == "api":
    if args[-1].endswith("/artifacts?per_page=100"):
        assert args[1:3] == ["--paginate", "--slurp"]
        print((fixture / "artifacts.json").read_text())
    elif args[-1].endswith("/zip"):
        sys.stdout.buffer.write((fixture / (args[-1].split("/")[-2] + ".zip")).read_bytes())
    else:
        print((fixture / "run.json").read_text())
elif args[:2] == ["attestation", "verify"]:
    with (fixture / "verification.log").open("a") as log:
        print(args, file=log)
    raw = Path(args[2]).read_bytes()
    signature = json.loads((fixture / "signed.json").read_text()).get(hashlib.sha256(raw).hexdigest())
    if signature is None:
        sys.exit("no attestation for digest")
    if "--signer-workflow" in args:
        assert args[args.index("--signer-workflow") + 1].endswith("/.github/workflows/nabedra-build.yml")
        assert args[args.index("--source-digest") + 1] == json.loads((fixture / "run.json").read_text())["head_sha"]
        print(json.dumps([signature]))
elif args[:2] == ["auth", "setup-git"]:
    pass
elif args[:2] == ["pr", "list"]:
    if (fixture / "pr-body").exists():
        print("17")
elif args[:2] == ["pr", "create"]:
    (fixture / "pr-body").write_text(sys.stdin.read())
elif args[:2] == ["pr", "merge"]:
    (fixture / "merge.json").write_text(json.dumps(args))
else:
    sys.exit(f"unexpected gh command: {args}")
'''


def git(repo, *args, check=True):
    return subprocess.run(["git", *args], cwd=repo, text=True, capture_output=True, check=check)


class EvidenceFixture(unittest.TestCase):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.fixture = Path(scratch.name)
        stub = self.fixture / "gh"
        stub.write_text(f"#!{sys.executable}\n" + GH_STUB, encoding="utf-8")
        stub.chmod(0o755)
        self.env = {**os.environ, "PATH": f"{self.fixture}:{os.environ['PATH']}",
                    "EVIDENCE_FIXTURE": str(self.fixture)}


class IngestTests(EvidenceFixture):
    def setUp(self):
        super().setUp()
        self.dest = self.fixture / "records"
        self.matrix_dir = self.fixture / "records-matrix"
        self.tested_sha, _ = evidence.git_state()
        self.run = {
            "id": int(RUN_ID), "path": ingest_evidence.BUILD_WORKFLOW, "status": "completed",
            "event": "workflow_dispatch", "head_repository": {"full_name": REPO},
            "head_sha": WORKFLOW_SHA, "display_title": f"{REQUEST_ID} @ {self.tested_sha}",
            "html_url": RUN_URL, "run_attempt": 1,
        }
        (self.fixture / "run.json").write_text(json.dumps(self.run))
        (self.fixture / "artifacts.json").write_text(json.dumps([
            {"artifacts": [{"id": 1, "name": "quarantine-evidence", "expired": False}]},
            {"artifacts": [{"id": 2, "name": "evidence-fixture", "expired": False}]},
        ]))
        self.signed = {}

    def record(self, check, sha=None, **changes):
        record = evidence.new_record(
            check, ["U"], result="pass", executed=1, failed=0, sha=sha or self.tested_sha,
            tree_clean=True, run={"url": RUN_URL, "attempt": 1, "request_id": REQUEST_ID,
                                 "date": "2026-11-01T00:30:00+01:00", "runner": "fixture"},
        )
        record.update(changes)
        # Vary the formatting so round-tripping the JSON would invalidate its signature.
        return json.dumps(record).encode()

    def artifact(self, files, signatures):
        with zipfile.ZipFile(self.fixture / "2.zip", "w") as archive:
            for name, data in files.items():
                archive.writestr(name, data)
        self.signed.update({hashlib.sha256(data).hexdigest(): uri for data, uri in signatures})
        (self.fixture / "signed.json").write_text(json.dumps(self.signed))

    def ingest(self, *args, check=True):
        return subprocess.run(
            [sys.executable, ingest_evidence.__file__, "--run", RUN_ID, "--dest", str(self.dest), *args],
            env=self.env, text=True, capture_output=True, check=check,
        )

    def test_accepts_signed_bytes_and_regenerates_matrix(self):
        data = self.record("good")
        self.artifact({"evidence/2026/10/good.json": data, "target/testing-logs/trace.json": b"trace"},
                      [(data, RUN_ATTEMPT)])
        self.assertIn("Accepted: 1 (attested), rejected: 0, duplicates: 0", self.ingest().stdout)
        record = self.dest / "2026" / "10" / "fixture--good.json"
        self.assertEqual(record.read_bytes(), data)
        matrix = json.loads((self.matrix_dir / "coverage-matrix.json").read_text())
        self.assertTrue(matrix["verified_attestations"])
        self.assertTrue((self.matrix_dir / "coverage-matrix.md").is_file())
        self.assertIn("Accepted: 0 (attested), rejected: 0, duplicates: 1", self.ingest().stdout)

    def test_rejects_tampered_record(self):
        original = self.record("tampered")
        tampered = original.replace(b'"executed": 1', b'"executed": 6')
        self.artifact({"evidence/tampered.json": tampered}, [(original, RUN_ATTEMPT)])
        output = self.ingest().stdout
        self.assertIn("attestation verification failed: no attestation for digest", output)
        self.assertIn("Accepted: 0 (attested), rejected: 1, duplicates: 0", output)
        self.assertFalse(self.dest.exists())

    def test_rejects_sha_from_workflow_head_instead_of_tested_revision(self):
        data = self.record("wrong-sha", sha=WORKFLOW_SHA)
        self.artifact({"evidence/wrong-sha.json": data}, [(data, RUN_ATTEMPT)])
        output = self.ingest().stdout
        self.assertIn(f"SHA mismatch: record {WORKFLOW_SHA}, run tested {self.tested_sha}", output)
        self.assertIn("Accepted: 0 (attested), rejected: 1, duplicates: 0", output)
        self.assertFalse((self.fixture / "verification.log").exists())

    def test_rejects_valid_signature_from_another_run(self):
        data = self.record("wrong-run")
        self.artifact({"evidence/wrong-run.json": data}, [(data, RUN_ATTEMPT.replace(RUN_ID, "9999"))])
        output = self.ingest().stdout
        self.assertIn("attestation belongs to a different run or attempt", output)
        self.assertFalse(self.dest.exists())

    def test_validates_schema_before_attestation(self):
        data = self.record("bad-schema", unexpected=True)
        self.artifact({"evidence/invalid.json": data}, [(data, RUN_ATTEMPT)])
        self.assertIn("invalid record", self.ingest().stdout)
        self.assertFalse((self.fixture / "verification.log").exists())

    def test_conflicting_record_never_overwrites_existing_bytes(self):
        data = self.record("good")
        self.artifact({"evidence/good.json": data}, [(data, RUN_ATTEMPT)])
        path = self.dest / "2026" / "10" / "fixture--good.json"
        path.parent.mkdir(parents=True)
        path.write_bytes(data + b"\n")
        output = self.ingest().stdout
        self.assertIn("different record already exists", output)
        self.assertEqual(path.read_bytes(), data + b"\n")

    def test_unverified_records_are_excluded_from_consumers(self):
        data = self.record("unattested", s3_requests=6)
        self.artifact({"evidence/unattested.json": data}, [])
        self.assertIn("Accepted: 1 (unattested)", self.ingest("--no-verify").stdout)
        self.assertEqual(list(self.dest.rglob("*.json")), [])
        saved = list(self.dest.rglob("*.json.unattested"))
        self.assertEqual([path.read_bytes() for path in saved], [data])
        self.assertFalse((self.fixture / "verification.log").exists())
        matrix = json.loads((self.matrix_dir / "coverage-matrix.json").read_text())
        self.assertEqual(matrix["ignored_records"], [])
        self.assertFalse(matrix["allow_unattested"])

    def test_rejects_pull_request_run_before_artifact_download(self):
        (self.fixture / "run.json").write_text(json.dumps({**self.run, "event": "pull_request"}))
        result = self.ingest(check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("expected a completed nabedra build dispatch or schedule", result.stderr)
        self.assertFalse(self.dest.exists())


class EvidenceWorkflowTests(EvidenceFixture):
    def setUp(self):
        super().setUp()
        self.repo = self.fixture / "checkout"
        self.repo.mkdir()
        origin = self.fixture / "origin.git"
        git(self.repo, "init", "-b", "main")
        git(self.repo, "init", "--bare", str(origin))
        git(self.repo, "remote", "add", "origin", str(origin))
        git(self.repo, "config", "user.name", "Fixture")
        git(self.repo, "config", "user.email", "fixture@example.com")
        git(self.repo, "config", "commit.gpgsign", "false")
        (self.repo / "README.md").write_text("original\n")
        git(self.repo, "add", "README.md")
        git(self.repo, "commit", "-m", "initial")
        git(self.repo, "push", "origin", "HEAD:refs/heads/main")
        testing = self.repo / "docs" / "testing"
        records = testing / "evidence" / "records" / "2026" / "11"
        records.mkdir(parents=True)
        (records / "fixture.json").write_text("{}\n")
        (testing / "coverage-matrix.json").write_text("{}\n")
        (testing / "coverage-matrix.md").write_text("# Matrix\n")

    def publish(self):
        workflow = evidence.REPO_ROOT / ".github" / "workflows" / "nabedra-evidence.yml"
        step = workflow.read_text().split("- name: Publish the evidence pull request\n", 1)[1]
        script = textwrap.dedent(step.split("        run: |\n", 1)[1])
        return subprocess.run(
            ["bash", "-eo", "pipefail", "-c", script], cwd=self.repo,
            env={**self.env, "RUN_ID": RUN_ID, "GITHUB_REPOSITORY": REPO},
            text=True, capture_output=True,
        )

    def test_publishes_evidence_only_and_pins_the_merge_head(self):
        result = self.publish()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        sha = git(self.repo, "rev-parse", "HEAD").stdout.strip()
        merge = json.loads((self.fixture / "merge.json").read_text())
        self.assertEqual(merge, ["pr", "merge", "17", "--repo", REPO, "--auto", "--merge",
                                 "--match-head-commit", sha])
        self.assertEqual(git(self.repo, "log", "-1", "--format=%s").stdout.strip(),
                         f"[skip ci] [evidence] run {RUN_ID}")

    def test_refuses_to_publish_an_unrelated_change(self):
        (self.repo / "README.md").write_text("unrelated change\n")
        git(self.repo, "add", "README.md")
        result = self.publish()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("README.md", result.stdout)
        ref = git(self.repo, "ls-remote", "origin", f"refs/heads/evidence/{RUN_ID}")
        self.assertEqual(ref.stdout, "")
        self.assertFalse((self.fixture / "merge.json").exists())
