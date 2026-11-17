#!/usr/bin/env python3.11
"""Run the 29 OLTP contract scenarios with one cached build and explicit outcomes.

Development: --scenario O07. Shared Cargo lane: --cargo test ...
Scenario tests end in acceptance_oNN. Existing regression tests remain intact.
"""
import argparse
from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
MARKER = "NABEDRA_OLTP_ACCEPTANCE "
CATEGORIES = {
    "compatibility": ("Compatibility and packaging", (1, 2)),
    "queries": ("Keys, query semantics and indexes", (3, 4, 12)),
    "transactions": ("Transactions and durable decisions", (6, 7, 8, 9, 10, 11, 29)),
    "recovery": ("Resource limits, queue survival and cold recovery", (5, 13, 14, 15, 22, 23)),
    "growth": ("Shards, volumes and unbounded table growth", (16, 21, 26, 28)),
    "operations": ("Deployment, autoscaling, backup and migration", (17, 18, 19, 20, 27)),
    "performance": ("Performance and scaling", (24, 25)),
}
TITLES = (
    "Unchanged Python ORM parity", "Independent engine builds and identity",
    "Indexed primary keys and routing", "Exact query oracle and bounded results",
    "Aggregate native resources under maintenance", "Durable quorum and leader authority",
    "Atomic single/multiple-shard batches", "Concurrent writes and schema lifecycle",
    "Snapshots, commit timestamps and intent visibility", "Exact decision and receipt recovery",
    "Log/state/snapshot crash boundaries", "Online secondary and covering indexes",
    "Admission, retention and settlement under saturation", "Surviving RAM copies and rolling joins",
    "Complete shutdown abandonment and durable commits", "Split/move lineage and snapshots",
    "Quorum-aware scale and drain", "Real EC2/EBS/EKS lifecycle",
    "Consistent backup and point-in-time restore", "Offline OLAP-to-OLTP migration",
    "Unbounded logical table growth", "Locality, fairness and dependent work",
    "All-ready queue replication under churn", "Matched workload and PostgreSQL measurements",
    "Measured scaling and maintenance", "Multiple verified storage pools",
    "Idempotent authorized volume/worker growth", "Bounded paged metadata",
    "Ambiguous native writes never become certain absence",
)
# Original scheduler assertions remain traceable; backend-specific OLAP T02 is
# retained in the OLAP regression suite rather than relabeled an OLTP assertion.
SCHEDULER = {
    1: "U01 T06", 2: "U01 U02 T02", 3: "T06 U02", 4: "C04 C05 C06 M01 U03",
    5: "C02 C07 M01 M02 M03 F01 F02 F03", 6: "R01 R05 R06",
    7: "T01 T03 T06", 8: "T03 T06 U03", 9: "Q06 R05 T05", 10: "Q05 Q06 T04",
    11: "R04 T01 T04 T05", 12: "C04 C07 F01", 13: "Q01 Q02 Q03 Q05 Q06 M01 M02 M03 F01 F02 F03",
    14: "Q03 Q04 R01 R02 R03 R06", 15: "Q02 Q06 R04 R05 T05",
    16: "R02 R03 R05", 17: "A01 A02 R02 R03", 18: "A01 A02 R03 R06",
    19: "R04 T01", 20: "U01 T06", 21: "C03 M01 T06", 22: "C01 C02 C03 C04 C05 C06 C07 F01 F02 F03 U01 U02 U03",
    23: "Q01 Q02 Q03 Q04 Q05 Q06 C07 M02 R03 B02", 24: "B01 B02", 25: "B01 B02 A02 R06",
    26: "C01 C02 C03 M02 A01 A02 R03", 27: "A01 A02 R03", 28: "M01 M02 C04 Q06 R03", 29: "T04 R04 F03",
}
REGRESSIONS = {
    "join-empty-single-paged-live": (14, 23), "small-control-message-progress": (5, 13, 22, 23),
    "bounded-observers-and-retention": (13, 23, 24), "peer-control-retry-and-signatures": (22, 23),
    "terminal-copy-receipt-race": (10, 13, 14), "receipt-observation-without-resubmit": (10, 14),
    "writer-wait-without-compute": (22,), "native-and-append-cancellation": (5, 11, 22),
    "wide-key-page-scratch": (4, 5), "consensus-progress-under-saturation": (5, 22),
    "snapshot-activation-and-membership": (11, 16), "unreadable-decision-proof": (10, 29),
    "atomic-snapshot-pin": (9,), "release-membership-fence": (9, 14),
    "terminal-retirement-and-delayed-UUID": (13, 14, 15), "raw-resubmit-after-cold-start": (13, 15),
    "actual-native-open-thread-budget": (5, 23), "reply-aging-and-cancellation": (5, 13),
}

@dataclass(frozen=True)
class Scenario:
    number: int
    category: str

    @property
    def id(self):
        return f"O{self.number:02d}"

    def result(self, status="not-run", reason="not selected"):
        return dict(scenario=self.id, category=self.category, title=TITLES[self.number - 1],
                    status=status, reason=reason, scheduler_ids=SCHEDULER[self.number].split(),
                    regressions=[name for name, ids in REGRESSIONS.items() if self.number in ids])

SCENARIOS = [Scenario(n, key) for key, (_, ids) in CATEGORIES.items() for n in ids]
assert sorted(s.number for s in SCENARIOS) == list(range(1, 30))


def target_directory(explicit):
    if explicit or os.environ.get("CARGO_TARGET_DIR"):
        return Path(explicit or os.environ["CARGO_TARGET_DIR"]).resolve()
    common = subprocess.check_output(["git", "rev-parse", "--path-format=absolute", "--git-common-dir"], cwd=ROOT, text=True).strip()
    shared = Path(common).parent / "server/target"
    return shared if shared.exists() else ROOT / "server/target"


@contextmanager
def cargo_lane(target):
    target.mkdir(parents=True, exist_ok=True)
    with (target / ".nabedra-cargo.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("Waiting for the shared Cargo lane...", flush=True)
            fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def environment(target):
    return dict(os.environ, CARGO_TARGET_DIR=str(target), CARGO_BUILD_JOBS="2",
                CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0", CARGO_INCREMENTAL="0",
                TOKIO_WORKER_THREADS="2", RUST_TEST_THREADS="1")


def source_identity():
    digest = hashlib.sha256()
    paths = [ROOT / "server/Cargo.toml", ROOT / "server/Cargo.lock", Path(__file__)]
    paths += sorted((ROOT / "server/src").rglob("*.rs"))
    paths += sorted((ROOT / "server/tests").rglob("*.rs"))
    paths += sorted((ROOT / "server/tests").rglob("*.py"))
    paths += sorted((ROOT / "scripts").glob("*.py"))
    paths += sorted((ROOT / "orm/python/src").rglob("*.py"))
    for path in sorted(set(paths)):
        digest.update(str(path.relative_to(ROOT)).encode() + b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return dict(commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                source_sha256=digest.hexdigest())


def build_tests(env, output):
    command = ["cargo", "test", "--locked", "--manifest-path", "server/Cargo.toml", "--no-default-features",
               "--features", "oltp", "--lib", "--tests", "--no-run", "--message-format=json-render-diagnostics"]
    executable = None
    server = None
    with output.open("w") as log:
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in process.stdout:
            try:
                record = json.loads(line)
            except ValueError:
                log.write(line)
                continue
            if record.get("reason") == "compiler-artifact" and record.get("executable"):
                if record.get("profile", {}).get("test") and "lib" in record.get("target", {}).get("kind", []):
                    executable = record["executable"]
                if not record.get("profile", {}).get("test") and record.get("target", {}).get("name") == "nabedra-db":
                    server = record["executable"]
            if record.get("reason") == "compiler-message":
                log.write(record["message"].get("rendered", ""))
        if process.wait() or executable is None:
            raise RuntimeError(f"OLTP test build failed; see {output}")
    return Path(executable), Path(server) if server else None


def run_scenario(binary, name, scenario, env, output, timeout):
    start = time.monotonic()
    with output.open("w") as log:
        process = subprocess.Popen([str(binary), "--exact", name, "--include-ignored", "--nocapture", "--test-threads=1"], cwd=ROOT,
                                   env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            return dict(scenario.result("failed", "scenario watchdog expired"), elapsed_seconds=time.monotonic() - start)
    text = output.read_text(errors="replace")
    status, reason = ("failed", f"assertion process exited {code}") if code else ("passed", "assertions executed")
    if not code and not re.search(r"test result: ok\. 1 passed; 0 failed; 0 ignored;", text):
        status, reason = "not-run", "the exact scenario did not execute one non-ignored test"
    for line in text.splitlines():
        if MARKER in line:
            try:
                marker = json.loads(line.split(MARKER, 1)[1])
                assert marker["scenario"] == scenario.id and marker["status"] in ("not-run", "failed")
                if not code or marker["status"] == "failed":
                    status, reason = marker["status"], marker["reason"]
            except (ValueError, KeyError, AssertionError):
                status, reason = "failed", "invalid scenario status marker"
    return dict(scenario.result(status, reason), elapsed_seconds=round(time.monotonic() - start, 3), log=str(output))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", action="append", choices=[f"O{n:02d}" for n in range(1, 30)])
    parser.add_argument("--category", action="append", choices=list(CATEGORIES))
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--target-dir")
    parser.add_argument("--test-binary", type=Path, help="use an already built library test executable")
    parser.add_argument("--server-binary", type=Path, help="already built OLTP server for process assertions")
    parser.add_argument("--olap-binary", type=Path, help="OLAP server for unchanged ORM parity")
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts/oltp-acceptance")
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--provider-config", type=Path)
    parser.add_argument("--cargo", nargs=argparse.REMAINDER, help="run Cargo through the shared build/test lane")
    args = parser.parse_args()
    if args.list:
        for key, (title, _) in CATEGORIES.items():
            print(title)
            for s in SCENARIOS:
                if s.category == key:
                    print(f"  {s.id} {TITLES[s.number - 1]}")
        return 0
    target = target_directory(args.target_dir)
    env = environment(target)
    if args.cargo is not None:
        if not args.cargo:
            parser.error("--cargo requires a Cargo subcommand")
        with cargo_lane(target):
            return subprocess.call(["cargo", *args.cargo], cwd=ROOT, env=env)
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.provider_config:
        env["NABEDRA_OLTP_PROVIDER_CONFIG"] = str(args.provider_config.resolve())
    if args.olap_binary:
        env["NABEDRA_OLAP_BINARY"] = str(args.olap_binary.resolve())
    selected = [s for s in SCENARIOS if (not args.scenario or s.id in args.scenario)
                and (not args.category or s.category in args.category)]
    if not selected:
        parser.error("the selected scenarios and categories do not overlap")
    args.output.mkdir(parents=True, exist_ok=True)
    report = dict(source=source_identity(), categories={k: v[0] for k, v in CATEGORIES.items()}, results=[])
    try:
        with cargo_lane(target), tempfile.TemporaryDirectory(prefix="nabedra-oltp-acceptance-") as temp:
            built, server = (args.test_binary, args.server_binary) if args.test_binary else build_tests(env, args.output / "build.log")
            binary = Path(temp) / "acceptance-tests"
            shutil.copy2(built, binary)
            report["test_binary_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
            if args.server_binary or server:
                server_copy = Path(temp) / "nabedra-db"
                shutil.copy2(args.server_binary or server, server_copy)
                env["NABEDRA_OLTP_BINARY"] = str(server_copy)
                report["server_binary_sha256"] = hashlib.sha256(server_copy.read_bytes()).hexdigest()
            listed = subprocess.check_output([str(binary), "--list"], cwd=ROOT, env=env, text=True)
            names = [line[:-6] for line in listed.splitlines() if line.endswith(": test")]
            for s in SCENARIOS:
                result = s.result()
                if s in selected:
                    matches = [name for name in names if name.rsplit("::", 1)[-1] == f"acceptance_o{s.number:02d}"]
                    if len(matches) == 1:
                        result = run_scenario(binary, matches[0], s, env, args.output / f"{s.id}.log", args.timeout)
                    else:
                        result = s.result("failed" if matches else "not-run", "duplicate scenario definitions" if matches else "production scenario assertions not integrated")
                    print(f"{s.id} {result['status']}: {result['reason']}", flush=True)
                report["results"].append(result)
    except (OSError, subprocess.SubprocessError, RuntimeError) as error:
        report["error"] = str(error)
        print(str(error), file=sys.stderr)
    recorded = {r["scenario"] for r in report["results"]}
    for s in SCENARIOS:
        if s.id not in recorded:
            report["results"].append(s.result(reason="build or runner failed before execution" if s in selected else "not selected"))
    report["source_after"] = source_identity()
    report["source_changed"] = report["source"]["source_sha256"] != report["source_after"]["source_sha256"]
    chosen = [r for r in report["results"] if r["scenario"] in {s.id for s in selected}]
    report["summary"] = {status: sum(r["status"] == status for r in chosen) for status in ("passed", "failed", "not-run")}
    report["selected"] = len(selected)
    (args.output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report["summary"]) + f"; report: {args.output / 'results.json'}")
    if report.get("error") or report["summary"]["failed"]:
        return 1
    return 2 if report["source_changed"] or report["summary"]["not-run"] or len(chosen) != len(selected) else 0


if __name__ == "__main__":
    raise SystemExit(main())
