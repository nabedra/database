from pathlib import Path
import hashlib, json, os, subprocess, time
root = Path(__file__).parent
source = root / "source"
evidence = root / "evidence"
def inputs():
    paths = [p for p in (source / "server").rglob("*") if p.is_file() and "target" not in p.parts]
    return {str(p.relative_to(source)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}
env = os.environ.copy()
env.update(CARGO_TARGET_DIR="/Users/antoniopenapena/Documents/ResearchProjects/AI/nabedra-database/server/target", CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0", CARGO_INCREMENTAL="0", CARGO_BUILD_JOBS="2", TOKIO_WORKER_THREADS="2", RUST_TEST_THREADS="1")
commands = [("format", ["cargo", "fmt", "--manifest-path", "server/Cargo.toml", "--all"]), ("library", ["cargo", "test", "--locked", "--manifest-path", "server/Cargo.toml", "--no-default-features", "--features", "oltp", "--lib"]), ("clippy", ["cargo", "clippy", "--locked", "--manifest-path", "server/Cargo.toml", "--no-default-features", "--features", "oltp", "--all-targets", "--", "-D", "warnings"]), ("format-check", ["cargo", "fmt", "--manifest-path", "server/Cargo.toml", "--all", "--", "--check"])]
records = []
original = inputs()
verified_inputs = None
for name, command in commands:
    start = time.monotonic()
    with (evidence / (name + ".log")).open("w") as output:
        result = subprocess.run(command, cwd=source, env=env, stdout=output, stderr=subprocess.STDOUT)
    if name == "format":
        verified_inputs = inputs()
    record = {"stage": name, "command": command, "exit_code": result.returncode, "elapsed_seconds": time.monotonic() - start}
    records.append(record)
    print(json.dumps(record), flush=True)
    summary = {"overlay": json.loads((root / "overlay.json").read_text()), "original_inputs": original, "verified_inputs": verified_inputs, "inputs_unchanged_after_format": verified_inputs == inputs(), "stages": records, "environment": {key: env[key] for key in ("CARGO_TARGET_DIR", "CARGO_PROFILE_DEV_DEBUG", "CARGO_PROFILE_TEST_DEBUG", "CARGO_INCREMENTAL", "CARGO_BUILD_JOBS", "TOKIO_WORKER_THREADS", "RUST_TEST_THREADS")}}
    (evidence / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    if result.returncode:
        raise SystemExit(result.returncode)
if verified_inputs != inputs():
    raise SystemExit(2)
