"""Select automatic PR jobs from the paths changed in the tested merge commit."""

from collections.abc import Iterable
from pathlib import PurePosixPath
import subprocess

BASE_JOBS = frozenset(("kv", "clippy", "features", "cache", "olap", "check-tests", "oltp", "events"))
OLTP = frozenset(("oltp", "clippy"))
OLAP = frozenset(("olap", "clippy"))
CACHE = frozenset(("cache", "clippy"))
# Only list paths whose consumers are known. Shared entry points, test helpers,
# build inputs, and unclassified paths use the full baseline.
PATH_JOBS = (
    ("server/events/", frozenset(("events",))),
    ("server/src/cache/", CACHE),
    ("server/src/cache.rs", CACHE),
    ("server/src/oltp_runtime/", OLTP),
    ("server/src/oltp_acceptance/", OLTP),
    ("server/src/oltp_acceptance.rs", OLTP),
    ("server/src/engine/", OLAP),
    ("server/src/engine.rs", OLAP),
    ("server/src/node/", OLAP),
    ("server/src/cluster/", OLAP),
    ("server/src/snapshot/", OLAP),
    ("server/src/snapshot.rs", OLAP),
    ("server/src/commit.rs", OLAP),
    ("server/src/olap_export.rs", OLAP),
    ("server/src/aws_http.rs", OLAP | {"features"}),
    ("server/kv/src/", frozenset(("kv", "oltp", "events", "clippy"))),
    ("server/check/", frozenset(("check-tests", "oltp", "olap", "cache", "events", "clippy"))),
    ("server/oltp-types/src/", OLTP | {"check-tests"}),
    ("server/sim/", frozenset()),
    ("server/sim-oltp/", frozenset()),
)


def select(paths: Iterable[str]) -> frozenset[str]:
    jobs: set[str] = set()
    for path in paths:
        name = PurePosixPath(path).name
        if name in {"Cargo.toml", "Cargo.lock", "config.toml", "config", "build.rs"}:
            return BASE_JOBS
        # Specs and the testing register have their own selected evidence.
        if path.endswith(".md") and path.startswith(("docs/", "plans/", "specs/", ".agents/")):
            continue
        for key, selected in PATH_JOBS:
            matches = path.startswith(key) if key.endswith("/") else path == key
            if matches:
                jobs.update(selected)
                break
        else:
            return BASE_JOBS
    return frozenset(jobs)


def main() -> None:
    # Disabling rename detection includes both sides of a move between suites.
    # Compare the merge commit against its first parent to include only the PR.
    changed = subprocess.check_output(
        ["git", "diff", "--name-only", "--no-renames", "-z", "HEAD^1", "HEAD"]
    )
    paths = changed.decode("utf-8", errors="surrogateescape").split("\0")
    jobs = select(filter(None, paths))
    print("," + ",".join(sorted(jobs)) + "," if jobs else ",")


if __name__ == "__main__":
    main()
