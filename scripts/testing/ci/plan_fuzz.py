#!/usr/bin/env python3
"""List the fuzz targets and their Cargo feature requirements for GitHub Actions."""

import json
from pathlib import Path
import tomllib


def fuzz_matrix(directory):
    manifest = directory / "Cargo.toml"
    if not manifest.is_file():
        return []
    with manifest.open("rb") as source:
        targets = {entry["path"]: entry for entry in tomllib.load(source).get("bin", [])}
    matrix = []
    for path in sorted((directory / "fuzz_targets").glob("*.rs")):
        relative = path.relative_to(directory).as_posix()
        if relative not in targets:
            raise ValueError(f"{path} has no [[bin]] in {manifest}")
        target = targets[relative]
        matrix.append({"target": target["name"], "features": ",".join(target.get("required-features", []))})
    return matrix


if __name__ == "__main__":
    try:
        print(json.dumps(fuzz_matrix(Path("server/fuzz"))))
    except ValueError as error:
        raise SystemExit(str(error)) from error
