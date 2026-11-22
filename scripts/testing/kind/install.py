#!/usr/bin/env python3
"""Install a checksum-pinned kind binary."""

import argparse
import hashlib
from pathlib import Path
import platform
import shutil
from tempfile import NamedTemporaryFile
from urllib.request import urlopen


VERSION = "v0.33.0"
RELEASE = f"https://github.com/kubernetes-sigs/kind/releases/download/{VERSION}"
# From the release assets named kind-<platform>.sha256sum.
SHA256 = {
    "linux-amd64": "aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d",
    "linux-arm64": "20022bee6cfcd5086cb7234d218e3454e6090022f2a8f55d1fa7fcf42c3867a2",
    "darwin-amd64": "5a99f26f57246dc9319dd294803313197a0f34d33c525b3ea8b655db5916ece0",
    "darwin-arm64": "0c8c7dbe5e23594a198b786c4bc13dacc101fa6196b0cb0b23a1ca44e61f4b4f",
}


def verify_checksum(path: Path, expected: str):
    with path.open("rb") as source:
        actual = hashlib.file_digest(source, "sha256").hexdigest()
    if actual != expected:
        raise ValueError(f"SHA-256 mismatch for {path.name}: expected {expected}, got {actual}")


def install(destination: Path):
    os_name, machine = platform.system().lower(), platform.machine().lower()
    arch = {"x86_64": "amd64", "aarch64": "arm64"}.get(machine, machine)
    target = f"{os_name}-{arch}"
    if target not in SHA256:
        raise ValueError(f"unsupported kind platform {target}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=destination.parent, delete=False) as output:
        temporary = Path(output.name)
    try:
        with urlopen(f"{RELEASE}/kind-{target}", timeout=30) as response, temporary.open("wb") as output:
            shutil.copyfileobj(response, output)
        verify_checksum(temporary, SHA256[target])
        temporary.chmod(0o755)
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path.home() / ".local/bin/kind")
    args = parser.parse_args()
    install(args.output)
    print(f"Installed kind {VERSION} to {args.output}")


if __name__ == "__main__":
    main()
