#!/usr/bin/env python3
"""Install this checkout's CLI and one locked Rust release build locally."""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import venv


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument('--prefix', default='.meshdb', help='new isolated environment directory')
    args = parser.parse_args(argv)
    if sys.version_info < (3, 11):
        parser.error('Python 3.11 or newer is required')
    if sys.platform not in ('linux', 'darwin'):
        parser.error('Server installation supports Linux and macOS')
    root = Path(__file__).resolve().parents[1]
    prefix = Path(args.prefix).absolute()
    if prefix.exists():
        parser.error('Installation prefix already exists; choose a new directory')
    if shutil.which('cargo') is None or shutil.which('rustc') is None:
        parser.error('Install the repository Rust toolchain with Cargo and rustc first')
    try:
        # Cargo resolves any repository toolchain override from the server directory.
        subprocess.run(['cargo', 'build', '--locked', '--release', '--bin', 'meshdb-server'],
                       cwd=root / 'server', check=True)
        target = Path(os.environ.get('CARGO_TARGET_DIR', str(root / 'server' / 'target')))
        if not target.is_absolute():
            target = root / 'server' / target
        binary = target / 'release' / 'meshdb-server'
        venv.EnvBuilder(with_pip=True).create(prefix)
        subprocess.run([str(prefix / 'bin' / 'python'), '-m', 'pip', 'install', str(root / 'orm')], check=True)
        shutil.copy2(binary, prefix / 'bin' / 'meshdb-server')
        subprocess.run([str(prefix / 'bin' / 'meshdb'), '--version', '--json'], check=True)
    except (OSError, subprocess.CalledProcessError):
        print('Installation failed. Check build tools and preceding diagnostics; remove the incomplete prefix before retrying.', file=sys.stderr)
        return 1
    print('Installed MeshDB in ' + str(prefix))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
