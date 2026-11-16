"""Private, on-demand resolution of the installed Rust executable."""
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

CLI_VERSION = '0.1.0'
CONFIG_VERSION = 2
PROTOCOL_VERSION = 1
BINARY_FAILURE = (
    'Cannot launch compatible nabedra-database-server. Set NABEDRA_SERVER_BINARY to an '
    'executable file path or put nabedra-database-server on PATH.'
)


def binary_version(path):
    # File-backed output avoids allocating unlimited diagnostics from a bad binary.
    try:
        with tempfile.TemporaryFile() as output:
            result = subprocess.run([path, '--version'], stdin=subprocess.DEVNULL,
                                    stdout=output, stderr=subprocess.DEVNULL, timeout=5)
            output.seek(0)
            data = output.read(257)
        match = re.fullmatch(rb'nabedra-database-server (0\.1\.\d+)\s*', data)
        if result.returncode == 0 and match:
            return match[1].decode('ascii')
    except (OSError, ValueError, subprocess.TimeoutExpired):
        pass
    raise RuntimeError(BINARY_FAILURE) from None


def resolve_server_binary(*, check=False):
    """Explicit overrides are errors; search the active environment before PATH.

    Launchers set check=True to probe compatibility. Resolution alone remains
    inert for callers that only need to inspect the executable path.
    """
    explicit = 'NABEDRA_SERVER_BINARY' in os.environ
    candidates = [os.environ['NABEDRA_SERVER_BINARY']] if explicit else [
        str(Path(sys.prefix) / 'bin' / 'nabedra-database-server'), shutil.which('nabedra-database-server')]
    for candidate in candidates:
        if candidate:
            path = os.path.abspath(candidate)
            try:
                if os.path.isfile(path) and os.access(path, os.X_OK):
                    if check:
                        binary_version(path)
                    return path
            except (OSError, ValueError, RuntimeError):
                pass
        if explicit:
            break
    raise RuntimeError(BINARY_FAILURE) from None
