"""Private, on-demand resolution of the separately built Rust executable."""
import os
import shutil


BINARY_FAILURE = (
    "Cannot launch meshdb-server. Set MESHDB_SERVER_BINARY to an executable "
    "file path or put meshdb-server on PATH."
)


def resolve_server_binary() -> str:
    """Return an absolute executable path, or raise a static RuntimeError.

    A present override is always a filesystem path relative to the current
    directory if needed. Invalid overrides never fall back to PATH. This
    function performs no bootstrap reads, downloads or process launches.
    """
    try:
        if "MESHDB_SERVER_BINARY" in os.environ:
            candidate = os.environ["MESHDB_SERVER_BINARY"]
        else:
            candidate = shutil.which("meshdb-server")
        if candidate:
            path = os.path.abspath(candidate)
            if os.path.isfile(path) and os.access(path, os.X_OK):
                return path
    except (OSError, ValueError):
        pass
    raise RuntimeError(BINARY_FAILURE) from None
