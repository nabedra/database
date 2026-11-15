"""Console entry point for launching the separately built Rust worker."""
import argparse
import os
import sys

from ._binary import BINARY_FAILURE, resolve_server_binary


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        # Argparse's default messages can echo arbitrarily large input and paths.
        super().error("missing or unsupported command arguments; see --help")


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and replace this process; return 1 if launching fails."""
    parser = _ArgumentParser(
        prog="meshdb", allow_abbrev=False,
        description="Launch a Rust worker with worker --bootstrap PATH.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    worker = commands.add_parser(
        "worker", help="launch the Rust worker", allow_abbrev=False,
        description="Launch the Rust worker; Rust validates the bootstrap file.",
    )
    worker.add_argument("--bootstrap", required=True, metavar="PATH",
                        help="protected worker bootstrap file passed directly to Rust")
    args = parser.parse_args(argv)
    try:
        binary = resolve_server_binary()
        os.execv(binary, [binary, "worker", "--bootstrap", args.bootstrap])
    except (OSError, ValueError, RuntimeError):
        print(BINARY_FAILURE, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
