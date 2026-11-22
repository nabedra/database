"""Real-environment helpers. Their modules use package-relative imports across
`scripts.testing.kind` and `scripts.testing.realenv`, so they only import under
that qualified name.

`python3 -m unittest discover -s scripts/testing` imports this directory as a
top-level package, where those imports fail. The hook below reloads the tests
under the qualified name instead, so discovery runs them rather than erroring.
"""

from pathlib import Path
import sys


def load_tests(loader, standard_tests, pattern):
    if __name__.startswith("scripts.testing."):
        return standard_tests
    root = str(Path(__file__).resolve().parents[3])
    if root not in sys.path:
        sys.path.insert(0, root)
    here = Path(__file__).resolve().parent
    names = sorted(f"scripts.testing.{here.name}.{p.stem}" for p in here.glob(pattern or "test*.py"))
    return loader.loadTestsFromNames(names)
