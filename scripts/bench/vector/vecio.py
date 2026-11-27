"""Files of the vector benchmark sets (contract 6.1). All little-endian.

fbin: u32 n, u32 d, then n*d f32.   ibin: u32 n, u32 d, then n*d u32.

A set directory holds base.fbin, query.fbin, cols.ibin (d=2: the synthetic int `u` in [0, 1000) and
the tag bits, bit 0 = tag `a`, bit 1 = tag `b`), optionally labels.ibin (d=1, a code per row, names in
manifest.json), vendor/*.ibin (ground truth shipped with the set, for cross-checks only) and
gt-<rows>-<queries>/ (ground truth of gt.py).
"""

import hashlib
import json
import os

import numpy as np

HEADER = np.dtype("<u4")
# A neighbour slot without a row (ragged shipped ground truth).
MISSING = 0xFFFFFFFF


def write_header(f, n, d):
    np.array([n, d], dtype=HEADER).tofile(f)


def write_bin(path, array):
    """Writes a 2-d f32 or u32 array as fbin or ibin."""
    array = np.ascontiguousarray(array)
    kind = {np.float32: "<f4", np.uint32: "<u4"}[array.dtype.type]
    with open(path + ".tmp", "wb") as f:
        write_header(f, *array.shape)
        array.astype(kind, copy=False).tofile(f)
    os.replace(path + ".tmp", path)


def read_bin(path, rows=None, mmap=True):
    """Maps a fbin/ibin file. `rows` takes a prefix."""
    with open(path, "rb") as f:
        n, d = np.fromfile(f, dtype=HEADER, count=2)
    n, d = int(n), int(d)
    kind = "<f4" if path.endswith(".fbin") or path.endswith(".f32") else "<u4"
    if rows is not None:
        n = min(n, rows)
    if mmap:
        return np.memmap(path, dtype=kind, mode="r", offset=8, shape=(n, d))
    return np.fromfile(path, dtype=kind, count=n * d, offset=8).reshape(n, d)


class Appender:
    """Writes a fbin/ibin row block by row block, header patched at the end."""

    def __init__(self, path, d, kind):
        self.path, self.d, self.kind, self.n = path, d, kind, 0
        self.f = open(path + ".tmp", "wb")
        write_header(self.f, 0, d)

    def add(self, block):
        block = np.ascontiguousarray(block, dtype=self.kind)
        assert block.ndim == 2 and block.shape[1] == self.d, block.shape
        block.tofile(self.f)
        self.n += block.shape[0]

    def close(self):
        self.f.seek(0)
        write_header(self.f, self.n, self.d)
        self.f.close()
        os.replace(self.path + ".tmp", self.path)


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 22), b""):
            h.update(chunk)
    return h.hexdigest()


def hash_tree(root, skip=("raw",)):
    """SHA-256 of every file under root, by relative path."""
    out = {}
    for dirpath, dirs, files in os.walk(root):
        dirs[:] = sorted(d for d in dirs if d not in skip)
        for name in sorted(files):
            if name.endswith(".tmp") or name == "manifest.json":
                continue
            path = os.path.join(dirpath, name)
            out[os.path.relpath(path, root)] = sha256(path)
    return out


def load_manifest(root):
    path = os.path.join(root, "manifest.json")
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)


def save_manifest(root, manifest):
    path = os.path.join(root, "manifest.json")
    with open(path + ".tmp", "w") as f:
        json.dump(manifest, f, indent=1, sort_keys=True)
        f.write("\n")
    os.replace(path + ".tmp", path)
