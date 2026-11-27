"""Converts a downloaded set (fetch.sh) to fbin/ibin, adds the synthetic filter columns and writes
manifest.json with SHA-256 of sources and outputs, licence notes and the norm distribution.

    python -I convert.py <set> <dir>      (<dir>/raw holds the downloads)
"""

import gzip
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np  # noqa: E402

import vecio  # noqa: E402

# Licence notes as contract-w2 section 6.1 states them. "not verified" stays until someone checks.
LICENCES = {
    "glove-100": "ann-benchmarks MIT; GloVe vectors PDDL (from memory, NOT VERIFIED)",
    "cohere-1m": "VectorDBBench MIT; Cohere data terms of this copy NOT VERIFIED (HF original answers 401)",
    "openai-500k": "VectorDBBench MIT; OpenAI embedding data terms NOT VERIFIED",
    "splade-small": "UNRESOLVED: README says CC BY 4.0, an earlier page says MS MARCO non-commercial and "
    "SPLADE CC BY-NC-SA; internal benchmarking only",
}

# Synthetic filter columns (contract 6.3): u uniform in [0, 1000); tags a and b, each 10%, independent.
COLUMN_SEED = 20261127
BATCH = 65_536


def synthetic_columns(n):
    rng = np.random.Generator(np.random.PCG64(COLUMN_SEED))
    u = rng.integers(0, 1000, size=n, dtype=np.uint32)
    a = rng.random(n) < 0.10
    b = rng.random(n) < 0.10
    tags = a.astype(np.uint32) | (b.astype(np.uint32) << 1)
    return np.stack([u, tags], axis=1)


def norm_stats(base):
    norms = np.empty(base.shape[0], dtype=np.float64)
    for s in range(0, base.shape[0], BATCH):
        block = np.asarray(base[s : s + BATCH], dtype=np.float64)
        norms[s : s + BATCH] = np.sqrt((block * block).sum(axis=1))
    q = np.quantile(norms, [0.0, 0.01, 0.5, 0.99, 1.0])
    return {
        "min": q[0], "p01": q[1], "p50": q[2], "p99": q[3], "max": q[4],
        "mean": float(norms.mean()),
        "unit_within_1e-3": float(np.mean(np.abs(norms - 1.0) <= 1e-3)),
        "zero": int(np.sum(norms == 0.0)),
    }


# ----- parquet (VectorDBBench)


def parquet_columns(path):
    import pyarrow.parquet as pq

    schema = pq.ParquetFile(path).schema_arrow
    print(path, schema, flush=True)
    return schema


def list_column(schema):
    import pyarrow as pa

    names = [f.name for f in schema if pa.types.is_list(f.type) or pa.types.is_large_list(f.type)]
    assert len(names) == 1, f"expected one list column: {schema}"
    return names[0]


def id_column(schema):
    import pyarrow as pa

    names = [f.name for f in schema if pa.types.is_integer(f.type)]
    assert names, f"no integer column: {schema}"
    return "id" if "id" in names else names[0]


def parquet_matrix(path, out, kind):
    """Streams the list column of a parquet file into a fbin/ibin. Returns the id column."""
    import pyarrow.parquet as pq

    schema = parquet_columns(path)
    col, idc = list_column(schema), id_column(schema)
    file = pq.ParquetFile(path)
    ids, writer = [], None
    for batch in file.iter_batches(batch_size=BATCH, columns=[idc, col]):
        lists = batch.column(col)
        flat = lists.flatten().to_numpy(zero_copy_only=False)
        rows = len(lists)
        d = flat.size // rows
        if d * rows != flat.size:
            # Ragged neighbour lists (some queries have fewer matches): pad with MISSING.
            assert kind == "<u4", "ragged vectors"
            values = lists.to_pylist()
            d = max(len(v) for v in values)
            block = np.full((rows, d), vecio.MISSING, dtype=np.uint32)
            for i, v in enumerate(values):
                block[i, : len(v)] = v
            assert writer is None, "ragged lists across batches"
            writer = vecio.Appender(out, d, kind)
            writer.add(block)
            ids.append(batch.column(idc).to_numpy())
            continue
        if writer is None:
            writer = vecio.Appender(out, d, kind)
        writer.add(flat.reshape(rows, d))
        ids.append(batch.column(idc).to_numpy())
    writer.close()
    return np.concatenate(ids)


def remap(ids, positions):
    """Maps vendor row ids to file positions."""
    order = np.argsort(positions)
    sorted_ids = positions[order]
    at = np.searchsorted(sorted_ids, ids)
    assert np.all(sorted_ids[np.minimum(at, len(sorted_ids) - 1)] == ids), "unknown neighbour id"
    return order[at].astype(np.uint32)


def convert_vdbb(name, root, train, manifest):
    raw = os.path.join(root, "raw")
    base_ids = parquet_matrix(os.path.join(raw, train), os.path.join(root, "base.fbin"), "<f4")
    in_order = bool(np.array_equal(base_ids, np.arange(len(base_ids))))
    manifest["base_ids_in_order"] = in_order
    if not in_order:
        vecio.write_bin(os.path.join(root, "ids.ibin"), base_ids.astype(np.uint32).reshape(-1, 1))
    query_ids = parquet_matrix(os.path.join(raw, "test.parquet"), os.path.join(root, "query.fbin"), "<f4")
    manifest["query_ids_in_order"] = bool(np.array_equal(query_ids, np.arange(len(query_ids))))
    os.makedirs(os.path.join(root, "vendor"), exist_ok=True)
    for f in sorted(os.listdir(raw)):
        if not f.startswith("neighbors") or not f.endswith(".parquet"):
            continue
        tmp = os.path.join(root, "vendor", "tmp.ibin")
        qids = parquet_matrix(os.path.join(raw, f), tmp, "<u4")
        knn = np.array(vecio.read_bin(tmp, mmap=False))
        os.remove(tmp)
        # Rows in the order of query positions, neighbour ids as base positions.
        knn = knn[np.argsort(qids)]
        knn = knn if in_order else remap(knn.ravel(), base_ids).reshape(knn.shape)
        vecio.write_bin(os.path.join(root, "vendor", f.replace(".parquet", ".ibin")), knn.astype(np.uint32))
    labels = os.path.join(raw, "scalar_labels.parquet")
    if os.path.exists(labels):
        import pyarrow.parquet as pq

        schema = parquet_columns(labels)
        table = pq.read_table(labels)
        lid = table.column(id_column(schema)).to_numpy()
        other = [f.name for f in schema if f.name != id_column(schema)]
        assert len(other) == 1, schema
        values = table.column(other[0]).to_pylist()
        names = sorted(set(values))
        code = {v: i for i, v in enumerate(names)}
        codes = np.array([code[v] for v in values], dtype=np.uint32)
        # Label rows are keyed by base id; place each at its file position.
        out = np.full(len(base_ids), 0xFFFFFFFF, dtype=np.uint32)
        out[lid if in_order else remap(lid, base_ids)] = codes
        assert np.all(out != 0xFFFFFFFF), "a row without a label"
        vecio.write_bin(os.path.join(root, "labels.ibin"), out.reshape(-1, 1))
        counts = np.bincount(out, minlength=len(names))
        manifest["labels"] = {"column": other[0], "names": [str(n) for n in names],
                              "share": {str(n): float(c) / len(out) for n, c in zip(names, counts)}}
    return len(base_ids)


# ----- hdf5 (ann-benchmarks)


def convert_glove(root, manifest):
    import h5py

    with h5py.File(os.path.join(root, "raw", "glove-100-angular.hdf5"), "r") as f:
        manifest["hdf5_attrs"] = {k: str(v) for k, v in f.attrs.items()}
        vecio.write_bin(os.path.join(root, "base.fbin"), np.asarray(f["train"], dtype=np.float32))
        vecio.write_bin(os.path.join(root, "query.fbin"), np.asarray(f["test"], dtype=np.float32))
        os.makedirs(os.path.join(root, "vendor"), exist_ok=True)
        vecio.write_bin(os.path.join(root, "vendor", "neighbors.ibin"), np.asarray(f["neighbors"], dtype=np.uint32))
        n = f["train"].shape[0]
    return n


# ----- csr (big-ann sparse track): kept as raw CSR, decompressed


def convert_splade(root, manifest):
    raw = os.path.join(root, "raw")
    for src, dst in [("base_small.csr.gz", "base.csr"), ("queries.dev.csr.gz", "query.csr")]:
        with gzip.open(os.path.join(raw, src), "rb") as fi, open(os.path.join(root, dst), "wb") as fo:
            while chunk := fi.read(1 << 22):
                fo.write(chunk)
    with open(os.path.join(root, "base.csr"), "rb") as f:
        nrow, ncol, nnz = np.fromfile(f, dtype="<i8", count=3)
    manifest["csr"] = {"rows": int(nrow), "dims": int(ncol), "nnz": int(nnz),
                       "layout": "i64 nrow, ncol, nnz; i64 indptr[nrow+1]; i32 indices[nnz]; f32 data[nnz]"}
    with open(os.path.join(raw, "base_small.dev.gt"), "rb") as f:
        n, k = np.fromfile(f, dtype="<u4", count=2)
        ids = np.fromfile(f, dtype="<u4", count=int(n) * int(k)).reshape(int(n), int(k))
        dist = np.fromfile(f, dtype="<f4", count=int(n) * int(k)).reshape(int(n), int(k))
    os.makedirs(os.path.join(root, "vendor"), exist_ok=True)
    vecio.write_bin(os.path.join(root, "vendor", "dev_gt.ibin"), ids)
    vecio.write_bin(os.path.join(root, "vendor", "dev_gt.scores.fbin"), dist)
    return int(nrow)


def main():
    name, root = sys.argv[1], sys.argv[2]
    manifest = vecio.load_manifest(root)
    manifest["set"] = name
    manifest["licence"] = LICENCES[name]
    with open(os.path.join(root, "source.sha256")) as f:
        manifest["sources"] = dict(reversed(line.split()) for line in f if line.strip())
    if name == "glove-100":
        n = convert_glove(root, manifest)
    elif name == "cohere-1m":
        n = convert_vdbb(name, root, "train.parquet", manifest)
    elif name == "openai-500k":
        n = convert_vdbb(name, root, "shuffle_train.parquet", manifest)
    elif name == "splade-small":
        n = convert_splade(root, manifest)
    else:
        raise SystemExit(f"unknown set {name}")
    manifest["rows"] = n
    manifest["columns"] = {"seed": COLUMN_SEED, "u": "uniform int [0,1000)", "tags": "bit0 a p=0.1, bit1 b p=0.1"}
    vecio.write_bin(os.path.join(root, "cols.ibin"), synthetic_columns(n))
    if os.path.exists(os.path.join(root, "base.fbin")):
        base = vecio.read_bin(os.path.join(root, "base.fbin"))
        manifest["dim"] = base.shape[1]
        manifest["queries"] = vecio.read_bin(os.path.join(root, "query.fbin")).shape[0]
        manifest["norms"] = norm_stats(base)
    manifest["files"] = vecio.hash_tree(root)
    vecio.save_manifest(root, manifest)
    print(json.dumps({k: v for k, v in manifest.items() if k != "files"}, indent=1, default=str))


if __name__ == "__main__":
    main()
