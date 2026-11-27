"""A small competitor client (contract 6.2): loads a set into Qdrant, pgvector or Milvus, sweeps the
system's search parameter and prints JSON lines in the format of bench_local.rs, scored against the
same numpy ground truth (gt.py). Run it through run_compete.sh, which starts the pinned container
and pins this client to other CPUs.

    python -I compete.py <qdrant|pgvector|milvus> --data DIR --gt DIR --op cosine
        [--cases all,u1] [--params 16,32,64,128,256] [--threads 1,4,8] [--qps-seconds 10]
        [--meta FILE] [--out FILE] [--label TEXT] [--skip-load]

Index under test (contract 6.2): HNSW m=16, ef_construct 128 in all three; the swept parameter is
Qdrant `hnsw_ef`, pgvector `hnsw.ef_search` (iterative scans on for filters), Milvus `ef`.
"""

import argparse
import json
import multiprocessing as mp
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np  # noqa: E402

import vecio  # noqa: E402

K = 10
BATCH = 2000
M, EF_CONSTRUCT = 16, 128


def parse_case(expr):
    """`all`, `u<X`, `ab`, `id>=X`, `label=C` as (kind, value)."""
    if expr == "all":
        return None
    if expr == "ab":
        return ("ab", None)
    for prefix, kind in (("u<", "u"), ("id>=", "id"), ("label=", "label")):
        if expr.startswith(prefix):
            return (kind, int(expr[len(prefix):]))
    raise SystemExit(f"unknown case {expr}")


# ----- Adapters. Each turns hits into (position, score in gt.py's units).


class Qdrant:
    name, param = "qdrant", "ef"

    def __init__(self, op, dim):
        from qdrant_client import QdrantClient

        self.op, self.dim = op, dim
        self.c = QdrantClient(host="127.0.0.1", port=6333, grpc_port=6334, prefer_grpc=True, timeout=600)

    def load(self, base, cols, labels):
        from qdrant_client import models as m

        dist = {"cosine": m.Distance.COSINE, "dot": m.Distance.DOT, "l2": m.Distance.EUCLID}[self.op]
        if self.c.collection_exists("bench"):
            self.c.delete_collection("bench")
        self.c.create_collection(
            "bench",
            vectors_config=m.VectorParams(size=self.dim, distance=dist),
            hnsw_config=m.HnswConfigDiff(m=M, ef_construct=EF_CONSTRUCT),
            # Small segments stay plain (brute force) below the threshold; 1 kB indexes every one,
            # so the swept `hnsw_ef` is what is measured. (0 would turn indexing off.)
            optimizers_config=m.OptimizersConfigDiff(indexing_threshold=1),
        )
        for field, schema in (("u", "integer"), ("id", "integer"), ("label", "integer"), ("tags", "keyword")):
            self.c.create_payload_index("bench", field, field_schema=schema)
        for s in range(0, base.shape[0], BATCH):
            vecs = np.asarray(base[s : s + BATCH])
            payload = [_payload(s + i, cols, labels) for i in range(vecs.shape[0])]
            self.c.upload_collection("bench", vectors=vecs, payload=payload, ids=list(range(s, s + vecs.shape[0])),
                                     batch_size=500, parallel=1, wait=True)
        while True:
            info = self.c.get_collection("bench")
            green = str(info.status).lower().endswith("green")
            if green and (info.indexed_vectors_count or 0) >= (info.points_count or 0):
                return
            time.sleep(2)

    def filter(self, case):
        from qdrant_client import models as m

        if case is None:
            return None
        kind, v = case
        if kind == "ab":
            return m.Filter(must=[m.FieldCondition(key="tags", match=m.MatchValue(value=t)) for t in "ab"])
        if kind == "u":
            return m.Filter(must=[m.FieldCondition(key="u", range=m.Range(lt=v))])
        if kind == "id":
            return m.Filter(must=[m.FieldCondition(key="id", range=m.Range(gte=v))])
        return m.Filter(must=[m.FieldCondition(key="label", match=m.MatchValue(value=v))])

    def search(self, q, flt, ef):
        from qdrant_client import models as m

        res = self.c.query_points("bench", query=q.tolist(), limit=K, query_filter=flt,
                                  search_params=m.SearchParams(hnsw_ef=ef), with_payload=False)
        return [(int(p.id), float(p.score)) for p in res.points]


def _payload(pos, cols, labels):
    tags = [t for bit, t in ((1, "a"), (2, "b")) if cols[pos, 1] & bit]
    return {"u": int(cols[pos, 0]), "tags": tags, "id": pos, "label": int(labels[pos]) if labels is not None else 0}


class Pgvector:
    name, param = "pgvector", "ef_search"
    OPS = {"cosine": ("vector_cosine_ops", "<=>"), "dot": ("vector_ip_ops", "<#>"), "l2": ("vector_l2_ops", "<->")}

    def __init__(self, op, dim):
        import psycopg

        self.op, self.dim = op, dim
        self.c = psycopg.connect("host=127.0.0.1 port=5432 user=postgres password=bench dbname=postgres",
                                 autocommit=True)
        self.c.execute("CREATE EXTENSION IF NOT EXISTS vector")
        self.ef = None

    def load(self, base, cols, labels):
        ops, _ = self.OPS[self.op]
        c = self.c
        c.execute("DROP TABLE IF EXISTS bench")
        c.execute(f"CREATE TABLE bench (id int PRIMARY KEY, u int, tags text[], label int, emb vector({self.dim}))")
        with c.cursor().copy("COPY bench (id, u, tags, label, emb) FROM STDIN") as cp:
            for pos in range(base.shape[0]):
                p = _payload(pos, cols, labels)
                vec = "[" + ",".join(f"{x:.9g}" for x in base[pos]) + "]"
                cp.write_row((pos, p["u"], p["tags"], p["label"], vec))
        for col in ("u", "label"):
            c.execute(f"CREATE INDEX ON bench ({col})")
        c.execute("CREATE INDEX ON bench USING gin (tags)")
        c.execute("SET maintenance_work_mem = '5GB'")
        c.execute("SET max_parallel_maintenance_workers = 7")
        c.execute(f"CREATE INDEX ON bench USING hnsw (emb {ops}) WITH (m = {M}, ef_construction = {EF_CONSTRUCT})")
        c.execute("VACUUM ANALYZE bench")

    def filter(self, case):
        if case is None:
            return ""
        kind, v = case
        return {"ab": "WHERE tags @> ARRAY['a','b']", "u": f"WHERE u < {v}", "id": f"WHERE id >= {v}",
                "label": f"WHERE label = {v}"}[kind]

    def search(self, q, where, ef):
        if ef != self.ef:
            self.c.execute(f"SET hnsw.ef_search = {int(ef)}")
            self.c.execute(f"SET hnsw.iterative_scan = {'relaxed_order' if where else 'off'}")
            self.ef = ef
        _, sym = self.OPS[self.op]
        vec = "[" + ",".join(f"{x:.9g}" for x in q) + "]"
        rows = self.c.execute(f"SELECT id, emb {sym} %s::vector AS d FROM bench {where} ORDER BY d LIMIT {K}",
                              (vec,)).fetchall()
        conv = {"cosine": lambda d: 1.0 - d, "dot": lambda d: -d, "l2": lambda d: d}[self.op]
        return [(int(i), conv(float(d))) for i, d in rows]


class Milvus:
    name, param = "milvus", "ef"

    def __init__(self, op, dim):
        from pymilvus import MilvusClient

        self.op, self.dim = op, dim
        self.c = MilvusClient(uri="http://127.0.0.1:19530")
        self.metric = {"cosine": "COSINE", "dot": "IP", "l2": "L2"}[op]

    def load(self, base, cols, labels):
        from pymilvus import DataType

        if self.c.has_collection("bench"):
            self.c.drop_collection("bench")
        s = self.c.create_schema(auto_id=False)
        s.add_field("id", DataType.INT64, is_primary=True)
        s.add_field("emb", DataType.FLOAT_VECTOR, dim=self.dim)
        s.add_field("u", DataType.INT64)
        s.add_field("label", DataType.INT64)
        s.add_field("tags", DataType.ARRAY, element_type=DataType.VARCHAR, max_capacity=2, max_length=1)
        self.c.create_collection("bench", schema=s)
        for st in range(0, base.shape[0], BATCH):
            vecs = np.asarray(base[st : st + BATCH])
            rows = [dict(_payload(st + i, cols, labels), emb=vecs[i].tolist()) for i in range(vecs.shape[0])]
            self.c.insert("bench", rows)
        self.c.flush("bench")
        ix = self.c.prepare_index_params()
        ix.add_index("emb", index_type="HNSW", metric_type=self.metric, params={"M": M, "efConstruction": EF_CONSTRUCT})
        for f in ("u", "label", "tags"):
            ix.add_index(f, index_type="INVERTED")
        self.c.create_index("bench", ix)
        self.c.load_collection("bench")

    def filter(self, case):
        if case is None:
            return ""
        kind, v = case
        return {"ab": "array_contains_all(tags, ['a', 'b'])", "u": f"u < {v}", "id": f"id >= {v}",
                "label": f"label == {v}"}[kind]

    def search(self, q, expr, ef):
        res = self.c.search("bench", data=[q.tolist()], limit=K, filter=expr,
                            search_params={"metric_type": self.metric, "params": {"ef": max(ef, K)}})
        conv = (lambda d: float(np.sqrt(max(d, 0.0)))) if self.op == "l2" else float
        return [(int(h["id"]), conv(h["distance"])) for h in res[0]]


SYSTEMS = {"qdrant": Qdrant, "pgvector": Pgvector, "milvus": Milvus}


# ----- Measurement (as bench_local.rs `found` and `measure`)


def found(op, hits, ids, scores):
    want = min(K, ids.shape[0])
    top = set(int(i) for i in ids[:want])
    kth = float(scores[want - 1])
    good = (lambda s: -s) if op == "l2" else (lambda s: s)
    tol = 1e-6 * max(abs(kth), 1.0)
    strict = sum(1 for p, _ in hits[:K] if p in top)
    tied = sum(1 for p, s in hits[:K] if p in top or good(s) >= good(kth) - tol)
    return min(tied, want), strict, want


def worker(system, op, dim, queries, flt_case, param, seconds, start, counter, barrier):
    s = SYSTEMS[system](op, dim)
    flt = s.filter(flt_case)
    s.search(queries[start % len(queries)], flt, param)
    i = start
    barrier.wait()
    deadline = time.time() + seconds
    while time.time() < deadline:
        s.search(queries[i % len(queries)], flt, param)
        i += 1
        with counter.get_lock():
            counter.value += 1


def docker_cpu(container):
    try:
        out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}} {{.MemUsage}}", container],
                             capture_output=True, text=True, timeout=30).stdout.strip()
        return out
    except (OSError, subprocess.TimeoutExpired):
        return ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("system", choices=sorted(SYSTEMS))
    ap.add_argument("--data", required=True)
    ap.add_argument("--gt", required=True)
    ap.add_argument("--op", default="cosine")
    ap.add_argument("--queries", type=int)
    ap.add_argument("--cases")
    ap.add_argument("--params", default="16,32,64,128,256,512")
    ap.add_argument("--threads", default="1,4,8")
    ap.add_argument("--qps-seconds", type=float, default=10)
    ap.add_argument("--meta")
    ap.add_argument("--out")
    ap.add_argument("--label", default="")
    ap.add_argument("--container", default="")
    ap.add_argument("--skip-load", action="store_true")
    a = ap.parse_args()
    rows = int(os.path.basename(a.gt.rstrip("/")).split("-")[1])
    base = vecio.read_bin(os.path.join(a.data, "base.fbin"), rows=rows)
    queries = np.asarray(vecio.read_bin(os.path.join(a.data, "query.fbin"), rows=a.queries))
    cols = np.asarray(vecio.read_bin(os.path.join(a.data, "cols.ibin"), rows=rows))
    lp = os.path.join(a.data, "labels.ibin")
    labels = np.asarray(vecio.read_bin(lp, rows=rows))[:, 0] if os.path.exists(lp) else None
    with open(os.path.join(a.gt, "cases.txt")) as f:
        cases = [line.split(" ", 1) for line in f.read().splitlines() if line]
    if a.cases:
        cases = [c for c in cases if c[0] in a.cases.split(",")]
    meta = open(a.meta).read().strip() if a.meta else "{}"
    out = open(a.out, "a") if a.out else sys.stdout
    common = {"label": a.label, "set": os.path.basename(a.data.rstrip("/")), "rows": rows,
              "dim": base.shape[1], "system": a.system}

    def line(kind, fields):
        out.write(json.dumps({"kind": kind, **common, **fields})[:-1] + ',"meta":' + meta + "}\n")
        out.flush()

    s = SYSTEMS[a.system](a.op, base.shape[1])
    if not a.skip_load:
        t = time.time()
        s.load(base, cols, labels)
        line("build", {"op": a.op, "load_and_index_seconds": round(time.time() - t, 1),
                       "rows_per_second": round(rows / (time.time() - t)), "docker": docker_cpu(a.container)})
    threads = [int(x) for x in a.threads.split(",")]
    for name, expr in cases:
        flt = s.filter(parse_case(expr.strip()))
        ids = np.asarray(vecio.read_bin(os.path.join(a.gt, f"{a.op}.{name}.ibin")))
        scores = np.asarray(vecio.read_bin(os.path.join(a.gt, f"{a.op}.{name}.scores.fbin")))
        for param in [int(x) for x in a.params.split(",")]:
            lat, tied, strict, want = [], 0, 0, 0
            for i, q in enumerate(queries):
                t = time.perf_counter()
                hits = s.search(q, flt, param)
                lat.append((time.perf_counter() - t) * 1e3)
                f, st, w = found(a.op, hits, ids[i], scores[i])
                tied, strict, want = tied + f, strict + st, want + w
            lat.sort()
            pct = lambda p: lat[max(0, int(np.ceil(p / 100 * len(lat))) - 1)]  # noqa: E731
            qps, cpu = {}, ""
            for n in threads:
                counter, barrier = mp.Value("q", 0), mp.Barrier(n + 1)
                procs = [mp.Process(target=worker, args=(a.system, a.op, base.shape[1], queries,
                                                          parse_case(expr.strip()), param, a.qps_seconds,
                                                          j * len(queries) // n, counter, barrier)) for j in range(n)]
                for p in procs:
                    p.start()
                barrier.wait()
                t = time.time()
                time.sleep(min(a.qps_seconds / 2, 5))
                if n == max(threads):
                    cpu = docker_cpu(a.container)
                for p in procs:
                    p.join()
                qps[str(n)] = round(counter.value / (time.time() - t), 1)
            line("point", {"op": a.op, "case": name, "stop": s.param, s.param: param, "limit": K,
                           "queries": len(queries), "recall": round(tied / want, 4),
                           "recall_ids": round(strict / want, 4), "mean_ms": round(float(np.mean(lat)), 3),
                           "p50_ms": round(pct(50), 3), "p95_ms": round(pct(95), 3), "p99_ms": round(pct(99), 3),
                           "qps": qps, "docker_at_peak": cpu})


if __name__ == "__main__":
    main()
