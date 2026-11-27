"""Ground truth in float64, outside the crate: brute force with numpy, per operation and filter case.
Never uses Nabedra's `exact` (contract 6.3).

    python -I gt.py <set dir> [--rows N] [--queries Q] [--k 100] [--ops cosine,dot,l2]

Writes <set dir>/gt-<N>-<Q>/: <op>.<case>.ibin (Q x k base positions, best first, ties by position),
<op>.<case>.scores.fbin (the score of each: cosine, dot product, or Euclidean distance for l2),
cases.txt (one case per line, `<name> <expr>`, read by bench_local.rs and compete.py) and info.json
(matching rows per case, and agreement with the ground truth shipped with the set).

Cases: `all`; `u<100`, `u<10`, `u<1` (10 %, 1 %, 0.1 %); `ab` (tags a and b, 1 %, two independent
10 % conditions); `id>=X` (VectorDBBench int filters, 99 % and 1 % pass); `label=C` per label code.
Set OPENBLAS_NUM_THREADS to limit the BLAS threads (default 4: the machine is shared).
"""

import argparse
import json
import os
import sys
import time

os.environ.setdefault("OPENBLAS_NUM_THREADS", "4")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np  # noqa: E402

import vecio  # noqa: E402

CHUNK = 25_000


def cases_of(root, n, manifest):
    cols = np.asarray(vecio.read_bin(os.path.join(root, "cols.ibin"), rows=n))
    u, tags = cols[:, 0], cols[:, 1]
    ids = np.arange(n)
    out = [
        ("all", "all", None),
        ("u10", "u<100", u < 100),
        ("u1", "u<10", u < 10),
        ("u01", "u<1", u < 1),
        ("ab", "ab", tags == 3),
        ("id99", f"id>={n // 100}", ids >= n // 100),
        ("id1", f"id>={n - n // 100}", ids >= n - n // 100),
    ]
    path = os.path.join(root, "labels.ibin")
    if os.path.exists(path):
        labels = np.asarray(vecio.read_bin(path, rows=n))[:, 0]
        names = manifest.get("labels", {}).get("names", [])
        for code, name in enumerate(names):
            out.append((f"label_{name}", f"label={code}", labels == code))
    return out


def goodness(op, dots, qn, xn, qn2, xn2):
    if op == "dot":
        return dots
    if op == "cosine":
        with np.errstate(divide="ignore", invalid="ignore"):
            g = dots / np.outer(qn, xn)
        g[:, xn == 0.0] = -np.inf
        return np.clip(g, -1.0, 1.0, out=g)
    # l2: the negated distance is the goodness.
    sq = qn2[:, None] + xn2[None, :] - 2.0 * dots
    return -np.sqrt(np.maximum(sq, 0.0, out=sq), out=sq)


def merge(best_g, best_i, g, ids, k):
    """Keeps the k best of the running top and a new block."""
    if g.shape[1] > k:
        part = np.argpartition(-g, k - 1, axis=1)[:, :k]
        g = np.take_along_axis(g, part, axis=1)
        ids = ids[part]
    else:
        ids = np.broadcast_to(ids, g.shape)
    all_g = np.concatenate([best_g, g], axis=1)
    all_i = np.concatenate([best_i, ids], axis=1)
    if all_g.shape[1] > k:
        part = np.argpartition(-all_g, k - 1, axis=1)[:, :k]
        all_g = np.take_along_axis(all_g, part, axis=1)
        all_i = np.take_along_axis(all_i, part, axis=1)
    return all_g, all_i


def ordered(g, ids):
    """Best first, ties by position (the index breaks ties by key, which follows the position)."""
    order = np.lexsort((ids, -g), axis=1)
    return np.take_along_axis(g, order, axis=1), np.take_along_axis(ids, order, axis=1)


def recall_against(ours_g, theirs, base_g_lookup, k=10):
    """Share of the shipped top-k found in our top-k, counting a tie with our k-th as found."""
    hits = want = 0
    for q in range(theirs.shape[0]):
        kth = ours_g[q, k - 1]
        top = [int(j) for j in theirs[q, :k] if j != vecio.MISSING]
        want += len(top)
        hits += sum(1 for j in top if base_g_lookup(q, j) >= kth - 1e-9 * max(1.0, abs(kth)))
    return hits / max(want, 1)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("root")
    p.add_argument("--rows", type=int)
    p.add_argument("--queries", type=int, default=1000)
    p.add_argument("--k", type=int, default=100)
    p.add_argument("--ops", default="cosine,dot,l2")
    a = p.parse_args()
    manifest = vecio.load_manifest(a.root)
    base = vecio.read_bin(os.path.join(a.root, "base.fbin"), rows=a.rows)
    n = base.shape[0]
    queries = np.asarray(vecio.read_bin(os.path.join(a.root, "query.fbin"), rows=a.queries), dtype=np.float64)
    nq, k = queries.shape[0], a.k
    out = os.path.join(a.root, f"gt-{n}-{nq}")
    os.makedirs(out, exist_ok=True)
    ops = a.ops.split(",")
    cases = cases_of(a.root, n, manifest)
    qn2 = (queries * queries).sum(axis=1)
    qn = np.sqrt(qn2)
    best = {(op, c[0]): (np.full((nq, 0), -np.inf), np.zeros((nq, 0), dtype=np.int64)) for op in ops for c in cases}
    started = time.time()
    for s in range(0, n, CHUNK):
        x = np.asarray(base[s : s + CHUNK], dtype=np.float64)
        xn2 = (x * x).sum(axis=1)
        xn = np.sqrt(xn2)
        dots = queries @ x.T
        ids = np.arange(s, s + x.shape[0])
        for op in ops:
            g = goodness(op, dots.copy(), qn, xn, qn2, xn2)
            for name, _, mask in cases:
                if mask is None:
                    gg, ii = g, ids
                else:
                    m = mask[s : s + x.shape[0]]
                    if not m.any():
                        continue
                    gg, ii = g[:, m], ids[m]
                best[op, name] = merge(*best[op, name], gg, ii, k)
        print(f"rows {s + x.shape[0]}/{n} {time.time() - started:.0f}s", flush=True)
    info = {"rows": n, "queries": nq, "k": k, "seconds": round(time.time() - started, 1), "cases": {}}
    with open(os.path.join(out, "cases.txt"), "w") as f:
        for name, expr, mask in cases:
            matching = n if mask is None else int(mask.sum())
            f.write(f"{name} {expr}\n")
            info["cases"][name] = {"expr": expr, "matching": matching, "share": matching / n}
    for (op, name), (g, ids) in best.items():
        g, ids = ordered(g, ids)
        score = -g if op == "l2" else g
        vecio.write_bin(os.path.join(out, f"{op}.{name}.ibin"), ids.astype(np.uint32))
        vecio.write_bin(os.path.join(out, f"{op}.{name}.scores.fbin"), score.astype(np.float32))
        if ids.shape[1] < min(k, 10):
            info["cases"][name][f"short_{op}"] = int(ids.shape[1])
    # Agreement with the shipped ground truth (only meaningful for the full base).
    vendor = os.path.join(a.root, "vendor")
    metric = "cosine" if "cosine" in ops else None
    if metric and os.path.isdir(vendor):
        checks = {}
        for f in sorted(os.listdir(vendor)):
            if not f.endswith(".ibin") or f.endswith("scores.fbin"):
                continue
            theirs = np.asarray(vecio.read_bin(os.path.join(vendor, f)))[:nq]
            if theirs[theirs != vecio.MISSING].max() >= n:
                checks[f] = "skipped: names rows beyond --rows"
                continue

            def lookup(q, j):
                v = np.asarray(base[j], dtype=np.float64)
                nv = np.sqrt(v @ v)
                return -np.inf if nv == 0 else float(queries[q] @ v / (qn[q] * nv))

            checks[f] = {}
            for name, _, _ in cases:
                g, _ = ordered(*best[metric, name])
                if g.shape[1] >= 10:
                    checks[f][name] = round(recall_against(g, theirs, lookup), 4)
        info["vendor_recall_at_10"] = checks
    with open(os.path.join(out, "info.json"), "w") as f:
        json.dump(info, f, indent=1)
        f.write("\n")
    manifest["files"] = vecio.hash_tree(a.root)
    vecio.save_manifest(a.root, manifest)
    print(json.dumps(info, indent=1))


if __name__ == "__main__":
    main()
