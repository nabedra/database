"""Render measured RF1 ORM latency samples, with no inferred HA comparison."""
import argparse
from pathlib import Path
import json
import hashlib
import sys
from datetime import datetime, timezone


def main(directory):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    root = Path(directory)
    raw = (root / "rf1-process.json").read_bytes()
    evidence = json.loads(raw)
    source_hash = hashlib.sha256(raw).hexdigest()
    renderer_hash = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    assert evidence["result"] == "passed"
    growth_note = "<p>The unchanged ORM counted 110,001 rows before and after SIGKILL and decoded an explicit 100,001 limit correctly.</p>" if evidence["accepted_identity_assertions"].get("ORM_growth_count_before_and_after_SIGKILL") == 110001 else ""
    summary = evidence["benchmark"]["summary"]
    kinds = list(summary)
    fig, axis = plt.subplots(figsize=(8, 4.4))
    colors = ["#3569a8", "#7c97b5", "#d79746"]
    for series, p in enumerate((50, 95, 99)):
        x = [i + (series - 1) * 0.24 for i in range(len(kinds))]
        values = [summary[k][f"p{p}_ms"] for k in kinds]
        bars = axis.bar(x, values, width=0.22, label=f"p{p}", color=colors[series])
        axis.bar_label(bars, fmt="%.2f", fontsize=8, padding=3)
    axis.set_xticks(range(len(kinds)), [k.replace("_", " ") for k in kinds])
    axis.set_ylabel("End-to-end ORM latency (ms)")
    axis.set_title("Local RF1, after warm-up, three-row query table, one client")
    axis.set_ylim(top=max(summary[k]["p99_ms"] for k in kinds) * 1.22)
    axis.legend(frameon=False)
    axis.spines[["top", "right"]].set_visible(False)
    axis.grid(axis="y", alpha=.15)
    axis.set_axisbelow(True)
    fig.text(.1, .015, "30 warm-up calls; 3 measured repetitions × 90 calls. HTTP loopback. No PostgreSQL or HA comparison.", fontsize=8)
    fig.tight_layout(rect=(0, .035, 1, 1))
    fig.savefig(root / "rf1-orm-latency.svg", metadata={"Description": "Source rf1-process.json SHA-256 " + source_hash + "; renderer SHA-256 " + renderer_hash})
    (root / "rf1-plot-provenance.json").write_text(json.dumps({"source_json_sha256": source_hash, "renderer_sha256": renderer_hash, "matplotlib": matplotlib.__version__, "python": sys.version, "created_at_utc": datetime.now(timezone.utc).isoformat()}, indent=2) + "\n")
    plt.close(fig)
    svg = (root / "rf1-orm-latency.svg").read_text()
    svg = svg[svg.index("<svg"):]
    (root / "rf1-process.html").write_text('''<!doctype html><html lang="en"><meta charset="utf-8"><title>OLTP RF1 process evidence</title>
<style>body{font:16px system-ui;max-width:1000px;margin:40px auto;padding:0 24px;color:#203040}h1{font-size:28px}svg{max-width:100%;height:auto}.path{display:flex;gap:12px;align-items:center;margin:28px 0}.path span{border:1px solid #aab9c8;border-radius:6px;padding:14px}.path b{color:#6382a4}small{color:#526577;overflow-wrap:anywhere}</style>
<h1>Experimental OLTP RF1 process evidence</h1><p>All 13 unchanged ORM operations, authenticated receipts, exact mutation replay after SIGKILL, retained rows, stale handles, database scope and a verified TLS round trip passed in the recorded local process fixture.</p>
<div class="path"><span>Authenticated HTTP API</span><b>→</b><span>Bounded queue in RAM<br>One ready worker</span><b>→</b><span>Shared resource scheduler</span><b>→</b><span>Local RocksDB<br>Snapshot reads, sync WAL writes</span></div>
<p>Committed mutation decisions survive restart. A new run starts with an empty request queue. Raft and remote workers are outside this local runtime's measured assertions.</p>
''' + growth_note + '<p><small>Host conditions: ' + evidence['benchmark'].get('host_conditions', evidence.get('host_conditions', 'not recorded')) + '</small></p>' + svg + '<p><small>Binary SHA-256: ' + evidence["binary_sha256"] + '</small></p><p><small>Source JSON SHA-256: ' + source_hash + '<br>Renderer SHA-256: ' + renderer_hash + '</small></p></html>\n')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directory")
    main(parser.parse_args().directory)
