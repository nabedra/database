#!/usr/bin/env python3
"""Render the actual process assertion artifact, without performance claims."""
import hashlib
import html
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
data = json.loads(path.read_text())
assert data["result"] == "passed"
assert data["process_tests"]["passed"] == 5
assert data["cleanup"]["started"] == data["cleanup"]["reaped"]
escape = html.escape
digest = hashlib.sha256(path.read_bytes()).hexdigest()
nodes = [
    (40, 135, 180, 95, "API process", "Owns no queue", "API restart tested", "#e4eff8"),
    (290, 135, 240, 95, "Owner worker RAM", "Original body + auth + pin", "Exact request/version identity", "#dcefe8"),
    (650, 135, 240, 95, "Every ready worker RAM", "Payload, then accepted version", "Same-run survivor takeover", "#dcefe8"),
    (650, 340, 240, 90, "Joining worker", "Paged snapshot + sealed delta", "Unready until published", "#e4eff8"),
    (290, 340, 240, 90, "Fixture authority + reads", "Explicit test-supplied fences", "Control/TSO/P5 not integrated", "#fff0cf"),
]
parts = [f'''<svg xmlns="http://www.w3.org/2000/svg" width="960" height="720" viewBox="0 0 960 720">
<metadata>{escape(json.dumps({"artifact_sha256": digest, "renderer_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}))}</metadata>
<defs><marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0,0 L8,4 L0,8 Z" fill="#365f6e"/></marker></defs>
<rect width="960" height="720" fill="#fafcfb"/>
<g font-family="system-ui,sans-serif" fill="#18333c">
<text x="40" y="43" font-size="24" font-weight="650">Worker-memory queue process assertions</text>
<text x="40" y="72" font-size="14">Real mTLS and OS-process failures. Authority and read backend are supplied by the fixture.</text>
''']
for x, y, w, h, title, line1, line2, color in nodes:
    parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="9" fill="{color}" stroke="#aac0c4"/>')
    for offset, text, size, weight in [(26, title, 16, 650), (49, line1, 12, 400), (70, line2, 12, 400)]:
        parts.append(f'<text x="{x + 13}" y="{y + offset}" font-size="{size}" font-weight="{weight}">{escape(text)}</text>')

def arrow(x1, y1, x2, y2, label, label_x, label_y):
    parts.append(f'<path d="M{x1},{y1} L{x2},{y2}" fill="none" stroke="#365f6e" stroke-width="2" marker-end="url(#arrow)"/>')
    parts.append(f'<text x="{label_x}" y="{label_y}" font-size="12">{escape(label)}</text>')

arrow(220, 154, 290, 154, "submit", 231, 142)
arrow(530, 150, 650, 150, "1. install payload", 535, 122)
arrow(650, 180, 530, 180, "all ACKs", 550, 171)
arrow(530, 205, 650, 205, "2. accepted version", 534, 254)
arrow(650, 220, 530, 220, "all ACKs", 550, 237)
arrow(290, 211, 220, 211, "then 202", 232, 200)
arrow(770, 230, 770, 340, "copy before readiness", 787, 293)
arrow(410, 340, 410, 230, "fresh authority", 423, 294)
parts.append('<text x="40" y="480" font-size="17" font-weight="650">Executed assertions</text>')
assertions = [
    "Q03/R01/Q06/C07: ACK barriers, owner kill, exact terminal replay, zero compute during network waits.",
    "Q04: API-only process restart and lost submit reply preserve the worker request sequence and context.",
    "R03/Q03: empty, single and multiple pages, live delta, readiness gate and last-holder takeover.",
    "U02/M02: a valid full 8 MiB operation copies and joins under the default 512 MiB task budget.",
    "U02: maximum logical predicate depth passes the real transport; one level over installs no record.",
    f'{data["cleanup"]["started"]} child processes started and reaped. Full-stop restart abandons old outstanding identities.',
]
for index, assertion in enumerate(assertions):
    parts.append(f'<text x="40" y="{507 + index * 23}" font-size="13">{escape(assertion)}</text>')
parts.append(f'<text x="40" y="676" font-size="11">Test binary SHA-256: {escape(data["binary_sha256"])}</text>')
parts.append('<text x="40" y="697" font-size="11">Correctness fixture. No production Control-Raft/P5, remote-command or performance claim.</text></g></svg>')
svg = path.with_suffix(".svg")
svg.write_text("\n".join(parts))
page = path.with_suffix(".html")
page.write_text(f'''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Worker queue process evidence</title>
<style>body{{margin:32px auto;max-width:1000px;background:#fafcfb;font:15px system-ui;color:#18333c}}figure{{margin:0}}svg{{width:100%;height:auto}}pre{{overflow:auto;background:#edf3f2;padding:18px;border-radius:8px}}</style>
<main><h1>Worker queue process evidence</h1><p>The process assertions use real mTLS and worker/API process loss with fixture-supplied authority and deterministic reads.</p>
<figure aria-label="Actual queue acceptance, takeover and join path with executed assertions">{svg.read_text()}</figure>
<details><summary>Exact configuration and provenance</summary><pre>{escape(json.dumps(data, indent=2))}</pre></details></main></html>''')
print(json.dumps({"svg": str(svg), "html": str(page), "artifact_sha256": digest}))
