#!/usr/bin/env python3.11
"""BENCH3: each worker's nabedra-kv storage counters at the end of a soak's last phase, and write
amplification = (flush + compaction + journal bytes) / user bytes. usage: storage_soak.py <soak run>..."""
import json, sys
from pathlib import Path
out = {}
for arg in sys.argv[1:]:
    run = Path(arg); d = json.loads((run / 'results.json').read_text())
    last = d['phases'][-1]['stage_stats_after']; rec = {'seed_seconds': d['seed_details']['seed']['seconds'],
                                                        'seed_rejected_retries': d['seed_details']['seed']['rejected_retries']}
    for w in ('w1', 'w2', 'w3'):
        s = (last.get(w) or {}).get('storage') or {}
        keep = {k: s.get(k) for k in ('user_bytes', 'flush_bytes', 'compaction_bytes', 'journal_bytes', 'journal_syncs',
                                      'stall_ms', 'write_delays', 'l0_runs_peak')}
        if s.get('user_bytes'):
            keep['write_amplification'] = round((s['flush_bytes'] + s['compaction_bytes'] + s['journal_bytes']) / s['user_bytes'], 2)
        rec[w] = keep
    out[run.name] = rec
print(json.dumps(out, indent=1))
