#!/usr/bin/env bash
# Prints the RD3 controlled-host tables (all finished runs) and the serial stage breakdown.
L=$HOME/.t3/worktrees/nabedra-database/nabedra-hardening-4cde81a5fa19a0c7/.nabedra/workflows/hardening/rd3
cd $L/runs
python3 $L/analyze.py $(ls -d fixed-* 2>/dev/null | grep -v '\.') 2>&1
python3 $L/analyze.py $(ls -d serial-* 2>/dev/null | grep -v '\.') --stages 2>&1 | grep -E '^##|^0.2|http.dispatch.total|client_write|propose_to|apply_entry|pool.data_apply.commit|raft.data.append |append_encode|net.append |net.append_handle|read_stored|read_settled'
