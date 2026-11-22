# Nabedra workflow ledgers

This orphan branch keeps workflow briefs, ledgers and reports after product worktrees are removed. Each workflow has its own top-level directory. Use `python3 scripts/testing/ledger_sync.py <name>` from a product checkout to publish UTF-8 Markdown and text from `.nabedra/workflows/<name>/`.

This branch is public. Keep credentials, private notes, logs and binaries out of it. Test evidence belongs on `main` through the evidence ingestion workflow.
