#!/bin/sh
# T9: run `nabedra-db` with `direct_oltp: false` for every `serve` that does not pass its
# own execution configuration, so suites that start their own server (both ORM suites,
# oltp_process.py) can run with the direct path off. Everything else passes through.
#   NABEDRA_DIRECT_OFF_BINARY   the real nabedra-db
set -eu
binary="${NABEDRA_DIRECT_OFF_BINARY:?set NABEDRA_DIRECT_OFF_BINARY to the nabedra-db binary}"
if [ "${1:-}" = serve ]; then
    case " $* " in
        *" --execution-config "*) exec "$binary" "$@" ;;
    esac
    config="${TMPDIR:-/tmp}/nabedra-direct-off-$$.json"
    # ExecutionConfig::default() with direct_oltp off.
    cat > "$config" <<'JSON'
{"max_workers_per_request":4,"max_pending_requests":1024,"max_queue_bytes_per_replica":268435456,
 "max_retained_result_bytes_per_worker":67108864,"result_retention_ms":300000,
 "queue_replication":"all_ready_workers","queue_lifetime":"live_cluster","partition_size":"64 MiB",
 "direct_oltp":false}
JSON
    exec "$binary" "$@" --execution-config "$config"
fi
exec "$binary" "$@"
