"""Own the API child and optional configured provider callback bridge."""
from __future__ import annotations

import secrets

from ._serve_args import _resolve_config_path, _validate_serve_args
from ._serve_guard import _reserve
from ._serve_process import _ExitReceipt, run_process
from .errors import ValidationError


def serve(name: str, nodes: int | tuple[int, int] = 1, *, start_node=None,
          stop_node=None, list_nodes=None, cold_start: bool = False,
          migrate_catalog: bool = False) -> None:
    """Block while the configured API runs; signals stop its owned process.

    Set MESHDB_CONFIG to the explicit API config file and MESHDB_SERVER_BINARY
    when the executable is not on PATH. A tuple enables bounded autoscaling
    only with all three synchronous provider callbacks. cold_start abandons
    old outstanding work after fencing its run. migrate_catalog confirms the
    caller stopped every legacy writer before enabling the catalog fence.
    """
    if type(cold_start) is not bool or type(migrate_catalog) is not bool:
        raise ValidationError('cold_start and migrate_catalog must be booleans')
    args = _validate_serve_args(name, nodes, start_node, stop_node, list_nodes)
    config = _resolve_config_path()
    reservation = _reserve()
    receipt = _ExitReceipt()
    bridge = None
    try:
        reservation._attach_process(receipt)
        if args['provisioning'] == 'managed':
            callback_token, authorize_token = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
            bridge = reservation._create_bridge(args['callbacks'], callback_token, authorize_token)
            wire_bridge = dict(url=bridge.url, callback_token=callback_token, authorize_token=authorize_token)
        else:
            wire_bridge = None
        startup = dict(version=1, type='init', name=args['name'],
                       initial_policy=args['initial_policy'], provisioning=args['provisioning'], bridge=wire_bridge)

        def prepared(record):
            if bridge is not None:
                bridge.activate(record['cluster_id'], record['authorize_url'])

        def stop():
            if bridge is not None:
                bridge.stop_admission()

        run_process(config, startup, prepared=prepared, stop=stop, _receipt=receipt,
                    cold_start=cold_start, migrate_catalog=migrate_catalog)
    finally:
        if not receipt._ready():
            receipt._retry()
        reservation._finish()
