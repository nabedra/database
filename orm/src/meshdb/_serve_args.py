"""Private serve validation, without runtime startup or config-content reads."""
import functools
import inspect
import os

from .errors import ValidationError
from .validation import validate_identifier


_CONFIG_FAILURE = "MESHDB_CONFIG must name an explicit readable regular file."


def _is_async_callback(callback: object) -> bool:
    # inspect handles partial-wrapped functions; unwrap also covers partials of
    # callable objects whose async behavior is declared on __call__.
    while isinstance(callback, functools.partial):
        callback = callback.func
    return inspect.iscoroutinefunction(callback) or inspect.iscoroutinefunction(
        getattr(callback, "__call__", None)
    )


def _validate_serve_args(
    name: str, nodes: int | tuple[int, int] = 1,
    start_node=None, stop_node=None, list_nodes=None,
) -> dict:
    """Return name, initial_policy, provisioning and original callback tuple.

    The callback tuple is ordered start/stop/list. It is private runtime state,
    never a wire record or diagnostic. Awaitable returns are checked by the
    bridge, since declaration validation must never invoke a callback.
    """
    name = validate_identifier(name)
    callbacks = (start_node, stop_node, list_nodes)
    managed = any(callback is not None for callback in callbacks)
    if managed and any(
        not callable(callback) or _is_async_callback(callback)
        for callback in callbacks
    ):
        raise ValidationError("Callbacks must be all synchronous callables or all None")

    if type(nodes) is int and 0 <= nodes <= 1024:
        mode, desired, low, high = "fixed", nodes, nodes, nodes
    elif (
        type(nodes) is tuple and len(nodes) == 2
        and all(type(count) is int for count in nodes)
        and 1 <= nodes[0] <= nodes[1] <= 1024
    ):
        if not managed:
            raise ValidationError("Tuple nodes require managed callbacks")
        low, high = nodes
        mode, desired = "autoscale", low
    else:
        raise ValidationError(
            "Nodes must be an exact integer in 0..1024 or a two-integer tuple "
            "with 1 <= min <= max <= 1024"
        )
    return {
        "name": name,
        "initial_policy": {
            "mode": mode, "desired": desired, "min_nodes": low, "max_nodes": high,
        },
        "provisioning": "managed" if managed else "external",
        "callbacks": callbacks,
    }


def _resolve_config_path() -> str:
    """Resolve only the explicit literal MESHDB_CONFIG path, without opening it.

    Relative paths use the current directory. No dotenv, PATH search, variable
    or tilde expansion is performed. Rust validates JSON and credential files.
    """
    try:
        candidate = os.environ.get("MESHDB_CONFIG")
        if candidate:
            path = os.path.abspath(candidate)
            if os.path.isfile(path) and os.access(path, os.R_OK):
                return path
    except (OSError, ValueError):
        pass
    raise ValidationError(_CONFIG_FAILURE) from None
