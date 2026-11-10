"""Shared local validation. Mapping results are detached, mutable copies.

These helpers raise local errors. Transport must translate invalid server data
into ProtocolError or UnknownOutcomeError as appropriate for the operation.
"""
from __future__ import annotations

import math
import re
from collections.abc import Mapping
from types import MappingProxyType
from typing import Literal, TypeAlias

from .errors import LimitExceededError, ValidationError

Scalar: TypeAlias = str | int | float | bool
Key: TypeAlias = str | int
Row: TypeAlias = dict[str, Scalar]
Schema: TypeAlias = dict[str, type]

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
MAX_COLUMNS = 128
_TYPE_TO_WIRE = MappingProxyType({str: "utf8", int: "int64", float: "float64", bool: "bool"})
_WIRE_TO_TYPE = MappingProxyType({wire: typ for typ, wire in _TYPE_TO_WIRE.items()})
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,63}")


def validate_identifier(value: object) -> str:
    if type(value) is not str or _IDENTIFIER.fullmatch(value) is None:
        raise ValidationError("Identifier must match [A-Za-z_][A-Za-z0-9_]{0,63}")
    return value


def validate_format(value: object) -> Literal["parquet"]:
    if type(value) is not str or value != "parquet":
        raise ValidationError("Only parquet format is supported")
    return "parquet"


def _mapping_copy(value: object, label: str) -> dict:
    if not isinstance(value, Mapping):
        raise ValidationError(f"{label} must be a mapping")
    return dict(value)


def validate_schema(schema: Mapping[str, type], *, key: str | None = None) -> Schema:
    """Copy and validate columns; validate the primary key when supplied."""
    result = _mapping_copy(schema, "Schema")
    if not result:
        raise ValidationError("Schema must contain at least one column")
    if len(result) > MAX_COLUMNS:
        raise LimitExceededError(f"Schema exceeds {MAX_COLUMNS} columns")
    for column, typ in result.items():
        validate_identifier(column)
        if not any(typ is supported for supported in _TYPE_TO_WIRE):
            raise ValidationError(f"Unsupported type for column {column!r}")
    if key is not None:
        validate_identifier(key)
        if key not in result or result[key] not in (str, int):
            raise ValidationError("Primary key must name a str or int column")
    return result


def schema_to_wire(schema: Mapping[str, type]) -> dict[str, str]:
    return {column: _TYPE_TO_WIRE[typ] for column, typ in validate_schema(schema).items()}


def schema_from_wire(schema: Mapping[str, str]) -> Schema:
    source = _mapping_copy(schema, "Wire schema")
    result = {}
    for column, wire in source.items():
        if type(wire) is not str or wire not in _WIRE_TO_TYPE:
            raise ValidationError("Unsupported wire schema type")
        result[column] = _WIRE_TO_TYPE[wire]
    return validate_schema(result)


def validate_value(value: object, expected_type: type, *, column: str = "value") -> Scalar:
    """Validate one scalar for rows, keys and later predicate builders."""
    if not any(expected_type is supported for supported in _TYPE_TO_WIRE):
        raise ValidationError("Unsupported scalar type")
    if type(value) is not expected_type:
        raise ValidationError(f"Column {column!r} requires exact {expected_type.__name__} values")
    if expected_type is int and not INT64_MIN <= value <= INT64_MAX:
        raise ValidationError(f"Column {column!r} requires a signed int64 value")
    if expected_type is float and not math.isfinite(value):
        raise ValidationError(f"Column {column!r} requires a finite float")
    return value


def validate_key(value: object, schema: Mapping[str, type], key: str) -> Key:
    validate_identifier(key)
    checked = validate_schema(schema, key=key)
    return validate_value(value, checked[key], column=key)


def validate_row(
    row: Mapping[str, Scalar], schema: Mapping[str, type], *, key: str, partial: bool = False
) -> Row:
    """Copy a full insert/upsert row or partial update, rejecting key fields in updates."""
    validate_identifier(key)
    checked = validate_schema(schema, key=key)
    result = _mapping_copy(row, "Row")
    for column in result:
        validate_identifier(column)
        if column not in checked:
            raise ValidationError(f"Unknown column {column!r}")
    if partial:
        if key in result:
            raise ValidationError("Updates cannot include the primary key")
    elif checked.keys() != result.keys():
        raise ValidationError("Full rows must include every schema column")
    for column, value in result.items():
        validate_value(value, checked[column], column=column)
    return result
