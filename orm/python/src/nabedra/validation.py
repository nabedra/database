"""Shared local validation. Mapping results are detached, mutable copies.

These helpers raise local errors. Transport must translate invalid server data
into ProtocolError or UnknownOutcomeError as appropriate for the operation.
"""
from __future__ import annotations

import math
import re
from collections.abc import Mapping
from types import GenericAlias, MappingProxyType
from typing import Literal, TypeAlias, get_args

from ._vector import (
    Vector, VectorValue as _VectorValue, check_value, column_kind, from_wire, type_from_wire, type_to_wire,
)
from .errors import LimitExceededError, ProtocolError, ValidationError

Scalar: TypeAlias = str | int | float | bool
VectorValue: TypeAlias = _VectorValue
Key: TypeAlias = str | int
Row: TypeAlias = dict[str, Scalar | list[str] | VectorValue | None]
ColumnType: TypeAlias = type | Vector | GenericAlias
Schema: TypeAlias = dict[str, ColumnType]

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


def _is_scalar(typ: object) -> bool:
    return any(typ is supported for supported in _TYPE_TO_WIRE)


def validate_schema(schema: Mapping[str, ColumnType], *, key: str | None = None) -> Schema:
    """Copy and validate columns; validate the primary key when supplied."""
    result = _mapping_copy(schema, "Schema")
    if not result:
        raise ValidationError("Schema must contain at least one column")
    if len(result) > MAX_COLUMNS:
        raise LimitExceededError(f"Schema exceeds {MAX_COLUMNS} columns")
    for column, typ in result.items():
        validate_identifier(column)
        kind = column_kind(typ)
        if kind in ("tags", "multi"):
            result[column] = list[get_args(typ)]
        elif kind is None and not _is_scalar(typ):
            raise ValidationError(f"Unsupported type for column {column!r}")
    if key is not None:
        validate_identifier(key)
        if key not in result or result[key] not in (str, int):
            raise ValidationError("Primary key must name a str or int column")
    return result


def schema_to_wire(schema: Mapping[str, ColumnType]) -> dict[str, str | dict]:
    return {column: type_to_wire(typ) if column_kind(typ) else _TYPE_TO_WIRE[typ]
            for column, typ in validate_schema(schema).items()}


def schema_from_wire(schema: Mapping[str, str | dict]) -> Schema:
    source = _mapping_copy(schema, "Wire schema")
    result = {}
    for column, wire in source.items():
        if type(wire) is str and wire in _WIRE_TO_TYPE:
            result[column] = _WIRE_TO_TYPE[wire]
        else:
            result[column] = type_from_wire(wire)
    return validate_schema(result)


def validate_value(
    value: object, expected_type: ColumnType, *, column: str = "value",
) -> Scalar | list[str] | VectorValue | None:
    """Validate one value for rows, keys and predicate builders, copying vectors and lists."""
    if column_kind(expected_type) is not None:
        return check_value(value, expected_type, column)
    if not _is_scalar(expected_type):
        raise ValidationError("Unsupported scalar type")
    if type(value) is not expected_type:
        raise ValidationError(f"Column {column!r} requires exact {expected_type.__name__} values")
    if expected_type is int and not INT64_MIN <= value <= INT64_MAX:
        raise ValidationError(f"Column {column!r} requires a signed int64 value")
    if expected_type is float and not math.isfinite(value):
        raise ValidationError(f"Column {column!r} requires a finite float")
    return value


def validate_key(value: object, schema: Mapping[str, ColumnType], key: str) -> Key:
    validate_identifier(key)
    checked = validate_schema(schema, key=key)
    return validate_value(value, checked[key], column=key)


def validate_row(
    row: Row, schema: Mapping[str, ColumnType], *, key: str, partial: bool = False
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
        result[column] = validate_value(value, checked[column], column=column)
    return result


def _decode_row(data: object, schema: Mapping[str, ColumnType]) -> Row:
    """Decode a response row that contains exactly these columns."""
    if type(data) is not dict or set(data) != set(schema):
        raise ProtocolError("Row must contain exactly the projected columns")
    return {column: from_wire(data[column], typ, column) if column_kind(typ)
            else validate_value(data[column], typ, column=column)
            for column, typ in schema.items()}
