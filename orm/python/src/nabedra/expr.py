"""Immutable predicates. Schema validation happens when attached to a query."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from ._vector import check_query, column_kind, has_zero_vector, query_value, to_wire
from .errors import ValidationError
from .validation import ColumnType, Scalar, VectorValue, validate_identifier, validate_value

_MAX_DEPTH = 32
_OPERATIONS = {"dense": ("cosine", "dot", "l2"), "sparse": ("cosine", "dot"), "multi": ("cosine", "dot")}
_COMPARISONS = frozenset(("eq", "ne", "gt", "ge", "lt", "le"))


@dataclass(frozen=True, slots=True, eq=False, init=False)
class Expr:
    """Build predicates with col(), comparisons, and the &, | and ~ operators."""

    _node: tuple
    __hash__ = None

    def __init__(self) -> None:
        raise TypeError("Construct expressions with col() and comparison operators")

    @classmethod
    def _from_node(cls, node: tuple) -> Expr:
        expression = object.__new__(cls)
        object.__setattr__(expression, "_node", node)
        return expression

    def _compare(self, op: str, value: Scalar) -> Expr:
        if isinstance(value, Expr) or callable(value):
            raise TypeError("Compare columns to scalar values, not expressions or callables")
        if self._node[0] != "column":
            raise TypeError("Only a col() reference can be compared to a scalar")
        checked = validate_value(value, type(value), column=self._node[1])
        return Expr._from_node((op, self._node[1], checked))

    def _name(self, method: str) -> str:
        if self._node[0] != "column":
            raise TypeError(f"Only a col() reference supports {method}")
        return self._node[1]

    def cosine(self, query: VectorValue | str) -> Ranking:
        """Rank rows by cosine similarity, highest first."""
        return Ranking._from_query(self._name("cosine"), "cosine", query)

    def dot(self, query: VectorValue | str) -> Ranking:
        """Rank rows by dot product, highest first."""
        return Ranking._from_query(self._name("dot"), "dot", query)

    def l2(self, query: VectorValue | str) -> Ranking:
        """Rank rows by Euclidean distance, lowest first."""
        return Ranking._from_query(self._name("l2"), "l2", query)

    def match(self, text: str) -> Ranking:
        """Rank rows of a full-text column by BM25, highest first."""
        column = self._name("match")
        return Ranking._from_query(column, "match", _string(text, column))

    def contains(self, value: str) -> Expr:
        """Keep rows whose list[str] column holds the value."""
        column = self._name("contains")
        return Expr._from_node(("contains", column, _string(value, column)))

    def contains_any(self, values: Iterable[str]) -> Expr:
        """Keep rows whose list[str] column holds at least one of the values."""
        column = self._name("contains_any")
        if isinstance(values, str) or not isinstance(values, Iterable):
            raise ValidationError(f"Column {column!r} requires a list of strings")
        checked = tuple(_string(value, column) for value in values)
        if isinstance(values, (set, frozenset)):
            # A set has no order. Sort by UTF-8 bytes so the request is the same in every process and SDK.
            # Other iterables, dict keys included, keep their order, as in TypeScript.
            checked = tuple(sorted(checked, key=lambda value: value.encode("utf-8", "surrogatepass")))
        return Expr._from_node(("contains_any", column, checked))

    def starts_with(self, prefix: str) -> Expr:
        """Keep rows whose str column begins with the prefix."""
        column = self._name("starts_with")
        return Expr._from_node(("starts_with", column, _string(prefix, column)))

    def __eq__(self, value: Scalar) -> Expr:
        return self._compare("eq", value)

    def __ne__(self, value: Scalar) -> Expr:
        return self._compare("ne", value)

    def __gt__(self, value: Scalar) -> Expr:
        return self._compare("gt", value)

    def __ge__(self, value: Scalar) -> Expr:
        return self._compare("ge", value)

    def __lt__(self, value: Scalar) -> Expr:
        return self._compare("lt", value)

    def __le__(self, value: Scalar) -> Expr:
        return self._compare("le", value)

    def _logical(self, op: str, other: Expr) -> Expr:
        if not isinstance(other, Expr):
            raise TypeError("Logical operators require another Expr")
        return Expr._from_node((op, self._node, other._node))

    def __and__(self, other: Expr) -> Expr:
        return self._logical("and", other)

    def __or__(self, other: Expr) -> Expr:
        return self._logical("or", other)

    def __invert__(self) -> Expr:
        return Expr._from_node(("not", self._node))

    def __bool__(self) -> bool:
        raise TypeError(
            "Python and, or, not and chained comparisons cannot construct query "
            "expressions; use parenthesized comparisons with &, | and ~"
        )

    def _to_wire(self, schema: Mapping[str, ColumnType]) -> dict:
        """Validate a complete predicate and return detached JSON data."""
        return _predicate_to_wire(self._node, schema, depth=1)


@dataclass(frozen=True, slots=True, eq=False, init=False)
class Ranking:
    """An operation that ranks rows. Build one with col() and pass it to Query.rank()."""

    _column: str
    _op: str
    _value: object

    def __init__(self) -> None:
        raise TypeError("Construct rankings with col() and its ranking methods")

    @classmethod
    def _from_query(cls, column: str, op: str, query: object) -> Ranking:
        ranking = object.__new__(cls)
        object.__setattr__(ranking, "_column", column)
        object.__setattr__(ranking, "_op", op)
        object.__setattr__(ranking, "_value", query_value(query, column))
        return ranking

    def __bool__(self) -> bool:
        raise TypeError("Pass several rankings to rank() instead of combining them with and, or and not")

    def _to_wire(self, schema: Mapping[str, ColumnType], full_text: Mapping[str, str],
                 embed: Mapping[str, dict]) -> dict:
        """Check the operation against its column and return JSON data."""
        column, op, value = self._column, self._op, self._value
        typ = schema[_known_column(column, schema)]
        supported = ("match",) if column in full_text else _OPERATIONS.get(column_kind(typ), ())
        if op not in supported:
            raise ValidationError(f"Column {column!r} does not support {op}")
        if op != "match" and type(value) is str:
            if column not in embed:
                raise ValidationError(f"Column {column!r} has no model to embed text with")
        elif op != "match":
            check_query(value, typ, column)
            if op == "cosine" and has_zero_vector(value):
                raise ValidationError(f"Column {column!r} requires nonzero query vectors for cosine")
            value = to_wire(value, typ)
        return {"column": column, "op": op, "value": value}


def _string(value: object, column: str) -> str:
    if type(value) is not str:
        raise ValidationError(f"Column {column!r} requires string values")
    return value


def col(name: str) -> Expr:
    """Reference a column; its existence is checked by Query.where()."""
    return Expr._from_node(("column", validate_identifier(name)))


def _known_column(column: object, schema: Mapping[str, ColumnType]) -> str:
    validate_identifier(column)
    if column not in schema:
        raise ValidationError(f"Unknown column {column!r}")
    return column


def _predicate_to_wire(node: tuple, schema: Mapping[str, ColumnType], *, depth: int) -> dict:
    if depth > _MAX_DEPTH:
        raise ValidationError(f"Predicate nesting exceeds {_MAX_DEPTH}")
    if type(node) is not tuple or not node or type(node[0]) is not str:
        raise ValidationError("Malformed predicate")
    op = node[0]
    if op in _COMPARISONS and len(node) == 3:
        column = _known_column(node[1], schema)
        if column_kind(schema[column]) is not None:
            raise ValidationError(f"Column {column!r} does not support {op}")
        value = validate_value(node[2], schema[column], column=column)
        if schema[column] is bool and op not in ("eq", "ne"):
            raise ValidationError("Boolean columns support only eq and ne")
        return {"op": op, "column": column, "value": value}
    if op in ("contains", "contains_any", "starts_with") and len(node) == 3:
        column = _known_column(node[1], schema)
        if not (schema[column] is str if op == "starts_with" else column_kind(schema[column]) == "tags"):
            raise ValidationError(f"Column {column!r} does not support {op}")
        if op == "contains_any":
            return {"op": op, "column": column, "values": list(node[2])}
        return {"op": op, "column": column, "value": node[2]}
    if op in ("and", "or") and len(node) > 1:
        return {"op": op, "args": [
            _predicate_to_wire(child, schema, depth=depth + 1) for child in node[1:]
        ]}
    if op == "not" and len(node) == 2:
        return {"op": op, "arg": _predicate_to_wire(node[1], schema, depth=depth + 1)}
    raise ValidationError("A filter must be a complete comparison or logical predicate")
