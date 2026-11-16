"""Immutable predicates. Schema validation happens when attached to a query."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from .errors import ValidationError
from .validation import Scalar, validate_identifier, validate_value

_MAX_DEPTH = 32
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

    def _to_wire(self, schema: Mapping[str, type]) -> dict:
        """Validate a complete predicate and return detached JSON data."""
        return _predicate_to_wire(self._node, schema, depth=1)


def col(name: str) -> Expr:
    """Reference a column; its existence is checked by Query.where()."""
    return Expr._from_node(("column", validate_identifier(name)))


def _predicate_to_wire(node: tuple, schema: Mapping[str, type], *, depth: int) -> dict:
    if depth > _MAX_DEPTH:
        raise ValidationError(f"Predicate nesting exceeds {_MAX_DEPTH}")
    if type(node) is not tuple or not node or type(node[0]) is not str:
        raise ValidationError("Malformed predicate")
    op = node[0]
    if op in _COMPARISONS and len(node) == 3:
        column = validate_identifier(node[1])
        if column not in schema:
            raise ValidationError(f"Unknown column {column!r}")
        value = validate_value(node[2], schema[column], column=column)
        if schema[column] is bool and op not in ("eq", "ne"):
            raise ValidationError("Boolean columns support only eq and ne")
        return {"op": op, "column": column, "value": value}
    if op in ("and", "or") and len(node) > 1:
        return {"op": op, "args": [
            _predicate_to_wire(child, schema, depth=depth + 1) for child in node[1:]
        ]}
    if op == "not" and len(node) == 2:
        return {"op": op, "arg": _predicate_to_wire(node[1], schema, depth=depth + 1)}
    raise ValidationError("A filter must be a complete comparison or logical predicate")
