"""Immutable query builders and synchronous terminal execution."""
from __future__ import annotations

from copy import copy
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING
from collections.abc import Mapping

from .errors import ProtocolError, ValidationError
from .expr import Expr, col
from .validation import Row, Scalar, validate_identifier, validate_schema, validate_value

if TYPE_CHECKING:
    from .table import Table


@dataclass(frozen=True, slots=True, eq=False, init=False)
class Query:
    """Retain a table handle and a schema snapshot without making requests.

    Table.query() constructs Query(self). Terminal methods use the retained
    handle's ID and transport, requesting a fresh server snapshot each time.
    """

    _table: Table
    _schema: Mapping[str, type]
    _selection: tuple[str, ...] | None
    _predicate: Expr | None
    _order: tuple[str, bool] | None
    _limit: int | None

    def __init__(self, table: Table) -> None:
        object.__setattr__(self, "_table", table)
        object.__setattr__(self, "_schema", MappingProxyType(validate_schema(table.schema, key=table.key)))
        object.__setattr__(self, "_selection", None)
        object.__setattr__(self, "_predicate", None)
        object.__setattr__(self, "_order", None)
        object.__setattr__(self, "_limit", None)

    def _replace(self, **changes: object) -> Query:
        query = copy(self)
        for field, value in changes.items():
            object.__setattr__(query, field, value)
        return query

    def _column(self, column: str) -> str:
        validate_identifier(column)
        if column not in self._schema:
            raise ValidationError(f"Unknown column {column!r}")
        return column

    def select(self, *columns: str) -> Query:
        if not columns:
            raise ValidationError("Select requires at least one column")
        checked = tuple(self._column(column) for column in columns)
        if len(set(checked)) != len(checked):
            raise ValidationError("Selected columns must be unique")
        return self._replace(_selection=checked)

    def where(self, expr: Expr | None = None, **filters: Scalar) -> Query:
        if expr is not None and not isinstance(expr, Expr):
            raise ValidationError("Filter must be an Expr")
        predicates = []
        if self._predicate is not None:
            predicates.append(self._predicate)
        if expr is not None:
            predicates.append(expr)
        predicates.extend(col(column) == value for column, value in filters.items())
        predicate = None
        if len(predicates) == 1:
            predicate = predicates[0]
        elif predicates:
            predicate = Expr._from_node(("and", *(item._node for item in predicates)))
        if predicate is not None:
            predicate._to_wire(self._schema)
        return self._replace(_predicate=predicate)

    def order_by(self, column: str, desc: bool = False) -> Query:
        checked = self._column(column)
        if type(desc) is not bool:
            raise ValidationError("Order desc must be an exact bool")
        return self._replace(_order=(checked, desc))

    def limit(self, n: int) -> Query:
        if type(n) is not int or n < 0:
            raise ValidationError("Limit must be a nonnegative integer, not bool")
        return self._replace(_limit=n)

    def _to_wire(self) -> dict:
        """Return fresh JSON data for the query field of an operation payload."""
        return {
            "select": list(self._selection) if self._selection is not None else None,
            "filter": self._predicate._to_wire(self._schema) if self._predicate is not None else None,
            "order_by": {"column": self._order[0], "desc": self._order[1]} if self._order is not None else None,
            "limit": self._limit,
        }

    def collect(self) -> list[Row]:
        """Fetch a fresh result, validating the exact projected columns."""
        columns = self._selection if self._selection is not None else tuple(self._schema)

        def rows_result(data: object) -> list[Row]:
            if type(data) is not list:
                raise ProtocolError('Expected a row list')
            if len(data) > 10_000 or (self._limit is not None and len(data) > self._limit):
                raise ProtocolError('Row result exceeds query or response row limit')
            rows = []
            for row in data:
                if type(row) is not dict or set(row) != set(columns):
                    raise ProtocolError('Row must contain exactly the projected columns')
                rows.append({column: validate_value(row[column], self._schema[column], column=column)
                             for column in columns})
            return rows

        return self._table._transport.request(
            'query.collect', {'table_id': self._table.id, 'query': self._to_wire()}, decode=rows_result)

    def count(self) -> int:
        """Count a fresh snapshot after applying the explicit query limit."""
        def count_result(data: object) -> int:
            if (type(data) is not int or not 0 <= data <= 100_000
                    or (self._limit is not None and data > self._limit)):
                raise ProtocolError('Count must be a nonnegative integer within table and query limits')
            return data

        return self._table._transport.request(
            'query.count', {'table_id': self._table.id, 'query': self._to_wire()}, decode=count_result)

    def explain(self) -> str:
        """Request the server's textual query plan."""
        def text_result(data: object) -> str:
            if type(data) is not str:
                raise ProtocolError('Expected query plan text')
            return data

        return self._table._transport.request(
            'query.explain', {'table_id': self._table.id, 'query': self._to_wire()}, decode=text_result)
