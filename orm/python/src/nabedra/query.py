"""Immutable query builders and synchronous terminal execution."""
from __future__ import annotations

from copy import copy
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, overload
from collections.abc import Mapping, Sequence

from ._layout import SCORE, Layout
from ._vector import column_kind
from .errors import LimitExceededError, ProtocolError, ValidationError
from .expr import Expr, Ranking, col
from .stream import BatchStream, StreamSnapshot, _batch_stream, _integer, _stream_snapshot
from .validation import ColumnType, Row, Scalar, _decode_row, validate_identifier, validate_schema

if TYPE_CHECKING:
    from pyarrow import RecordBatch

    from .table import Table

_MAX_RESULTS = 1_000
_MAX_BATCH = 256
_MAX_CANDIDATES = 2**32 - 1


def _rank_options(fusion: object, candidates: object, recall: object, exact: object,
                  consistency: object, *, count: int) -> dict:
    if type(fusion) in (list, tuple):
        if (len(fusion) != count or not set(map(type, fusion)) <= {float, int}
                or not all(0 <= weight <= sys.float_info.max for weight in fusion)):
            raise ValidationError("fusion must be 'rrf' or one nonnegative finite weight per ranking")
        fusion = [float(weight) for weight in fusion]
    elif type(fusion) is not str or fusion != "rrf":
        raise ValidationError("fusion must be 'rrf' or one nonnegative finite weight per ranking")
    candidates = _integer(candidates, 1, _MAX_CANDIDATES, "candidates")
    if type(recall) not in (float, int) or not 0 < recall <= 1:
        raise ValidationError("recall must be greater than 0 and at most 1")
    if type(exact) is not bool:
        raise ValidationError("exact must be a bool")
    if type(consistency) is not str or consistency not in ("latest", "snapshot"):
        raise ValidationError("consistency must be latest or snapshot")
    return {"fusion": fusion, "candidates": candidates, "recall": float(recall),
            "exact": exact, "consistency": consistency}


def _ranked_rows(data: object, columns: Mapping[str, ColumnType], limit: int) -> list[Row]:
    if type(data) is not list or len(data) > limit:
        raise ProtocolError("Expected a row list within the query limit")
    return [_decode_row(row, {**columns, SCORE: float}) for row in data]


@dataclass(frozen=True, slots=True, eq=False, init=False)
class Query:
    """Retain a table handle and a schema snapshot without making requests.

    Table.query() constructs Query(self). Terminal methods use the retained
    handle's ID and transport. Reads capture a fresh snapshot by default.
    Stream methods can reuse a shared snapshot.
    """

    _table: Table
    _schema: Mapping[str, ColumnType]
    _layout: Layout
    _selection: tuple[str, ...] | None
    _predicate: Expr | None
    _order: tuple[str, bool] | None
    _limit: int | None
    _rank: dict | None
    _rerank: dict | None

    def __init__(self, table: Table) -> None:
        object.__setattr__(self, "_table", table)
        layout = getattr(table, "_layout", None)
        if layout is None:
            layout = Layout.build(validate_schema(table.schema, key=table.key))
        object.__setattr__(self, "_schema", layout.schema)
        object.__setattr__(self, "_layout", layout)
        object.__setattr__(self, "_selection", None)
        object.__setattr__(self, "_predicate", None)
        object.__setattr__(self, "_order", None)
        object.__setattr__(self, "_limit", None)
        object.__setattr__(self, "_rank", None)
        object.__setattr__(self, "_rerank", None)

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
        if isinstance(expr, Ranking):
            raise ValidationError("A ranking cannot be used as a filter")
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
        if column_kind(self._schema[checked]) is not None:
            raise ValidationError(f"Column {checked!r} does not support order_by")
        if self._rank is not None:
            raise ValidationError("order_by cannot be combined with rank")
        return self._replace(_order=(checked, desc))

    def _projection(self) -> dict[str, ColumnType]:
        return {column: self._schema[column]
                for column in (self._schema if self._selection is None else self._selection)}

    def limit(self, n: int) -> Query:
        if type(n) is not int or n < 0:
            raise ValidationError("Limit must be a nonnegative integer, not bool")
        return self._replace(_limit=n)

    def rank(
        self, *rankings: Ranking, fusion: Literal["rrf"] | Sequence[float] = "rrf", candidates: int = 100,
        recall: float = 0.95, exact: bool = False, consistency: Literal["latest", "snapshot"] = "latest",
    ) -> Query:
        """Order the matching rows by one operation, or by several fused operations."""
        if not rankings:
            raise ValidationError("Rank requires at least one ranking")
        if len(rankings) > _MAX_BATCH:
            raise LimitExceededError(f"rank exceeds {_MAX_BATCH} rankings")
        options = _rank_options(fusion, candidates, recall, exact, consistency, count=len(rankings))
        wire = [self._ranking(ranking) for ranking in rankings]
        self._check_rankable()
        return self._replace(_rank={"rankings": wire, **options})

    def rerank(self, ranking: Ranking, *, candidates: int = 200) -> Query:
        """Score the best candidates of a ranked query again with another ranking."""
        if self._rank is None:
            raise ValidationError("rerank requires rank")
        candidates = _integer(candidates, 1, _MAX_CANDIDATES, "candidates")
        return self._replace(_rerank={"ranking": self._ranking(ranking), "candidates": candidates})

    def rank_many(
        self, rankings: Sequence[Ranking], *, limit: int, fusion: Literal["rrf"] | Sequence[float] = "rrf",
        candidates: int = 100, recall: float = 0.95, exact: bool = False,
        consistency: Literal["latest", "snapshot"] = "latest",
    ) -> list[list[Row]]:
        """Run one ranked query per ranking and return a result list for each, in order."""
        if self._rank is not None:
            raise ValidationError("rank_many cannot be combined with rank")
        options = _rank_options(fusion, candidates, recall, exact, consistency, count=1)
        if not isinstance(rankings, Sequence) or not rankings:
            raise ValidationError("rank_many requires a sequence of rankings")
        if len(rankings) > _MAX_BATCH:
            raise LimitExceededError(f"rank_many exceeds {_MAX_BATCH} rankings")
        batch = [self._ranking(ranking) for ranking in rankings]
        query = self.limit(limit)
        query._check_rankable()
        columns = query._ranked_columns()

        def lists_result(data: object) -> list[list[Row]]:
            if type(data) is not list or len(data) != len(batch):
                raise ProtocolError("Expected one row list for each ranking")
            return [_ranked_rows(rows, columns, limit) for rows in data]

        return self._table._transport.request("query.rank_many", {
            "table_id": self._table.id, "query": query._ranked_wire(columns),
            "rank": {"rankings": [], **options, "rerank": None}, "batch": batch,
        }, decode=lists_result)

    def _ranking(self, ranking: object) -> dict:
        if not isinstance(ranking, Ranking):
            raise ValidationError("Rank requires rankings")
        return ranking._to_wire(self._schema, self._layout.full_text, self._layout.embed)

    def _check_rankable(self) -> None:
        if self._order is not None:
            raise ValidationError("order_by cannot be combined with rank")

    def _check_unranked(self) -> None:
        if self._rank is not None:
            raise ValidationError("Ranked queries support only collect")

    def _ranked_columns(self) -> dict[str, ColumnType]:
        columns = self._selection if self._selection is not None else self._layout.ranked
        return {column: self._schema[column] for column in columns}

    def _ranked_wire(self, columns: Mapping[str, ColumnType]) -> dict:
        if self._limit is None:
            raise ValidationError("Ranked queries require a limit")
        if self._limit > _MAX_RESULTS:
            raise LimitExceededError(f"Ranked queries return at most {_MAX_RESULTS} rows")
        wire = self._to_wire()
        return {"select": list(columns), "filter": wire["filter"], "limit": wire["limit"]}

    def _to_wire(self) -> dict:
        """Return fresh JSON data for the query field of an operation payload."""
        return {
            "select": list(self._selection) if self._selection is not None else None,
            "filter": self._predicate._to_wire(self._schema) if self._predicate is not None else None,
            "order_by": {"column": self._order[0], "desc": self._order[1]} if self._order is not None else None,
            "limit": self._limit,
        }

    @overload
    def stream(
        self, *, batch_rows: int = 64, prefetch_batches: int = 1,
        format: Literal["rows"] = "rows", drop_last: bool = False,
        snapshot: str | None = None, shard: int = 0,
    ) -> BatchStream[list[Row]]: ...

    @overload
    def stream(
        self, *, batch_rows: int = 64, prefetch_batches: int = 1,
        format: Literal["arrow"], drop_last: bool = False,
        snapshot: str | None = None, shard: int = 0,
    ) -> BatchStream[RecordBatch]: ...

    def stream(
        self, *, batch_rows: int = 64, prefetch_batches: int = 1,
        format: Literal["rows", "arrow"] = "rows", drop_last: bool = False,
        snapshot: str | None = None, shard: int = 0,
    ) -> BatchStream[list[Row]] | BatchStream[RecordBatch]:
        """Prepare a single-use OLAP batch iterator. Use it in a with block."""
        self._check_unranked()
        columns = self._projection()
        if any(column_kind(typ) is not None for typ in columns.values()):
            raise ValidationError("Streams support only scalar columns")
        return _batch_stream(
            self._table._transport, self._table.id, self._to_wire(), columns,
            batch_rows=batch_rows, prefetch_batches=prefetch_batches, format=format,
            drop_last=drop_last, snapshot=snapshot, shard=shard, limit=self._limit,
        )

    def stream_snapshot(self, *, shards: int) -> StreamSnapshot:
        """Prepare a pinned snapshot of this query for disjoint shard streams."""
        self._check_unranked()
        return _stream_snapshot(self._table._transport, self._table.id, self._to_wire(), shards)

    def collect(self) -> list[Row]:
        """Fetch a fresh result, validating the exact projected columns."""
        if self._rank is not None:
            return self._collect_ranked()
        columns = self._projection()

        def rows_result(data: object) -> list[Row]:
            if type(data) is not list:
                raise ProtocolError('Expected a row list')
            if len(data) > 10_000 or (self._limit is not None and len(data) > self._limit):
                raise ProtocolError('Row result exceeds query or response row limit')
            return [_decode_row(row, columns) for row in data]

        return self._table._transport.request(
            'query.collect', {'table_id': self._table.id, 'query': self._to_wire()}, decode=rows_result)

    def _collect_ranked(self) -> list[Row]:
        columns = self._ranked_columns()
        return self._table._transport.request("query.rank", {
            "table_id": self._table.id, "query": self._ranked_wire(columns),
            "rank": {**self._rank, "rerank": self._rerank},
        }, decode=lambda data: _ranked_rows(data, columns, self._limit))

    def count(self) -> int:
        """Count a fresh snapshot after applying the explicit query limit."""
        self._check_unranked()

        def count_result(data: object) -> int:
            if (type(data) is not int or data < 0
                    or (self._limit is not None and data > self._limit)):
                raise ProtocolError('Count must be a nonnegative integer within the query limit')
            return data

        return self._table._transport.request(
            'query.count', {'table_id': self._table.id, 'query': self._to_wire()}, decode=count_result)

    def explain(self) -> str:
        """Request the server's textual query plan."""
        self._check_unranked()

        def text_result(data: object) -> str:
            if type(data) is not str:
                raise ProtocolError('Expected query plan text')
            return data

        return self._table._transport.request(
            'query.explain', {'table_id': self._table.id, 'query': self._to_wire()}, decode=text_result)
