"""Pure query construction, without transport or row execution."""
import json
import unittest
from dataclasses import FrozenInstanceError, dataclass

from meshdb import Expr, Query, ValidationError, col


@dataclass(frozen=True)
class TableContext:
    id: str = "table-1"
    key: str = "id"

    @property
    def schema(self):
        return {"id": str, "age": int, "active": bool, "score": float}


class QueryTests(unittest.TestCase):
    def setUp(self):
        self.table = TableContext()
        self.query = Query(self.table)

    def test_example_and_detached_wire_data(self):
        expr = (col("age") >= 18) & (col("active") == True)
        query = self.query.where(expr).select("id", "age").order_by("age", desc=True).limit(100)
        expected = {
            "select": ["id", "age"],
            "filter": {"op": "and", "args": [
                {"op": "ge", "column": "age", "value": 18},
                {"op": "eq", "column": "active", "value": True},
            ]},
            "order_by": {"column": "age", "desc": True}, "limit": 100,
        }
        self.assertEqual(query._to_wire(), expected)
        self.assertEqual(json.loads(json.dumps(query._to_wire(), allow_nan=False)), expected)
        wire = query._to_wire()
        wire["select"].clear()
        wire["filter"]["args"][0]["value"] = 0
        wire["order_by"]["desc"] = False
        self.assertEqual(query._to_wire(), expected)
        self.assertIs(query._table, self.table)
        for obj, attr, value in [(query, "_limit", 1), (expr, "_node", ())]:
            with self.assertRaises((FrozenInstanceError, AttributeError)):
                setattr(obj, attr, value)

    def test_branching_replacement_and_where_combination(self):
        defaults = dict.fromkeys(("select", "filter", "order_by", "limit"))
        self.assertEqual(self.query._to_wire(), defaults)
        first = self.query.where(col("age") > 10).select("id").order_by("id").limit(5)
        branch = first.where(col("age") < 30, active=True).select("age").order_by("age").limit(0)
        sibling = first.where(id="alice")
        self.assertEqual(self.query._to_wire(), defaults)
        self.assertEqual(first._to_wire()["filter"], {"op": "gt", "column": "age", "value": 10})
        self.assertEqual(first._to_wire()["select"], ["id"])
        self.assertEqual(first._to_wire()["limit"], 5)
        wire = branch._to_wire()
        self.assertEqual(wire["select"], ["age"])
        self.assertEqual(wire["order_by"], {"column": "age", "desc": False})
        self.assertEqual(wire["limit"], 0)
        self.assertEqual(wire["filter"], {"op": "and", "args": [
            first._to_wire()["filter"],
            {"op": "lt", "column": "age", "value": 30},
            {"op": "eq", "column": "active", "value": True},
        ]})
        self.assertEqual(sibling._to_wire()["filter"]["args"][-1]["value"], "alice")
        self.assertEqual(first.where()._to_wire(), first._to_wire())
        self.assertIsNot(first.where(), first)
        self.assertEqual(self.query.where(active=True)._to_wire(), self.query.where(col("active") == True)._to_wire())

    def test_schema_and_exact_value_validation(self):
        invalid = [
            lambda: self.query.select(), lambda: self.query.select("id", "id"),
            lambda: self.query.select("missing"), lambda: self.query.select(1),
            lambda: self.query.order_by("missing"), lambda: self.query.order_by("age", desc=1),
            lambda: self.query.limit(True), lambda: self.query.limit(-1), lambda: self.query.limit(1.0),
            lambda: self.query.where(col("missing") == 1), lambda: self.query.where(age=True),
            lambda: self.query.where(age=1.0), lambda: self.query.where(score=1),
            lambda: self.query.where(active=1), lambda: self.query.where(id=None),
            lambda: self.query.where(age=2**63), lambda: self.query.where(score=float("nan")),
            lambda: self.query.where(score=float("inf")), lambda: self.query.where(missing="x"),
            lambda: self.query.where(col("active") > False),
            lambda: self.query.where(col("age")),
            lambda: self.query.where(col("age") & (col("active") == True)),
            lambda: self.query.where("age > 1"), lambda: col("bad name"),
        ]
        for operation in invalid:
            with self.subTest(operation=operation), self.assertRaises(ValidationError):
                operation()
        valid = (~((col("id") != "alice") | (col("age") <= 20))) & (col("score") < 1.5)
        self.assertEqual(self.query.where(valid)._to_wire()["filter"]["args"][0]["op"], "not")
        self.assertEqual(self.query.where(col("active") != False)._to_wire()["filter"]["op"], "ne")

    def test_python_boolean_and_unsupported_operands(self):
        expression = col("age") == 18
        for operation in [lambda: bool(expression), lambda: expression and expression,
                          lambda: expression or expression, lambda: not expression,
                          lambda: 1 < col("age") < 30]:
            with self.assertRaisesRegex(TypeError, "Python.*and.*or.*not.*chained comparisons"):
                operation()
        for operation in [lambda: col("age") == col("score"), lambda: expression == 1,
                          lambda: col("age") == (lambda: 1), lambda: expression & True,
                          lambda: expression | (lambda: True)]:
            with self.assertRaises(TypeError):
                operation()
        with self.assertRaises((TypeError, ValidationError)):
            self.query.where(lambda row: True)

    def test_predicate_depth_boundary_and_combined_where(self):
        expr = col("age") == 1  # A comparison has depth one.
        for _ in range(31):
            expr = ~expr
        self.query.where(expr)._to_wire()
        with self.assertRaises(ValidationError):
            self.query.where(~expr)
        with self.assertRaises(ValidationError):
            self.query.where(expr).where(active=True)
        with self.assertRaises(ValidationError):
            self.query.where(expr, active=True)


if __name__ == "__main__":
    unittest.main()
