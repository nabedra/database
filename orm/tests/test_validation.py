import unittest
from collections import UserDict

from nabedra import errors
from nabedra.validation import (
    schema_from_wire, schema_to_wire, validate_format, validate_identifier,
    validate_key, validate_row, validate_schema,
)


class ValidationTests(unittest.TestCase):
    schema = {"id": str, "age": int, "score": float, "active": bool}
    row = {"id": "alice", "age": 24, "score": 1.5, "active": True}

    def test_schema_round_trip_and_mapping_ownership(self):
        source = UserDict(self.schema)
        checked = validate_schema(source, key="id")
        wire = schema_to_wire(checked)
        self.assertEqual(wire, {"id": "utf8", "age": "int64", "score": "float64", "active": "bool"})
        decoded = schema_from_wire(wire)
        source["age"] = str
        checked["id"] = int
        wire["active"] = "utf8"
        self.assertEqual(decoded, self.schema)
        self.assertEqual(validate_schema({f"c{i}": str for i in range(128)}, key="c0")["c127"], str)
        self.assertEqual(validate_identifier("_" + "a" * 63), "_" + "a" * 63)
        self.assertEqual(validate_format("parquet"), "parquet")

    def test_invalid_schema_name_key_and_format(self):
        class MyInt(int):
            pass
        for name in ("", "1id", "a-b", "é", "a" * 65, "id\n", 1):
            with self.subTest(name=name), self.assertRaises(errors.ValidationError):
                validate_identifier(name)
        for schema, key, exc in (
            ({}, "id", errors.ValidationError),
            ({"id": bytes}, "id", errors.ValidationError),
            ({"id": MyInt}, "id", errors.ValidationError),
            ({"bad-name": str}, "bad-name", errors.ValidationError),
            ({"id": str}, "missing", errors.ValidationError),
            ({"id": float}, "id", errors.ValidationError),
            ({"id": bool}, "id", errors.ValidationError),
            ({"id": []}, "id", errors.ValidationError),
            ({f"c{i}": str for i in range(129)}, "c0", errors.LimitExceededError),
        ):
            with self.subTest(schema=schema), self.assertRaises(exc):
                validate_schema(schema, key=key)
        for value in ("csv", None, 1):
            with self.subTest(format=value), self.assertRaises(errors.ValidationError):
                validate_format(value)
        for value in ({"id": "integer"}, {"id": 1}, []):
            with self.subTest(wire=value), self.assertRaises(errors.ValidationError):
                schema_from_wire(value)

    def test_full_and_partial_rows(self):
        source = UserDict(self.row)
        result = validate_row(source, self.schema, key="id")
        source["age"] = 50
        self.assertEqual(result, self.row)
        self.assertEqual(validate_row({"age": 25}, self.schema, key="id", partial=True), {"age": 25})
        self.assertEqual(validate_row({}, self.schema, key="id", partial=True), {})
        for row, partial in (({"id": "alice"}, False), ({**self.row, "extra": 1}, False),
                             ({"extra": 1}, True), ({"id": "bob"}, True),
                             ({"id": "alice"}, True), ({"age": None}, True), ([], False)):
            with self.subTest(row=row, partial=partial), self.assertRaises(errors.ValidationError):
                validate_row(row, self.schema, key="id", partial=partial)

    def test_exact_values_int64_and_key_validation(self):
        for value in (-(2**63), 2**63 - 1):
            with self.subTest(value=value):
                self.assertEqual(validate_key(value, {"id": int}, "id"), value)
                self.assertEqual(validate_row({**self.row, "age": value}, self.schema, key="id")["age"], value)
        class MyStr(str):
            pass
        for column, value in (("age", -(2**63)-1), ("age", 2**63), ("age", True),
                              ("age", 1.0), ("score", 1), ("score", float("nan")),
                              ("score", float("inf")), ("score", -float("inf")),
                              ("active", 1), ("id", MyStr("alice"))):
            with self.subTest(column=column, value=value), self.assertRaises(errors.ValidationError):
                validate_row({**self.row, column: value}, self.schema, key="id")
        for value in (True, 1.0, None, 2**63):
            with self.subTest(key=value), self.assertRaises(errors.ValidationError):
                validate_key(value, {"id": int}, "id")
        self.assertEqual(validate_key("alice", self.schema, "id"), "alice")


if __name__ == "__main__":
    unittest.main()
