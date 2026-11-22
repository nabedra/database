#!/usr/bin/env python3
"""Validate RESP2 success values with the vendored Valkey reply schemas."""

import json
from pathlib import Path
import re


SCHEMA_DIR = Path(__file__).resolve().parents[2] / "server/tests/data/valkey-commands"
TYPES = {"array": list, "integer": int, "null": type(None), "string": bytes}
KEYWORDS = {"description", "type", "const", "minimum", "minItems", "items", "anyOf", "oneOf"}
ERROR_REPLY = re.compile(rb"[A-Z][A-Z0-9_-]*(?: [^\r\n]+)?")


class SchemaError(ValueError):
    pass


def check_schema(schema):
    """Reject new keywords at load time, even on unvisited schema branches."""
    unknown = schema.keys() - KEYWORDS
    if unknown:
        raise SchemaError(f"unsupported schema keywords {sorted(unknown)}")
    if "type" in schema and schema["type"] not in TYPES:
        raise SchemaError(f"unsupported schema type {schema['type']}")
    for keyword in ("anyOf", "oneOf"):
        for child in schema.get(keyword, ()):
            check_schema(child)
    if "items" in schema:
        check_schema(schema["items"])


def matches(schema, value):
    if "type" in schema and type(value) is not TYPES[schema["type"]]:
        return False
    if "const" in schema:
        literal = schema["const"]
        if isinstance(literal, str):
            literal = literal.encode()
        if type(value) is not type(literal) or value != literal:
            return False
    if "minimum" in schema and type(value) is int and value < schema["minimum"]:
        return False
    if isinstance(value, list):
        if len(value) < schema.get("minItems", 0):
            return False
        if "items" in schema and not all(matches(schema["items"], item) for item in value):
            return False
    if "anyOf" in schema and not any(matches(child, value) for child in schema["anyOf"]):
        return False
    if "oneOf" in schema and sum(matches(child, value) for child in schema["oneOf"]) != 1:
        return False
    return True


class ReplySchemas:
    def __init__(self, commands, directory=SCHEMA_DIR):
        self.schemas = {}
        for command in commands:
            name = command.upper()
            document = json.loads((directory / f"{command.lower()}.json").read_text(encoding="utf-8"))
            schema = document[name]["reply_schema"]
            check_schema(schema)
            self.schemas[name.encode()] = schema

    def validate(self, command, prefix, value):
        schema = self.schemas[command.upper()]
        if prefix == b"-":
            # Upstream schemas describe successful replies. The differential
            # still checks RESP error framing and compares each error verbatim.
            if not ERROR_REPLY.fullmatch(value):
                raise SchemaError(f"invalid RESP error {value!r}")
        elif not matches(schema, value):
            raise SchemaError(f"{command.decode()} reply does not match its schema: {value!r}")
