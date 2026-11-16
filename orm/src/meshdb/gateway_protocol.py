"""Bounded wire validation, without ORM reconstruction."""
from __future__ import annotations
import json
import math
import re
import uuid
from types import MappingProxyType

MAX_BODY = 8 * 1024 * 1024
MUTATIONS = frozenset({'table.create', 'table.drop', 'table.insert', 'table.insert_many',
                       'table.update', 'table.upsert', 'table.delete'})
FIELDS = {
    'table.create': {'name', 'schema', 'key', 'format'}, 'table.describe': {'name'},
    'table.list': set(), 'table.drop': {'table_id'},
    'table.insert': {'table_id', 'row'}, 'table.insert_many': {'table_id', 'rows'},
    'table.get': {'table_id', 'key'}, 'table.update': {'table_id', 'key', 'values'},
    'table.upsert': {'table_id', 'row'}, 'table.delete': {'table_id', 'key'},
    'query.collect': {'table_id', 'query'}, 'query.count': {'table_id', 'query'},
    'query.explain': {'table_id', 'query'},
}

def require(ok):
    if not ok:
        raise ValueError('Invalid MeshDB protocol')

def identity(value):
    require(type(value) is str and str(uuid.UUID(value)) == value)
    return value

def identifier(value):
    require(type(value) is str and re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,63}', value))

def fields(value, expected):
    require(type(value) is dict and set(value) == expected)

def scalar(value):
    require(type(value) in (str, int, float, bool))
    if type(value) is int:
        require(-(2**63) <= value < 2**63)
    if type(value) is float:
        require(math.isfinite(value))

def row(value):
    require(type(value) is dict and len(value) <= 128)
    for key, item in value.items():
        identifier(key)
        scalar(item)

def predicate(value, depth=1):
    require(depth <= 32 and type(value) is dict)
    op = value.get('op')
    if op in ('and', 'or'):
        fields(value, {'op', 'args'})
        require(type(value['args']) is list and len(value['args']) >= 1)
        for arg in value['args']:
            predicate(arg, depth + 1)
    elif op == 'not':
        fields(value, {'op', 'arg'})
        predicate(value['arg'], depth + 1)
    else:
        fields(value, {'op', 'column', 'value'})
        require(op in ('eq', 'ne', 'gt', 'ge', 'lt', 'le'))
        identifier(value['column'])
        scalar(value['value'])
        require(type(value['value']) is not bool or op in ('eq', 'ne'))

def payload(name, p):
    require(type(name) is str and name in FIELDS)
    fields(p, FIELDS[name])
    if 'table_id' in p:
        identity(p['table_id'])
    if 'name' in p:
        identifier(p['name'])
    if name == 'table.create':
        s = p['schema']
        require(type(s) is dict and 0 < len(s) <= 128)
        for key, typ in s.items():
            identifier(key)
            require(type(typ) is str and typ in ('utf8', 'int64', 'float64', 'bool'))
        identifier(p['key'])
        require(s.get(p['key']) in ('utf8', 'int64') and p['format'] == 'parquet')
    elif 'key' in p:
        require(type(p['key']) in (str, int))
        scalar(p['key'])
    for key in ('row', 'values'):
        if key in p:
            row(p[key])
    if 'rows' in p:
        require(type(p['rows']) is list and 0 < len(p['rows']) <= 10000)
        for item in p['rows']:
            row(item)
    if 'query' in p:
        q = p['query']
        fields(q, {'select', 'filter', 'order_by', 'limit'})
        if q['select'] is not None:
            cols = q['select']
            require(type(cols) is list and 0 < len(cols) <= 128)
            for col in cols:
                identifier(col)
            require(len(set(cols)) == len(cols))
        if q['filter'] is not None:
            predicate(q['filter'])
        if q['order_by'] is not None:
            fields(q['order_by'], {'column', 'desc'})
            identifier(q['order_by']['column'])
            require(type(q['order_by']['desc']) is bool)
        require(q['limit'] is None or type(q['limit']) is int and q['limit'] >= 0)

def loads(raw):
    require(type(raw) is bytes and len(raw) <= MAX_BODY)
    # Check structural depth before the JSON decoder allocates nested containers.
    depth = 0
    quoted = escaped = False
    for ch in raw:
        if quoted:
            if escaped:
                escaped = False
            elif ch == 92:
                escaped = True
            elif ch == 34:
                quoted = False
        elif ch == 34:
            quoted = True
        elif ch in (91, 123):
            depth += 1
            require(depth <= 96)
        elif ch in (93, 125):
            depth -= 1
    def unique(pairs):
        out = {}
        for k, v in pairs:
            require(k not in out)
            out[k] = v
        return out
    def finite(s):
        v = float(s)
        require(math.isfinite(v))
        return v
    def invalid(s):
        raise ValueError('Invalid number')
    value = json.loads(raw.decode('utf-8'), object_pairs_hook=unique,
                       parse_float=finite, parse_constant=invalid)
    # Reject lone surrogates, including those introduced by JSON escapes.
    def strings(v):
        if isinstance(v, str):
            v.encode('utf-8')
        elif isinstance(v, dict):
            for k, item in v.items():
                strings(k)
                strings(item)
        elif isinstance(v, list):
            for item in v:
                strings(item)
    strings(value)
    return value

def freeze(value):
    if isinstance(value, dict):
        return MappingProxyType({k: freeze(v) for k, v in value.items()})
    if isinstance(value, list):
        return tuple(freeze(v) for v in value)
    return value
