"""A table's columns and search options, checked locally and decoded strictly."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import re
from types import MappingProxyType

from ._vector import column_kind, to_wire, well_formed
from .errors import LimitExceededError, ValidationError
from .validation import Schema, validate_identifier, validate_schema

MAX_SEARCH_COLUMNS = 8
SCORE = '_score'
_VECTOR_KINDS = ('dense', 'sparse', 'multi')
OPTION_FIELDS = ('partition_by', 'full_text', 'embed')
_MODEL_NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,63}')
# Keep in step with the language set published by the server's text analyzers.
_LANGUAGES = frozenset((
    'arabic', 'danish', 'dutch', 'english', 'finnish', 'french', 'german', 'greek', 'hungarian',
    'italian', 'norwegian', 'portuguese', 'romanian', 'russian', 'simple', 'spanish', 'swedish',
    'tamil', 'turkish',
))


def _text(value: object, label: str) -> str:
    if type(value) is not str or not value:
        raise ValidationError(f'{label} must be a nonempty string')
    if not well_formed(value):
        raise ValidationError(f'{label} must be a well-formed string')
    return value


def _options(value: object, label: str) -> dict:
    if not isinstance(value, Mapping):
        raise ValidationError(f'{label} must be a mapping')
    return dict(value)


@dataclass(frozen=True, slots=True)
class Layout:
    """Columns plus search options. Absent options are None or empty."""

    schema: Mapping[str, type]
    partition_by: str | None
    full_text: Mapping[str, str]
    embed: Mapping[str, dict[str, str]]

    @classmethod
    def build(cls, schema: Schema, partition_by: object = None, full_text: object = None,
              embed: object = None) -> Layout:
        """Check options against a validated schema."""
        if partition_by is not None and schema.get(validate_identifier(partition_by)) not in (str, int):
            raise ValidationError('partition_by must name a string or integer column')
        texts = _options({} if full_text is None else full_text, 'full_text')
        for column, language in texts.items():
            if schema.get(validate_identifier(column)) is not str:
                raise ValidationError(f'Column {column!r} must be a string column for full text')
            if _text(language, 'Language') not in _LANGUAGES:
                raise ValidationError(f'Unsupported full-text language {language!r}')
        embedded = {
            column: _embed(column, options, schema)
            for column, options in _options({} if embed is None else embed, 'embed').items()
        }
        columns = [column for column, typ in schema.items()
                   if column_kind(typ) in _VECTOR_KINDS or column in texts]
        if len(columns) > MAX_SEARCH_COLUMNS:
            raise LimitExceededError(f'Table exceeds {MAX_SEARCH_COLUMNS} search columns')
        if columns and SCORE in schema:
            raise ValidationError("Column '_score' is reserved on tables with search columns")
        return cls(MappingProxyType(schema), partition_by, texts, embedded)

    @classmethod
    def from_descriptor(cls, schema: Schema, options: Mapping[str, object]) -> Layout:
        """Decode the optional descriptor fields. Absent options are omitted, never null or empty."""
        if any(value is None or value == {} for value in options.values()):
            raise ValidationError('Descriptor options must be omitted when absent')
        return cls.build(schema, **options)

    @property
    def has_search(self) -> bool:
        return bool(self.full_text) or any(column_kind(typ) in _VECTOR_KINDS for typ in self.schema.values())

    @property
    def written(self) -> dict[str, type]:
        """The columns that a full row contains."""
        return {column: typ for column, typ in self.schema.items() if column not in self.embed}

    @property
    def ranked(self) -> tuple[str, ...]:
        """The columns that a ranked query returns unless others are selected."""
        return tuple(column for column, typ in self.schema.items() if column_kind(typ) not in _VECTOR_KINDS)

    def wire_options(self) -> dict:
        return {name: value if name == 'partition_by' else dict(value) for name in OPTION_FIELDS
                if (value := getattr(self, name))}

    def row_to_wire(self, row: Mapping[str, object]) -> dict:
        return {column: to_wire(value, self.schema[column]) for column, value in row.items()}


def _embed(column: object, options: object, schema: Mapping[str, type]) -> dict[str, str]:
    if column_kind(schema.get(validate_identifier(column))) != 'dense':
        raise ValidationError(f'Column {column!r} must be a dense vector column for embed')
    return check_embed(options, schema)


def check_embed(options: object, schema: Mapping[str, type]) -> dict[str, str]:
    checked = _options(options, 'Embed options')
    if set(checked) != {'source', 'model'}:
        raise ValidationError('Embed options must contain exactly source and model')
    if schema.get(validate_identifier(checked['source'])) is not str:
        raise ValidationError('Embed source must name a string column')
    model = _text(checked['model'], 'Embed model')
    if not _MODEL_NAME.fullmatch(model):
        raise ValidationError('Invalid embed model name')
    return {'source': checked['source'], 'model': model}


def extend(layout: Layout, name: str, typ: object, embed: object) -> Layout:
    """Check a new vector column against the table it joins."""
    validate_identifier(name)
    if name in layout.schema:
        raise ValidationError(f'Column {name!r} already exists')
    if column_kind(typ) not in _VECTOR_KINDS:
        raise ValidationError('Added columns must have a vector type')
    schema = validate_schema({**layout.schema, name: typ})
    embedded = dict(layout.embed)
    if embed is not None:
        embedded[name] = embed
    return Layout.build(schema, layout.partition_by, layout.full_text, embedded)
