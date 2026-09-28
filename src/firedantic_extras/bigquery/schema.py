"""BigQuery schema generation from Firedantic / Pydantic model classes.

The schema describes the documents as firedantic *stores* them, not the
Python annotations: firedantic (0.20+) writes a model with
``model_dump(by_alias=True)`` and converts values Firestore can't hold with
``to_firestore_value()`` -- enums become their value, ``timedelta`` total
seconds, UUIDs / URLs / IP addresses and similar their JSON string form,
sets and tuples lists.  Column names are therefore the fields' aliases and
column types follow the stored value.

Install the optional dependency first::

    pip install altissimo-firedantic-extras[bigquery]
"""

from __future__ import annotations

import enum
import inspect
import types as _types
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal, Union, get_args, get_origin

from pydantic import BaseModel, Secret, SecretBytes, SecretStr, TypeAdapter

if TYPE_CHECKING:
    from pydantic.fields import FieldInfo

try:
    from google.cloud.bigquery import SchemaField
except ImportError as _exc:
    raise ImportError(
        "google-cloud-bigquery is required for BigQuery schema generation. "
        "Install it with: pip install altissimo-firedantic-extras[bigquery]"
    ) from _exc

# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

__all__ = [
    "SchemaDiff",
    "compare_schemas",
    "model_to_bq_schema",
    "models_to_bq_schemas",
    "schema_to_dict",
]


@dataclass
class SchemaDiff:
    """Result of comparing two BigQuery schemas at the top level.

    Attributes:
        only_in_a: Field names present in ``a`` but not ``b``.
        only_in_b: Field names present in ``b`` but not ``a``.
        type_mismatches: 3-tuples of ``(field_name, type_in_a, type_in_b)``
            for fields that exist in both schemas but with different BQ types.
    """

    only_in_a: list[str]
    only_in_b: list[str]
    type_mismatches: list[tuple[str, str, str]]

    @property
    def is_equal(self) -> bool:
        """``True`` if the two schemas have the same fields and types."""
        return not self.only_in_a and not self.only_in_b and not self.type_mismatches


# ---------------------------------------------------------------------------
# Internal: scalar type map
# ---------------------------------------------------------------------------

_SCALAR_MAP: dict[type, str] = {
    str: "STRING",
    int: "INTEGER",
    float: "FLOAT",
    bool: "BOOLEAN",
    datetime: "TIMESTAMP",
    date: "DATE",
    time: "TIME",
    bytes: "BYTES",
    # Firestore has no decimal type: firedantic stores the exact string, which
    # is also BigQuery's recommended input form for NUMERIC.
    Decimal: "NUMERIC",
    # firedantic stores a timedelta as its total seconds.
    timedelta: "FLOAT",
}

# pydantic-core's own scalar schema-node names, keyed the same way _SCALAR_MAP
# is keyed by Python type. Used as a fallback for types like EmailStr that
# validate to a plain string but aren't themselves subclasses of str -- see
# _resolve_via_core_schema below.
_CORE_SCHEMA_SCALAR_MAP: dict[str, str] = {
    "str": "STRING",
    "int": "INTEGER",
    "float": "FLOAT",
    "bool": "BOOLEAN",
    "datetime": "TIMESTAMP",
    "date": "DATE",
    "time": "TIME",
    "bytes": "BYTES",
    "decimal": "NUMERIC",
    "timedelta": "FLOAT",
}

# JSON-schema "type" of a value's serialized form → BQ type.  firedantic
# stores values Firestore can't hold natively in pydantic's JSON form, so
# this is what lands in Firestore for UUIDs, URLs, IP addresses, paths, ...
_JSON_SCHEMA_TYPE_MAP: dict[str, str] = {
    "string": "STRING",
    "integer": "INTEGER",
    "number": "FLOAT",
    "boolean": "BOOLEAN",
}

# Collection types firedantic stores as a Firestore array.
_SEQUENCE_ORIGINS = (list, set, frozenset, tuple)

# pydantic-core schema node types that wrap a single inner schema under a
# "schema" key, purely for validation/serialization purposes -- the type
# BigQuery cares about is whatever they ultimately wrap.
_CORE_SCHEMA_WRAPPER_TYPES = frozenset({"function-after", "function-before", "function-wrap", "nullable", "default"})


# ---------------------------------------------------------------------------
# Internal: type-inspection helpers
# ---------------------------------------------------------------------------


def _is_union(annotation: Any) -> bool:
    """Return True for typing.Union[...] and Python-3.10+ ``X | Y`` unions."""
    if get_origin(annotation) is Union:
        return True
    # Python 3.10+ bare union syntax (e.g. str | None) creates types.UnionType
    return isinstance(annotation, _types.UnionType)


def _unwrap_optional(annotation: Any) -> tuple[Any, bool]:
    """Unwrap ``Optional[T]`` / ``T | None`` → ``(T, True)``.

    Returns ``(annotation, False)`` if the type is not Optional-flavoured.
    If the union has multiple non-None members (rare), returns ``(Any, True)``.
    """
    if _is_union(annotation):
        args = get_args(annotation)
        non_none = [a for a in args if a is not type(None)]
        has_none = type(None) in args
        if has_none:
            return (non_none[0], True) if len(non_none) == 1 else (Any, True)
    return annotation, False


def _is_dict_like(python_type: Any) -> bool:
    """Return True for ``dict``, ``Dict``, ``dict[str, X]``, etc."""
    if python_type is dict:
        return True
    origin = get_origin(python_type)
    return origin is dict


def _bq_type_from_core_schema(node: Any, _depth: int = 0) -> str | None:
    """Recursively unwrap a pydantic-core schema dict to find a scalar leaf type."""
    if _depth > 10 or not isinstance(node, dict):
        return None
    node_type = node.get("type")
    if node_type in _CORE_SCHEMA_SCALAR_MAP:
        return _CORE_SCHEMA_SCALAR_MAP[node_type]
    if node_type in _CORE_SCHEMA_WRAPPER_TYPES:
        inner = node.get("schema")
        if inner is not None:
            return _bq_type_from_core_schema(inner, _depth + 1)
    return None


def _values_bq_type(values: list[Any]) -> str:
    """BQ type for a column holding any of *values* as stored.

    Used for ``Enum`` members (stored as their value) and ``Literal`` choices.
    ``bool`` is checked first since it is a subclass of ``int``; a mix of
    ints and floats is FLOAT; anything else mixed can only be JSON.
    """
    values = [v.value if isinstance(v, enum.Enum) else v for v in values]
    if not values or all(isinstance(v, str) for v in values):
        return "STRING"
    if all(isinstance(v, bool) for v in values):
        return "BOOLEAN"
    if any(isinstance(v, bool) for v in values):
        return "JSON"
    if all(isinstance(v, int) for v in values):
        return "INTEGER"
    if all(isinstance(v, int | float) for v in values):
        return "FLOAT"
    return "JSON"


def _resolve_via_json_schema(python_type: type) -> str | None:
    """Resolve a BQ type from the JSON form pydantic serializes the type to.

    firedantic stores values Firestore can't hold natively via pydantic's
    ``to_jsonable_python``, so a ``UUID``, ``HttpUrl``, ``IPv4Address``,
    ``Path``, ``SecretStr`` ... ends up in Firestore as a string.  The
    serialization JSON schema says which JSON type that is.
    """
    try:
        json_schema = TypeAdapter(python_type).json_schema(mode="serialization")
    except Exception:  # not every class is a valid pydantic type
        return None
    json_type = json_schema.get("type")
    return _JSON_SCHEMA_TYPE_MAP.get(json_type) if isinstance(json_type, str) else None


def _resolve_via_core_schema(python_type: type) -> str | None:
    """Resolve a BQ type via pydantic-core's actual validation schema.

    Handles "validator marker" types like pydantic's ``EmailStr`` or
    ``SecretStr``: these validate to a plain string via
    ``__get_pydantic_core_schema__`` but, unlike a real ``class Foo(str)``
    subclass, don't appear in an ``issubclass`` scan against ``_SCALAR_MAP``
    at all -- ``EmailStr.__mro__`` is just ``(EmailStr, object)``. Following
    what pydantic-core actually resolves the type to (rather than guessing
    from the Python class hierarchy) catches these correctly.
    """
    try:
        core_schema = TypeAdapter(python_type).core_schema
    except Exception:  # not every class is a valid pydantic type
        return None
    return _bq_type_from_core_schema(core_schema)


def _unwrap_secret(python_type: Any) -> Any:
    """The type a pydantic secret holds: firedantic (0.22.3+) stores its value.

    ``SecretStr`` → ``str``, ``SecretBytes`` → ``bytes``, ``Secret[T]`` → ``T``.
    Anything else is returned unchanged.
    """
    if python_type is SecretStr:
        return str
    if python_type is SecretBytes:
        return bytes
    if get_origin(python_type) is Secret:
        args = get_args(python_type)
        return args[0] if args else Any
    return python_type


def _scalar_bq_type(python_type: Any) -> str | None:
    """Return the BQ type string for a scalar type, or None if not scalar."""
    python_type = _unwrap_secret(python_type)
    if python_type in _SCALAR_MAP:
        return _SCALAR_MAP[python_type]
    # Enums are stored as their value, Literals as themselves: the column type
    # follows the values (checked before the subclass scan, so an IntEnum is
    # INTEGER because its values are ints, not because it subclasses int).
    if inspect.isclass(python_type) and issubclass(python_type, enum.Enum):
        return _values_bq_type(list(python_type))
    if get_origin(python_type) is Literal:
        return _values_bq_type(list(get_args(python_type)))
    if inspect.isclass(python_type):
        # Subclasses of a scalar type (e.g. a plain `class MyId(str): ...`)
        # aren't exact dict-key matches above but should map to the same BQ
        # type. bool is excluded from the scan since it's a subclass of int
        # but already has its own exact-match entry.
        for base_type, bq_type in _SCALAR_MAP.items():
            if base_type is not bool and issubclass(python_type, base_type):
                return bq_type
        # Not a real subclass of anything scalar, and not a nested model --
        # fall back to what pydantic-core actually validates it to.
        if not issubclass(python_type, BaseModel):
            resolved = _resolve_via_core_schema(python_type) or _resolve_via_json_schema(python_type)
            if resolved is not None:
                return resolved
    return None


def _sequence_element_type(python_type: Any) -> tuple[bool, Any]:
    """``(True, element_type)`` if firedantic stores *python_type* as an array.

    ``list[T]``, ``set[T]``, ``frozenset[T]`` and ``tuple[T, ...]`` are all
    written as a Firestore array of ``T``.  A fixed-shape tuple like
    ``tuple[int, str]`` is an array too, but of mixed types, so its element
    type is ``Any`` (→ JSON).
    """
    origin = get_origin(python_type)
    if python_type in _SEQUENCE_ORIGINS:
        return True, Any
    if origin not in _SEQUENCE_ORIGINS:
        return False, None
    args = get_args(python_type)
    if origin is tuple:
        if len(args) == 2 and args[1] is Ellipsis:
            return True, args[0]
        return True, args[0] if len(set(args)) == 1 else Any
    return True, args[0] if args else Any


def _column_name(field_name: str, field_info: FieldInfo) -> str:
    """The key firedantic stores the field under: its (serialization) alias."""
    return field_info.serialization_alias or field_info.alias or field_name


def _type_to_bq(python_type: Any) -> tuple[str, tuple[SchemaField, ...]]:
    """Map an (already-unwrapped, non-list) Python type to ``(bq_type, sub_fields)``.

    ``sub_fields`` is non-empty only for RECORD types.
    """
    python_type = _unwrap_secret(python_type)

    # None / Any / object → JSON
    if python_type is None or python_type is Any or python_type is object:
        return "JSON", ()

    # Scalars
    scalar = _scalar_bq_type(python_type)
    if scalar is not None:
        return scalar, ()

    # dict-like → JSON
    if _is_dict_like(python_type):
        return "JSON", ()

    # Nested BaseModel → RECORD (recurse; no json_fields at nested level)
    if inspect.isclass(python_type) and issubclass(python_type, BaseModel):
        sub = _model_to_fields(python_type, json_fields=set(), exclude_fields=set(), is_nested=True)
        return "RECORD", tuple(sub)

    # Fallback → JSON
    return "JSON", ()


def _field_mode(is_optional: bool, field_info: FieldInfo) -> str:
    """Return ``NULLABLE`` or ``REQUIRED`` based on Pydantic field optionality."""
    if is_optional or not field_info.is_required():
        return "NULLABLE"
    return "REQUIRED"


def _annotation_to_schema_field(
    field_name: str,
    annotation: Any,
    field_info: FieldInfo,
    json_fields: set[str],
) -> SchemaField:
    """Convert a single Pydantic field annotation to a ``SchemaField``."""
    column = _column_name(field_name, field_info)

    # json_fields override → always JSON NULLABLE (backward-compat escape hatch).
    # Matches the field name or the stored (alias) name.
    if field_name in json_fields or column in json_fields:
        return SchemaField(column, "JSON", mode="NULLABLE")

    # Unwrap Optional / X | None, then a secret wrapper (Secret[list[str]] ...)
    inner_type, is_optional = _unwrap_optional(annotation)
    inner_type = _unwrap_secret(inner_type)

    # Sequences (list / set / frozenset / tuple) are stored as arrays
    is_sequence, element_type = _sequence_element_type(inner_type)
    if is_sequence:
        # Unwrap Optional element (e.g. list[str | None])
        element_type, _ = _unwrap_optional(element_type)

        # list[dict] / list[Any] / list[object] → JSON NULLABLE
        # (BQ has no REPEATED JSON type)
        if _is_dict_like(element_type) or element_type is Any or element_type is object:
            return SchemaField(column, "JSON", mode="NULLABLE")

        # list[BaseModel] → REPEATED RECORD
        if inspect.isclass(element_type) and issubclass(element_type, BaseModel):
            sub = _model_to_fields(element_type, json_fields=set(), exclude_fields=set(), is_nested=True)
            return SchemaField(column, "RECORD", mode="REPEATED", fields=tuple(sub))

        # list[scalar / enum / Literal] → REPEATED <type>
        bq_type, sub_fields = _type_to_bq(element_type)
        if bq_type == "JSON":
            # BQ has no REPEATED JSON; store the whole array as one JSON value
            return SchemaField(column, "JSON", mode="NULLABLE")
        return SchemaField(column, bq_type, mode="REPEATED", fields=sub_fields)

    # Scalar / dict / nested BaseModel
    mode = _field_mode(is_optional, field_info)
    bq_type, sub_fields = _type_to_bq(inner_type)
    return SchemaField(column, bq_type, mode=mode, fields=sub_fields)


def _model_to_fields(
    model_class: type[BaseModel],
    json_fields: set[str],
    exclude_fields: set[str],
    is_nested: bool = False,
) -> list[SchemaField]:
    """Walk a model's fields and return a flat list of SchemaFields."""
    result: list[SchemaField] = []

    if not is_nested and "id" not in exclude_fields:
        # Top-level: always emit id first as STRING NULLABLE.
        # Firedantic's BareModel declares id: str | None = None, so NULLABLE
        # matches the model definition, and in practice saved docs always have it.
        result.append(SchemaField("id", "STRING", mode="NULLABLE"))

    for field_name, field_info in model_class.model_fields.items():
        if field_name == "id":
            continue  # handled above for top-level; not a stored field in Firestore
        if field_name in exclude_fields or _column_name(field_name, field_info) in exclude_fields:
            continue

        annotation = field_info.annotation
        if annotation is None:
            # Pydantic occasionally stores None for internal / computed fields
            continue

        result.append(_annotation_to_schema_field(field_name, annotation, field_info, json_fields))

    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def model_to_bq_schema(
    model_class: type[BaseModel],
    *,
    json_fields: set[str] | None = None,
    exclude_fields: set[str] | None = None,
    extra_fields: list[SchemaField] | None = None,
) -> list[SchemaField]:
    """Generate a BigQuery schema from a Firedantic / Pydantic model.

    Type inference rules:

    * Required Pydantic fields → ``REQUIRED`` mode.
    * ``Optional[T]`` / ``T | None`` / fields with defaults → ``NULLABLE``.
    * ``list[T]`` / ``set[T]`` / ``frozenset[T]`` / ``tuple[T, ...]`` where
      *T* is a scalar → ``REPEATED <type>`` (all are stored as arrays).
    * ``list[BaseModel]`` → ``REPEATED RECORD``.
    * ``list[dict]`` / ``list[Any]`` → ``JSON NULLABLE``
      (BQ does not support ``REPEATED JSON``).
    * ``dict`` / ``Dict`` / ``dict[str, X]`` → ``JSON``.
    * Nested ``BaseModel`` subclass → ``RECORD`` with auto-derived sub-fields.
    * ``Enum`` / ``Literal`` → the type of their values as stored
      (``STRING`` for string values, ``INTEGER`` for an ``IntEnum``, ...).
    * ``timedelta`` → ``FLOAT`` (stored as total seconds).
    * Types stored in their JSON string form (``UUID``, ``HttpUrl``, IP
      addresses, ...) → ``STRING``.
    * ``SecretStr`` / ``SecretBytes`` / ``Secret[T]`` → the type of the value
      they hold (``STRING`` / ``BYTES`` / ``T``), which firedantic 0.22.3+
      stores in plain text.
    * Column names are the fields' aliases, which is how firedantic stores
      them; ``json_fields`` and ``exclude_fields`` accept either name.
    * The Firedantic document ``id`` is always ``STRING NULLABLE`` and is
      always the first field in the schema (unless excluded).

    Args:
        model_class: The Pydantic ``BaseModel`` or Firedantic ``Model`` class
            to introspect.
        json_fields: Field names to emit as ``JSON NULLABLE`` regardless of
            their Python type. Use this for backward compatibility when existing
            BQ tables store nested objects or arrays as JSON columns.
        exclude_fields: Field names to omit from the generated schema entirely.
        extra_fields: Additional ``SchemaField`` objects to append at the end
            (e.g., load-time metadata columns not modelled in Pydantic).

    Returns:
        A list of ``google.cloud.bigquery.SchemaField`` objects ready to pass
        to ``LoadJobConfig.schema`` or ``Client.create_table()``.
    """
    result = _model_to_fields(
        model_class,
        json_fields=json_fields or set(),
        exclude_fields=exclude_fields or set(),
        is_nested=False,
    )
    if extra_fields:
        result.extend(extra_fields)
    return result


def models_to_bq_schemas(
    model_classes: list[type[BaseModel]],
    **kwargs: Any,
) -> dict[str, list[SchemaField]]:
    """Generate schemas for multiple models, keyed by ``__collection__`` name.

    Args:
        model_classes: Firedantic model classes (must have ``__collection__``
            defined as a class attribute).
        **kwargs: Forwarded to :func:`model_to_bq_schema`.

    Returns:
        A dict mapping ``__collection__`` names → ``list[SchemaField]``.

    Raises:
        ValueError: If any model does not define ``__collection__``.
    """
    result: dict[str, list[SchemaField]] = {}
    for model_class in model_classes:
        collection: str | None = getattr(model_class, "__collection__", None)
        if collection is None:
            raise ValueError(
                f"{model_class.__name__} does not define __collection__. "
                "Use model_to_bq_schema() directly for plain Pydantic models."
            )
        result[collection] = model_to_bq_schema(model_class, **kwargs)
    return result


def schema_to_dict(schema: list[SchemaField]) -> list[dict[str, Any]]:
    """Serialise a schema to a JSON-serialisable list of dicts.

    The output format matches the BigQuery REST API representation and can
    be round-tripped via ``google.cloud.bigquery.Client.schema_from_json()``.

    Args:
        schema: List of ``SchemaField`` objects.

    Returns:
        A JSON-serialisable ``list[dict]``.
    """
    return [f.to_api_repr() for f in schema]


def compare_schemas(
    a: list[SchemaField],
    b: list[SchemaField],
) -> SchemaDiff:
    """Diff two BigQuery schemas at the top level (field names and BQ types).

    Useful for verifying that a model-derived schema matches an existing live
    BQ table schema before cutting over from hand-written schema definitions.

    .. note::
        Only **top-level** fields are compared. Nested ``RECORD`` sub-fields
        are not recursively diffed in this version.

    Args:
        a: First schema (e.g., from :func:`model_to_bq_schema`).
        b: Second schema (e.g., from ``client.get_table(table_ref).schema``).

    Returns:
        A :class:`SchemaDiff` describing the differences.
    """
    a_by_name = {f.name: f for f in a}
    b_by_name = {f.name: f for f in b}

    a_names = set(a_by_name)
    b_names = set(b_by_name)

    only_in_a = sorted(a_names - b_names)
    only_in_b = sorted(b_names - a_names)

    type_mismatches: list[tuple[str, str, str]] = [
        (name, a_by_name[name].field_type, b_by_name[name].field_type)
        for name in sorted(a_names & b_names)
        if a_by_name[name].field_type != b_by_name[name].field_type
    ]

    return SchemaDiff(
        only_in_a=only_in_a,
        only_in_b=only_in_b,
        type_mismatches=type_mismatches,
    )
