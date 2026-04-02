"""BigQuery schema generation from Firedantic / Pydantic model classes.

Install the optional dependency first::

    pip install firedantic-extras[bigquery]
"""

from __future__ import annotations

import enum
import inspect
import types as _types
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Literal, Union, get_args, get_origin

from pydantic import BaseModel
from pydantic.fields import FieldInfo

try:
    from google.cloud.bigquery import SchemaField
except ImportError as _exc:
    raise ImportError(
        "google-cloud-bigquery is required for BigQuery schema generation. "
        "Install it with: pip install firedantic-extras[bigquery]"
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
    Decimal: "NUMERIC",
}


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


def _scalar_bq_type(python_type: Any) -> str | None:
    """Return the BQ type string for a scalar type, or None if not scalar."""
    if python_type in _SCALAR_MAP:
        return _SCALAR_MAP[python_type]
    if inspect.isclass(python_type) and issubclass(python_type, enum.Enum):
        return "STRING"
    if get_origin(python_type) is Literal:
        return "STRING"
    return None


def _type_to_bq(python_type: Any) -> tuple[str, tuple[SchemaField, ...]]:
    """Map an (already-unwrapped, non-list) Python type to ``(bq_type, sub_fields)``.

    ``sub_fields`` is non-empty only for RECORD types.
    """
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
    # json_fields override → always JSON NULLABLE (backward-compat escape hatch)
    if field_name in json_fields:
        return SchemaField(field_name, "JSON", mode="NULLABLE")

    # Unwrap Optional / X | None
    inner_type, is_optional = _unwrap_optional(annotation)

    # Check for list
    origin = get_origin(inner_type)
    if origin is list:
        args = get_args(inner_type)
        element_type = args[0] if args else Any

        # Unwrap Optional element (e.g. list[str | None])
        element_type, _ = _unwrap_optional(element_type)

        # list[dict] / list[Any] / list[object] → JSON NULLABLE
        # (BQ has no REPEATED JSON type)
        if _is_dict_like(element_type) or element_type is Any or element_type is object:
            return SchemaField(field_name, "JSON", mode="NULLABLE")

        # list[BaseModel] → REPEATED RECORD
        if inspect.isclass(element_type) and issubclass(element_type, BaseModel):
            sub = _model_to_fields(element_type, json_fields=set(), exclude_fields=set(), is_nested=True)
            return SchemaField(field_name, "RECORD", mode="REPEATED", fields=tuple(sub))

        # list[scalar / enum / Literal] → REPEATED <type>
        bq_type, sub_fields = _type_to_bq(element_type)
        return SchemaField(field_name, bq_type, mode="REPEATED", fields=sub_fields)

    # Scalar / dict / nested BaseModel
    mode = _field_mode(is_optional, field_info)
    bq_type, sub_fields = _type_to_bq(inner_type)
    return SchemaField(field_name, bq_type, mode=mode, fields=sub_fields)


def _model_to_fields(
    model_class: type[BaseModel],
    json_fields: set[str],
    exclude_fields: set[str],
    is_nested: bool = False,
) -> list[SchemaField]:
    """Walk a model's fields and return a flat list of SchemaFields."""
    result: list[SchemaField] = []

    if not is_nested:
        # Top-level: always emit id first as STRING NULLABLE.
        # Firedantic's BareModel declares id: str | None = None, so NULLABLE
        # matches the model definition, and in practice saved docs always have it.
        if "id" not in exclude_fields:
            result.append(SchemaField("id", "STRING", mode="NULLABLE"))

    for field_name, field_info in model_class.model_fields.items():
        if field_name == "id":
            continue  # handled above for top-level; not a stored field in Firestore
        if field_name in exclude_fields:
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
    * ``list[T]`` where *T* is a scalar → ``REPEATED <type>``.
    * ``list[BaseModel]`` → ``REPEATED RECORD``.
    * ``list[dict]`` / ``list[Any]`` → ``JSON NULLABLE``
      (BQ does not support ``REPEATED JSON``).
    * ``dict`` / ``Dict`` / ``dict[str, X]`` → ``JSON``.
    * Nested ``BaseModel`` subclass → ``RECORD`` with auto-derived sub-fields.
    * ``Enum`` / ``Literal`` → ``STRING``.
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
