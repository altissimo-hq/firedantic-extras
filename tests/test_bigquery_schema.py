"""Unit tests for BigQuery schema generation.

These tests are pure Python — no BQ API calls, no emulator, no network.
They exercise type introspection and the public API surface.
"""

from __future__ import annotations

import enum
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pytest
from google.cloud.bigquery import SchemaField
from pydantic import BaseModel

from firedantic_extras.bigquery.schema import (
    compare_schemas,
    model_to_bq_schema,
    models_to_bq_schemas,
    schema_to_dict,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def field_by_name(schema: list[SchemaField], name: str) -> SchemaField:
    by_name = {f.name: f for f in schema}
    assert name in by_name, f"Field '{name}' not found; schema has: {list(by_name)}"
    return by_name[name]


# ---------------------------------------------------------------------------
# Sample models
# ---------------------------------------------------------------------------


class Colour(str, enum.Enum):
    RED = "red"
    GREEN = "green"
    BLUE = "blue"


class Address(BaseModel):
    street: str
    city: str
    postcode: str | None = None


class SimpleModel(BaseModel):
    name: str
    age: int
    score: float
    active: bool
    created_at: datetime
    birthday: date
    weight: Decimal


class OptionalAndDefaultModel(BaseModel):
    required_str: str
    optional_str: str | None = None
    str_with_default: str = "hello"
    nullable_union: str | None = None


class ListModel(BaseModel):
    tags: list[str]
    counts: list[int]
    scores: list[float]
    flags: list[bool]
    nested_dicts: list[dict]
    any_list: list[Any]


class NestedModel(BaseModel):
    label: str
    address: Address
    optional_address: Address | None = None
    address_list: list[Address]


class DictModel(BaseModel):
    metadata: dict
    typed_dict: dict[str, Any]
    bare_dict: dict[str, str]


class EnumAndLiteralModel(BaseModel):
    colour: Colour
    optional_colour: Colour | None = None


class FiredanticLikeModel(BaseModel):
    """Mimics a Firedantic BareModel with an id field."""

    id: str | None = None
    name: str
    tags: list[str]


class WithCollection(BaseModel):
    __collection__ = "widgets"

    label: str


# ---------------------------------------------------------------------------
# id field handling
# ---------------------------------------------------------------------------


class TestIdField:
    def test_id_is_first_field(self) -> None:
        schema = model_to_bq_schema(SimpleModel)
        assert schema[0].name == "id"

    def test_id_is_string_nullable(self) -> None:
        schema = model_to_bq_schema(SimpleModel)
        f = schema[0]
        assert f.field_type == "STRING"
        assert f.mode == "NULLABLE"

    def test_id_from_model_fields_skipped(self) -> None:
        """If the model itself defines id, we should not double-emit it."""
        schema = model_to_bq_schema(FiredanticLikeModel)
        names = [f.name for f in schema]
        assert names.count("id") == 1

    def test_exclude_id(self) -> None:
        schema = model_to_bq_schema(SimpleModel, exclude_fields={"id"})
        names = [f.name for f in schema]
        assert "id" not in names


# ---------------------------------------------------------------------------
# Scalar type mapping
# ---------------------------------------------------------------------------


class TestScalarTypes:
    def test_str_field(self) -> None:
        schema = model_to_bq_schema(SimpleModel)
        f = field_by_name(schema, "name")
        assert f.field_type == "STRING"

    def test_int_field(self) -> None:
        schema = model_to_bq_schema(SimpleModel)
        f = field_by_name(schema, "age")
        assert f.field_type == "INTEGER"

    def test_float_field(self) -> None:
        schema = model_to_bq_schema(SimpleModel)
        f = field_by_name(schema, "score")
        assert f.field_type == "FLOAT"

    def test_bool_field(self) -> None:
        schema = model_to_bq_schema(SimpleModel)
        f = field_by_name(schema, "active")
        assert f.field_type == "BOOLEAN"

    def test_datetime_field(self) -> None:
        schema = model_to_bq_schema(SimpleModel)
        f = field_by_name(schema, "created_at")
        assert f.field_type == "TIMESTAMP"

    def test_date_field(self) -> None:
        schema = model_to_bq_schema(SimpleModel)
        f = field_by_name(schema, "birthday")
        assert f.field_type == "DATE"

    def test_decimal_field(self) -> None:
        schema = model_to_bq_schema(SimpleModel)
        f = field_by_name(schema, "weight")
        assert f.field_type == "NUMERIC"


# ---------------------------------------------------------------------------
# REQUIRED vs NULLABLE mode
# ---------------------------------------------------------------------------


class TestMode:
    def test_required_field_is_required(self) -> None:
        schema = model_to_bq_schema(OptionalAndDefaultModel)
        f = field_by_name(schema, "required_str")
        assert f.mode == "REQUIRED"

    def test_optional_field_is_nullable(self) -> None:
        schema = model_to_bq_schema(OptionalAndDefaultModel)
        f = field_by_name(schema, "optional_str")
        assert f.mode == "NULLABLE"

    def test_field_with_default_is_nullable(self) -> None:
        schema = model_to_bq_schema(OptionalAndDefaultModel)
        f = field_by_name(schema, "str_with_default")
        assert f.mode == "NULLABLE"

    def test_bare_union_with_none_is_nullable(self) -> None:
        """str | None should also produce NULLABLE."""
        schema = model_to_bq_schema(OptionalAndDefaultModel)
        f = field_by_name(schema, "nullable_union")
        assert f.mode == "NULLABLE"
        assert f.field_type == "STRING"


# ---------------------------------------------------------------------------
# list → REPEATED
# ---------------------------------------------------------------------------


class TestListFields:
    def test_list_str_repeated_string(self) -> None:
        schema = model_to_bq_schema(ListModel)
        f = field_by_name(schema, "tags")
        assert f.field_type == "STRING"
        assert f.mode == "REPEATED"

    def test_list_int_repeated_integer(self) -> None:
        schema = model_to_bq_schema(ListModel)
        f = field_by_name(schema, "counts")
        assert f.field_type == "INTEGER"
        assert f.mode == "REPEATED"

    def test_list_float_repeated_float(self) -> None:
        schema = model_to_bq_schema(ListModel)
        f = field_by_name(schema, "scores")
        assert f.field_type == "FLOAT"
        assert f.mode == "REPEATED"

    def test_list_bool_repeated_boolean(self) -> None:
        schema = model_to_bq_schema(ListModel)
        f = field_by_name(schema, "flags")
        assert f.field_type == "BOOLEAN"
        assert f.mode == "REPEATED"

    def test_list_dict_becomes_json(self) -> None:
        """list[dict] → JSON NULLABLE (BQ has no REPEATED JSON)."""
        schema = model_to_bq_schema(ListModel)
        f = field_by_name(schema, "nested_dicts")
        assert f.field_type == "JSON"
        assert f.mode == "NULLABLE"

    def test_list_any_becomes_json(self) -> None:
        """list[Any] → JSON NULLABLE."""
        schema = model_to_bq_schema(ListModel)
        f = field_by_name(schema, "any_list")
        assert f.field_type == "JSON"
        assert f.mode == "NULLABLE"


# ---------------------------------------------------------------------------
# dict-like fields → JSON
# ---------------------------------------------------------------------------


class TestDictFields:
    def test_plain_dict_is_json(self) -> None:
        schema = model_to_bq_schema(DictModel)
        f = field_by_name(schema, "metadata")
        assert f.field_type == "JSON"

    def test_typed_dict_is_json(self) -> None:
        schema = model_to_bq_schema(DictModel)
        f = field_by_name(schema, "typed_dict")
        assert f.field_type == "JSON"

    def test_bare_dict_typed_is_json(self) -> None:
        schema = model_to_bq_schema(DictModel)
        f = field_by_name(schema, "bare_dict")
        assert f.field_type == "JSON"


# ---------------------------------------------------------------------------
# Enum → STRING
# ---------------------------------------------------------------------------


class TestEnumFields:
    def test_enum_is_string(self) -> None:
        schema = model_to_bq_schema(EnumAndLiteralModel)
        f = field_by_name(schema, "colour")
        assert f.field_type == "STRING"
        assert f.mode == "REQUIRED"

    def test_optional_enum_is_nullable_string(self) -> None:
        schema = model_to_bq_schema(EnumAndLiteralModel)
        f = field_by_name(schema, "optional_colour")
        assert f.field_type == "STRING"
        assert f.mode == "NULLABLE"


# ---------------------------------------------------------------------------
# Nested BaseModel → RECORD
# ---------------------------------------------------------------------------


class TestNestedRecord:
    def test_nested_model_is_record(self) -> None:
        schema = model_to_bq_schema(NestedModel)
        f = field_by_name(schema, "address")
        assert f.field_type == "RECORD"
        assert f.mode == "REQUIRED"

    def test_nested_model_has_subfields(self) -> None:
        schema = model_to_bq_schema(NestedModel)
        f = field_by_name(schema, "address")
        sub_names = {sf.name for sf in f.fields}
        assert "street" in sub_names
        assert "city" in sub_names
        assert "postcode" in sub_names

    def test_nested_subfield_types(self) -> None:
        schema = model_to_bq_schema(NestedModel)
        f = field_by_name(schema, "address")
        sub_by_name = {sf.name: sf for sf in f.fields}
        assert sub_by_name["street"].field_type == "STRING"
        assert sub_by_name["street"].mode == "REQUIRED"
        assert sub_by_name["postcode"].field_type == "STRING"
        assert sub_by_name["postcode"].mode == "NULLABLE"

    def test_optional_nested_model_is_nullable_record(self) -> None:
        schema = model_to_bq_schema(NestedModel)
        f = field_by_name(schema, "optional_address")
        assert f.field_type == "RECORD"
        assert f.mode == "NULLABLE"

    def test_list_of_nested_model_is_repeated_record(self) -> None:
        schema = model_to_bq_schema(NestedModel)
        f = field_by_name(schema, "address_list")
        assert f.field_type == "RECORD"
        assert f.mode == "REPEATED"

    def test_nested_record_has_no_extra_id_field(self) -> None:
        """RECORD subfields should NOT get an injected 'id' field."""
        schema = model_to_bq_schema(NestedModel)
        f = field_by_name(schema, "address")
        sub_names = {sf.name for sf in f.fields}
        assert "id" not in sub_names


# ---------------------------------------------------------------------------
# json_fields override
# ---------------------------------------------------------------------------


class TestJsonFieldsOverride:
    def test_json_fields_forces_json_on_scalar(self) -> None:
        """Even a plain str field becomes JSON if in json_fields."""
        schema = model_to_bq_schema(SimpleModel, json_fields={"name"})
        f = field_by_name(schema, "name")
        assert f.field_type == "JSON"
        assert f.mode == "NULLABLE"

    def test_json_fields_forces_json_on_nested_model(self) -> None:
        """A field that would be RECORD becomes JSON."""
        schema = model_to_bq_schema(NestedModel, json_fields={"address"})
        f = field_by_name(schema, "address")
        assert f.field_type == "JSON"
        assert f.mode == "NULLABLE"
        # No subfields
        assert len(f.fields) == 0

    def test_json_fields_forces_json_on_list_model(self) -> None:
        """A list[BaseModel] that would be REPEATED RECORD becomes JSON."""
        schema = model_to_bq_schema(NestedModel, json_fields={"address_list"})
        f = field_by_name(schema, "address_list")
        assert f.field_type == "JSON"
        assert f.mode == "NULLABLE"

    def test_non_overridden_fields_unaffected(self) -> None:
        schema = model_to_bq_schema(NestedModel, json_fields={"address"})
        f = field_by_name(schema, "label")
        # label is str and not in json_fields — should be STRING REQUIRED
        assert f.field_type == "STRING"
        assert f.mode == "REQUIRED"


# ---------------------------------------------------------------------------
# exclude_fields
# ---------------------------------------------------------------------------


class TestExcludeFields:
    def test_excluded_field_absent(self) -> None:
        schema = model_to_bq_schema(SimpleModel, exclude_fields={"age", "score"})
        names = {f.name for f in schema}
        assert "age" not in names
        assert "score" not in names

    def test_non_excluded_fields_present(self) -> None:
        schema = model_to_bq_schema(SimpleModel, exclude_fields={"age"})
        names = {f.name for f in schema}
        assert "name" in names


# ---------------------------------------------------------------------------
# extra_fields
# ---------------------------------------------------------------------------


class TestExtraFields:
    def test_extra_fields_appended(self) -> None:
        extra = [SchemaField("load_time", "TIMESTAMP", mode="NULLABLE")]
        schema = model_to_bq_schema(SimpleModel, extra_fields=extra)
        assert schema[-1].name == "load_time"
        assert schema[-1].field_type == "TIMESTAMP"

    def test_extra_fields_after_model_fields(self) -> None:
        extra = [SchemaField("_source", "STRING", mode="NULLABLE")]
        schema = model_to_bq_schema(SimpleModel, extra_fields=extra)
        model_names = {f.name for f in schema[:-1]}
        assert "name" in model_names


# ---------------------------------------------------------------------------
# models_to_bq_schemas
# ---------------------------------------------------------------------------


class TestModelsToBqSchemas:
    def test_keyed_by_collection(self) -> None:
        schemas = models_to_bq_schemas([WithCollection])
        assert "widgets" in schemas

    def test_schema_contents_correct(self) -> None:
        schemas = models_to_bq_schemas([WithCollection])
        names = {f.name for f in schemas["widgets"]}
        assert "id" in names
        assert "label" in names

    def test_model_without_collection_raises(self) -> None:
        with pytest.raises(ValueError, match="__collection__"):
            models_to_bq_schemas([SimpleModel])


# ---------------------------------------------------------------------------
# schema_to_dict
# ---------------------------------------------------------------------------


class TestSchemaToDict:
    def test_returns_list_of_dicts(self) -> None:
        schema = model_to_bq_schema(SimpleModel)
        result = schema_to_dict(schema)
        assert isinstance(result, list)
        assert all(isinstance(d, dict) for d in result)

    def test_dict_has_expected_keys(self) -> None:
        schema = [SchemaField("name", "STRING", mode="REQUIRED")]
        result = schema_to_dict(schema)
        assert result[0]["name"] == "name"
        assert result[0]["type"] == "STRING"
        assert result[0]["mode"] == "REQUIRED"

    def test_full_round_trip_via_api_repr(self) -> None:
        schema = model_to_bq_schema(SimpleModel)
        dicts = schema_to_dict(schema)
        # Reconstruct from dict representation
        reconstructed = [SchemaField.from_api_repr(d) for d in dicts]
        assert [f.name for f in reconstructed] == [f.name for f in schema]
        assert [f.field_type for f in reconstructed] == [f.field_type for f in schema]


# ---------------------------------------------------------------------------
# compare_schemas
# ---------------------------------------------------------------------------


class TestCompareSchemas:
    def _make(self, *fields: tuple[str, str]) -> list[SchemaField]:
        return [SchemaField(name, ftype, mode="NULLABLE") for name, ftype in fields]

    def test_equal_schemas(self) -> None:
        a = self._make(("id", "STRING"), ("name", "STRING"), ("age", "INTEGER"))
        b = self._make(("id", "STRING"), ("name", "STRING"), ("age", "INTEGER"))
        diff = compare_schemas(a, b)
        assert diff.is_equal

    def test_field_only_in_a(self) -> None:
        a = self._make(("id", "STRING"), ("extra", "BOOLEAN"))
        b = self._make(("id", "STRING"))
        diff = compare_schemas(a, b)
        assert "extra" in diff.only_in_a
        assert not diff.only_in_b
        assert not diff.is_equal

    def test_field_only_in_b(self) -> None:
        a = self._make(("id", "STRING"))
        b = self._make(("id", "STRING"), ("new_col", "FLOAT"))
        diff = compare_schemas(a, b)
        assert not diff.only_in_a
        assert "new_col" in diff.only_in_b

    def test_type_mismatch(self) -> None:
        a = self._make(("id", "STRING"), ("score", "FLOAT"))
        b = self._make(("id", "STRING"), ("score", "INTEGER"))
        diff = compare_schemas(a, b)
        assert not diff.is_equal
        assert len(diff.type_mismatches) == 1
        name, type_a, type_b = diff.type_mismatches[0]
        assert name == "score"
        assert type_a == "FLOAT"
        assert type_b == "INTEGER"

    def test_empty_schemas_are_equal(self) -> None:
        diff = compare_schemas([], [])
        assert diff.is_equal
