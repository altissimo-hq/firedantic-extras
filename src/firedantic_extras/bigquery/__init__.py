"""Firedantic Extras: BigQuery integration (schema generation).

Usage::

    from firedantic_extras.bigquery import model_to_bq_schema, schema_to_dict

Requires the optional dependency::

    pip install firedantic-extras[bigquery]
"""

from firedantic_extras.bigquery.schema import (
    SchemaDiff,
    compare_schemas,
    model_to_bq_schema,
    models_to_bq_schemas,
    schema_to_dict,
)

__all__ = [
    "SchemaDiff",
    "compare_schemas",
    "model_to_bq_schema",
    "models_to_bq_schemas",
    "schema_to_dict",
]
