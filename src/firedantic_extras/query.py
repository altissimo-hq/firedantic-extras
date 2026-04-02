"""Firestore-specific query helpers for Firedantic models.

These utilities complement Firedantic's ``find()`` API with server-side
operations (COUNT aggregation) and common query patterns (prefix search).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from google.cloud.firestore_v1 import FieldFilter
from google.cloud.firestore_v1.base_query import BaseQuery

if TYPE_CHECKING:
    from firedantic import BareModel

# Firedantic filter_ dict: {"field": value} or {"field": {"op": value, ...}}
FilterDict = dict[str, Any]


def _apply_filter_dict(
    query: BaseQuery,
    filter_: FilterDict,
) -> BaseQuery:
    """Apply a Firedantic-style filter_ dict to a Firestore query.

    Mirrors the logic inside ``BareModel._add_filter`` so that all utilities
    in this module accept the same filter convention as ``BareModel.find()``.

    Args:
        query: A Firestore query or collection reference.
        filter_: A Firedantic-style filter dict.

    Returns:
        The query with all filters applied.
    """
    for key, value in filter_.items():
        if isinstance(value, dict):
            for operator, operand in value.items():
                query = query.where(filter=FieldFilter(key, operator, operand))
        else:
            query = query.where(filter=FieldFilter(key, "==", value))
    return query


def count_model(
    model_class: type[BareModel],
    filter_: FilterDict | None = None,
) -> int:
    """Return the count of documents in a Firedantic model's collection.

    Uses Firestore's native server-side COUNT aggregation — zero documents are
    transferred over the wire regardless of collection size.

    Accepts the same ``filter_`` dict convention as ``BareModel.find()``:
    - ``{"field": value}`` → equality filter
    - ``{"field": {">=": value}}`` → comparison filter
    - ``{"field": {">=": low, "<": high}}`` → multiple operators on one field

    Args:
        model_class: The Firedantic model class whose collection to count.
        filter_: Optional Firedantic-style filter dict to narrow the count.

    Returns:
        The number of matching documents as an integer.

    Example::

        total = count_model(Kit)
        dog_kits = count_model(Kit, filter_={"species": "dog"})
        recent = count_model(Order, filter_={"created_at": {">=": cutoff}})
    """
    query: BaseQuery = model_class._get_col_ref()  # type: ignore[attr-defined]
    if filter_:
        query = _apply_filter_dict(query, filter_)
    result = query.count().get()  # type: ignore[union-attr]
    return result[0][0].value


def build_prefix_filters(field: str, prefix: str) -> FilterDict:
    """Return a Firedantic-style filter_ dict for a string prefix search.

    Firestore does not support ``LIKE`` or regex queries, but an ASCII/Unicode
    range query achieves the same effect for prefix matching.  The sentinel
    ``\\uf8ff`` is the highest code point in Unicode's Private Use Area and
    effectively acts as a "wildcard suffix", matching any string that starts
    with ``prefix``.

    Args:
        field: The Firestore field name to search on.
        prefix: The prefix string to search for.

    Returns:
        A Firedantic-compatible filter dict ready to pass to ``BareModel.find()``
        or ``cursor_paginate()`` or ``count_model()``.

    Example::

        filters = build_prefix_filters("barcode", "DA-0001")
        # Returns: {"barcode": {">=": "DA-0001", "<": "DA-0001\uf8ff"}}

        results = Kit.find(filters)
        page = cursor_paginate(Kit, filters=filters, order_by="barcode")
        total = count_model(Kit, filter_=filters)
    """
    if not prefix:
        raise ValueError("prefix must be a non-empty string")
    return {field: {">=": prefix, "<": prefix + "\uf8ff"}}
