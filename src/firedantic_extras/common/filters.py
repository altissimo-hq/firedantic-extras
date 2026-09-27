"""Firedantic-style ``filter_`` dict helpers.  Pure — no Firestore I/O."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar

from google.cloud.firestore_v1 import FieldFilter

if TYPE_CHECKING:
    from google.cloud.firestore_v1.base_query import BaseQuery

# Firedantic filter_ dict: {"field": value} or {"field": {"op": value, ...}}
FilterDict = dict[str, Any]

# ``Query`` or ``AsyncQuery`` — ``where()`` returns the same flavour it was
# called on, so the filter helper is generic over it.
_QueryT = TypeVar("_QueryT", bound="BaseQuery")


def _apply_filter_dict(
    query: _QueryT,
    filter_: FilterDict,
) -> _QueryT:
    """Apply a Firedantic-style filter_ dict to a Firestore query.

    Mirrors the logic inside ``BareModel._add_filter`` so that all utilities
    in this library accept the same filter convention as ``BareModel.find()``.

    Only builds the query object — nothing is sent to Firestore, so the same
    code serves both the sync and async query classes.

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
        # Returns: {"barcode": {">=": "DA-0001", "<": "DA-0001"}}

        results = Kit.find(filters)
        page = cursor_paginate(Kit, filters=filters, order_by="barcode")
        total = count_model(Kit, filter_=filters)
    """
    if not prefix:
        raise ValueError("prefix must be a non-empty string")
    return {field: {">=": prefix, "<": prefix + ""}}
