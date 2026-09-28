"""Firedantic-style ``filter_`` dict helpers.  Pure — no Firestore I/O.

Filters are passed straight through to firedantic's ``find()`` / ``count()``,
so everything they accept works here too: ``{"field": value}``,
``{"field": {"op": value, ...}}`` and the ``$or`` / ``$and`` composites
(``firedantic.operators.OR`` / ``AND``).
"""

from __future__ import annotations

from typing import Any

# Firedantic filter_ dict: {"field": value}, {"field": {"op": value, ...}},
# {"$or": [filter, ...]} or {"$and": [filter, ...]}.
FilterDict = dict[str, Any]


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
