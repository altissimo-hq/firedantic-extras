"""Firestore-specific query helpers for Firedantic models.

These utilities complement Firedantic's ``find()`` API with server-side
operations (COUNT aggregation) and common query patterns (prefix search).

``count_model`` is for ``firedantic.Model`` subclasses; ``async_count_model``
is the same for ``firedantic.AsyncModel``.  ``build_prefix_filters`` only
builds a filter dict and works with either.
"""

from firedantic_extras._async.query import async_count_model
from firedantic_extras._sync.query import count_model
from firedantic_extras.common.filters import (
    FilterDict,
    _apply_filter_dict,
    build_prefix_filters,
)

__all__ = [
    "FilterDict",
    "_apply_filter_dict",
    "async_count_model",
    "build_prefix_filters",
    "count_model",
]
