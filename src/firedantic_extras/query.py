"""Firestore-specific query helpers for Firedantic models.

These utilities complement Firedantic's ``find()`` API with server-side
operations (COUNT aggregation) and common query patterns (prefix search).

``count_model`` is for ``firedantic.Model`` subclasses; ``async_count_model``
is the same for ``firedantic.AsyncModel``.  ``build_prefix_filters`` only
builds a filter dict and works with either.  Filters go straight to
firedantic, so ``$or`` / ``$and`` (``firedantic.operators.OR`` / ``AND``)
work everywhere a ``filter_`` is accepted.
"""

from firedantic_extras._async.query import async_count_model
from firedantic_extras._sync.query import count_model
from firedantic_extras.common.filters import FilterDict, build_prefix_filters

__all__ = [
    "FilterDict",
    "async_count_model",
    "build_prefix_filters",
    "count_model",
]
