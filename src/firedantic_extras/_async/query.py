"""Firestore query helpers that need a round-trip: COUNT aggregation."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from firedantic import AsyncBareModel

    from firedantic_extras.common.filters import FilterDict


async def async_count_model(
    model_class: type[AsyncBareModel],
    filter_: FilterDict | None = None,
) -> int:
    """Return the count of documents in a Firedantic model's collection.

    Uses Firestore's native server-side COUNT aggregation — zero documents are
    transferred over the wire regardless of collection size.

    A thin wrapper over firedantic's ``Model.count()`` (0.16+), kept so that
    callers have one place to import the sync and async variants from and so
    the ``filter_`` convention matches ``cursor_paginate``:
    - ``{"field": value}`` → equality filter
    - ``{"field": {">=": value}}`` → comparison filter
    - ``{"field": {">=": low, "<": high}}`` → multiple operators on one field
    - ``{"$or": [filter, ...]}`` / ``{"$and": [filter, ...]}`` → composites

    Args:
        model_class: The Firedantic model class whose collection to count.
        filter_: Optional Firedantic-style filter dict to narrow the count.

    Returns:
        The number of matching documents as an integer.

    Example::

        total = await async_count_model(Kit)
        dog_kits = await async_count_model(Kit, filter_={"species": "dog"})
        recent = await async_count_model(Order, filter_={"created_at": {">=": cutoff}})
    """
    return await model_class.count(filter_)
