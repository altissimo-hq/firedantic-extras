"""Firestore query helpers that need a round-trip: COUNT aggregation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from firedantic_extras.common.filters import FilterDict, _apply_filter_dict

if TYPE_CHECKING:
    from firedantic import AsyncBareModel
    from google.cloud.firestore_v1.async_query import AsyncQuery


async def async_count_model(
    model_class: type[AsyncBareModel],
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

        total = await async_count_model(Kit)
        dog_kits = await async_count_model(Kit, filter_={"species": "dog"})
        recent = await async_count_model(Order, filter_={"created_at": {">=": cutoff}})
    """
    query: AsyncQuery = model_class._get_col_ref()
    if filter_:
        query = _apply_filter_dict(query, filter_)
    # google-cloud-firestore annotates ``count()`` as returning the aggregation
    # *class* rather than an instance, which trips mypy on ``.get()``.
    aggregation: Any = query.count()
    result = await aggregation.get()
    return int(result[0][0].value)
