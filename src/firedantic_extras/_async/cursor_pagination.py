"""Cursor-based pagination for Firedantic models — the Firestore round-trips.

All argument validation, sort-order planning and page assembly is in
:mod:`firedantic_extras.common.pagination`; this module only runs the
``find()`` / ``count()`` calls and applies the ``fallback_order_by`` retry.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, TypeVar

from firedantic import AsyncBareModel, ModelNotFoundError
from google.api_core.exceptions import FailedPrecondition

from firedantic_extras.common.pagination import (
    CursorPage,
    Direction,
    OrderByInput,
    _assemble_page,
    _plan_page,
    _reverse_pairs,
)

if TYPE_CHECKING:
    from firedantic_extras.common.filters import FilterDict

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=AsyncBareModel)


async def _cursor_paginate_once(
    model_class: type[T],
    *,
    limit: int,
    cursor: str | None,
    direction: Direction,
    filter_: FilterDict | None,
    order_by: OrderByInput | None,
    include_total: bool,
    exclude_null_sort_field: bool,
    used_fallback: bool = False,
) -> CursorPage[T]:
    """Single query attempt behind :func:`async_cursor_paginate` — no retry logic."""
    plan = _plan_page(
        limit=limit,
        direction=direction,
        filter_=filter_,
        order_by=order_by,
        exclude_null_sort_field=exclude_null_sort_field,
    )

    # A cursor is only unambiguous under the *full* ordering Firestore uses:
    # the caller's sort, then every inequality-filtered field, then __name__
    # (which must come last).  firedantic computes that — including fields
    # inside $or / $and — so we don't keep a copy of the rule here.  For the
    # prev direction every field is reversed so start_after + limit works
    # instead of limit_to_last; the rows are flipped back below.
    ordering = model_class.get_full_ordering(plan.filter_, plan.order_by)
    if direction == "prev":
        ordering = _reverse_pairs(ordering)

    try:
        # ``find`` resolves a bare document ID against the model's collection,
        # which is exactly what ``next_cursor`` / ``prev_cursor`` hold.  That
        # is the one extra read per page-turn.
        items = await model_class.find(
            plan.filter_,
            order_by=ordering,
            limit=plan.fetch_limit,
            start_after=cursor,
        )
    except ModelNotFoundError as exc:
        raise ValueError(
            f"Cursor document {cursor!r} not found in "
            f"collection {model_class.get_collection_name()!r}. The document may have been deleted."
        ) from exc

    if direction == "prev":
        # The prev query runs with every sort reversed; flip back to ascending.
        items.reverse()

    total = await model_class.count(plan.filter_) if include_total else None

    return _assemble_page(
        items,
        limit=limit,
        direction=direction,
        has_cursor=cursor is not None,
        total=total,
        used_fallback=used_fallback,
    )


async def async_cursor_paginate(
    model_class: type[T],
    *,
    limit: int = 50,
    cursor: str | None = None,
    direction: Direction = "next",
    filter_: FilterDict | None = None,
    order_by: OrderByInput | None = None,
    include_total: bool = False,
    exclude_null_sort_field: bool = False,
    fallback_order_by: OrderByInput | None = None,
) -> CursorPage[T]:
    """Fetch one page of results for a Firedantic model using cursor pagination.

    Uses Firestore's ``start_after`` cursor method for efficient page
    traversal — no offsets, no full-collection scans.

    The cursor is a **Firestore document ID**.  On each page, ``next_cursor``
    is the ID of the last item and ``prev_cursor`` is the ID of the first
    item.  Pass one of these back as ``cursor`` on the next request.

    Resolving the cursor requires one extra Firestore document read per
    page-turn (to fetch the ``DocumentSnapshot`` needed by the Firestore
    cursor API).

    ``__name__`` (Firestore's internal document ID) is always appended as the
    final sort key to guarantee stable pagination when the caller's sort fields
    contain duplicate values.

    Args:
        model_class:    The Firedantic model class to query.
        limit:          Maximum items to return per page (default 50).
        cursor:         Document ID marking the page boundary.  ``None``
                        returns the first page (or last page when
                        ``direction="prev"``).
        direction:      ``"next"`` (default) moves forward in the sort order;
                        ``"prev"`` moves backward.
        filter_:        Optional Firedantic-style filter dict — same format as
                        ``BareModel.find()``, including ``$or`` / ``$and``.
        order_by:       Sort specification.  A field name string, or a list of
                        strings / ``(field, direction)`` tuples.  Direction
                        must be ``"ASCENDING"`` or ``"DESCENDING"``.
        include_total:  If ``True``, runs a secondary COUNT aggregation query
                        and populates :attr:`CursorPage.total`.
        exclude_null_sort_field: If ``True``, adds a ``!=`` filter that excludes
                        documents where the primary sort field (the first
                        entry in ``order_by``) is ``null`` or missing.  Firestore
                        sorts ``null`` values before all others in ascending
                        order (after, in descending), so paginating over an
                        optional field otherwise surfaces a run of useless
                        null-valued documents before any real data appears.
                        Requires ``order_by`` to be set, and conflicts with a
                        ``filter_`` entry already present for that field.
        fallback_order_by: If the query raises ``FailedPrecondition`` (a
                        missing Firestore composite index for this
                        filter + ``order_by`` combination — firedantic's
                        ``MissingIndexError``, whose message names the index
                        to add), retry once with this sort spec instead.  The
                        cursor and direction are reset (``cursor=None``,
                        ``direction="next"``) since a cursor encoded for one
                        sort order is meaningless under another.
                        :attr:`CursorPage.used_fallback` is ``True`` on the
                        returned page so callers can flash a message.  With no
                        ``fallback_order_by``, the error propagates as usual.

    Returns:
        A :class:`CursorPage` instance.

    Raises:
        ValueError: If ``limit`` < 1, the ``exclude_null_sort_field`` options
                    conflict, or the ``cursor`` document no longer exists.

    Example::

        # First 100 kits sorted by barcode
        page = await async_cursor_paginate(Kit, limit=100, order_by="barcode")

        # Next 100 (using cursor from previous response)
        page2 = await async_cursor_paginate(
            Kit,
            limit=100,
            order_by="barcode",
            cursor=page.next_cursor,
            direction="next",
        )

        # Prefix search + pagination
        from firedantic_extras.query import build_prefix_filters
        page = await async_cursor_paginate(
            Kit,
            limit=50,
            filter_=build_prefix_filters("barcode", "DA-0001"),
            order_by="barcode",
        )

        # OR filters, nested with $and
        from firedantic import operators as op
        page = await async_cursor_paginate(
            Kit,
            limit=50,
            filter_={op.OR: [{"status": "new"}, {op.AND: [{"status": "open"}, {"priority": {op.GTE: 3}}]}]},
            order_by="barcode",
        )

        # Sorting by an optional field — skip null/missing values
        page = await async_cursor_paginate(
            Kit,
            limit=100,
            order_by="order_id",
            exclude_null_sort_field=True,
        )

        # Missing composite index — fall back to a safe default sort
        page = await async_cursor_paginate(
            Kit,
            limit=100,
            filter_=filter_,
            order_by=[("barcode", "ASCENDING")],
            fallback_order_by=[("updated_at", "DESCENDING")],
        )
        if page.used_fallback:
            flash("Sorting by barcode isn't available with this filter — showing most recently updated instead.")
    """
    if limit < 1:
        raise ValueError(f"limit must be >= 1, got {limit}")

    try:
        return await _cursor_paginate_once(
            model_class,
            limit=limit,
            cursor=cursor,
            direction=direction,
            filter_=filter_,
            order_by=order_by,
            include_total=include_total,
            exclude_null_sort_field=exclude_null_sort_field,
        )
    except FailedPrecondition as exc:
        if fallback_order_by is None:
            raise
        logger.warning(
            "cursor_paginate: %s for %s with order_by=%r; retrying with fallback_order_by=%r",
            exc,
            model_class.__name__,
            order_by,
            fallback_order_by,
        )
        return await _cursor_paginate_once(
            model_class,
            limit=limit,
            cursor=None,
            direction="next",
            filter_=filter_,
            order_by=fallback_order_by,
            include_total=include_total,
            exclude_null_sort_field=exclude_null_sort_field,
            used_fallback=True,
        )
