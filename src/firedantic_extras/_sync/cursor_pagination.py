# GENERATED FILE — do not edit.
# Source: src/firedantic_extras/_async/cursor_pagination.py. Regenerate with `poetry run python unasync.py`.
"""Cursor-based pagination for Firedantic models — the Firestore round-trips.

All argument validation, sort-order planning and page assembly is in
:mod:`firedantic_extras.common.pagination`; this module only builds the query,
resolves the cursor document, streams the rows and applies the
``fallback_order_by`` retry.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, TypeVar

from firedantic import BareModel
from google.api_core.exceptions import FailedPrecondition

from firedantic_extras._sync.query import count_model
from firedantic_extras.common.filters import FilterDict, _apply_filter_dict
from firedantic_extras.common.pagination import (
    CursorPage,
    Direction,
    OrderByInput,
    _assemble_page,
    _plan_page,
)

if TYPE_CHECKING:
    from google.cloud.firestore_v1.query import Query

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BareModel)


def _build_query(
    model_class: type[BareModel],
    ordered_pairs: list[tuple[str, str]],
    filter_: FilterDict | None,
) -> Query:
    """Build a Firestore query with filters and an explicit ordered sort list.

    The caller is responsible for including the ``__name__`` tiebreaker in
    *ordered_pairs* (see :func:`~firedantic_extras.common.pagination._plan_page`).
    """
    query: Query = model_class._get_col_ref()

    if filter_:
        query = _apply_filter_dict(query, filter_)

    for field, direction in ordered_pairs:
        query = query.order_by(field, direction=direction)

    return query


def _fetch_cursor_snapshot(
    model_class: type[BareModel],
    cursor_doc_id: str,
) -> Any:
    """Fetch the Firestore DocumentSnapshot for the given document ID.

    This is the one extra read per page-turn that allows us to use
    ``start_after(snapshot)`` without needing to encode complex field values
    into the cursor token.

    Args:
        model_class: The model whose collection contains the cursor document.
        cursor_doc_id: The document ID of the cursor document.

    Returns:
        A Firestore ``DocumentSnapshot``.

    Raises:
        ValueError: If the cursor document does not exist in Firestore.
    """
    col_ref = model_class._get_col_ref()
    doc_ref = col_ref.document(cursor_doc_id)
    snapshot = doc_ref.get()
    if not snapshot.exists:
        raise ValueError(
            f"Cursor document {cursor_doc_id!r} not found in "
            f"collection {col_ref.id!r}. The document may have been deleted."
        )
    return snapshot


def _cursor_paginate_once(
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
    """Single query attempt behind :func:`cursor_paginate` — no retry logic."""
    plan = _plan_page(
        limit=limit,
        direction=direction,
        filter_=filter_,
        order_by=order_by,
        exclude_null_sort_field=exclude_null_sort_field,
    )

    query = _build_query(model_class, plan.pairs, plan.filter_)
    if cursor is not None:
        query = query.start_after(_fetch_cursor_snapshot(model_class, cursor))
    query = query.limit(plan.fetch_limit)

    snapshots = [snap for snap in query.stream()]
    if direction == "prev":
        # The prev query runs with every sort reversed; flip back to ascending.
        snapshots.reverse()

    total = count_model(model_class, filter_=plan.filter_) if include_total else None

    return _assemble_page(
        model_class,
        snapshots,
        limit=limit,
        direction=direction,
        has_cursor=cursor is not None,
        total=total,
        used_fallback=used_fallback,
    )


def cursor_paginate(
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
                        ``BareModel.find()``.
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
                        filter + ``order_by`` combination), retry once with
                        this sort spec instead.  The cursor and direction are
                        reset (``cursor=None``, ``direction="next"``) since a
                        cursor encoded for one sort order is meaningless
                        under another.  :attr:`CursorPage.used_fallback` is
                        ``True`` on the returned page so callers can flash a
                        message.  With no ``fallback_order_by``, a
                        ``FailedPrecondition`` propagates as usual.

    Returns:
        A :class:`CursorPage` instance.

    Example::

        # First 100 kits sorted by barcode
        page = cursor_paginate(Kit, limit=100, order_by="barcode")

        # Next 100 (using cursor from previous response)
        page2 = cursor_paginate(
            Kit,
            limit=100,
            order_by="barcode",
            cursor=page.next_cursor,
            direction="next",
        )

        # Prefix search + pagination
        from firedantic_extras.query import build_prefix_filters
        page = cursor_paginate(
            Kit,
            limit=50,
            filter_=build_prefix_filters("barcode", "DA-0001"),
            order_by="barcode",
        )

        # Sorting by an optional field — skip null/missing values
        page = cursor_paginate(
            Kit,
            limit=100,
            order_by="order_id",
            exclude_null_sort_field=True,
        )

        # Missing composite index — fall back to a safe default sort
        page = cursor_paginate(
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
        return _cursor_paginate_once(
            model_class,
            limit=limit,
            cursor=cursor,
            direction=direction,
            filter_=filter_,
            order_by=order_by,
            include_total=include_total,
            exclude_null_sort_field=exclude_null_sort_field,
        )
    except FailedPrecondition:
        if fallback_order_by is None:
            raise
        logger.warning(
            "cursor_paginate: FailedPrecondition for %s with order_by=%r; retrying with fallback_order_by=%r",
            model_class.__name__,
            order_by,
            fallback_order_by,
        )
        return _cursor_paginate_once(
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
