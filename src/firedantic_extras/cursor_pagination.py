"""Cursor-based pagination for Firedantic models.

Framework-agnostic: works with Flask, FastAPI, CLIs, or any Python code.
Framework adapters (e.g. ``firedantic_extras.fastapi.pagination``) build
thin wrappers on top of this module.

Typical usage::

    from firedantic_extras.cursor_pagination import CursorPage, cursor_paginate

    # First page
    page = cursor_paginate(Kit, limit=100, order_by="barcode")

    # Next page
    page2 = cursor_paginate(Kit, limit=100, order_by="barcode",
                            cursor=page.next_cursor, direction="next")

    # Previous page (from page2 back to page1)
    page1_again = cursor_paginate(Kit, limit=100, order_by="barcode",
                                  cursor=page2.prev_cursor, direction="prev")
"""

from __future__ import annotations

import logging
from typing import Any, Generic, Literal, TypeVar

from firedantic import BareModel
from google.cloud.firestore_v1.base_query import BaseQuery
from pydantic import BaseModel

from firedantic_extras.query import FilterDict, _apply_filter_dict

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BareModel)

# Firedantic / Firestore direction literals
ASCENDING = "ASCENDING"
DESCENDING = "DESCENDING"

OrderByInput = str | list[str | tuple[str, str]]
"""
Flexible order_by input:
- ``"field"``                       → sort field ASC
- ``["field1", ("field2", "DESCENDING")]``  → mixed list
"""


class CursorPage(BaseModel, Generic[T]):
    """A single page of results from :func:`cursor_paginate`.

    Attributes:
        items:        The hydrated model instances for this page.
        has_next:     Whether a next page exists.
        has_prev:     Whether a previous page exists.
        next_cursor:  Document ID of the last item — pass as ``cursor``
                      with ``direction="next"`` to fetch the next page.
        prev_cursor:  Document ID of the first item — pass as ``cursor``
                      with ``direction="prev"`` to fetch the previous page.
        total:        Total matching documents (populated only when
                      ``include_total=True`` is passed).
    """

    model_config = {"arbitrary_types_allowed": True}

    items: list[Any]  # list[T] — kept as Any for Pydantic Generic compat
    has_next: bool
    has_prev: bool
    next_cursor: str | None
    prev_cursor: str | None
    total: int | None = None


def _normalise_order_by(order_by: OrderByInput | None) -> list[tuple[str, str]]:
    """Convert the flexible OrderByInput into a list of (field, direction) pairs."""
    if order_by is None:
        return []
    if isinstance(order_by, str):
        return [(order_by, ASCENDING)]
    result: list[tuple[str, str]] = []
    for item in order_by:
        if isinstance(item, str):
            result.append((item, ASCENDING))
        else:
            field, direction = item
            if direction not in (ASCENDING, DESCENDING):
                raise ValueError(f"Invalid sort direction {direction!r}. " f"Use {ASCENDING!r} or {DESCENDING!r}.")
            result.append((field, direction))
    return result


def _with_tiebreaker(
    pairs: list[tuple[str, str]],
    direction: str = ASCENDING,
) -> list[tuple[str, str]]:
    """Append ``__name__`` as the final tiebreak field.

    Firestore silently skips duplicates at page boundaries without a unique
    final sort key.  ``__name__`` (the document ID) is always unique.
    """
    return pairs + [("__name__", direction)]


def _reverse_pairs(pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Flip every sort direction in *pairs* (ASCENDING ↔ DESCENDING)."""
    return [(field, ASCENDING if direction == DESCENDING else DESCENDING) for field, direction in pairs]


def _build_query(
    model_class: type[BareModel],
    ordered_pairs: list[tuple[str, str]],
    filter_: FilterDict | None,
) -> BaseQuery:
    """Build a Firestore query with filters and an explicit ordered sort list.

    Unlike the old ``_build_base_query``, the caller is responsible for
    including the ``__name__`` tiebreaker in *ordered_pairs*.
    """
    query: BaseQuery = model_class._get_col_ref()  # type: ignore[attr-defined]

    if filter_:
        query = _apply_filter_dict(query, filter_)

    for field, direction in ordered_pairs:
        query = query.order_by(field, direction=direction)

    return query


def _hydrate(
    model_class: type[BareModel],
    snapshot: Any,
) -> BareModel:
    """Hydrate a raw ``DocumentSnapshot`` into a model instance."""
    doc_id: str = snapshot.id
    data: dict[str, Any] = snapshot.to_dict() or {}
    doc_id_field = model_class.__document_id__
    data[doc_id_field] = doc_id
    instance = model_class(**data)
    setattr(instance, doc_id_field, doc_id)
    return instance


def _fetch_cursor_snapshot(
    model_class: type[BareModel],
    cursor_doc_id: str,
) -> Any:
    """Fetch the Firestore DocumentSnapshot for the given document ID.

    This is the one extra read per page-turn that allows us to use
    ``start_after(snapshot)`` / ``end_before(snapshot)`` without needing
    to encode complex field values into the cursor token.

    Args:
        model_class: The model whose collection contains the cursor document.
        cursor_doc_id: The document ID of the cursor document.

    Returns:
        A Firestore ``DocumentSnapshot``.

    Raises:
        ValueError: If the cursor document does not exist in Firestore.
    """
    col_ref = model_class._get_col_ref()  # type: ignore[attr-defined]
    doc_ref = col_ref.document(cursor_doc_id)
    snapshot = doc_ref.get()
    if not snapshot.exists:
        raise ValueError(
            f"Cursor document {cursor_doc_id!r} not found in "
            f"collection {col_ref.id!r}. The document may have been deleted."
        )
    return snapshot


def cursor_paginate(
    model_class: type[T],
    *,
    limit: int = 50,
    cursor: str | None = None,
    direction: Literal["next", "prev"] = "next",
    filter_: FilterDict | None = None,
    order_by: OrderByInput | None = None,
    include_total: bool = False,
) -> CursorPage[T]:
    """Fetch one page of results for a Firedantic model using cursor pagination.

    Uses Firestore's ``start_after`` / ``end_before`` cursor methods for
    efficient page traversal — no offsets, no full-collection scans.

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
    """
    if limit < 1:
        raise ValueError(f"limit must be >= 1, got {limit}")

    order_by_pairs = _normalise_order_by(order_by)

    # Build canonical sort pairs (user fields + __name__ tiebreaker)
    fwd_pairs = _with_tiebreaker(order_by_pairs, ASCENDING)
    # For prev direction we reverse every sort field so we can use
    # start_after + limit (+ .stream()) instead of limit_to_last.
    # After fetching, we reverse the result list to restore ascending order.
    rev_pairs = _reverse_pairs(fwd_pairs)

    # We request one extra row as a sentinel to cheaply detect whether
    # another page exists without a separate COUNT query.
    fetch_limit = limit + 1

    if direction == "next":
        fwd_query = _build_query(model_class, fwd_pairs, filter_)
        if cursor is None:
            query = fwd_query.limit(fetch_limit)
        else:
            cursor_snapshot = _fetch_cursor_snapshot(model_class, cursor)
            query = fwd_query.start_after(cursor_snapshot).limit(fetch_limit)
        snapshots = list(query.stream())  # type: ignore[arg-type]
    else:  # direction == "prev"
        rev_query = _build_query(model_class, rev_pairs, filter_)
        if cursor is None:
            query = rev_query.limit(fetch_limit)
        else:
            cursor_snapshot = _fetch_cursor_snapshot(model_class, cursor)
            query = rev_query.start_after(cursor_snapshot).limit(fetch_limit)
        # Results arrive in reversed order; flip back to ascending.
        snapshots = list(reversed(list(query.stream())))  # type: ignore[arg-type]

    # --- Determine has_next / has_prev from the sentinel ---
    #
    # direction=="next"  : sentinel (if any) is at the END   (index -1)
    # direction=="prev"  : sentinel (if any) is at the START (index 0)
    #                      because we reversed the list; the item fetched
    #                      "furthest back" is first after the flip.
    if direction == "next":
        has_next = len(snapshots) == fetch_limit
        has_prev = cursor is not None
        if has_next:
            snapshots = snapshots[:limit]  # drop sentinel at the end
    else:  # prev
        has_prev = len(snapshots) == fetch_limit
        has_next = cursor is not None
        if has_prev:
            snapshots = snapshots[1:]  # drop sentinel at the start

    items = [_hydrate(model_class, snap) for snap in snapshots]

    # next_cursor → ID of the last visible item  (direction="next" to go fwd)
    # prev_cursor → ID of the first visible item (direction="prev" to go bwd)
    next_cursor: str | None = snapshots[-1].id if items and has_next else None
    prev_cursor: str | None = snapshots[0].id if items and has_prev else None

    total: int | None = None
    if include_total:
        from firedantic_extras.query import count_model

        total = count_model(model_class, filter_=filter_)

    return CursorPage(
        items=items,
        has_next=has_next,
        has_prev=has_prev,
        next_cursor=next_cursor,
        prev_cursor=prev_cursor,
        total=total,
    )
