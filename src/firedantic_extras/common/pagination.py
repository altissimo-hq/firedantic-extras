"""Cursor-pagination logic that needs no Firestore round-trip.

Everything here is a pure function of its inputs.  The I/O layers
(``firedantic_extras._async.cursor_pagination`` and its generated sync twin)
call :func:`_plan_page` before hitting Firestore and :func:`_assemble_page`
afterwards, so the code that actually differs between flavours is a handful
of lines.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, Generic, Literal, Protocol, TypeVar, cast

# (Final literals keep ASCENDING / DESCENDING assignable to firedantic's
# OrderDirection.)
from pydantic import BaseModel

if TYPE_CHECKING:
    from firedantic.common import OrderDirection

    from firedantic_extras.common.filters import FilterDict


class _Paged(Protocol):
    """What a page item must offer: firedantic models qualify."""

    def get_document_id(self) -> str | None: ...


T = TypeVar("T", bound=_Paged)

# Firedantic / Firestore direction literals
ASCENDING: Final = "ASCENDING"
DESCENDING: Final = "DESCENDING"

OrderByInput = str | list[str | tuple[str, str]]
"""
Flexible order_by input:
- ``"field"``                       → sort field ASC
- ``["field1", ("field2", "DESCENDING")]``  → mixed list
"""

#: Validated ``(field, direction)`` pairs — what firedantic's ``find()`` takes.
OrderByPairs = list[tuple[str, "OrderDirection"]]

Direction = Literal["next", "prev"]


class CursorPage(BaseModel, Generic[T]):
    """A single page of results from ``cursor_paginate`` / ``async_cursor_paginate``.

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
        used_fallback: ``True`` if the primary ``order_by`` hit a
                      ``FailedPrecondition`` (missing composite index) and
                      this page was fetched with ``fallback_order_by`` instead.
    """

    model_config = {"arbitrary_types_allowed": True}

    items: list[Any]  # list[T] — kept as Any for Pydantic Generic compat
    has_next: bool
    has_prev: bool
    next_cursor: str | None
    prev_cursor: str | None
    total: int | None = None
    used_fallback: bool = False


def _normalise_order_by(order_by: OrderByInput | None) -> OrderByPairs:
    """Convert the flexible OrderByInput into a list of (field, direction) pairs."""
    if order_by is None:
        return []
    if isinstance(order_by, str):
        return [(order_by, ASCENDING)]
    result: OrderByPairs = []
    for item in order_by:
        if isinstance(item, str):
            result.append((item, ASCENDING))
        else:
            field, direction = item
            if direction not in (ASCENDING, DESCENDING):
                raise ValueError(f"Invalid sort direction {direction!r}. Use {ASCENDING!r} or {DESCENDING!r}.")
            result.append((field, cast("OrderDirection", direction)))
    return result


def _reverse_pairs(pairs: OrderByPairs) -> OrderByPairs:
    """Flip every sort direction in *pairs* (ASCENDING ↔ DESCENDING)."""
    return [(field, ASCENDING if direction == DESCENDING else DESCENDING) for field, direction in pairs]


@dataclass(frozen=True)
class _PagePlan:
    """What to ask Firestore for, decided before any I/O happens.

    Attributes:
        filter_:     The effective filter dict (``exclude_null_sort_field``
                     may have added a ``!=`` clause to the caller's filter).
        order_by:    The caller's sort, validated, as ``(field, direction)``
                     pairs.  The I/O layer completes it (inequality-filtered
                     fields, then the ``__name__`` tiebreak) with firedantic's
                     full-ordering helper and reverses it for
                     ``direction="prev"``.
        fetch_limit: ``limit + 1`` — the extra row is a sentinel used to detect
                     whether another page exists without a COUNT query.
    """

    filter_: FilterDict | None
    order_by: OrderByPairs
    fetch_limit: int


def _plan_page(
    *,
    limit: int,
    direction: Direction,
    filter_: FilterDict | None,
    order_by: OrderByInput | None,
    exclude_null_sort_field: bool,
) -> _PagePlan:
    """Validate the pagination arguments and work out the query shape."""
    order_by_pairs = _normalise_order_by(order_by)

    if exclude_null_sort_field:
        if not order_by_pairs:
            raise ValueError("exclude_null_sort_field=True requires order_by to be set.")
        sort_field = order_by_pairs[0][0]
        if filter_ and sort_field in filter_:
            raise ValueError(
                f"exclude_null_sort_field=True conflicts with an existing filter_ entry for {sort_field!r}."
            )
        filter_ = {**(filter_ or {}), sort_field: {"!=": None}}

    return _PagePlan(filter_=filter_, order_by=order_by_pairs, fetch_limit=limit + 1)


def _assemble_page(
    items: list[T],
    *,
    limit: int,
    direction: Direction,
    has_cursor: bool,
    total: int | None,
    used_fallback: bool,
) -> CursorPage[T]:
    """Turn the fetched models (already in ascending order) into a page.

    ``items`` must be at most ``limit + 1`` rows.  The sentinel row, when
    present, is at the END for ``direction="next"`` and at the START for
    ``direction="prev"`` (the prev query runs reversed and the caller flips the
    result list, so the row fetched "furthest back" comes first).
    """
    fetch_limit = limit + 1

    if direction == "next":
        has_next = len(items) == fetch_limit
        has_prev = has_cursor
        if has_next:
            items = items[:limit]  # drop sentinel at the end
    else:  # prev
        has_prev = len(items) == fetch_limit
        has_next = has_cursor
        if has_prev:
            items = items[1:]  # drop sentinel at the start

    # next_cursor → ID of the last visible item  (direction="next" to go fwd)
    # prev_cursor → ID of the first visible item (direction="prev" to go bwd)
    next_cursor = items[-1].get_document_id() if items and has_next else None
    prev_cursor = items[0].get_document_id() if items and has_prev else None

    return CursorPage(
        items=items,
        has_next=has_next,
        has_prev=has_prev,
        next_cursor=next_cursor,
        prev_cursor=prev_cursor,
        total=total,
        used_fallback=used_fallback,
    )
