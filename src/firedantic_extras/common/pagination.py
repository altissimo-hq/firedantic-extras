"""Cursor-pagination logic that needs no Firestore round-trip.

Everything here is a pure function of its inputs.  The I/O layers
(``firedantic_extras._async.cursor_pagination`` and its generated sync twin)
call :func:`_plan_page` before hitting Firestore and :func:`_assemble_page`
afterwards, so the code that actually differs between flavours is a handful
of lines.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar

from pydantic import BaseModel

if TYPE_CHECKING:
    from firedantic_extras.common.filters import FilterDict

T = TypeVar("T")

# Firedantic / Firestore direction literals
ASCENDING = "ASCENDING"
DESCENDING = "DESCENDING"

OrderByInput = str | list[str | tuple[str, str]]
"""
Flexible order_by input:
- ``"field"``                       → sort field ASC
- ``["field1", ("field2", "DESCENDING")]``  → mixed list
"""

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
                raise ValueError(f"Invalid sort direction {direction!r}. Use {ASCENDING!r} or {DESCENDING!r}.")
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
    return [*pairs, ("__name__", direction)]


def _reverse_pairs(pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Flip every sort direction in *pairs* (ASCENDING ↔ DESCENDING)."""
    return [(field, ASCENDING if direction == DESCENDING else DESCENDING) for field, direction in pairs]


def _hydrate(
    model_class: type[T],
    snapshot: Any,
) -> T:
    """Hydrate a raw ``DocumentSnapshot`` into a model instance."""
    doc_id: str = snapshot.id
    data: dict[str, Any] = snapshot.to_dict() or {}
    doc_id_field: str = model_class.__document_id__  # type: ignore[attr-defined]
    data[doc_id_field] = doc_id
    instance = model_class(**data)
    setattr(instance, doc_id_field, doc_id)
    return instance


@dataclass(frozen=True)
class _PagePlan:
    """What to ask Firestore for, decided before any I/O happens.

    Attributes:
        filter_:     The effective filter dict (``exclude_null_sort_field``
                     may have added a ``!=`` clause to the caller's filter).
        pairs:       Ordered ``(field, direction)`` list to apply — already
                     includes the ``__name__`` tiebreaker and is reversed for
                     ``direction="prev"``.
        fetch_limit: ``limit + 1`` — the extra row is a sentinel used to detect
                     whether another page exists without a COUNT query.
    """

    filter_: FilterDict | None
    pairs: list[tuple[str, str]]
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

    # Canonical sort pairs (user fields + __name__ tiebreaker).  For the prev
    # direction every sort field is reversed so we can use start_after + limit
    # (+ .stream()) instead of limit_to_last; the fetched rows are flipped back
    # into ascending order by the caller.
    fwd_pairs = _with_tiebreaker(order_by_pairs, ASCENDING)
    pairs = fwd_pairs if direction == "next" else _reverse_pairs(fwd_pairs)

    return _PagePlan(filter_=filter_, pairs=pairs, fetch_limit=limit + 1)


def _assemble_page(
    model_class: type[T],
    snapshots: list[Any],
    *,
    limit: int,
    direction: Direction,
    has_cursor: bool,
    total: int | None,
    used_fallback: bool,
) -> CursorPage[T]:
    """Turn the fetched snapshots (already in ascending order) into a page.

    ``snapshots`` must be at most ``limit + 1`` rows.  The sentinel row, when
    present, is at the END for ``direction="next"`` and at the START for
    ``direction="prev"`` (the prev query runs reversed and the caller flips the
    result list, so the row fetched "furthest back" comes first).
    """
    fetch_limit = limit + 1

    if direction == "next":
        has_next = len(snapshots) == fetch_limit
        has_prev = has_cursor
        if has_next:
            snapshots = snapshots[:limit]  # drop sentinel at the end
    else:  # prev
        has_prev = len(snapshots) == fetch_limit
        has_next = has_cursor
        if has_prev:
            snapshots = snapshots[1:]  # drop sentinel at the start

    items = [_hydrate(model_class, snap) for snap in snapshots]

    # next_cursor → ID of the last visible item  (direction="next" to go fwd)
    # prev_cursor → ID of the first visible item (direction="prev" to go bwd)
    next_cursor: str | None = snapshots[-1].id if items and has_next else None
    prev_cursor: str | None = snapshots[0].id if items and has_prev else None

    return CursorPage(
        items=items,
        has_next=has_next,
        has_prev=has_prev,
        next_cursor=next_cursor,
        prev_cursor=prev_cursor,
        total=total,
        used_fallback=used_fallback,
    )
