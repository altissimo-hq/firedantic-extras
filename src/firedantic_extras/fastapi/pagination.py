"""FastAPI adapter for cursor-based pagination.

Provides a ``Depends``-injectable :class:`PaginationParams` class and
re-exports the core :class:`~firedantic_extras.cursor_pagination.CursorPage`
and :func:`~firedantic_extras.cursor_pagination.cursor_paginate` so that
FastAPI routes only need a single import.

Typical usage::

    from fastapi import APIRouter, Depends
    from firedantic_extras.fastapi.pagination import (
        PaginationParams,
        CursorPage,
        cursor_paginate,
    )

    router = APIRouter()

    @router.get("/kits", response_model=CursorPage)
    def list_kits(page: PaginationParams = Depends()):
        return cursor_paginate(
            Kit,
            limit=page.limit,
            cursor=page.cursor,
            direction=page.direction,
            order_by="barcode",
        )
"""

from __future__ import annotations

from typing import Literal

try:
    from fastapi import Query
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "fastapi is required to use firedantic_extras.fastapi.pagination. " "Install it with: pip install fastapi"
    ) from exc

# Re-export the core types so callers can use a single import.
from firedantic_extras.cursor_pagination import CursorPage, cursor_paginate

__all__ = [
    "CursorPage",
    "PaginationParams",
    "cursor_paginate",
]


class PaginationParams:
    """FastAPI-injectable pagination parameters.

    Declare as a ``Depends`` argument in any route that uses
    :func:`cursor_paginate`.  All three parameters are read from the
    query string automatically by FastAPI.

    Attributes:
        cursor:    Document ID of the page boundary (opaque to callers —
                   copy directly from a previous :class:`CursorPage`
                   response's ``next_cursor`` or ``prev_cursor``).
        direction: ``"next"`` (default) to move forward; ``"prev"`` to
                   move backward.
        limit:     Number of items per page (1–500, default 50).

    Example::

        @router.get("/kits", response_model=CursorPage)
        def list_kits(page: PaginationParams = Depends()):
            return cursor_paginate(
                Kit,
                limit=page.limit,
                cursor=page.cursor,
                direction=page.direction,
                order_by="barcode",
            )
    """

    def __init__(
        self,
        cursor: str | None = Query(
            None,
            description=(
                "Opaque page cursor — the document ID returned as "
                "``next_cursor`` or ``prev_cursor`` from a previous page."
            ),
        ),
        direction: Literal["next", "prev"] = Query(
            "next",
            description='Page direction: "next" moves forward, "prev" moves backward.',
        ),
        limit: int = Query(
            50,
            ge=1,
            le=500,
            description="Number of items per page (1–500).",
        ),
    ) -> None:
        self.cursor = cursor
        self.direction = direction
        self.limit = limit
