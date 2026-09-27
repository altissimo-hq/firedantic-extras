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

With ``firedantic.AsyncModel`` subclasses use :func:`async_cursor_paginate`
instead — same signature, ``await`` it.

Implementation lives in ``firedantic_extras.common.pagination`` (pure logic),
``firedantic_extras._async.cursor_pagination`` (async I/O) and the generated
``firedantic_extras._sync.cursor_pagination`` (sync I/O).
"""

from firedantic_extras._async.cursor_pagination import async_cursor_paginate
from firedantic_extras._sync.cursor_pagination import cursor_paginate
from firedantic_extras.common.pagination import (
    ASCENDING,
    DESCENDING,
    CursorPage,
    Direction,
    OrderByInput,
)

__all__ = [
    "ASCENDING",
    "DESCENDING",
    "CursorPage",
    "Direction",
    "OrderByInput",
    "async_cursor_paginate",
    "cursor_paginate",
]
