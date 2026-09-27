"""Firedantic Extras: add-on utilities for Firedantic.

Every Firestore-touching helper comes in two flavours: the plain name works
with ``firedantic.Model`` (sync) and the ``async_`` / ``Async`` name with
``firedantic.AsyncModel``.
"""

__version__ = "0.1.8"

from firedantic_extras.cursor_pagination import CursorPage, async_cursor_paginate, cursor_paginate
from firedantic_extras.query import async_count_model, build_prefix_filters, count_model
from firedantic_extras.update_collection import (
    AsyncCollectionSync,
    CollectionSync,
    DocumentDiff,
    DuplicateKeyError,
    FieldDiff,
    SyncError,
    SyncResult,
    UpdateCollection,
    build_sync_plan,
)

__all__ = [
    # CollectionSync
    "AsyncCollectionSync",
    "CollectionSync",
    # Pagination
    "CursorPage",
    "DocumentDiff",
    "DuplicateKeyError",
    "FieldDiff",
    "SyncError",
    "SyncResult",
    "UpdateCollection",
    "async_count_model",
    "async_cursor_paginate",
    # Query helpers
    "build_prefix_filters",
    "build_sync_plan",
    "count_model",
    "cursor_paginate",
]
