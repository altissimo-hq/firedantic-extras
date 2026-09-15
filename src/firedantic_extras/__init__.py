"""Firedantic Extras: add-on utilities for Firedantic."""

__version__ = "0.1.7"

from firedantic_extras.cursor_pagination import CursorPage, cursor_paginate
from firedantic_extras.query import build_prefix_filters, count_model
from firedantic_extras.update_collection import (
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
    "CollectionSync",
    # Pagination
    "CursorPage",
    "DocumentDiff",
    "DuplicateKeyError",
    "FieldDiff",
    "SyncError",
    "SyncResult",
    "UpdateCollection",
    # Query helpers
    "build_prefix_filters",
    "build_sync_plan",
    "count_model",
    "cursor_paginate",
]
