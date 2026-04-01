"""Firedantic Extras: add-on utilities for Firedantic."""

__version__ = "0.1.0"

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
    "CollectionSync",
    "DocumentDiff",
    "DuplicateKeyError",
    "FieldDiff",
    "SyncError",
    "SyncResult",
    "UpdateCollection",
    "build_sync_plan",
]
