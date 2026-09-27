"""CollectionSync — synchronize a Firestore collection from a desired-state list.

Computes the difference between a desired list of Firedantic model instances and
the live Firestore collection, then applies the necessary adds, updates, and
(optionally) deletes using batched Firestore write operations.

The comparison is performed against **raw Firestore data**, not re-hydrated
model instances.  This means extra or stale fields stored in Firestore (e.g.
from a previous schema version or an external writer) are visible during the
diff and will cause an update, ensuring Firestore always converges to the exact
shape described by the model.

Typical usage::

    from firedantic_extras import CollectionSync

    result = CollectionSync.sync(User, desired_users, delete_items=True, diff=True)
    print(result.summary())

    # firedantic.AsyncModel subclasses:
    from firedantic_extras import AsyncCollectionSync

    result = await AsyncCollectionSync.sync(User, desired_users, delete_items=True)

Public surface
--------------
CollectionSync       Main class (sync models).
AsyncCollectionSync  Main class (async models) — same options, ``await`` it.
SyncResult           Typed result returned by .run() / .sync().
DocumentDiff         Per-document field-level diff (when diff=True).
FieldDiff            Single field change (before / after).
SyncError            Error for one document (when on_error != "raise").
DuplicateKeyError    Raised when sync_key matches multiple existing docs.
build_sync_plan      Pure function; useful for testing / inspection.
UpdateCollection     Backward-compatible alias for CollectionSync.

Implementation lives in ``firedantic_extras.common.sync_plan`` (all the logic,
pure), ``firedantic_extras._async.update_collection`` (async I/O) and the
generated ``firedantic_extras._sync.update_collection`` (sync I/O).
"""

from firedantic_extras._async.update_collection import AsyncCollectionSync
from firedantic_extras._sync.update_collection import CollectionSync
from firedantic_extras.common.sync_plan import _MISSING as _MISSING
from firedantic_extras.common.sync_plan import (
    DocumentDiff,
    DuplicateKeyError,
    FieldDiff,
    OnDuplicateKeys,
    OnError,
    SyncError,
    SyncResult,
    build_sync_plan,
)
from firedantic_extras.common.sync_plan import _compute_field_diffs as _compute_field_diffs
from firedantic_extras.common.sync_plan import _index_desired as _index_desired

# Backward-compatible alias matching the README and prior implementations.
UpdateCollection = CollectionSync

__all__ = [
    "AsyncCollectionSync",
    "CollectionSync",
    "DocumentDiff",
    "DuplicateKeyError",
    "FieldDiff",
    "OnDuplicateKeys",
    "OnError",
    "SyncError",
    "SyncResult",
    "UpdateCollection",
    "build_sync_plan",
]
