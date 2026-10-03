"""AsyncCollectionSync — synchronize a Firestore collection from a desired-state list.

Computes the difference between a desired list of Firedantic model instances and
the live Firestore collection, then applies the necessary adds, updates, and
(optionally) deletes using batched Firestore write operations.

The comparison is performed against **raw Firestore data**, not re-hydrated
model instances.  This means extra or stale fields stored in Firestore (e.g.
from a previous schema version or an external writer) are visible during the
diff and will cause an update, ensuring Firestore always converges to the exact
shape described by the model.

Only the two Firestore steps live here — streaming the existing collection and
committing the batched writes.  The diff itself is
:func:`~firedantic_extras.common.sync_plan.build_sync_plan`.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from firedantic import get_async_batch

from firedantic_extras.common.sync_plan import (
    OnDuplicateKeys,
    OnError,
    SyncResult,
    _ExistingDoc,
    _ExistingIndex,
    _handle_error,
    _index_desired,
    _iter_chunks,
    _load_existing_doc,
    _reconcile_desired,
    _resolve_duplicates,
    _SyncPlan,
    build_sync_plan,
    resolve_preserved_keys,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Sequence

    from firedantic import AsyncBareModel

logger = logging.getLogger(__name__)


async def _fetch_existing(
    model: type[AsyncBareModel],
    sync_key: str | None,
    on_duplicate_keys: OnDuplicateKeys,
) -> _ExistingIndex:
    """Fetch all existing documents from the Firestore collection.

    Streams the collection, hydrates each document into a model instance, and
    indexes the results by the configured sync key.

    Args:
        model:              The Firedantic model class.
        sync_key:           Field to index on.  ``None`` uses the document ID.
        on_duplicate_keys:  How to handle multiple documents sharing the same
                            key value (``"raise"``, ``"skip"``, or
                            ``"update_all"``).

    Returns:
        An :class:`~firedantic_extras.common.sync_plan._ExistingIndex`.

    Raises:
        DuplicateKeyError: When ``on_duplicate_keys="raise"`` and duplicates
                           are found.
    """
    key_field = sync_key if sync_key is not None else model.__document_id__

    # First pass: collect all snapshots grouped by key value so we can detect
    # and handle duplicates before committing anything to the result dicts.
    # The raw snapshot data is what the diff compares against, so this stays
    # on the collection stream rather than find().
    seen: dict[str, list[_ExistingDoc]] = {}

    async for doc_snap in model._get_col_ref().stream():
        doc_id: str = doc_snap.id
        raw: dict[str, Any] = doc_snap.to_dict() or {}
        loaded = _load_existing_doc(model, doc_id, raw, sync_key=sync_key)
        if loaded is None:
            continue
        key_value, model_instance = loaded
        seen.setdefault(key_value, []).append((doc_id, model_instance, raw))

    return _resolve_duplicates(seen, key_field=key_field, on_duplicate_keys=on_duplicate_keys)


async def _apply_plan(
    plan: _SyncPlan,
    model: type[AsyncBareModel],
    existing_by_doc_id: dict[str, AsyncBareModel],
    *,
    chunk_size: int,
    dry_run: bool,
    on_error: OnError,
    output_writer: Callable[[str], None] | None,
) -> SyncResult:
    """Apply a :class:`_SyncPlan` to Firestore using firedantic's batched writes.

    This is the only I/O-heavy step.  All logic (what to write, what to skip)
    has already been decided by :func:`build_sync_plan`.  Every write goes
    through the model — ``save(batch=...)`` for adds and updates (a full
    ``set``, so stale fields are removed) and ``delete(batch=...)`` for
    deletes — so ID generation, aliases, stored-value conversion and
    ``__db_config__`` routing are firedantic's.  An update with a payload in
    :attr:`_SyncPlan.partial_updates` (preserved fields) goes through the
    model's ``update(batch=...)`` instead of ``save()``: a field-level write
    of just that payload, which leaves the preserved fields as they are in
    Firestore.

    Args:
        plan:          The sync plan to execute.
        model:         The Firedantic model class (used for config + col ref).
        existing_by_doc_id: The existing documents' model instances by
                       document ID, for the deletes.
        chunk_size:    Max operations per Firestore batch commit (≤ 500).
        dry_run:       When ``True``, skip all writes — return a result that
                       reflects what *would* have happened.
        on_error:      Per-document error strategy.
        output_writer: Logging callable, or ``None`` to suppress output.

    Returns:
        A :class:`SyncResult` reflecting the applied changes.
    """
    result = SyncResult(dry_run=dry_run, diffs=plan.diffs)
    result.skips = len(plan.to_skip)

    doc_id_field = model.__document_id__
    config_name = getattr(model, "__db_config__", "(default)")

    def _log(msg: str) -> None:
        if output_writer:
            output_writer(msg)

    async def _commit(batch: Any, count: int) -> None:
        if not dry_run:
            await batch.commit()
            _log(f"  Committed batch ({count} operations).")

    # --- Adds ---
    if plan.to_add:
        _log(f"{'[DRY RUN] ' if dry_run else ''}Adding {len(plan.to_add)} document(s)...")
        for chunk in _iter_chunks(plan.to_add, chunk_size):
            batch = get_async_batch(config_name)
            batch_count = 0
            for model_instance in chunk:
                try:
                    if not dry_run:
                        # Generates and sets the ID on the model if it has none.
                        await model_instance.save(batch=batch)
                    result.adds += 1
                    batch_count += 1
                except Exception as exc:
                    _handle_error(exc, str(getattr(model_instance, doc_id_field, "?")), result, on_error)
            await _commit(batch, batch_count)

    # --- Updates ---
    if plan.to_update:
        _log(f"{'[DRY RUN] ' if dry_run else ''}Updating {len(plan.to_update)} document(s)...")
        for chunk in _iter_chunks(plan.to_update, chunk_size):
            batch = get_async_batch(config_name)
            batch_count = 0
            for doc_id, model_instance in chunk:
                try:
                    if not dry_run:
                        # The existing document's ID wins (sync_key matching may
                        # have paired the item with a differently-named doc, and
                        # "update_all" pairs one item with several); a copy keeps
                        # the caller's instance untouched.
                        target = model_instance.model_copy(update={doc_id_field: doc_id})
                        payload = plan.partial_updates.get(doc_id)
                        if payload is None:
                            await target.save(batch=batch)
                        else:
                            await target.update(payload, batch=batch)
                    result.updates += 1
                    batch_count += 1
                except Exception as exc:
                    _handle_error(exc, doc_id, result, on_error)
            await _commit(batch, batch_count)

    # --- Deletes ---
    if plan.to_delete:
        _log(f"{'[DRY RUN] ' if dry_run else ''}Deleting {len(plan.to_delete)} document(s)...")
        for chunk in _iter_chunks(plan.to_delete, chunk_size):
            batch = get_async_batch(config_name)
            batch_count = 0
            for doc_id in chunk:
                try:
                    if not dry_run:
                        await existing_by_doc_id[doc_id].delete(batch=batch)
                    result.deletes += 1
                    batch_count += 1
                except Exception as exc:
                    _handle_error(exc, doc_id, result, on_error)
            await _commit(batch, batch_count)

    return result


class AsyncCollectionSync:
    """Reconcile a Firestore collection to match a desired list of models.

    Computes adds, updates, and (optionally) deletes by comparing a desired-
    state list of Firedantic model instances against the live Firestore
    collection.  Writes are batched for efficiency.

    The comparison is done against **raw Firestore data** (not re-hydrated
    models), so stale or extra fields stored in Firestore are visible and will
    trigger updates — ensuring Firestore always converges to the exact shape
    described by the model.

    Args:
        model:              Firedantic model class for the target collection.
        items:              Desired state — every model instance that should
                            exist after the sync.
        delete_items:       If ``True``, documents in Firestore that are not
                            present in ``items`` are deleted.  Defaults to
                            ``False`` — must be explicitly enabled to avoid
                            accidental data loss.
        dry_run:            If ``True``, compute and log the plan but make no
                            writes to Firestore.
        diff:               If ``True``, collect field-level diffs for all
                            updated documents.  Available in
                            :attr:`SyncResult.diffs`.
        output_writer:      Callable for progress messages (e.g.
                            ``logger.info``).  Pass ``None`` to suppress all
                            output.
        sync_key:           Field name to match incoming items to existing
                            documents.  Defaults to ``None``, which uses the
                            model's ``__document_id__`` field.  Set to a field
                            name (e.g. ``"email"``) to match on a non-ID field.
        on_duplicate_keys:  What to do when ``sync_key`` matches more than one
                            existing document.  One of ``"raise"`` (default),
                            ``"skip"``, or ``"update_all"``.
        on_error:           Per-document error strategy.  One of ``"raise"``
                            (default), ``"collect"``, or ``"skip"``.
        chunk_size:         Maximum operations per Firestore batch write.
                            Capped at 500 (Firestore hard limit).
        preserve_fields:    Top-level fields another writer owns.  On
                            existing documents they are never compared or
                            written: they don't trigger updates or count as
                            stale, and updates write only the changed
                            fields (a field-level ``update()`` instead of a
                            full ``set``), so a concurrent write to a
                            preserved field is kept.  New documents are
                            still written whole, with the model's values.
                            Model field names resolve to their aliases;
                            other names are taken as stored keys.

    Example::

        desired = [
            User(id="u1", name="Alice", email="alice@example.com"),
            User(id="u2", name="Bob",   email="bob@example.com"),
        ]

        # Additive sync — adds / updates only, never deletes.
        result = await AsyncCollectionSync.sync(User, desired)

        # Full sync — also removes documents not in the desired list.
        result = await AsyncCollectionSync.sync(User, desired, delete_items=True)

        # Dry run with field-level diff output.
        result = await AsyncCollectionSync.sync(
            User, desired, delete_items=True, diff=True, dry_run=True,
        )
        print(result.summary())

        # Keep fields a webhook writes, e.g. email bookkeeping.
        result = await AsyncCollectionSync.sync(
            Order, desired, preserve_fields=["delivered_email_sent_at"],
        )
    """

    def __init__(
        self,
        model: type[AsyncBareModel],
        items: Sequence[AsyncBareModel],
        *,
        delete_items: bool = False,
        dry_run: bool = False,
        diff: bool = False,
        output_writer: Callable[[str], None] | None = print,
        sync_key: str | None = None,
        on_duplicate_keys: OnDuplicateKeys = "raise",
        on_error: OnError = "raise",
        chunk_size: int = 500,
        preserve_fields: Collection[str] = (),
    ) -> None:
        self._model = model
        self._items = list(items)
        self._delete_items = delete_items
        self._dry_run = dry_run
        self._diff = diff
        self._output_writer = output_writer
        self._sync_key = sync_key
        self._on_duplicate_keys = on_duplicate_keys
        self._on_error = on_error
        self._chunk_size = min(chunk_size, 500)  # enforce Firestore hard limit
        # Resolved up front so a bad name fails before anything is read.
        self._preserve_keys = resolve_preserved_keys(model, preserve_fields, model.__document_id__)

    async def run(self) -> SyncResult:
        """Execute the sync and return a :class:`SyncResult`.

        Steps:
          1. Index the desired items by sync key.
          2. Fetch existing documents from Firestore.
          3. :func:`build_sync_plan` — pure logic, no I/O.
          4. :func:`_apply_plan` — batched Firestore writes.
        """
        doc_id_field = self._model.__document_id__

        # 1. Index desired items.
        desired = _index_desired(self._items, self._sync_key, doc_id_field)

        # 2. Fetch existing from Firestore.
        existing = await _fetch_existing(self._model, self._sync_key, self._on_duplicate_keys)

        # 3. Build plan (pure, no I/O).  Duplicate-key handling must be applied
        #    to both sides, or the desired item looks brand new to the planner.
        plan = build_sync_plan(
            desired=_reconcile_desired(desired, existing),
            existing_models=existing.models,
            existing_raw=existing.raw,
            doc_id_field=doc_id_field,
            delete_items=self._delete_items,
            diff=self._diff,
            preserve_keys=self._preserve_keys,
        )

        if self._output_writer:
            col_name = self._model.get_collection_name()
            self._output_writer(
                f"CollectionSync '{col_name}': "
                f"{len(plan.to_add)} add(s), "
                f"{len(plan.to_update)} update(s), "
                f"{len(plan.to_delete)} delete(s), "
                f"{len(plan.to_skip)} unchanged."
            )

        # 4. Apply plan (I/O).
        result = await _apply_plan(
            plan,
            self._model,
            {doc_id: m for m in existing.models.values() if (doc_id := m.get_document_id()) is not None},
            chunk_size=self._chunk_size,
            dry_run=self._dry_run,
            on_error=self._on_error,
            output_writer=self._output_writer,
        )
        result.skipped_duplicate_keys = existing.skipped_keys
        return result

    @classmethod
    async def sync(
        cls,
        model: type[AsyncBareModel],
        items: Sequence[AsyncBareModel],
        **kwargs: Any,
    ) -> SyncResult:
        """Convenience class method — construct and run in one call.

        Equivalent to ``await AsyncCollectionSync(model, items, **kwargs).run()``.
        """
        return await cls(model, items, **kwargs).run()
