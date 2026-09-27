# GENERATED FILE — do not edit.
# Source: src/firedantic_extras/_async/update_collection.py. Regenerate with `poetry run python unasync.py`.
"""CollectionSync — synchronize a Firestore collection from a desired-state list.

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

from firedantic.configurations import configuration

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
    _model_data,
    _reconcile_desired,
    _resolve_duplicates,
    _SyncPlan,
    build_sync_plan,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from firedantic import BareModel

logger = logging.getLogger(__name__)


def _fetch_existing(
    model: type[BareModel],
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
    seen: dict[str, list[_ExistingDoc]] = {}

    for doc_snap in model._get_col_ref().stream():
        doc_id: str = doc_snap.id
        raw: dict[str, Any] = doc_snap.to_dict() or {}
        loaded = _load_existing_doc(model, doc_id, raw, sync_key=sync_key)
        if loaded is None:
            continue
        key_value, model_instance = loaded
        seen.setdefault(key_value, []).append((doc_id, model_instance, raw))

    return _resolve_duplicates(seen, key_field=key_field, on_duplicate_keys=on_duplicate_keys)


def _apply_plan(
    plan: _SyncPlan,
    model: type[BareModel],
    *,
    chunk_size: int,
    dry_run: bool,
    on_error: OnError,
    output_writer: Callable[[str], None] | None,
) -> SyncResult:
    """Apply a :class:`_SyncPlan` to Firestore using batched writes.

    This is the only I/O-heavy step.  All logic (what to write, what to skip)
    has already been decided by :func:`build_sync_plan`.

    Args:
        plan:          The sync plan to execute.
        model:         The Firedantic model class (used for config + col ref).
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
    client = configuration.get_client(config_name)
    col_ref = model._get_col_ref()

    def _log(msg: str) -> None:
        if output_writer:
            output_writer(msg)

    def _commit(batch: Any, count: int) -> None:
        if not dry_run:
            batch.commit()
            _log(f"  Committed batch ({count} operations).")

    # --- Adds ---
    if plan.to_add:
        _log(f"{'[DRY RUN] ' if dry_run else ''}Adding {len(plan.to_add)} document(s)...")
        for chunk in _iter_chunks(plan.to_add, chunk_size):
            batch = client.batch()
            batch_count = 0
            for model_instance in chunk:
                try:
                    doc_id = model_instance.get_document_id()
                    data = _model_data(model_instance, doc_id_field)
                    doc_ref = col_ref.document(doc_id) if doc_id else col_ref.document()
                    if not dry_run:
                        batch.set(doc_ref, data)
                    result.adds += 1
                    batch_count += 1
                except Exception as exc:
                    _handle_error(exc, str(getattr(model_instance, doc_id_field, "?")), result, on_error)
            _commit(batch, batch_count)

    # --- Updates ---
    if plan.to_update:
        _log(f"{'[DRY RUN] ' if dry_run else ''}Updating {len(plan.to_update)} document(s)...")
        for chunk in _iter_chunks(plan.to_update, chunk_size):
            batch = client.batch()
            batch_count = 0
            for doc_id, model_instance in chunk:
                try:
                    data = _model_data(model_instance, doc_id_field)
                    doc_ref = col_ref.document(doc_id)
                    if not dry_run:
                        batch.set(doc_ref, data)
                    result.updates += 1
                    batch_count += 1
                except Exception as exc:
                    _handle_error(exc, doc_id, result, on_error)
            _commit(batch, batch_count)

    # --- Deletes ---
    if plan.to_delete:
        _log(f"{'[DRY RUN] ' if dry_run else ''}Deleting {len(plan.to_delete)} document(s)...")
        for chunk in _iter_chunks(plan.to_delete, chunk_size):
            batch = client.batch()
            batch_count = 0
            for doc_id in chunk:
                try:
                    doc_ref = col_ref.document(doc_id)
                    if not dry_run:
                        batch.delete(doc_ref)
                    result.deletes += 1
                    batch_count += 1
                except Exception as exc:
                    _handle_error(exc, doc_id, result, on_error)
            _commit(batch, batch_count)

    return result


class CollectionSync:
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

    Example::

        desired = [
            User(id="u1", name="Alice", email="alice@example.com"),
            User(id="u2", name="Bob",   email="bob@example.com"),
        ]

        # Additive sync — adds / updates only, never deletes.
        result = CollectionSync.sync(User, desired)

        # Full sync — also removes documents not in the desired list.
        result = CollectionSync.sync(User, desired, delete_items=True)

        # Dry run with field-level diff output.
        result = CollectionSync.sync(
            User, desired, delete_items=True, diff=True, dry_run=True,
        )
        print(result.summary())
    """

    def __init__(
        self,
        model: type[BareModel],
        items: Sequence[BareModel],
        *,
        delete_items: bool = False,
        dry_run: bool = False,
        diff: bool = False,
        output_writer: Callable[[str], None] | None = print,
        sync_key: str | None = None,
        on_duplicate_keys: OnDuplicateKeys = "raise",
        on_error: OnError = "raise",
        chunk_size: int = 500,
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

    def run(self) -> SyncResult:
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
        existing = _fetch_existing(self._model, self._sync_key, self._on_duplicate_keys)

        # 3. Build plan (pure, no I/O).  Duplicate-key handling must be applied
        #    to both sides, or the desired item looks brand new to the planner.
        plan = build_sync_plan(
            desired=_reconcile_desired(desired, existing),
            existing_models=existing.models,
            existing_raw=existing.raw,
            doc_id_field=doc_id_field,
            delete_items=self._delete_items,
            diff=self._diff,
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
        result = _apply_plan(
            plan,
            self._model,
            chunk_size=self._chunk_size,
            dry_run=self._dry_run,
            on_error=self._on_error,
            output_writer=self._output_writer,
        )
        result.skipped_duplicate_keys = existing.skipped_keys
        return result

    @classmethod
    def sync(
        cls,
        model: type[BareModel],
        items: Sequence[BareModel],
        **kwargs: Any,
    ) -> SyncResult:
        """Convenience class method — construct and run in one call.

        Equivalent to ``CollectionSync(model, items, **kwargs).run()``.
        """
        return cls(model, items, **kwargs).run()
