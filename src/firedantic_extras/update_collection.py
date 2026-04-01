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

Public surface
--------------
CollectionSync       Main class.
SyncResult           Typed result returned by .run() / .sync().
DocumentDiff         Per-document field-level diff (when diff=True).
FieldDiff            Single field change (before / after).
SyncError            Error for one document (when on_error != "raise").
DuplicateKeyError    Raised when sync_key matches multiple existing docs.
build_sync_plan      Pure function; useful for testing / inspection.
UpdateCollection     Backward-compatible alias for CollectionSync.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from firedantic._sync.model import BareModel
from firedantic.configurations import configuration

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Option types
# ---------------------------------------------------------------------------

#: How to handle errors for individual documents during apply.
OnError = Literal["raise", "collect", "skip"]

#: What to do when a custom sync_key matches more than one existing document.
OnDuplicateKeys = Literal["raise", "skip", "update_all"]

# Sentinel — represents a field that is absent on one side of a diff.
_MISSING: Any = object()


# ---------------------------------------------------------------------------
# Public exception
# ---------------------------------------------------------------------------


class DuplicateKeyError(ValueError):
    """Raised when ``on_duplicate_keys="raise"`` and duplicates are detected.

    Indicates that the Firestore collection has multiple documents sharing the
    same value for the configured ``sync_key`` field.  Resolve the duplicates
    in Firestore before syncing, or choose a different ``on_duplicate_keys``
    strategy.
    """


# ---------------------------------------------------------------------------
# Public result types
# ---------------------------------------------------------------------------


@dataclass
class FieldDiff:
    """A single field-level change between the existing and desired state.

    Attributes:
        field:  The field name.
        before: Value currently in Firestore.  ``_MISSING`` if the field was
                absent in the stored document.
        after:  Value in the desired model.  ``_MISSING`` if the field is not
                present in the incoming model (i.e. the field will be removed).
    """

    field: str
    before: Any
    after: Any


@dataclass
class DocumentDiff:
    """All field-level changes for a single document.

    Attributes:
        doc_id:         Firestore document ID.
        sync_key_value: Value of the sync key used to match this document.
        changes:        Ordered list of :class:`FieldDiff` objects.
    """

    doc_id: str
    sync_key_value: str
    changes: list[FieldDiff] = field(default_factory=list)


@dataclass
class SyncError:
    """An error encountered while processing a single document.

    Attributes:
        sync_key_value: The sync key value of the failing document.
        error:          The original exception.
    """

    sync_key_value: str
    error: Exception


@dataclass
class SyncResult:
    """The outcome of a :class:`CollectionSync` run.

    Attributes:
        adds:     Number of documents added.
        updates:  Number of documents updated.
        deletes:  Number of documents deleted.
        skips:    Number of documents that were identical and required no write.
        diffs:    Mapping of sync_key_value → :class:`DocumentDiff`.
                  Populated only when ``diff=True`` was passed.
        errors:   List of :class:`SyncError` objects.
                  Populated only when ``on_error != "raise"``.
        dry_run:  Whether this was a dry run (no writes were made).
    """

    adds: int = 0
    updates: int = 0
    deletes: int = 0
    skips: int = 0
    diffs: dict[str, DocumentDiff] = field(default_factory=dict)
    errors: list[SyncError] = field(default_factory=list)
    dry_run: bool = False

    @property
    def has_errors(self) -> bool:
        """True if any document-level errors were collected."""
        return bool(self.errors)

    @property
    def total_changes(self) -> int:
        """Total number of write operations (adds + updates + deletes)."""
        return self.adds + self.updates + self.deletes

    def summary(self) -> str:
        """Return a compact human-readable one-liner."""
        parts = [
            f"adds={self.adds}",
            f"updates={self.updates}",
            f"deletes={self.deletes}",
            f"skips={self.skips}",
        ]
        if self.errors:
            parts.append(f"errors={len(self.errors)}")
        if self.dry_run:
            parts.append("DRY RUN")
        return "SyncResult(" + ", ".join(parts) + ")"


# ---------------------------------------------------------------------------
# Internal plan type (not part of the public API)
# ---------------------------------------------------------------------------


@dataclass
class _SyncPlan:
    """Output of :func:`build_sync_plan`.  Pure data — no I/O."""

    #: Model instances to be written as new Firestore documents.
    to_add: list[BareModel] = field(default_factory=list)

    #: (firestore_doc_id, model_instance) pairs whose documents need updating.
    to_update: list[tuple[str, BareModel]] = field(default_factory=list)

    #: Firestore document IDs to delete.
    to_delete: list[str] = field(default_factory=list)

    #: sync_key values for documents that were identical (no write needed).
    to_skip: list[str] = field(default_factory=list)

    #: sync_key_value → DocumentDiff; populated only when diff=True.
    diffs: dict[str, DocumentDiff] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Pure helper — field-level diff computation
# ---------------------------------------------------------------------------


def _compute_field_diffs(
    doc_id: str,
    sync_key_value: str,
    existing_data: dict[str, Any],
    incoming_data: dict[str, Any],
) -> DocumentDiff:
    """Compute field-level differences between two plain dicts.

    Returns a :class:`DocumentDiff` containing one :class:`FieldDiff` per
    field that differs.  Fields absent on one side are represented with the
    module-level ``_MISSING`` sentinel so callers can distinguish "field was
    removed" from "field was set to None".

    This is a pure function — no I/O.
    """
    all_keys = set(existing_data) | set(incoming_data)
    changes: list[FieldDiff] = []
    for k in sorted(all_keys):
        before = existing_data.get(k, _MISSING)
        after = incoming_data.get(k, _MISSING)
        if before != after:
            changes.append(FieldDiff(field=k, before=before, after=after))
    return DocumentDiff(doc_id=doc_id, sync_key_value=sync_key_value, changes=changes)


# ---------------------------------------------------------------------------
# Pure function — the heart of the sync logic
# ---------------------------------------------------------------------------


def build_sync_plan(
    desired: dict[str, BareModel],
    existing_models: dict[str, BareModel],
    existing_raw: dict[str, dict[str, Any]],
    *,
    doc_id_field: str,
    delete_items: bool = False,
    diff: bool = False,
) -> _SyncPlan:
    """Compute what needs to change.  **Pure function — no I/O.**

    This is the core logic of the sync.  All Firestore reads and writes happen
    outside this function, making it trivial to unit-test with plain dicts.

    Args:
        desired:         ``sync_key_value → model`` for every document that
                         should exist after the sync.
        existing_models: ``sync_key_value → model`` loaded from Firestore.
                         Used only to retrieve the Firestore document ID.
        existing_raw:    ``sync_key_value → dict`` — the **raw** data exactly
                         as stored in Firestore, *including* any extra fields
                         that may not be present on the model.  This is what
                         makes stale-field detection possible.
        doc_id_field:    The model's ``__document_id__`` attribute (e.g.
                         ``"id"``).  Stripped from both sides before comparison
                         so it never causes false-positive updates.
        delete_items:    When ``True``, keys present in ``existing_raw`` but
                         absent from ``desired`` are added to
                         :attr:`_SyncPlan.to_delete`.
        diff:            When ``True``, populate :attr:`_SyncPlan.diffs` with
                         field-level :class:`DocumentDiff` objects for every
                         updated document.

    Returns:
        A :class:`_SyncPlan` with the computed changes.
    """
    plan = _SyncPlan()

    for sync_key_value, desired_model in desired.items():
        if sync_key_value not in existing_raw:
            # Brand-new document — does not exist in Firestore.
            plan.to_add.append(desired_model)
        else:
            # Document exists — compare raw Firestore data vs incoming model.
            raw = existing_raw[sync_key_value]
            doc_id = existing_models[sync_key_value].get_document_id()

            # Strip the document-ID field from both sides before comparing so
            # a mismatch between the ID field and the stored value (common when
            # the model's id field appears in the raw dict) is never treated as
            # a data change.
            existing_data = {k: v for k, v in raw.items() if k != doc_id_field}
            incoming_data = {k: v for k, v in desired_model.model_dump(by_alias=True).items() if k != doc_id_field}

            if incoming_data == existing_data:
                plan.to_skip.append(sync_key_value)
            else:
                plan.to_update.append((doc_id, desired_model))
                if diff:
                    plan.diffs[sync_key_value] = _compute_field_diffs(
                        doc_id, sync_key_value, existing_data, incoming_data
                    )

    if delete_items:
        for sync_key_value in existing_raw:
            if sync_key_value not in desired:
                doc_id = existing_models[sync_key_value].get_document_id()
                plan.to_delete.append(doc_id)

    return plan


# ---------------------------------------------------------------------------
# I/O helpers — thin wrappers; minimal logic, maximum testability boundary
# ---------------------------------------------------------------------------


def _index_desired(
    items: Sequence[BareModel],
    sync_key: str | None,
    doc_id_field: str,
) -> dict[str, BareModel]:
    """Build a ``sync_key_value → model`` dict from the desired item list.

    Args:
        items:        The desired model instances.
        sync_key:     Field name to key on.  ``None`` means use
                      ``doc_id_field`` (the Firestore document ID field).
        doc_id_field: The model's ``__document_id__`` field name.

    Raises:
        ValueError: If any item is missing a value for the key field.
        ValueError: If duplicate key values exist in the desired list.
    """
    key_field = sync_key if sync_key is not None else doc_id_field
    result: dict[str, BareModel] = {}

    for item in items:
        raw_value = getattr(item, key_field, None)
        if raw_value is None:
            raise ValueError(
                f"Item {item!r} has no value for sync_key '{key_field}'. "
                f"All items must have this field set before syncing."
            )
        value = str(raw_value)
        if value in result:
            raise ValueError(
                f"Duplicate sync_key value '{value}' found in the desired items list. "
                f"Each item must have a unique '{key_field}' value."
            )
        result[value] = item

    return result


def _fetch_existing(
    model: type[BareModel],
    sync_key: str | None,
    on_duplicate_keys: OnDuplicateKeys,
) -> tuple[dict[str, BareModel], dict[str, dict[str, Any]]]:
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
        A 2-tuple of ``(existing_models, existing_raw)`` — both dicts are
        keyed by ``sync_key_value``.

    Raises:
        DuplicateKeyError: When ``on_duplicate_keys="raise"`` and duplicates
                           are found.
    """
    doc_id_field = model.__document_id__
    key_field = sync_key if sync_key is not None else doc_id_field
    col_ref = model._get_col_ref()

    # First pass: collect all snapshots grouped by key value so we can detect
    # and handle duplicates before committing anything to the result dicts.
    seen: dict[str, list[tuple[str, BareModel, dict[str, Any]]]] = {}

    for doc_snap in col_ref.stream():
        doc_id: str = doc_snap.id
        raw: dict[str, Any] = doc_snap.to_dict() or {}

        # Hydrate into the model so we can read field values.
        # Inject the doc ID so the model's id field is populated.
        try:
            model_instance = model(**{**raw, doc_id_field: doc_id})
        except Exception:
            logger.warning(
                "Could not load document '%s' into model %s — skipping.",
                doc_id,
                model.__name__,
            )
            continue

        if sync_key is None:
            key_value = doc_id
        else:
            raw_key = getattr(model_instance, key_field, None)
            if raw_key is None:
                logger.warning(
                    "Document '%s' has no value for sync_key '%s' — skipping.",
                    doc_id,
                    key_field,
                )
                continue
            key_value = str(raw_key)

        seen.setdefault(key_value, []).append((doc_id, model_instance, raw))

    existing_models: dict[str, BareModel] = {}
    existing_raw: dict[str, dict[str, Any]] = {}

    for key_value, docs in seen.items():
        if len(docs) == 1:
            doc_id, model_instance, raw = docs[0]
            existing_models[key_value] = model_instance
            existing_raw[key_value] = raw
        else:
            # Duplicate key value detected.
            if on_duplicate_keys == "raise":
                doc_ids = [d[0] for d in docs]
                raise DuplicateKeyError(
                    f"sync_key '{key_field}' value '{key_value}' matches "
                    f"{len(docs)} Firestore documents: {doc_ids}. "
                    f"Resolve the duplicates manually, or choose a different "
                    f"on_duplicate_keys strategy ('skip' or 'update_all')."
                )
            elif on_duplicate_keys == "skip":
                logger.warning(
                    "Skipping sync_key value '%s' — matched %d documents (on_duplicate_keys='skip').",
                    key_value,
                    len(docs),
                )
                # Not added to result — excluded from the plan entirely.
            elif on_duplicate_keys == "update_all":
                # Add each duplicate under a disambiguated key so build_sync_plan
                # sees them as separate entries and updates all of them.
                for i, (doc_id, model_instance, raw) in enumerate(docs):
                    disambig = f"{key_value}\x00dup{i}"
                    existing_models[disambig] = model_instance
                    existing_raw[disambig] = raw

    return existing_models, existing_raw


def _iter_chunks(items: list[Any], size: int) -> Iterator[list[Any]]:
    """Yield successive fixed-size chunks from a list."""
    for i in range(0, len(items), size):
        yield items[i : i + size]


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
                    data = {k: v for k, v in model_instance.model_dump(by_alias=True).items() if k != doc_id_field}
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
                    data = {k: v for k, v in model_instance.model_dump(by_alias=True).items() if k != doc_id_field}
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


def _handle_error(
    exc: Exception,
    key: str,
    result: SyncResult,
    on_error: OnError,
) -> None:
    """Dispatch an error according to the on_error strategy."""
    if on_error == "raise":
        raise exc
    elif on_error == "collect":
        result.errors.append(SyncError(sync_key_value=key, error=exc))
    elif on_error == "skip":
        logger.warning("Skipping document '%s' due to error: %s", key, exc)


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------


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
        existing_models, existing_raw = _fetch_existing(self._model, self._sync_key, self._on_duplicate_keys)

        # 3. Build plan (pure, no I/O).
        plan = build_sync_plan(
            desired=desired,
            existing_models=existing_models,
            existing_raw=existing_raw,
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
        return _apply_plan(
            plan,
            self._model,
            chunk_size=self._chunk_size,
            dry_run=self._dry_run,
            on_error=self._on_error,
            output_writer=self._output_writer,
        )

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


# Backward-compatible alias matching the README and prior implementations.
UpdateCollection = CollectionSync
