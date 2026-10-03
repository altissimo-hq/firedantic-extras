"""CollectionSync's pure core: result types, the diff/plan computation, and the
bookkeeping around it.  **No Firestore I/O anywhere in this module.**

The I/O steps — streaming the existing collection and committing batched
writes — live in ``firedantic_extras._async.update_collection`` (and its
generated sync twin) and are deliberately thin so this module carries all the
logic worth unit-testing.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from firedantic import to_firestore_value
from google.cloud.firestore_v1 import DELETE_FIELD
from google.cloud.firestore_v1.field_path import FieldPath

if TYPE_CHECKING:
    from collections.abc import Collection, Iterator, Sequence

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
    """The outcome of a ``CollectionSync`` / ``AsyncCollectionSync`` run.

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
        skipped_duplicate_keys:
                  ``sync_key`` values that matched more than one existing
                  document and were therefore left untouched — the duplicates
                  were neither updated nor deleted, and the incoming item was
                  not added.  Populated only when ``on_duplicate_keys="skip"``.
    """

    adds: int = 0
    updates: int = 0
    deletes: int = 0
    skips: int = 0
    diffs: dict[str, DocumentDiff] = field(default_factory=dict)
    errors: list[SyncError] = field(default_factory=list)
    dry_run: bool = False
    skipped_duplicate_keys: list[str] = field(default_factory=list)

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
        if self.skipped_duplicate_keys:
            parts.append(f"skipped_duplicates={len(self.skipped_duplicate_keys)}")
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
    to_add: list[Any] = field(default_factory=list)

    #: (firestore_doc_id, model_instance) pairs whose documents need updating.
    to_update: list[tuple[str, Any]] = field(default_factory=list)

    #: Firestore document IDs to delete.
    to_delete: list[str] = field(default_factory=list)

    #: sync_key values for documents that were identical (no write needed).
    to_skip: list[str] = field(default_factory=list)

    #: sync_key_value → DocumentDiff; populated only when diff=True.
    diffs: dict[str, DocumentDiff] = field(default_factory=dict)

    #: firestore_doc_id → field-level ``update()`` payload for each document in
    #: :attr:`to_update`.  Populated only when fields are preserved; those
    #: documents are updated field by field instead of replaced, so the
    #: preserved fields are never written.
    partial_updates: dict[str, dict[str, Any]] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Pure helpers
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


def _document_id_key(model_class: type[Any], doc_id_field: str) -> str:
    """The key ``model_dump(by_alias=True)`` uses for the document-ID field.

    That is its alias when it has one — the same key firedantic's ``save()``
    drops from the payload.
    """
    model_field = getattr(model_class, "model_fields", {}).get(doc_id_field)
    alias = getattr(model_field, "alias", None)
    return alias or doc_id_field


def _model_data(model_instance: Any, doc_id_field: str) -> dict[str, Any]:
    """The document firedantic's ``save()`` would write for *model_instance*.

    Built the same way: ``model_dump(by_alias=True)``, minus the document-ID
    field (the ID lives in the document path, not the body), converted with
    ``to_firestore_value`` — so enums, dates, decimals and the like compare
    equal to what Firestore actually stores and a sync of unchanged data
    reports skips rather than rewriting every document.
    """
    data = model_instance.model_dump(by_alias=True)
    data.pop(_document_id_key(type(model_instance), doc_id_field), None)
    converted: dict[str, Any] = to_firestore_value(data)
    return converted


def resolve_preserved_keys(
    model_class: type[Any],
    preserve_fields: Collection[str],
    doc_id_field: str,
) -> frozenset[str]:
    """The stored (top-level) keys for the ``preserve_fields`` option.

    A model field name resolves to the key firedantic stores it under — its
    alias when it has one.  Any other name is taken as a stored key as-is, so
    fields another writer adds without the model declaring them can be
    preserved too.

    Raises:
        TypeError:  If ``preserve_fields`` is a single string.
        ValueError: If a name is the document-ID field.
    """
    if isinstance(preserve_fields, str):
        raise TypeError("preserve_fields takes a collection of field names, not a single string")
    model_fields = getattr(model_class, "model_fields", {})
    id_keys = {doc_id_field, _document_id_key(model_class, doc_id_field)}
    keys: set[str] = set()
    for name in preserve_fields:
        if name in id_keys:
            raise ValueError(f"preserve_fields can't include the document ID field '{name}'")
        model_field = model_fields.get(name)
        keys.add(getattr(model_field, "alias", None) or name)
    return frozenset(keys)


def _partial_update_payload(
    existing_data: dict[str, Any],
    incoming_data: dict[str, Any],
) -> dict[str, Any]:
    """The ``update()`` payload that turns *existing_data* into *incoming_data*.

    Both sides already have the preserved keys removed.  Changed and new
    fields are written whole; fields only in *existing_data* (stale ones) are
    removed with ``DELETE_FIELD``.  Keys are quoted as field paths so a key
    containing dots is written as one top-level field, not a nested path.
    """
    payload: dict[str, Any] = {}
    for key, value in incoming_data.items():
        if key not in existing_data or existing_data[key] != value:
            payload[FieldPath(key).to_api_repr()] = value
    for key in sorted(existing_data.keys() - incoming_data.keys()):
        payload[FieldPath(key).to_api_repr()] = DELETE_FIELD
    return payload


# ---------------------------------------------------------------------------
# Pure function — the heart of the sync logic
# ---------------------------------------------------------------------------


def build_sync_plan(
    desired: dict[str, Any],
    existing_models: dict[str, Any],
    existing_raw: dict[str, dict[str, Any]],
    *,
    doc_id_field: str,
    delete_items: bool = False,
    diff: bool = False,
    preserve_keys: Collection[str] = frozenset(),
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
        preserve_keys:   Stored keys (see :func:`resolve_preserved_keys`) the
                         sync never compares or writes on existing documents.
                         When given, each update gets a field-level payload in
                         :attr:`_SyncPlan.partial_updates` instead of
                         replacing the whole document.

    Returns:
        A :class:`_SyncPlan` with the computed changes.
    """
    plan = _SyncPlan()
    preserve_keys = frozenset(preserve_keys)

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
            # Preserved keys are owned by another writer: dropped from both
            # sides, so they neither trigger an update nor get overwritten.
            ignored_keys = {doc_id_field, _document_id_key(type(desired_model), doc_id_field)} | preserve_keys
            existing_data = {k: v for k, v in raw.items() if k not in ignored_keys}
            incoming_data = {
                k: v for k, v in _model_data(desired_model, doc_id_field).items() if k not in preserve_keys
            }

            if incoming_data == existing_data:
                plan.to_skip.append(sync_key_value)
            else:
                plan.to_update.append((doc_id, desired_model))
                if preserve_keys:
                    plan.partial_updates[doc_id] = _partial_update_payload(existing_data, incoming_data)
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
# Indexing helpers — used on either side of the Firestore read
# ---------------------------------------------------------------------------


def _index_desired(
    items: Sequence[Any],
    sync_key: str | None,
    doc_id_field: str,
) -> dict[str, Any]:
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
    result: dict[str, Any] = {}

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


#: One existing Firestore document: ``(doc_id, hydrated_model, raw_data)``.
_ExistingDoc = tuple[str, Any, dict[str, Any]]


def _load_existing_doc(
    model: type[Any],
    doc_id: str,
    raw: dict[str, Any],
    *,
    sync_key: str | None,
) -> tuple[str, Any] | None:
    """Hydrate one streamed document and work out its sync-key value.

    Returns ``(key_value, model_instance)``, or ``None`` (after logging a
    warning) when the document can't be loaded into the model or has no value
    for the sync key — such documents are left untouched by the sync.
    """
    doc_id_field = model.__document_id__
    key_field = sync_key if sync_key is not None else doc_id_field

    # Inject the doc ID so the model's id field is populated.
    try:
        model_instance = model(**{**raw, doc_id_field: doc_id})
    except Exception:
        logger.warning(
            "Could not load document '%s' into model %s — skipping.",
            doc_id,
            model.__name__,
        )
        return None

    if sync_key is None:
        return doc_id, model_instance

    raw_key = getattr(model_instance, key_field, None)
    if raw_key is None:
        logger.warning(
            "Document '%s' has no value for sync_key '%s' — skipping.",
            doc_id,
            key_field,
        )
        return None
    return str(raw_key), model_instance


@dataclass
class _ExistingIndex:
    """Existing Firestore documents indexed by sync-key value, duplicates resolved.

    ``models`` / ``raw`` are what :func:`build_sync_plan` consumes.  A key that
    matched several documents is handled per ``on_duplicate_keys``:

    - ``"raise"``      → :class:`DuplicateKeyError`; this object is never built.
    - ``"skip"``       → absent from ``models`` / ``raw``, listed in
                         ``skipped_keys``.
    - ``"update_all"`` → each document stored under its own alias key
                         (:func:`_alias_key`), listed in ``aliases``.

    :func:`_reconcile_desired` applies the matching treatment to the desired
    side so the two line up when they reach :func:`build_sync_plan`.
    """

    models: dict[str, Any] = field(default_factory=dict)
    raw: dict[str, dict[str, Any]] = field(default_factory=dict)
    skipped_keys: list[str] = field(default_factory=list)
    aliases: dict[str, list[str]] = field(default_factory=dict)


def _alias_key(key_value: str, doc_id: str) -> str:
    """Per-document key for one of several documents sharing a sync-key value.

    Readable on purpose — it is what ``SyncResult.diffs`` and
    ``DocumentDiff.sync_key_value`` show for ``on_duplicate_keys="update_all"``.
    """
    return f"{key_value} (doc {doc_id})"


def _resolve_duplicates(
    seen: dict[str, list[_ExistingDoc]],
    *,
    key_field: str,
    on_duplicate_keys: OnDuplicateKeys,
) -> _ExistingIndex:
    """Collapse ``key_value → [docs]`` into an :class:`_ExistingIndex`.

    Keys matched by exactly one document pass straight through.  Keys matched
    by several are handled per ``on_duplicate_keys``.

    Raises:
        DuplicateKeyError: When ``on_duplicate_keys="raise"`` and duplicates
                           are found.
    """
    index = _ExistingIndex()

    for key_value, docs in seen.items():
        if len(docs) == 1:
            _doc_id, model_instance, raw = docs[0]
            index.models[key_value] = model_instance
            index.raw[key_value] = raw
        elif on_duplicate_keys == "raise":
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
            index.skipped_keys.append(key_value)
        elif on_duplicate_keys == "update_all":
            # One entry per document so build_sync_plan updates each of them;
            # _reconcile_desired fans the desired model out under the same aliases.
            for doc_id, model_instance, raw in docs:
                alias = _alias_key(key_value, doc_id)
                index.models[alias] = model_instance
                index.raw[alias] = raw
                index.aliases.setdefault(key_value, []).append(alias)

    return index


def _reconcile_desired(desired: dict[str, Any], existing: _ExistingIndex) -> dict[str, Any]:
    """Line the desired side up with how duplicates were resolved in *existing*.

    - Keys in ``existing.skipped_keys`` are dropped: the incoming item is
      neither added as a new document nor used to update the duplicates (which
      are absent from ``existing`` too, so they are not deleted either).
    - Keys in ``existing.aliases`` are fanned out: the one desired model is
      keyed under every alias, so :func:`build_sync_plan` matches — and
      updates — each duplicate instead of treating the key as brand new.

    Pure function — no I/O.
    """
    reconciled: dict[str, Any] = {}
    for key_value, model in desired.items():
        if key_value in existing.skipped_keys:
            continue
        for key in existing.aliases.get(key_value, [key_value]):
            reconciled[key] = model
    return reconciled


def _iter_chunks(items: list[Any], size: int) -> Iterator[list[Any]]:
    """Yield successive fixed-size chunks from a list."""
    for i in range(0, len(items), size):
        yield items[i : i + size]


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
