"""Tests for firedantic_extras.update_collection.

Structure
---------
TestComputeFieldDiffs   Unit tests for the _compute_field_diffs() helper.
TestIndexDesired        Unit tests for _index_desired().
TestBuildSyncPlan       Unit tests for build_sync_plan() — pure, no Firestore.
TestSyncResult          Unit tests for SyncResult helpers.

All pure-function tests require zero Firestore connectivity.  The Firestore
model class (User) is instantiated but never persisted, so these tests run
without any emulator or service account.
"""

from __future__ import annotations

import pytest
from firedantic import Model

from firedantic_extras.update_collection import (
    _MISSING,
    SyncError,
    SyncResult,
    _compute_field_diffs,
    _index_desired,
    build_sync_plan,
)

# ---------------------------------------------------------------------------
# Minimal Firedantic model for tests (no Firestore connection required)
# ---------------------------------------------------------------------------


class User(Model):
    """Test-only model.  Never saved to Firestore in unit tests."""

    __collection__ = "users"

    name: str
    email: str
    active: bool = True


def _user(uid: str, name: str, email: str, active: bool = True) -> User:
    """Construct a User with a preset document ID."""
    u = User(name=name, email=email, active=active)
    u.id = uid
    return u


def _raw(name: str, email: str, active: bool = True) -> dict:
    """Simulate the raw Firestore payload (without the doc-ID field)."""
    return {"name": name, "email": email, "active": active}


# ---------------------------------------------------------------------------
# TestComputeFieldDiffs
# ---------------------------------------------------------------------------


class TestComputeFieldDiffs:
    def test_single_changed_field(self):
        diff = _compute_field_diffs(
            "doc1",
            "key1",
            {"name": "Alice", "active": True},
            {"name": "Alicia", "active": True},
        )
        assert len(diff.changes) == 1
        assert diff.changes[0].field == "name"
        assert diff.changes[0].before == "Alice"
        assert diff.changes[0].after == "Alicia"

    def test_multiple_changed_fields(self):
        diff = _compute_field_diffs(
            "doc1",
            "key1",
            {"name": "Alice", "email": "old@example.com", "active": True},
            {"name": "Alice", "email": "new@example.com", "active": False},
        )
        changed = {c.field for c in diff.changes}
        assert changed == {"email", "active"}

    def test_no_changes_returns_empty_changes_list(self):
        diff = _compute_field_diffs("doc1", "key1", {"name": "Alice"}, {"name": "Alice"})
        assert diff.changes == []

    def test_field_added_in_incoming(self):
        diff = _compute_field_diffs(
            "doc1",
            "key1",
            {"name": "Alice"},
            {"name": "Alice", "email": "alice@example.com"},
        )
        assert len(diff.changes) == 1
        assert diff.changes[0].field == "email"
        assert diff.changes[0].before is _MISSING

    def test_field_removed_in_incoming(self):
        diff = _compute_field_diffs(
            "doc1",
            "key1",
            {"name": "Alice", "legacy": "old"},
            {"name": "Alice"},
        )
        assert len(diff.changes) == 1
        assert diff.changes[0].field == "legacy"
        assert diff.changes[0].before == "old"
        assert diff.changes[0].after is _MISSING

    def test_doc_id_and_key_value_are_recorded_on_diff(self):
        diff = _compute_field_diffs("my-doc-id", "the-key", {"x": 1}, {"x": 2})
        assert diff.doc_id == "my-doc-id"
        assert diff.sync_key_value == "the-key"

    def test_changes_are_sorted_by_field_name(self):
        diff = _compute_field_diffs(
            "d",
            "k",
            {"z": 1, "a": 2, "m": 3},
            {"z": 9, "a": 9, "m": 9},
        )
        field_names = [c.field for c in diff.changes]
        assert field_names == sorted(field_names)


# ---------------------------------------------------------------------------
# TestIndexDesired
# ---------------------------------------------------------------------------


class TestIndexDesired:
    def test_defaults_to_doc_id_field(self):
        users = [_user("u1", "Alice", "a@e.com"), _user("u2", "Bob", "b@e.com")]
        result = _index_desired(users, sync_key=None, doc_id_field="id")
        assert set(result.keys()) == {"u1", "u2"}

    def test_custom_sync_key(self):
        users = [_user("u1", "Alice", "a@e.com"), _user("u2", "Bob", "b@e.com")]
        result = _index_desired(users, sync_key="email", doc_id_field="id")
        assert set(result.keys()) == {"a@e.com", "b@e.com"}

    def test_raises_on_missing_key_value(self):
        user = User(name="Alice", email="a@e.com")  # id not set → None
        with pytest.raises(ValueError, match="no value for sync_key"):
            _index_desired([user], sync_key=None, doc_id_field="id")

    def test_raises_on_duplicate_desired_keys(self):
        users = [_user("u1", "Alice", "same@e.com"), _user("u1", "Bob", "b@e.com")]
        with pytest.raises(ValueError, match="Duplicate sync_key"):
            _index_desired(users, sync_key=None, doc_id_field="id")

    def test_empty_list_returns_empty_dict(self):
        assert _index_desired([], sync_key=None, doc_id_field="id") == {}

    def test_values_are_coerced_to_str(self):
        """Key values must be strings regardless of field type."""
        u = _user("123", "Alice", "a@e.com")  # id stored as str "123"
        result = _index_desired([u], sync_key=None, doc_id_field="id")
        assert "123" in result


# ---------------------------------------------------------------------------
# TestBuildSyncPlan — pure unit tests, zero I/O
# ---------------------------------------------------------------------------


class TestBuildSyncPlan:
    """All tests use plain dicts.  No Firestore connection is required."""

    # ------------------------------------------------------------------
    # Basic cases
    # ------------------------------------------------------------------

    def test_all_new_items_added(self):
        desired = {
            "u1": _user("u1", "Alice", "a@e.com"),
            "u2": _user("u2", "Bob", "b@e.com"),
        }
        plan = build_sync_plan(
            desired=desired,
            existing_models={},
            existing_raw={},
            doc_id_field="id",
        )
        assert len(plan.to_add) == 2
        assert plan.to_update == []
        assert plan.to_delete == []
        assert plan.to_skip == []

    def test_identical_docs_are_skipped(self):
        user = _user("u1", "Alice", "a@e.com")
        desired = {"u1": user}
        existing_models = {"u1": user}
        existing_raw = {"u1": _raw("Alice", "a@e.com")}

        plan = build_sync_plan(
            desired=desired,
            existing_models=existing_models,
            existing_raw=existing_raw,
            doc_id_field="id",
        )
        assert plan.to_skip == ["u1"]
        assert plan.to_add == []
        assert plan.to_update == []

    def test_changed_field_triggers_update(self):
        desired = {"u1": _user("u1", "Alice Smith", "a@e.com")}
        existing_models = {"u1": _user("u1", "Alice", "a@e.com")}
        existing_raw = {"u1": _raw("Alice", "a@e.com")}

        plan = build_sync_plan(
            desired=desired,
            existing_models=existing_models,
            existing_raw=existing_raw,
            doc_id_field="id",
        )
        assert len(plan.to_update) == 1
        doc_id, model = plan.to_update[0]
        assert doc_id == "u1"
        assert plan.to_skip == []
        assert plan.to_add == []

    def test_missing_from_desired_not_deleted_by_default(self):
        existing_models = {"u1": _user("u1", "Alice", "a@e.com")}
        existing_raw = {"u1": _raw("Alice", "a@e.com")}

        plan = build_sync_plan(
            desired={},
            existing_models=existing_models,
            existing_raw=existing_raw,
            doc_id_field="id",
            delete_items=False,
        )
        assert plan.to_delete == []

    def test_missing_from_desired_deleted_when_enabled(self):
        existing_models = {"u1": _user("u1", "Alice", "a@e.com")}
        existing_raw = {"u1": _raw("Alice", "a@e.com")}

        plan = build_sync_plan(
            desired={},
            existing_models=existing_models,
            existing_raw=existing_raw,
            doc_id_field="id",
            delete_items=True,
        )
        assert plan.to_delete == ["u1"]

    def test_empty_desired_empty_existing_is_no_op(self):
        plan = build_sync_plan(
            desired={},
            existing_models={},
            existing_raw={},
            doc_id_field="id",
        )
        assert plan.to_add == []
        assert plan.to_update == []
        assert plan.to_delete == []
        assert plan.to_skip == []

    # ------------------------------------------------------------------
    # Mixed scenarios
    # ------------------------------------------------------------------

    def test_mixed_add_update_skip_delete(self):
        desired = {
            "u1": _user("u1", "Alice Updated", "a@e.com"),  # update
            "u2": _user("u2", "Bob", "b@e.com"),  # skip (no change)
            "u3": _user("u3", "Carol", "c@e.com"),  # add (brand new)
        }
        existing_models = {
            "u1": _user("u1", "Alice", "a@e.com"),
            "u2": _user("u2", "Bob", "b@e.com"),
            "u4": _user("u4", "Dave", "d@e.com"),  # will be deleted
        }
        existing_raw = {
            "u1": _raw("Alice", "a@e.com"),
            "u2": _raw("Bob", "b@e.com"),
            "u4": _raw("Dave", "d@e.com"),
        }
        plan = build_sync_plan(
            desired=desired,
            existing_models=existing_models,
            existing_raw=existing_raw,
            doc_id_field="id",
            delete_items=True,
        )
        assert len(plan.to_add) == 1
        assert len(plan.to_update) == 1
        assert len(plan.to_skip) == 1
        assert len(plan.to_delete) == 1

    def test_multiple_deletes(self):
        existing_keys = {f"u{i}": _user(f"u{i}", f"Person {i}", f"p{i}@e.com") for i in range(5)}
        plan = build_sync_plan(
            desired={},
            existing_models=existing_keys,
            existing_raw={k: _raw(f"Person {i}", f"p{i}@e.com") for i, k in enumerate(existing_keys)},
            doc_id_field="id",
            delete_items=True,
        )
        assert len(plan.to_delete) == 5

    # ------------------------------------------------------------------
    # Stale / extra fields in Firestore
    # ------------------------------------------------------------------

    def test_extra_field_in_firestore_triggers_update(self):
        """A stale field in Firestore that the model doesn't know about
        should cause an update so that Firestore converges to the model's
        shape."""
        desired = {"u1": _user("u1", "Alice", "a@e.com")}
        existing_models = {"u1": _user("u1", "Alice", "a@e.com")}
        # Firestore has a 'legacy_field' the model doesn't define.
        existing_raw = {"u1": {**_raw("Alice", "a@e.com"), "legacy_field": "stale"}}

        plan = build_sync_plan(
            desired=desired,
            existing_models=existing_models,
            existing_raw=existing_raw,
            doc_id_field="id",
        )
        assert len(plan.to_update) == 1, "Extra fields in Firestore must trigger an update to remove them"
        assert plan.to_skip == []

    def test_no_false_positive_when_only_doc_id_differs(self):
        """Stripping the doc_id_field before comparison must prevent a
        false-positive update when the raw dict includes the ID and the
        model also serialises it."""
        user = _user("u1", "Alice", "a@e.com")
        desired = {"u1": user}
        existing_models = {"u1": user}
        # Raw does not include 'id' (as is normal — stored as the doc key).
        existing_raw = {"u1": _raw("Alice", "a@e.com")}

        plan = build_sync_plan(
            desired=desired,
            existing_models=existing_models,
            existing_raw=existing_raw,
            doc_id_field="id",
        )
        assert plan.to_skip == ["u1"]
        assert plan.to_update == []

    # ------------------------------------------------------------------
    # Diff output
    # ------------------------------------------------------------------

    def test_diff_not_populated_by_default(self):
        desired = {"u1": _user("u1", "Alice Smith", "a@e.com")}
        existing_models = {"u1": _user("u1", "Alice", "a@e.com")}
        existing_raw = {"u1": _raw("Alice", "a@e.com")}

        plan = build_sync_plan(
            desired=desired,
            existing_models=existing_models,
            existing_raw=existing_raw,
            doc_id_field="id",
            diff=False,
        )
        assert plan.diffs == {}

    def test_diff_populated_for_updates_when_enabled(self):
        desired = {"u1": _user("u1", "Alice Smith", "a@e.com")}
        existing_models = {"u1": _user("u1", "Alice", "a@e.com")}
        existing_raw = {"u1": _raw("Alice", "a@e.com")}

        plan = build_sync_plan(
            desired=desired,
            existing_models=existing_models,
            existing_raw=existing_raw,
            doc_id_field="id",
            diff=True,
        )
        assert "u1" in plan.diffs
        doc_diff = plan.diffs["u1"]
        assert len(doc_diff.changes) == 1
        assert doc_diff.changes[0].field == "name"
        assert doc_diff.changes[0].before == "Alice"
        assert doc_diff.changes[0].after == "Alice Smith"

    def test_diff_not_populated_for_adds(self):
        """New documents have no 'before' state — diffs are not generated."""
        desired = {"u1": _user("u1", "Alice", "a@e.com")}

        plan = build_sync_plan(
            desired=desired,
            existing_models={},
            existing_raw={},
            doc_id_field="id",
            diff=True,
        )
        assert plan.diffs == {}
        assert len(plan.to_add) == 1

    def test_diff_not_populated_for_skips(self):
        user = _user("u1", "Alice", "a@e.com")
        plan = build_sync_plan(
            desired={"u1": user},
            existing_models={"u1": user},
            existing_raw={"u1": _raw("Alice", "a@e.com")},
            doc_id_field="id",
            diff=True,
        )
        assert plan.diffs == {}
        assert plan.to_skip == ["u1"]

    def test_diff_with_extra_firestore_field(self):
        """Diffs must include fields that exist in Firestore but not in the
        model — these will appear with after=_MISSING."""
        desired = {"u1": _user("u1", "Alice", "a@e.com")}
        existing_models = {"u1": _user("u1", "Alice", "a@e.com")}
        existing_raw = {"u1": {**_raw("Alice", "a@e.com"), "old_field": "legacy_value"}}

        plan = build_sync_plan(
            desired=desired,
            existing_models=existing_models,
            existing_raw=existing_raw,
            doc_id_field="id",
            diff=True,
        )
        assert "u1" in plan.diffs
        extra = next(c for c in plan.diffs["u1"].changes if c.field == "old_field")
        assert extra.before == "legacy_value"
        assert extra.after is _MISSING


# ---------------------------------------------------------------------------
# TestSyncResult
# ---------------------------------------------------------------------------


class TestSyncResult:
    def test_summary_contains_all_counts(self):
        r = SyncResult(adds=5, updates=3, deletes=1, skips=10)
        s = r.summary()
        assert "adds=5" in s
        assert "updates=3" in s
        assert "deletes=1" in s
        assert "skips=10" in s

    def test_summary_includes_dry_run_label(self):
        r = SyncResult(adds=2, dry_run=True)
        assert "DRY RUN" in r.summary()

    def test_summary_includes_error_count_when_present(self):
        r = SyncResult()
        r.errors.append(SyncError("key", ValueError("boom")))
        assert "errors=1" in r.summary()

    def test_summary_excludes_error_line_when_no_errors(self):
        r = SyncResult(adds=1)
        assert "errors" not in r.summary()

    def test_has_errors_false_by_default(self):
        assert SyncResult().has_errors is False

    def test_has_errors_true_when_errors_present(self):
        r = SyncResult()
        r.errors.append(SyncError("key1", RuntimeError("fail")))
        assert r.has_errors is True

    def test_total_changes(self):
        r = SyncResult(adds=3, updates=2, deletes=1)
        assert r.total_changes == 6

    def test_total_changes_excludes_skips(self):
        r = SyncResult(adds=1, skips=100)
        assert r.total_changes == 1

    def test_total_changes_zero_when_empty(self):
        assert SyncResult().total_changes == 0
