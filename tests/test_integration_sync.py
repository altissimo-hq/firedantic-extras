"""Integration tests for CollectionSync against the Firestore emulator.

These tests require a running Firestore emulator (port 8686) and are
excluded from the default ``pytest`` run.

Run them explicitly::

    FIRESTORE_EMULATOR_HOST=127.0.0.1:8686 poetry run pytest -m integration -v

Or start the emulator first::

    ./scripts/start_emulator.sh   # in another terminal
    poetry run pytest -m integration -v
"""

from __future__ import annotations

import pytest
from firedantic import Model

from firedantic_extras.update_collection import CollectionSync

from .conftest import COLLECTION_PREFIX

# ---------------------------------------------------------------------------
# Test model
# ---------------------------------------------------------------------------


class User(Model):
    __collection__ = "users"

    name: str
    email: str
    active: bool = True


def _prefixed_collection() -> str:
    """Return the full collection name as it appears in Firestore (with prefix)."""
    return f"{COLLECTION_PREFIX}users"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_user(uid: str, name: str, email: str, active: bool = True) -> User:
    u = User(name=name, email=email, active=active)
    u.id = uid
    return u


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestCollectionSyncEndToEnd:
    """Full round-trip integration tests via the Firestore emulator."""

    def test_sync_into_empty_collection(self, configure_firedantic, clean_collection):
        clean_collection(_prefixed_collection())
        desired = [
            _make_user("u1", "Alice", "alice@example.com"),
            _make_user("u2", "Bob", "bob@example.com"),
        ]

        result = CollectionSync.sync(User, desired, output_writer=None)

        assert result.adds == 2
        assert result.updates == 0
        assert result.deletes == 0
        assert result.skips == 0

        # Verify they're actually in Firestore.
        found = User.find()
        assert len(found) == 2
        names = {u.name for u in found}
        assert names == {"Alice", "Bob"}

    def test_sync_no_change_produces_all_skips(self, configure_firedantic, clean_collection):
        clean_collection(_prefixed_collection())
        desired = [_make_user("u1", "Alice", "alice@example.com")]

        # First sync — creates.
        CollectionSync.sync(User, desired, output_writer=None)

        # Second sync — everything should be skipped.
        result = CollectionSync.sync(User, desired, output_writer=None)

        assert result.adds == 0
        assert result.updates == 0
        assert result.skips == 1

    def test_sync_detects_update(self, configure_firedantic, clean_collection):
        clean_collection(_prefixed_collection())

        # Create initial state.
        CollectionSync.sync(User, [_make_user("u1", "Alice", "alice@example.com")], output_writer=None)

        # Sync with changed name.
        result = CollectionSync.sync(
            User,
            [_make_user("u1", "Alice Smith", "alice@example.com")],
            output_writer=None,
        )

        assert result.updates == 1
        assert result.adds == 0

        # Verify the update persisted.
        found = User.find({"name": "Alice Smith"})
        assert len(found) == 1

    def test_sync_deletes_when_enabled(self, configure_firedantic, clean_collection):
        clean_collection(_prefixed_collection())

        # Create two users.
        CollectionSync.sync(
            User,
            [
                _make_user("u1", "Alice", "a@e.com"),
                _make_user("u2", "Bob", "b@e.com"),
            ],
            output_writer=None,
        )

        # Sync with only Alice — Bob should be deleted.
        result = CollectionSync.sync(
            User,
            [_make_user("u1", "Alice", "a@e.com")],
            delete_items=True,
            output_writer=None,
        )

        assert result.deletes == 1
        assert result.skips == 1

        found = User.find()
        assert len(found) == 1
        assert found[0].name == "Alice"

    def test_sync_does_not_delete_by_default(self, configure_firedantic, clean_collection):
        clean_collection(_prefixed_collection())

        # Create two users.
        CollectionSync.sync(
            User,
            [
                _make_user("u1", "Alice", "a@e.com"),
                _make_user("u2", "Bob", "b@e.com"),
            ],
            output_writer=None,
        )

        # Sync with only Alice — Bob should NOT be deleted (default behavior).
        result = CollectionSync.sync(
            User,
            [_make_user("u1", "Alice", "a@e.com")],
            delete_items=False,
            output_writer=None,
        )

        assert result.deletes == 0
        found = User.find()
        assert len(found) == 2

    def test_dry_run_makes_no_writes(self, configure_firedantic, clean_collection):
        clean_collection(_prefixed_collection())

        result = CollectionSync.sync(
            User,
            [_make_user("u1", "Alice", "a@e.com")],
            dry_run=True,
            output_writer=None,
        )

        assert result.adds == 1
        assert result.dry_run is True

        # Verify nothing was written.
        found = User.find()
        assert len(found) == 0

    def test_diff_output_on_update(self, configure_firedantic, clean_collection):
        clean_collection(_prefixed_collection())

        # Create initial state.
        CollectionSync.sync(User, [_make_user("u1", "Alice", "alice@example.com")], output_writer=None)

        # Sync with change + diff=True.
        result = CollectionSync.sync(
            User,
            [_make_user("u1", "Alice Smith", "alice@example.com")],
            diff=True,
            output_writer=None,
        )

        assert result.updates == 1
        assert "u1" in result.diffs
        name_change = next(c for c in result.diffs["u1"].changes if c.field == "name")
        assert name_change.before == "Alice"
        assert name_change.after == "Alice Smith"

    def test_stale_firestore_field_triggers_update(self, configure_firedantic, clean_collection):
        """Write a document with an extra field directly via the client,
        then sync — the extra field should cause an update."""
        clean_collection(_prefixed_collection())

        # Write directly to Firestore with an extra field the model doesn't know about.
        from firedantic.configurations import configuration as cfg

        client = cfg.get_client()
        col_ref = client.collection(_prefixed_collection())
        col_ref.document("u1").set(
            {
                "name": "Alice",
                "email": "a@e.com",
                "active": True,
                "legacy_field": "should_be_removed",
            }
        )

        # Now sync — the model doesn't have legacy_field, so it should update.
        result = CollectionSync.sync(
            User,
            [_make_user("u1", "Alice", "a@e.com")],
            diff=True,
            output_writer=None,
        )

        assert result.updates == 1
        assert "u1" in result.diffs
        legacy_diff = next(c for c in result.diffs["u1"].changes if c.field == "legacy_field")
        assert legacy_diff.before == "should_be_removed"

    def test_mixed_add_update_delete(self, configure_firedantic, clean_collection):
        clean_collection(_prefixed_collection())

        # Create initial users.
        CollectionSync.sync(
            User,
            [
                _make_user("u1", "Alice", "a@e.com"),
                _make_user("u2", "Bob", "b@e.com"),
                _make_user("u3", "Carol", "c@e.com"),
            ],
            output_writer=None,
        )

        # Desired state: Alice updated, Bob untouched, Carol gone, Dave new.
        result = CollectionSync.sync(
            User,
            [
                _make_user("u1", "Alice Updated", "a@e.com"),
                _make_user("u2", "Bob", "b@e.com"),
                _make_user("u4", "Dave", "d@e.com"),
            ],
            delete_items=True,
            output_writer=None,
        )

        assert result.adds == 1  # Dave
        assert result.updates == 1  # Alice
        assert result.skips == 1  # Bob
        assert result.deletes == 1  # Carol

        found = User.find()
        names = {u.name for u in found}
        assert names == {"Alice Updated", "Bob", "Dave"}

    def test_sync_key_match_on_email(self, configure_firedantic, clean_collection):
        """Use a non-ID field (email) as the sync key."""
        clean_collection(_prefixed_collection())

        # Seed the collection.
        CollectionSync.sync(
            User,
            [_make_user("u1", "Alice", "alice@example.com")],
            output_writer=None,
        )

        # Sync by email — the incoming model has a different doc ID but same email.
        result = CollectionSync.sync(
            User,
            [_make_user("different-id", "Alice Updated", "alice@example.com")],
            sync_key="email",
            output_writer=None,
        )

        assert result.updates == 1
        assert result.adds == 0

    def test_summary_output(self, configure_firedantic, clean_collection):
        clean_collection(_prefixed_collection())

        result = CollectionSync.sync(
            User,
            [_make_user("u1", "Alice", "a@e.com")],
            output_writer=None,
        )

        summary = result.summary()
        assert "adds=1" in summary
        assert "updates=0" in summary
