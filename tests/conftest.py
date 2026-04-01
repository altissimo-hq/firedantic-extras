"""Shared test fixtures and configuration.

Provides:
- ``require_emulator`` — auto-use fixture that skips integration tests when
  the Firestore emulator is not running.
- ``firestore_client`` — session-scoped Firestore client pointing at the emulator.
- ``configure_firedantic`` — session-scoped fixture that registers a firedantic
  configuration backed by the emulator.
- ``clean_collection`` — per-test fixture that deletes all documents in a
  collection after the test completes.
"""

from __future__ import annotations

import os
from typing import Any
from unittest.mock import Mock

import google.auth.credentials
import pytest
from firedantic.configurations import configuration
from google.cloud.firestore_v1.client import Client

# ---------------------------------------------------------------------------
# Emulator connection constants
# ---------------------------------------------------------------------------

EMULATOR_HOST = "127.0.0.1:8686"
EMULATOR_PROJECT = "firedantic-extras-test"
COLLECTION_PREFIX = "test-"


# ---------------------------------------------------------------------------
# Markers
# ---------------------------------------------------------------------------


def pytest_configure(config: Any) -> None:
    """Register custom markers so pytest doesn't warn about unknown marks."""
    config.addinivalue_line(
        "markers",
        "integration: marks tests that require the Firestore emulator (deselect with '-m \"not integration\"')",
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def require_emulator(request: pytest.FixtureRequest) -> None:
    """Skip integration-marked tests unless the emulator env var is set."""
    marker = request.node.get_closest_marker("integration")
    if marker is not None and not os.environ.get("FIRESTORE_EMULATOR_HOST"):
        pytest.skip("Firestore emulator not running (set FIRESTORE_EMULATOR_HOST)")


@pytest.fixture(scope="session")
def firestore_client() -> Client:
    """Return a Firestore client connected to the emulator.

    Sets ``FIRESTORE_EMULATOR_HOST`` so the google-cloud-firestore SDK
    auto-routes all traffic to the local emulator instead of production.
    """
    os.environ.setdefault("FIRESTORE_EMULATOR_HOST", EMULATOR_HOST)
    return Client(
        project=EMULATOR_PROJECT,
        credentials=Mock(spec=google.auth.credentials.Credentials),
    )


@pytest.fixture(scope="session")
def configure_firedantic(firestore_client: Client) -> None:
    """Register a firedantic configuration backed by the emulator.

    Uses the session-scoped ``firestore_client`` so only one client is
    created for the entire test run.
    """
    configuration.add(
        prefix=COLLECTION_PREFIX,
        project=EMULATOR_PROJECT,
        credentials=Mock(spec=google.auth.credentials.Credentials),
    )


@pytest.fixture()
def clean_collection(firestore_client: Client):
    """Factory fixture — returns a callable that deletes all docs in a collection.

    Usage inside a test::

        def test_something(clean_collection, configure_firedantic):
            # ... do work ...
            clean_collection("test-users")

    Can be called multiple times for different collections.
    """
    collections_to_clean: list[str] = []

    def _register(collection_name: str) -> None:
        collections_to_clean.append(collection_name)

    yield _register

    # Teardown — delete all documents in registered collections.
    for col_name in collections_to_clean:
        col_ref = firestore_client.collection(col_name)
        for doc in col_ref.stream():
            doc.reference.delete()
