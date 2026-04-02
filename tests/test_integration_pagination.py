"""Integration tests for cursor_paginate, count_model, and build_prefix_filters.

Requires the Firestore emulator to be running:
    FIRESTORE_EMULATOR_HOST=127.0.0.1:8686 poetry run pytest -m integration
"""

from __future__ import annotations

import pytest
from firedantic import Model

from firedantic_extras.cursor_pagination import CursorPage, cursor_paginate
from firedantic_extras.query import build_prefix_filters, count_model

# ---------------------------------------------------------------------------
# Test model
# ---------------------------------------------------------------------------

COLLECTION = "test-widgets"


class Widget(Model):
    """A simple model for pagination integration tests."""

    __collection__ = "widgets"

    label: str
    score: int
    category: str | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_widgets(labels: list[str], score_start: int = 0, category: str | None = None) -> None:
    """Create Widget documents with predictable data."""
    for i, label in enumerate(labels):
        Widget(label=label, score=score_start + i, category=category).save()


def _all_labels(page: CursorPage) -> list[str]:
    return [w.label for w in page.items]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _cleanup(clean_collection: callable, configure_firedantic: None) -> None:  # type: ignore[type-arg]
    """Register the test-widgets collection for cleanup after every test."""
    clean_collection(COLLECTION)


# ---------------------------------------------------------------------------
# count_model tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestCountModel:
    def test_count_all(self, configure_firedantic: None) -> None:
        _make_widgets(["a", "b", "c"])
        assert count_model(Widget) == 3

    def test_count_empty(self, configure_firedantic: None) -> None:
        assert count_model(Widget) == 0

    def test_count_with_equality_filter(self, configure_firedantic: None) -> None:
        _make_widgets(["a", "b"], category="X")
        _make_widgets(["c"], category="Y")
        assert count_model(Widget, filter_={"category": "X"}) == 2
        assert count_model(Widget, filter_={"category": "Y"}) == 1

    def test_count_with_comparison_filter(self, configure_firedantic: None) -> None:
        _make_widgets(["low", "mid", "high"], score_start=1)
        # scores: 1, 2, 3
        assert count_model(Widget, filter_={"score": {">=": 2}}) == 2
        assert count_model(Widget, filter_={"score": {">=": 1, "<": 3}}) == 2

    def test_count_no_match(self, configure_firedantic: None) -> None:
        _make_widgets(["a", "b"])
        assert count_model(Widget, filter_={"category": "nonexistent"}) == 0


# ---------------------------------------------------------------------------
# build_prefix_filters (integration confirms Firestore accepts the query)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestPrefixFiltersIntegration:
    def test_prefix_returns_matching_docs(self, configure_firedantic: None) -> None:
        _make_widgets(["DA-00010", "DA-00011", "DA-00012", "DA-00020", "CB-00001"])
        filters = build_prefix_filters("label", "DA-0001")
        results = Widget.find(filters, order_by=[("label", "ASCENDING")])
        assert [w.label for w in results] == ["DA-00010", "DA-00011", "DA-00012"]

    def test_prefix_excludes_non_matching(self, configure_firedantic: None) -> None:
        _make_widgets(["alpha", "alphabet", "beta"])
        filters = build_prefix_filters("label", "alpha")
        results = Widget.find(filters)
        labels = {w.label for w in results}
        assert "beta" not in labels
        assert "alpha" in labels
        assert "alphabet" in labels

    def test_prefix_with_count(self, configure_firedantic: None) -> None:
        _make_widgets(["DA-001", "DA-002", "DA-003", "CB-001"])
        assert count_model(Widget, filter_=build_prefix_filters("label", "DA-")) == 3


# ---------------------------------------------------------------------------
# cursor_paginate — forward pagination
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestCursorPaginateForward:
    def test_single_page_no_cursor(self, configure_firedantic: None) -> None:
        _make_widgets(["a", "b", "c"])
        page = cursor_paginate(Widget, limit=10, order_by="label")
        assert _all_labels(page) == ["a", "b", "c"]
        assert page.has_next is False
        assert page.has_prev is False
        assert page.next_cursor is None
        assert page.prev_cursor is None

    def test_multi_page_forward(self, configure_firedantic: None) -> None:
        _make_widgets(["a", "b", "c", "d", "e"])

        page1 = cursor_paginate(Widget, limit=2, order_by="label")
        assert _all_labels(page1) == ["a", "b"]
        assert page1.has_next is True
        assert page1.has_prev is False
        assert page1.next_cursor is not None

        page2 = cursor_paginate(Widget, limit=2, order_by="label", cursor=page1.next_cursor, direction="next")
        assert _all_labels(page2) == ["c", "d"]
        assert page2.has_next is True
        assert page2.has_prev is True

        page3 = cursor_paginate(Widget, limit=2, order_by="label", cursor=page2.next_cursor, direction="next")
        assert _all_labels(page3) == ["e"]
        assert page3.has_next is False
        assert page3.has_prev is True

    def test_full_iteration_covers_all_items(self, configure_firedantic: None) -> None:
        """Iterate forward page by page and confirm every item is visited exactly once."""
        labels = [f"item-{i:03d}" for i in range(13)]
        _make_widgets(labels)

        seen: list[str] = []
        cursor = None
        while True:
            page = cursor_paginate(Widget, limit=4, order_by="label", cursor=cursor, direction="next")
            seen.extend(_all_labels(page))
            if not page.has_next:
                break
            cursor = page.next_cursor

        assert seen == sorted(labels)

    def test_empty_collection(self, configure_firedantic: None) -> None:
        page = cursor_paginate(Widget, limit=10, order_by="label")
        assert page.items == []
        assert page.has_next is False
        assert page.has_prev is False

    def test_exactly_limit_items(self, configure_firedantic: None) -> None:
        """Exactly limit items should not set has_next."""
        _make_widgets(["a", "b", "c"])
        page = cursor_paginate(Widget, limit=3, order_by="label")
        assert len(page.items) == 3
        assert page.has_next is False


# ---------------------------------------------------------------------------
# cursor_paginate — backward pagination
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestCursorPaginateBackward:
    def test_prev_from_second_page(self, configure_firedantic: None) -> None:
        _make_widgets(["a", "b", "c", "d"])

        page2 = cursor_paginate(Widget, limit=2, order_by="label")
        # advance to page 2
        page2 = cursor_paginate(Widget, limit=2, order_by="label", cursor=page2.next_cursor, direction="next")
        assert _all_labels(page2) == ["c", "d"]

        # step back to page 1
        page1_again = cursor_paginate(Widget, limit=2, order_by="label", cursor=page2.prev_cursor, direction="prev")
        assert _all_labels(page1_again) == ["a", "b"]
        assert page1_again.has_prev is False
        assert page1_again.has_next is True

    def test_full_round_trip(self, configure_firedantic: None) -> None:
        """Go forward to the end, then traverse backward covering all items."""
        labels = [f"r-{i:02d}" for i in range(9)]
        _make_widgets(labels)

        # Collect all pages going forward
        forward_pages: list[CursorPage] = []
        cursor = None
        while True:
            page = cursor_paginate(Widget, limit=3, order_by="label", cursor=cursor, direction="next")
            forward_pages.append(page)
            if not page.has_next:
                break
            cursor = page.next_cursor

        # Now traverse backward from the last page
        backward_pages: list[CursorPage] = []
        cursor = forward_pages[-1].prev_cursor
        while cursor is not None:
            page = cursor_paginate(Widget, limit=3, order_by="label", cursor=cursor, direction="prev")
            backward_pages.append(page)
            cursor = page.prev_cursor if page.has_prev else None

        # Forward pages: [r-00..r-02], [r-03..r-05], [r-06..r-08]
        # Backward from last page prev_cursor=id_of_r-06 yields [r-03..r-05] then [r-00..r-02]
        forward_labels = [lbl for p in forward_pages for lbl in _all_labels(p)]
        backward_labels = [lbl for p in reversed(backward_pages) for lbl in _all_labels(p)]

        assert forward_labels == sorted(labels)
        # Backward traversal starting from the last page's prev_cursor should
        # cover all pages EXCEPT the last page itself.
        assert backward_labels == sorted(labels)[:-3]

    def test_prev_no_cursor_returns_last_page(self, configure_firedantic: None) -> None:
        _make_widgets(["a", "b", "c", "d", "e"])
        page = cursor_paginate(Widget, limit=2, order_by="label", direction="prev")
        assert _all_labels(page) == ["d", "e"]
        # Last page: nothing after "e" going forward, but a/b/c exist before "d"
        assert page.has_next is False
        assert page.has_prev is True
        assert page.next_cursor is None
        assert page.prev_cursor is not None


# ---------------------------------------------------------------------------
# cursor_paginate — filters
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestCursorPaginateFilters:
    def test_equality_filter(self, configure_firedantic: None) -> None:
        _make_widgets(["a", "b"], category="X")
        _make_widgets(["c", "d"], category="Y")
        page = cursor_paginate(Widget, limit=10, order_by="label", filter_={"category": "X"})
        assert _all_labels(page) == ["a", "b"]

    def test_prefix_filter_with_pagination(self, configure_firedantic: None) -> None:
        _make_widgets(["DA-001", "DA-002", "DA-003", "DA-004", "CB-001"])
        filters = build_prefix_filters("label", "DA-")

        page1 = cursor_paginate(Widget, limit=2, order_by="label", filter_=filters)
        assert _all_labels(page1) == ["DA-001", "DA-002"]
        assert page1.has_next is True

        page2 = cursor_paginate(
            Widget, limit=2, order_by="label", filter_=filters, cursor=page1.next_cursor, direction="next"
        )
        assert _all_labels(page2) == ["DA-003", "DA-004"]
        assert page2.has_next is False

    def test_no_results_with_filter(self, configure_firedantic: None) -> None:
        _make_widgets(["a", "b"])
        page = cursor_paginate(Widget, limit=10, order_by="label", filter_={"category": "nonexistent"})
        assert page.items == []
        assert page.has_next is False


# ---------------------------------------------------------------------------
# cursor_paginate — include_total
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestIncludeTotal:
    def test_include_total_all(self, configure_firedantic: None) -> None:
        _make_widgets(["a", "b", "c", "d", "e"])
        page = cursor_paginate(Widget, limit=2, order_by="label", include_total=True)
        assert page.total == 5
        assert len(page.items) == 2

    def test_include_total_with_filter(self, configure_firedantic: None) -> None:
        _make_widgets(["a", "b"], category="X")
        _make_widgets(["c"], category="Y")
        page = cursor_paginate(Widget, limit=10, order_by="label", filter_={"category": "X"}, include_total=True)
        assert page.total == 2

    def test_total_none_by_default(self, configure_firedantic: None) -> None:
        _make_widgets(["a", "b"])
        page = cursor_paginate(Widget, limit=10, order_by="label")
        assert page.total is None


# ---------------------------------------------------------------------------
# cursor_paginate — stable sort with duplicate values
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestStableSort:
    def test_duplicate_sort_values_no_skips(self, configure_firedantic: None) -> None:
        """
        When sort field has duplicate values, __name__ tiebreak must prevent
        any item being skipped or duplicated at page boundaries.
        """
        # 6 items with score=1 (all duplicates on the sort key)
        labels = [f"dup-{i}" for i in range(6)]
        for label in labels:
            Widget(label=label, score=1).save()

        seen: list[str] = []
        cursor = None
        page_count = 0
        while True:
            page = cursor_paginate(
                Widget,
                limit=2,
                order_by=[("score", "ASCENDING")],
                cursor=cursor,
                direction="next",
            )
            seen.extend(_all_labels(page))
            page_count += 1
            if not page.has_next:
                break
            cursor = page.next_cursor

        # Every label seen exactly once
        assert sorted(seen) == sorted(labels)
        assert len(seen) == len(labels)
        assert len(set(seen)) == len(labels)  # no duplicates
