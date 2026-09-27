"""Integration tests for cursor pagination, count_model, and build_prefix_filters.

Requires the Firestore emulator to be running:
    FIRESTORE_EMULATOR_HOST=127.0.0.1:8686 poetry run pytest -m integration
"""

from __future__ import annotations

import pytest
from firedantic import AsyncModel

from firedantic_extras.cursor_pagination import CursorPage, async_cursor_paginate
from firedantic_extras.query import async_count_model, build_prefix_filters

# ---------------------------------------------------------------------------
# Test model
# ---------------------------------------------------------------------------

COLLECTION = "test-widgets"


class Widget(AsyncModel):
    """A simple model for pagination integration tests."""

    __collection__ = "widgets"

    label: str
    score: int
    category: str | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _make_widgets(labels: list[str], score_start: int = 0, category: str | None = None) -> None:
    """Create Widget documents with predictable data."""
    for i, label in enumerate(labels):
        await Widget(label=label, score=score_start + i, category=category).save()


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
@pytest.mark.asyncio
class TestCountModel:
    async def test_count_all(self, configure_firedantic: None) -> None:
        await _make_widgets(["a", "b", "c"])
        assert await async_count_model(Widget) == 3

    async def test_count_empty(self, configure_firedantic: None) -> None:
        assert await async_count_model(Widget) == 0

    async def test_count_with_equality_filter(self, configure_firedantic: None) -> None:
        await _make_widgets(["a", "b"], category="X")
        await _make_widgets(["c"], category="Y")
        assert await async_count_model(Widget, filter_={"category": "X"}) == 2
        assert await async_count_model(Widget, filter_={"category": "Y"}) == 1

    async def test_count_with_comparison_filter(self, configure_firedantic: None) -> None:
        await _make_widgets(["low", "mid", "high"], score_start=1)
        # scores: 1, 2, 3
        assert await async_count_model(Widget, filter_={"score": {">=": 2}}) == 2
        assert await async_count_model(Widget, filter_={"score": {">=": 1, "<": 3}}) == 2

    async def test_count_no_match(self, configure_firedantic: None) -> None:
        await _make_widgets(["a", "b"])
        assert await async_count_model(Widget, filter_={"category": "nonexistent"}) == 0


# ---------------------------------------------------------------------------
# build_prefix_filters (integration confirms Firestore accepts the query)
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
class TestPrefixFiltersIntegration:
    async def test_prefix_returns_matching_docs(self, configure_firedantic: None) -> None:
        await _make_widgets(["DA-00010", "DA-00011", "DA-00012", "DA-00020", "CB-00001"])
        filters = build_prefix_filters("label", "DA-0001")
        results = await Widget.find(filters, order_by=[("label", "ASCENDING")])
        assert [w.label for w in results] == ["DA-00010", "DA-00011", "DA-00012"]

    async def test_prefix_excludes_non_matching(self, configure_firedantic: None) -> None:
        await _make_widgets(["alpha", "alphabet", "beta"])
        filters = build_prefix_filters("label", "alpha")
        results = await Widget.find(filters)
        labels = {w.label for w in results}
        assert "beta" not in labels
        assert "alpha" in labels
        assert "alphabet" in labels

    async def test_prefix_with_count(self, configure_firedantic: None) -> None:
        await _make_widgets(["DA-001", "DA-002", "DA-003", "CB-001"])
        assert await async_count_model(Widget, filter_=build_prefix_filters("label", "DA-")) == 3


# ---------------------------------------------------------------------------
# cursor_paginate — forward pagination
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
class TestCursorPaginateForward:
    async def test_single_page_no_cursor(self, configure_firedantic: None) -> None:
        await _make_widgets(["a", "b", "c"])
        page = await async_cursor_paginate(Widget, limit=10, order_by="label")
        assert _all_labels(page) == ["a", "b", "c"]
        assert page.has_next is False
        assert page.has_prev is False
        assert page.next_cursor is None
        assert page.prev_cursor is None

    async def test_multi_page_forward(self, configure_firedantic: None) -> None:
        await _make_widgets(["a", "b", "c", "d", "e"])

        page1 = await async_cursor_paginate(Widget, limit=2, order_by="label")
        assert _all_labels(page1) == ["a", "b"]
        assert page1.has_next is True
        assert page1.has_prev is False
        assert page1.next_cursor is not None

        page2 = await async_cursor_paginate(
            Widget, limit=2, order_by="label", cursor=page1.next_cursor, direction="next"
        )
        assert _all_labels(page2) == ["c", "d"]
        assert page2.has_next is True
        assert page2.has_prev is True

        page3 = await async_cursor_paginate(
            Widget, limit=2, order_by="label", cursor=page2.next_cursor, direction="next"
        )
        assert _all_labels(page3) == ["e"]
        assert page3.has_next is False
        assert page3.has_prev is True

    async def test_full_iteration_covers_all_items(self, configure_firedantic: None) -> None:
        """Iterate forward page by page and confirm every item is visited exactly once."""
        labels = [f"item-{i:03d}" for i in range(13)]
        await _make_widgets(labels)

        seen: list[str] = []
        cursor = None
        while True:
            page = await async_cursor_paginate(Widget, limit=4, order_by="label", cursor=cursor, direction="next")
            seen.extend(_all_labels(page))
            if not page.has_next:
                break
            cursor = page.next_cursor

        assert seen == sorted(labels)

    async def test_empty_collection(self, configure_firedantic: None) -> None:
        page = await async_cursor_paginate(Widget, limit=10, order_by="label")
        assert page.items == []
        assert page.has_next is False
        assert page.has_prev is False

    async def test_exactly_limit_items(self, configure_firedantic: None) -> None:
        """Exactly limit items should not set has_next."""
        await _make_widgets(["a", "b", "c"])
        page = await async_cursor_paginate(Widget, limit=3, order_by="label")
        assert len(page.items) == 3
        assert page.has_next is False

    async def test_missing_cursor_document_raises(self, configure_firedantic: None) -> None:
        await _make_widgets(["a", "b"])
        with pytest.raises(ValueError, match="not found"):
            await async_cursor_paginate(Widget, limit=10, order_by="label", cursor="does-not-exist")


# ---------------------------------------------------------------------------
# cursor_paginate — backward pagination
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
class TestCursorPaginateBackward:
    async def test_prev_from_second_page(self, configure_firedantic: None) -> None:
        await _make_widgets(["a", "b", "c", "d"])

        page2 = await async_cursor_paginate(Widget, limit=2, order_by="label")
        # advance to page 2
        page2 = await async_cursor_paginate(
            Widget, limit=2, order_by="label", cursor=page2.next_cursor, direction="next"
        )
        assert _all_labels(page2) == ["c", "d"]

        # step back to page 1
        page1_again = await async_cursor_paginate(
            Widget, limit=2, order_by="label", cursor=page2.prev_cursor, direction="prev"
        )
        assert _all_labels(page1_again) == ["a", "b"]
        assert page1_again.has_prev is False
        assert page1_again.has_next is True

    async def test_full_round_trip(self, configure_firedantic: None) -> None:
        """Go forward to the end, then traverse backward covering all items."""
        labels = [f"r-{i:02d}" for i in range(9)]
        await _make_widgets(labels)

        # Collect all pages going forward
        forward_pages: list[CursorPage] = []
        cursor = None
        while True:
            page = await async_cursor_paginate(Widget, limit=3, order_by="label", cursor=cursor, direction="next")
            forward_pages.append(page)
            if not page.has_next:
                break
            cursor = page.next_cursor

        # Now traverse backward from the last page
        backward_pages: list[CursorPage] = []
        cursor = forward_pages[-1].prev_cursor
        while cursor is not None:
            page = await async_cursor_paginate(Widget, limit=3, order_by="label", cursor=cursor, direction="prev")
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

    async def test_prev_no_cursor_returns_last_page(self, configure_firedantic: None) -> None:
        await _make_widgets(["a", "b", "c", "d", "e"])
        page = await async_cursor_paginate(Widget, limit=2, order_by="label", direction="prev")
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
@pytest.mark.asyncio
class TestCursorPaginateFilters:
    async def test_equality_filter(self, configure_firedantic: None) -> None:
        await _make_widgets(["a", "b"], category="X")
        await _make_widgets(["c", "d"], category="Y")
        page = await async_cursor_paginate(Widget, limit=10, order_by="label", filter_={"category": "X"})
        assert _all_labels(page) == ["a", "b"]

    async def test_prefix_filter_with_pagination(self, configure_firedantic: None) -> None:
        await _make_widgets(["DA-001", "DA-002", "DA-003", "DA-004", "CB-001"])
        filters = build_prefix_filters("label", "DA-")

        page1 = await async_cursor_paginate(Widget, limit=2, order_by="label", filter_=filters)
        assert _all_labels(page1) == ["DA-001", "DA-002"]
        assert page1.has_next is True

        page2 = await async_cursor_paginate(
            Widget, limit=2, order_by="label", filter_=filters, cursor=page1.next_cursor, direction="next"
        )
        assert _all_labels(page2) == ["DA-003", "DA-004"]
        assert page2.has_next is False

    async def test_no_results_with_filter(self, configure_firedantic: None) -> None:
        await _make_widgets(["a", "b"])
        page = await async_cursor_paginate(Widget, limit=10, order_by="label", filter_={"category": "nonexistent"})
        assert page.items == []
        assert page.has_next is False


# ---------------------------------------------------------------------------
# cursor_paginate — exclude_null_sort_field (issue #6)
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
class TestExcludeNullSortField:
    async def test_null_valued_docs_lead_a_plain_sort(self, configure_firedantic: None) -> None:
        """Baseline: without the option, null-category docs sort first (ASC)."""
        await _make_widgets(["with-a", "with-b"], category="X")
        await _make_widgets(["null-a", "null-b"], category=None)
        page = await async_cursor_paginate(Widget, limit=10, order_by="category")
        # Ties (both nulls, both "X") settle by __name__, so only the
        # grouping is deterministic, not the order within each group.
        assert set(_all_labels(page)[:2]) == {"null-a", "null-b"}
        assert set(_all_labels(page)[2:]) == {"with-a", "with-b"}

    async def test_excludes_null_category_ascending(self, configure_firedantic: None) -> None:
        await Widget(label="with-a", score=1, category="A").save()
        await Widget(label="with-b", score=2, category="B").save()
        await _make_widgets(["null-a", "null-b"], category=None)
        page = await async_cursor_paginate(Widget, limit=10, order_by="category", exclude_null_sort_field=True)
        assert _all_labels(page) == ["with-a", "with-b"]

    async def test_excludes_null_category_descending(self, configure_firedantic: None) -> None:
        await Widget(label="with-a", score=1, category="A").save()
        await Widget(label="with-b", score=2, category="B").save()
        await _make_widgets(["null-a", "null-b"], category=None)
        page = await async_cursor_paginate(
            Widget, limit=10, order_by=[("category", "DESCENDING")], exclude_null_sort_field=True
        )
        assert _all_labels(page) == ["with-b", "with-a"]

    async def test_pagination_over_non_null_docs_only(self, configure_firedantic: None) -> None:
        await Widget(label="with-a", score=1, category="A").save()
        await Widget(label="with-b", score=2, category="B").save()
        await Widget(label="with-c", score=3, category="C").save()
        await _make_widgets(["null-a", "null-b"], category=None)

        page1 = await async_cursor_paginate(Widget, limit=2, order_by="category", exclude_null_sort_field=True)
        assert _all_labels(page1) == ["with-a", "with-b"]
        assert page1.has_next is True

        page2 = await async_cursor_paginate(
            Widget,
            limit=2,
            order_by="category",
            exclude_null_sort_field=True,
            cursor=page1.next_cursor,
            direction="next",
        )
        assert _all_labels(page2) == ["with-c"]
        assert page2.has_next is False

    async def test_combines_with_other_field_filter(self, configure_firedantic: None) -> None:
        await Widget(label="keep", score=1, category="X").save()
        await Widget(label="wrong-score", score=2, category="X").save()
        await Widget(label="null-category", score=1, category=None).save()

        page = await async_cursor_paginate(
            Widget,
            limit=10,
            order_by="category",
            filter_={"score": 1},
            exclude_null_sort_field=True,
        )
        assert _all_labels(page) == ["keep"]

    async def test_requires_order_by(self, configure_firedantic: None) -> None:
        with pytest.raises(ValueError, match="requires order_by"):
            await async_cursor_paginate(Widget, limit=10, exclude_null_sort_field=True)

    async def test_conflicts_with_existing_filter_on_sort_field(self, configure_firedantic: None) -> None:
        with pytest.raises(ValueError, match="category"):
            await async_cursor_paginate(
                Widget,
                limit=10,
                order_by="category",
                filter_={"category": "X"},
                exclude_null_sort_field=True,
            )


# ---------------------------------------------------------------------------
# cursor_paginate — include_total
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
class TestIncludeTotal:
    async def test_include_total_all(self, configure_firedantic: None) -> None:
        await _make_widgets(["a", "b", "c", "d", "e"])
        page = await async_cursor_paginate(Widget, limit=2, order_by="label", include_total=True)
        assert page.total == 5
        assert len(page.items) == 2

    async def test_include_total_with_filter(self, configure_firedantic: None) -> None:
        await _make_widgets(["a", "b"], category="X")
        await _make_widgets(["c"], category="Y")
        page = await async_cursor_paginate(
            Widget, limit=10, order_by="label", filter_={"category": "X"}, include_total=True
        )
        assert page.total == 2

    async def test_total_none_by_default(self, configure_firedantic: None) -> None:
        await _make_widgets(["a", "b"])
        page = await async_cursor_paginate(Widget, limit=10, order_by="label")
        assert page.total is None


# ---------------------------------------------------------------------------
# cursor_paginate — stable sort with duplicate values
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
class TestStableSort:
    async def test_duplicate_sort_values_no_skips(self, configure_firedantic: None) -> None:
        """
        When sort field has duplicate values, __name__ tiebreak must prevent
        any item being skipped or duplicated at page boundaries.
        """
        # 6 items with score=1 (all duplicates on the sort key)
        labels = [f"dup-{i}" for i in range(6)]
        for label in labels:
            await Widget(label=label, score=1).save()

        seen: list[str] = []
        cursor = None
        page_count = 0
        while True:
            page = await async_cursor_paginate(
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
