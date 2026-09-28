"""Unit tests for cursor pagination — pure logic, no Firestore required."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from firedantic import ModelNotFoundError
from firedantic import operators as op
from google.api_core.exceptions import FailedPrecondition

from firedantic_extras._async import cursor_pagination as cursor_pagination_module
from firedantic_extras._async.cursor_pagination import async_cursor_paginate
from firedantic_extras.common.pagination import CursorPage


def _stub_model_class() -> MagicMock:
    """A model_class whose ``find()`` returns no results."""
    model_class = MagicMock(__name__="Kit")
    model_class.find = AsyncMock(return_value=[])
    model_class.count = AsyncMock(return_value=0)
    model_class.get_collection_name.return_value = "kits"
    # Stand-in for firedantic's full-ordering rule: the given sort, then the
    # document ID in the direction of the last given ordering.
    model_class._get_full_ordering = lambda _filter, order_by: [
        *order_by,
        ("__name__", order_by[-1][1] if order_by else "ASCENDING"),
    ]
    return model_class


@pytest.mark.asyncio
class TestFindCall:
    """What reaches firedantic's ``find()`` — the filter is passed through
    untouched, the sort gets the ``__name__`` tiebreak, and the cursor is
    handed over as the document ID it is.
    """

    async def test_filter_including_or_is_passed_through_verbatim(self) -> None:
        model_class = _stub_model_class()
        filter_ = {op.OR: [{"status": "new"}, {op.AND: [{"status": "open"}, {"priority": {op.GTE: 3}}]}]}

        await async_cursor_paginate(model_class, limit=10, order_by="barcode", filter_=filter_)

        model_class.find.assert_awaited_once_with(
            filter_,
            order_by=[("barcode", "ASCENDING"), ("__name__", "ASCENDING")],
            limit=11,
            start_after=None,
        )

    async def test_prev_direction_reverses_sort_and_passes_cursor(self) -> None:
        model_class = _stub_model_class()

        await async_cursor_paginate(model_class, limit=10, order_by="barcode", cursor="doc-7", direction="prev")

        model_class.find.assert_awaited_once_with(
            None,
            order_by=[("barcode", "DESCENDING"), ("__name__", "DESCENDING")],
            limit=11,
            start_after="doc-7",
        )

    async def test_full_ordering_comes_from_the_model(self) -> None:
        """Inequality-filtered fields must be ordered before __name__ — that
        rule lives in firedantic, and cursor_paginate uses its answer verbatim."""
        model_class = _stub_model_class()
        model_class._get_full_ordering = MagicMock(
            return_value=[("barcode", "ASCENDING"), ("score", "ASCENDING"), ("__name__", "ASCENDING")]
        )
        filter_ = {"score": {op.GTE: 5}}

        await async_cursor_paginate(model_class, limit=10, order_by="barcode", filter_=filter_, direction="prev")

        model_class._get_full_ordering.assert_called_once_with(filter_, [("barcode", "ASCENDING")])
        assert model_class.find.await_args.kwargs["order_by"] == [
            ("barcode", "DESCENDING"),
            ("score", "DESCENDING"),
            ("__name__", "DESCENDING"),
        ]

    async def test_exclude_null_sort_field_adds_not_null_filter(self) -> None:
        model_class = _stub_model_class()

        await async_cursor_paginate(model_class, order_by="order_id", exclude_null_sort_field=True)

        assert model_class.find.await_args.args[0] == {"order_id": {"!=": None}}

    async def test_missing_cursor_document_raises_value_error(self) -> None:
        model_class = _stub_model_class()
        model_class.find = AsyncMock(side_effect=ModelNotFoundError("Cursor document 'kits/gone' does not exist"))

        with pytest.raises(ValueError, match="'gone' not found in collection 'kits'"):
            await async_cursor_paginate(model_class, order_by="barcode", cursor="gone")

    async def test_include_total_counts_with_the_effective_filter(self) -> None:
        model_class = _stub_model_class()
        model_class.count = AsyncMock(return_value=42)

        page = await async_cursor_paginate(
            model_class,
            order_by="order_id",
            filter_={"category": "X"},
            exclude_null_sort_field=True,
            include_total=True,
        )

        assert page.total == 42
        model_class.count.assert_awaited_once_with({"category": "X", "order_id": {"!=": None}})


@pytest.mark.asyncio
class TestExcludeNullSortFieldValidation:
    """These error paths are raised before any Firestore access, so a plain
    MagicMock model_class is enough — no emulator needed.
    """

    async def test_requires_order_by(self) -> None:
        with pytest.raises(ValueError, match="requires order_by"):
            await async_cursor_paginate(MagicMock(), order_by=None, exclude_null_sort_field=True)

    async def test_conflicts_with_existing_filter_on_same_field(self) -> None:
        with pytest.raises(ValueError, match="order_id"):
            await async_cursor_paginate(
                MagicMock(),
                order_by="order_id",
                filter_={"order_id": {">=": 0}},
                exclude_null_sort_field=True,
            )

    async def test_does_not_conflict_with_filter_on_other_field(self) -> None:
        model_class = _stub_model_class()
        page = await async_cursor_paginate(
            model_class,
            order_by="order_id",
            filter_={"category": "X"},
            exclude_null_sort_field=True,
        )
        assert page.items == []


def _fake_page(**overrides: object) -> CursorPage[object]:
    defaults: dict[str, object] = {
        "items": [],
        "has_next": False,
        "has_prev": False,
        "next_cursor": None,
        "prev_cursor": None,
        "total": None,
        "used_fallback": False,
    }
    defaults.update(overrides)
    return CursorPage(**defaults)  # type: ignore[arg-type]


@pytest.mark.asyncio
class TestFallbackOrderBy:
    """The FailedPrecondition retry, tested by mocking the single-attempt
    helper directly -- the Firestore emulator doesn't enforce composite index
    requirements, so FailedPrecondition can't be reproduced against it
    (verified: an equality filter + order_by on a different, unindexed field
    succeeds on the emulator, unlike production Firestore).
    """

    async def test_propagates_when_no_fallback_configured(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            cursor_pagination_module,
            "_cursor_paginate_once",
            AsyncMock(side_effect=FailedPrecondition("missing index")),
        )
        with pytest.raises(FailedPrecondition):
            await async_cursor_paginate(MagicMock(), order_by="barcode")

    async def test_retries_with_fallback_and_resets_cursor_and_direction(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_once = AsyncMock(side_effect=[FailedPrecondition("missing index"), _fake_page(used_fallback=True)])
        monkeypatch.setattr(cursor_pagination_module, "_cursor_paginate_once", mock_once)

        model_class = MagicMock(__name__="Kit")
        page = await async_cursor_paginate(
            model_class,
            limit=10,
            cursor="stale-cursor",
            direction="prev",
            filter_={"category": "X"},
            order_by="barcode",
            fallback_order_by="updated_at",
        )

        assert page.used_fallback is True
        assert mock_once.call_count == 2

        primary_call, fallback_call = mock_once.call_args_list
        assert primary_call.kwargs["order_by"] == "barcode"
        assert primary_call.kwargs["cursor"] == "stale-cursor"
        assert primary_call.kwargs["direction"] == "prev"

        assert fallback_call.kwargs["order_by"] == "updated_at"
        assert fallback_call.kwargs["cursor"] is None
        assert fallback_call.kwargs["direction"] == "next"
        assert fallback_call.kwargs["used_fallback"] is True
        # filter_, limit, include_total, exclude_null_sort_field carry over unchanged.
        assert fallback_call.kwargs["filter_"] == {"category": "X"}
        assert fallback_call.kwargs["limit"] == 10

    async def test_no_retry_when_primary_query_succeeds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_once = AsyncMock(return_value=_fake_page())
        monkeypatch.setattr(cursor_pagination_module, "_cursor_paginate_once", mock_once)

        page = await async_cursor_paginate(MagicMock(), order_by="barcode", fallback_order_by="updated_at")

        assert page.used_fallback is False
        mock_once.assert_called_once()
