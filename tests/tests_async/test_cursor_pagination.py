"""Unit tests for cursor pagination — pure logic, no Firestore required."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from google.api_core.exceptions import FailedPrecondition

from firedantic_extras._async import cursor_pagination as cursor_pagination_module
from firedantic_extras._async.cursor_pagination import async_cursor_paginate
from firedantic_extras.common.pagination import CursorPage


def _stub_model_class() -> MagicMock:
    """A model_class whose Firestore query chain returns no results.

    Every chained call (where/order_by/limit) returns the same mock so the
    query-building loop doesn't produce an unconfigured, unstreamable mock.
    MagicMock iterates over whatever ``__aiter__.return_value`` holds.
    """
    query = MagicMock()
    query.where.return_value = query
    query.order_by.return_value = query
    query.limit.return_value = query
    query.stream.return_value.__aiter__.return_value = []

    model_class = MagicMock()
    model_class._get_col_ref.return_value = query
    return model_class


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
