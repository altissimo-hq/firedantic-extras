"""Unit tests for firedantic_extras.flask.pagination — pure logic, no Firestore required."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from flask import Flask

from firedantic_extras.cursor_pagination import ASCENDING, DESCENDING, CursorPage
from firedantic_extras.flask import pagination as pagination_module
from firedantic_extras.flask.pagination import (
    DEFAULT_LIMIT,
    MAX_LIMIT,
    FlaskPaginationParams,
    PaginatedContext,
    paginate_model,
)

app = Flask(__name__)


def make_page(**overrides: Any) -> CursorPage[Any]:
    defaults: dict[str, Any] = {
        "items": [],
        "has_next": False,
        "has_prev": False,
        "next_cursor": None,
        "prev_cursor": None,
        "total": None,
    }
    defaults.update(overrides)
    return CursorPage(**defaults)


class TestFromRequest:
    def test_defaults_with_no_query_string(self) -> None:
        with app.test_request_context("/things"):
            params = FlaskPaginationParams.from_request()
        assert params == FlaskPaginationParams(
            cursor=None, direction="next", limit=DEFAULT_LIMIT, order_by="updated_at", order_dir=DESCENDING
        )

    def test_reads_all_fields_from_query_string(self) -> None:
        with app.test_request_context("/things?cursor=abc&direction=prev&limit=25&order_by=name&order_dir=ASCENDING"):
            params = FlaskPaginationParams.from_request()
        assert params.cursor == "abc"
        assert params.direction == "prev"
        assert params.limit == 25
        assert params.order_by == "name"
        assert params.order_dir == ASCENDING

    def test_unknown_direction_falls_back_to_next(self) -> None:
        with app.test_request_context("/things?direction=sideways"):
            params = FlaskPaginationParams.from_request()
        assert params.direction == "next"

    @pytest.mark.parametrize("raw_limit", ["0", "-5"])
    def test_non_positive_limit_means_all(self, raw_limit: str) -> None:
        with app.test_request_context(f"/things?limit={raw_limit}"):
            params = FlaskPaginationParams.from_request()
        assert params.limit == MAX_LIMIT

    def test_limit_above_max_is_clamped(self) -> None:
        with app.test_request_context("/things?limit=5000000"):
            params = FlaskPaginationParams.from_request()
        assert params.limit == MAX_LIMIT

    def test_non_numeric_limit_falls_back_to_default(self) -> None:
        with app.test_request_context("/things?limit=abc"):
            params = FlaskPaginationParams.from_request()
        assert params.limit == DEFAULT_LIMIT

    def test_invalid_order_dir_falls_back_to_default(self) -> None:
        with app.test_request_context("/things?order_dir=bogus"):
            params = FlaskPaginationParams.from_request()
        assert params.order_dir == DESCENDING

    def test_invalid_order_dir_falls_back_to_custom_default(self) -> None:
        with app.test_request_context("/things?order_dir=bogus"):
            params = FlaskPaginationParams.from_request(default_order_dir=ASCENDING)
        assert params.order_dir == ASCENDING

    def test_custom_defaults_apply_when_unset(self) -> None:
        with app.test_request_context("/things"):
            params = FlaskPaginationParams.from_request(default_order_by="name", default_order_dir=ASCENDING)
        assert params.order_by == "name"
        assert params.order_dir == ASCENDING

    def test_empty_cursor_param_is_none(self) -> None:
        with app.test_request_context("/things?cursor="):
            params = FlaskPaginationParams.from_request()
        assert params.cursor is None


class TestBuildQueryParams:
    def test_all_defaults_yields_empty_dict(self) -> None:
        params = FlaskPaginationParams()
        assert params.build_query_params() == {}

    def test_non_default_values_are_included(self) -> None:
        params = FlaskPaginationParams(cursor="xyz", direction="prev", limit=25, order_by="name", order_dir=ASCENDING)
        assert params.build_query_params() == {
            "cursor": "xyz",
            "direction": "prev",
            "limit": "25",
            "order_by": "name",
            "order_dir": "ASCENDING",
        }

    def test_overrides_take_precedence(self) -> None:
        params = FlaskPaginationParams(limit=25)
        result = params.build_query_params(limit=100)
        assert result == {"limit": "100"}

    def test_none_override_omits_key(self) -> None:
        params = FlaskPaginationParams(cursor="abc")
        result = params.build_query_params(cursor=None)
        assert "cursor" not in result

    def test_override_back_to_default_omits_key(self) -> None:
        params = FlaskPaginationParams(direction="prev")
        result = params.build_query_params(direction="next")
        assert "direction" not in result


class TestPaginatedContext:
    def test_next_params_uses_next_cursor(self) -> None:
        ctx = PaginatedContext(page=make_page(next_cursor="c2", has_next=True), params=FlaskPaginationParams())
        assert ctx.next_params() == {"cursor": "c2"}

    def test_prev_params_uses_prev_cursor_and_direction(self) -> None:
        ctx = PaginatedContext(page=make_page(prev_cursor="c1", has_prev=True), params=FlaskPaginationParams())
        assert ctx.prev_params() == {"cursor": "c1", "direction": "prev"}

    def test_sort_params_toggles_direction_on_active_column(self) -> None:
        # order_dir toggles ASCENDING -> DESCENDING, which is the dataclass's
        # own field default, so it's omitted from the built query params.
        params = FlaskPaginationParams(order_by="name", order_dir=ASCENDING)
        ctx = PaginatedContext(page=make_page(), params=params)
        assert ctx.sort_params("name") == {"order_by": "name"}

    def test_sort_params_defaults_to_ascending_on_new_column(self) -> None:
        params = FlaskPaginationParams(order_by="name", order_dir=DESCENDING)
        ctx = PaginatedContext(page=make_page(), params=params)
        assert ctx.sort_params("status") == {"order_by": "status", "order_dir": ASCENDING}

    def test_sort_params_resets_cursor(self) -> None:
        params = FlaskPaginationParams(cursor="abc", order_by="name")
        ctx = PaginatedContext(page=make_page(), params=params)
        assert "cursor" not in ctx.sort_params("name")

    def test_sort_indicator_shows_arrow_for_active_column(self) -> None:
        params = FlaskPaginationParams(order_by="name", order_dir=DESCENDING)
        ctx = PaginatedContext(page=make_page(), params=params)
        assert ctx.sort_indicator("name") == "▼"

    def test_sort_indicator_ascending_arrow(self) -> None:
        params = FlaskPaginationParams(order_by="name", order_dir=ASCENDING)
        ctx = PaginatedContext(page=make_page(), params=params)
        assert ctx.sort_indicator("name") == "▲"

    def test_sort_indicator_empty_for_inactive_column(self) -> None:
        params = FlaskPaginationParams(order_by="name")
        ctx = PaginatedContext(page=make_page(), params=params)
        assert ctx.sort_indicator("status") == ""

    def test_limit_params_resets_cursor(self) -> None:
        params = FlaskPaginationParams(cursor="abc", limit=50)
        ctx = PaginatedContext(page=make_page(), params=params)
        assert ctx.limit_params(100) == {"limit": "100"}


class TestPaginateModel:
    def test_defaults_order_by_to_params_sort_state(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_cursor_paginate = MagicMock(return_value=make_page(total=3))
        monkeypatch.setattr(pagination_module, "cursor_paginate", mock_cursor_paginate)

        model_class = MagicMock()
        params = FlaskPaginationParams(order_by="name", order_dir=ASCENDING, limit=10, cursor="c1", direction="prev")
        ctx = paginate_model(model_class, params)

        mock_cursor_paginate.assert_called_once_with(
            model_class,
            limit=10,
            cursor="c1",
            direction="prev",
            filter_=None,
            order_by=[("name", ASCENDING)],
            include_total=True,
            exclude_null_sort_field=False,
        )
        assert isinstance(ctx, PaginatedContext)
        assert ctx.total == 3
        assert ctx.params is params

    def test_explicit_order_by_overrides_params(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_cursor_paginate = MagicMock(return_value=make_page())
        monkeypatch.setattr(pagination_module, "cursor_paginate", mock_cursor_paginate)

        paginate_model(MagicMock(), FlaskPaginationParams(order_by="name"), order_by="status")

        assert mock_cursor_paginate.call_args.kwargs["order_by"] == "status"

    def test_filter_and_include_total_are_forwarded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_cursor_paginate = MagicMock(return_value=make_page())
        monkeypatch.setattr(pagination_module, "cursor_paginate", mock_cursor_paginate)

        filter_ = {"species": "dog"}
        paginate_model(MagicMock(), FlaskPaginationParams(), filter_=filter_, include_total=False)

        assert mock_cursor_paginate.call_args.kwargs["filter_"] == filter_
        assert mock_cursor_paginate.call_args.kwargs["include_total"] is False

    def test_exclude_null_sort_field_is_forwarded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_cursor_paginate = MagicMock(return_value=make_page())
        monkeypatch.setattr(pagination_module, "cursor_paginate", mock_cursor_paginate)

        paginate_model(MagicMock(), FlaskPaginationParams(), exclude_null_sort_field=True)

        assert mock_cursor_paginate.call_args.kwargs["exclude_null_sort_field"] is True
