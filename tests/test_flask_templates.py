"""Tests for firedantic_extras.flask.templates and the bundled Jinja macros (issue #7)."""

from __future__ import annotations

from typing import Any

import pytest
from flask import Flask

from firedantic_extras.cursor_pagination import CursorPage
from firedantic_extras.flask.pagination import FlaskPaginationParams, PaginatedContext
from firedantic_extras.flask.templates import register_macros


def make_app() -> Flask:
    app = Flask(__name__)
    app.add_url_rule("/things", endpoint="list_things", view_func=lambda: "")
    register_macros(app)
    return app


def make_ctx(**page_overrides: Any) -> PaginatedContext:
    defaults: dict[str, Any] = {
        "items": ["a", "b"],
        "has_next": False,
        "has_prev": False,
        "next_cursor": None,
        "prev_cursor": None,
        "total": None,
    }
    defaults.update(page_overrides)
    page = CursorPage(**defaults)
    return PaginatedContext(page=page, params=FlaskPaginationParams(), total=page.total)


def render(app: Flask, snippet: str, **context: Any) -> str:
    template = '{% import "firedantic_extras/macros.html" as fe_macros %}\n' + snippet
    with app.test_request_context():
        return app.jinja_env.from_string(template).render(**context)


class TestRegisterMacros:
    def test_macros_template_is_importable(self) -> None:
        app = make_app()
        with app.test_request_context():
            app.jinja_env.get_template("firedantic_extras/macros.html")

    def test_preserves_existing_loader(self) -> None:
        app = Flask(__name__)
        original_loader = app.jinja_loader
        register_macros(app)
        assert original_loader in app.jinja_loader.loaders  # type: ignore[union-attr]


class TestSortableHeader:
    def test_renders_label_and_link(self) -> None:
        app = make_app()
        ctx = make_ctx()
        html = render(app, '{{ fe_macros.sortable_header(ctx, "name", "Name", "list_things") }}', ctx=ctx)
        assert "Name" in html
        assert "/things?order_by=name" in html

    def test_includes_sort_indicator_for_active_column(self) -> None:
        app = make_app()
        ctx = PaginatedContext(page=make_ctx().page, params=FlaskPaginationParams(order_by="name"))
        html = render(app, '{{ fe_macros.sortable_header(ctx, "name", "Name", "list_things") }}', ctx=ctx)
        # order_dir defaults to DESCENDING, so the active column shows ▼.
        assert "▼" in html


class TestPaginationControls:
    def test_no_nav_when_single_page(self) -> None:
        app = make_app()
        ctx = make_ctx(has_prev=False, has_next=False)
        html = render(app, "{{ fe_macros.pagination_controls(ctx, 'list_things') }}", ctx=ctx)
        assert "<nav" not in html

    def test_prev_disabled_when_no_prev_page(self) -> None:
        app = make_app()
        ctx = make_ctx(has_prev=False, has_next=True, next_cursor="c2")
        html = render(app, "{{ fe_macros.pagination_controls(ctx, 'list_things') }}", ctx=ctx)
        assert "disabled" in html
        assert "← Previous" in html

    def test_next_link_enabled_when_has_next(self) -> None:
        app = make_app()
        ctx = make_ctx(has_prev=False, has_next=True, next_cursor="c2")
        html = render(app, "{{ fe_macros.pagination_controls(ctx, 'list_things') }}", ctx=ctx)
        assert "/things?cursor=c2" in html

    def test_total_shown_when_present(self) -> None:
        app = make_app()
        ctx = make_ctx(has_next=True, next_cursor="c2", total=42)
        html = render(app, "{{ fe_macros.pagination_controls(ctx, 'list_things') }}", ctx=ctx)
        assert "42 total items" in html


class TestResultInfo:
    def test_shows_total_and_showing_count(self) -> None:
        app = make_app()
        ctx = make_ctx(items=["a", "b", "c"], total=10)
        html = render(app, "{{ fe_macros.result_info(ctx) }}", ctx=ctx)
        assert "10 items found" in html
        assert "showing 3 on this page" in html

    def test_singular_item_wording(self) -> None:
        app = make_app()
        ctx = make_ctx(items=["a"], total=1)
        html = render(app, "{{ fe_macros.result_info(ctx) }}", ctx=ctx)
        assert "1 item found" in html

    def test_omits_total_when_none(self) -> None:
        app = make_app()
        ctx = make_ctx(items=["a"], total=None)
        html = render(app, "{{ fe_macros.result_info(ctx) }}", ctx=ctx)
        assert "found" not in html


class TestPageSizeSelector:
    def test_marks_current_limit_selected(self) -> None:
        # 100 is not DEFAULT_LIMIT (50), so it survives build_query_params's
        # default-omission and shows up in the option's href.
        app = make_app()
        ctx = PaginatedContext(page=make_ctx().page, params=FlaskPaginationParams(limit=100))
        html = render(app, "{{ fe_macros.page_size_selector(ctx, 'list_things') }}", ctx=ctx)
        assert '<option value="/things?limit=100" selected' in html

    def test_default_limit_option_href_omits_query_param(self) -> None:
        # limit=50 equals FlaskPaginationParams' own field default, so
        # build_query_params omits it from the URL entirely.
        app = make_app()
        ctx = PaginatedContext(page=make_ctx().page, params=FlaskPaginationParams(limit=50))
        html = render(app, "{{ fe_macros.page_size_selector(ctx, 'list_things') }}", ctx=ctx)
        assert '<option value="/things" selected' in html

    def test_all_option_selected_when_limit_outside_sizes(self) -> None:
        app = make_app()
        ctx = PaginatedContext(page=make_ctx().page, params=FlaskPaginationParams(limit=1000))
        html = render(app, "{{ fe_macros.page_size_selector(ctx, 'list_things', sizes=[25, 50]) }}", ctx=ctx)
        assert ">All</option>" in html
        assert 'value="/things?limit=25" selected' not in html
        assert 'value="/things?limit=50" selected' not in html

    @pytest.mark.parametrize("size", [25, 100, 200])
    def test_non_default_sizes_render_with_limit_param(self, size: int) -> None:
        app = make_app()
        ctx = PaginatedContext(page=make_ctx().page, params=FlaskPaginationParams(limit=size))
        html = render(app, "{{ fe_macros.page_size_selector(ctx, 'list_things') }}", ctx=ctx)
        assert f"limit={size}" in html
