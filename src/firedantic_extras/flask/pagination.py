"""Flask adapter for cursor-based pagination.

Provides the request-parsing and template-rendering plumbing that every
paginated, sortable Flask list page needs, built on top of the core
:func:`~firedantic_extras.cursor_pagination.cursor_paginate`.

Typical usage::

    from firedantic_extras.flask.pagination import FlaskPaginationParams, paginate_model

    @blueprint.route("/things")
    def list_things():
        params = FlaskPaginationParams.from_request(default_order_by="name")
        ctx = paginate_model(Thing, params)
        return render_template("things.html", ctx=ctx)

In the template::

    <a href="{{ url_for(endpoint, **ctx.sort_params('name')) }}">
        Name {{ ctx.sort_indicator('name') }}
    </a>
    {% if ctx.page.has_next %}
      <a href="{{ url_for(endpoint, **ctx.next_params()) }}">Next</a>
    {% endif %}
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import TYPE_CHECKING, Any, Literal

try:
    from flask import request
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "flask is required to use firedantic_extras.flask.pagination. Install it with: pip install flask"
    ) from exc

from firedantic_extras.cursor_pagination import ASCENDING, DESCENDING, CursorPage, cursor_paginate

if TYPE_CHECKING:
    from firedantic import BareModel

    from firedantic_extras.query import FilterDict

__all__ = [
    "DEFAULT_LIMIT",
    "MAX_LIMIT",
    "FlaskPaginationParams",
    "PaginatedContext",
    "paginate_model",
]

DEFAULT_LIMIT = 50
MAX_LIMIT = 1000


@dataclass
class FlaskPaginationParams:
    """Pagination + sort state extracted from a Flask request's query string.

    Attributes:
        cursor:     Document ID of the page boundary, or ``None`` for the
                    first (or last, with ``direction="prev"``) page.
        direction:  ``"next"`` (default) moves forward; ``"prev"`` moves
                    backward.
        limit:      Items per page. A query value of ``0`` or less means
                    "All" and is normalised to :data:`MAX_LIMIT`.
        order_by:   Field name currently sorted on.
        order_dir:  ``"ASCENDING"`` or ``"DESCENDING"``.
    """

    cursor: str | None = None
    direction: Literal["next", "prev"] = "next"
    limit: int = DEFAULT_LIMIT
    order_by: str = "updated_at"
    order_dir: str = DESCENDING

    @classmethod
    def from_request(
        cls,
        default_order_by: str = "updated_at",
        default_order_dir: str = DESCENDING,
    ) -> FlaskPaginationParams:
        """Extract pagination params from the current Flask request.args."""
        args = request.args
        limit = int(args.get("limit", DEFAULT_LIMIT))
        if limit < 1:
            limit = MAX_LIMIT
        direction: Literal["next", "prev"] = "prev" if args.get("direction") == "prev" else "next"
        return cls(
            cursor=args.get("cursor") or None,
            direction=direction,
            limit=limit,
            order_by=args.get("order_by", default_order_by),
            order_dir=args.get("order_dir", default_order_dir),
        )

    def build_query_params(self, **overrides: Any) -> dict[str, str]:
        """Build URL query params preserving current state, with overrides.

        Values equal to this class's field defaults, and ``None`` values,
        are omitted so generated URLs stay short and readable.
        """
        merged: dict[str, Any] = {f.name: getattr(self, f.name) for f in fields(self)}
        merged.update(overrides)

        defaults = {f.name: f.default for f in fields(self)}
        params: dict[str, str] = {}
        for key, value in merged.items():
            if value is None or value == defaults.get(key):
                continue
            params[key] = str(value)
        return params


@dataclass
class PaginatedContext:
    """Everything a Jinja template needs to render a paginated, sortable table."""

    page: CursorPage[Any]
    params: FlaskPaginationParams
    total: int | None = None

    @property
    def items(self) -> list[Any]:
        """Shortcut for ``page.items``."""
        return self.page.items

    @property
    def showing_count(self) -> int:
        """Number of items on the current page."""
        return len(self.page.items)

    def next_params(self) -> dict[str, str]:
        """URL query params for the next page."""
        return self.params.build_query_params(cursor=self.page.next_cursor, direction="next")

    def prev_params(self) -> dict[str, str]:
        """URL query params for the previous page."""
        return self.params.build_query_params(cursor=self.page.prev_cursor, direction="prev")

    def sort_params(self, field_name: str) -> dict[str, str]:
        """URL query params to sort by *field_name*, toggling direction if already active."""
        if self.params.order_by == field_name:
            new_dir = ASCENDING if self.params.order_dir == DESCENDING else DESCENDING
        else:
            new_dir = ASCENDING
        return self.params.build_query_params(order_by=field_name, order_dir=new_dir, cursor=None)

    def sort_indicator(self, field_name: str) -> str:
        """Return "▲"/"▼" for the active sort column, else an empty string."""
        if self.params.order_by != field_name:
            return ""
        return "▼" if self.params.order_dir == DESCENDING else "▲"

    def limit_params(self, new_limit: int) -> dict[str, str]:
        """URL query params to switch the page size to *new_limit*."""
        return self.params.build_query_params(limit=new_limit, cursor=None)


def paginate_model(
    model_class: type[BareModel],
    params: FlaskPaginationParams,
    *,
    filter_: FilterDict | None = None,
    order_by: str | list[str | tuple[str, str]] | None = None,
    include_total: bool = True,
) -> PaginatedContext:
    """Run :func:`cursor_paginate` and return a :class:`PaginatedContext`.

    Args:
        model_class:    The Firedantic model class to query.
        params:         Pagination + sort state, typically from
                        :meth:`FlaskPaginationParams.from_request`.
        filter_:        Optional Firedantic-style filter dict.
        order_by:       Overrides the sort spec passed to ``cursor_paginate``.
                        Defaults to ``[(params.order_by, params.order_dir)]``
                        so that sortable-column links (:meth:`PaginatedContext.sort_params`)
                        stay in sync with the query actually run.
        include_total:  If ``True`` (default), populates ``ctx.total`` with a
                        COUNT aggregation query.
    """
    resolved_order_by = order_by if order_by is not None else [(params.order_by, params.order_dir)]
    page = cursor_paginate(
        model_class,
        limit=params.limit,
        cursor=params.cursor,
        direction=params.direction,
        filter_=filter_,
        order_by=resolved_order_by,
        include_total=include_total,
    )
    return PaginatedContext(page=page, params=params, total=page.total)
