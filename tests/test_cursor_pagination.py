"""Unit tests for firedantic_extras.cursor_pagination — pure logic, no Firestore required."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from firedantic_extras.cursor_pagination import cursor_paginate


def _stub_model_class() -> MagicMock:
    """A model_class whose Firestore query chain returns no results.

    Every chained call (where/order_by/limit) returns the same mock so the
    query-building loop doesn't produce an unconfigured, unstreamable mock.
    """
    query = MagicMock()
    query.where.return_value = query
    query.order_by.return_value = query
    query.limit.return_value = query
    query.stream.return_value = iter([])

    model_class = MagicMock()
    model_class._get_col_ref.return_value = query
    return model_class


class TestExcludeNullSortFieldValidation:
    """These error paths are raised before any Firestore access, so a plain
    MagicMock model_class is enough — no emulator needed.
    """

    def test_requires_order_by(self) -> None:
        with pytest.raises(ValueError, match="requires order_by"):
            cursor_paginate(MagicMock(), order_by=None, exclude_null_sort_field=True)

    def test_conflicts_with_existing_filter_on_same_field(self) -> None:
        with pytest.raises(ValueError, match="order_id"):
            cursor_paginate(
                MagicMock(),
                order_by="order_id",
                filter_={"order_id": {">=": 0}},
                exclude_null_sort_field=True,
            )

    def test_does_not_conflict_with_filter_on_other_field(self) -> None:
        model_class = _stub_model_class()
        page = cursor_paginate(
            model_class,
            order_by="order_id",
            filter_={"category": "X"},
            exclude_null_sort_field=True,
        )
        assert page.items == []
