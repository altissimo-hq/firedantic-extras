"""Unit tests for firedantic_extras.query — pure logic, no Firestore required."""

from __future__ import annotations

import pytest

from firedantic_extras.query import build_prefix_filters


class TestBuildPrefixFilters:
    def test_basic_prefix(self) -> None:
        result = build_prefix_filters("barcode", "DA-0001")
        assert result == {"barcode": {">=": "DA-0001", "<": "DA-0001\uf8ff"}}

    def test_single_char_prefix(self) -> None:
        result = build_prefix_filters("name", "A")
        assert result == {"name": {">=": "A", "<": "A\uf8ff"}}

    def test_field_name_preserved(self) -> None:
        result = build_prefix_filters("some_field", "prefix")
        assert "some_field" in result
        assert len(result) == 1

    def test_lower_bound_inclusive(self) -> None:
        filters = build_prefix_filters("code", "X")
        assert filters["code"][">="] == "X"

    def test_upper_bound_exclusive(self) -> None:
        filters = build_prefix_filters("code", "X")
        assert filters["code"]["<"] == "X\uf8ff"

    def test_sentinel_is_highest_bmp_char(self) -> None:
        """Verify the sentinel is the documented Unicode high water mark."""
        filters = build_prefix_filters("f", "p")
        upper = filters["f"]["<"]
        assert upper.endswith("\uf8ff")
        assert ord(upper[-1]) == 0xF8FF

    def test_empty_prefix_raises(self) -> None:
        with pytest.raises(ValueError, match="non-empty"):
            build_prefix_filters("field", "")

    def test_numeric_string_prefix(self) -> None:
        result = build_prefix_filters("code", "00012")
        assert result == {"code": {">=": "00012", "<": "00012\uf8ff"}}

    def test_unicode_prefix(self) -> None:
        result = build_prefix_filters("name", "Ä")
        assert result["name"][">="] == "Ä"
        assert result["name"]["<"] == "Ä\uf8ff"
