"""Helper utilities."""

from collections.abc import Sequence
from typing import TypeVar

T = TypeVar("T")


def chunks(items: Sequence[T], size: int) -> list[list[T]]:
    """Yield successive chunks of ``size`` from ``items``."""
    result: list[list[T]] = []
    for i in range(0, len(items), size):
        result.append(list(items[i : i + size]))
    return result
