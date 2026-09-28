#!/usr/bin/env python
"""Generate the sync flavour of firedantic-extras from the async sources.

The async code is the source of truth:

    src/firedantic_extras/_async/   ->   src/firedantic_extras/_sync/
    tests/tests_async/              ->   tests/tests_sync/

Every ``.py`` file is rewritten line by line with the substitutions in ``SUBS``
(``async def`` -> ``def``, ``await x`` -> ``x``, ``AsyncFoo`` -> ``Foo`` ...),
then ruff-formatted.  The generated files are committed, so the pre-commit hook
that runs this script doubles as a staleness check in CI: if regenerating
changes anything, the hook fails.

Run directly::

    poetry run python unasync.py

Same idea as https://github.com/python-trio/unasync and the copy of it that
firedantic itself uses.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PACKAGE = ROOT / "src" / "firedantic_extras"
TESTS = ROOT / "tests"

# (regex, replacement) — applied in order to every line.  Patterns carry their
# own ``\b`` anchors so that e.g. ``await `` still matches before ``(``.
SUBS: list[tuple[str, str]] = [
    # Package paths
    (r"firedantic_extras\._async", "firedantic_extras._sync"),
    (r"\btests_async\b", "tests_sync"),
    # google-cloud-firestore (AsyncQuery -> Query is handled by the generic
    # Async* rule below)
    (r"google\.cloud\.firestore_v1\.async_query", "google.cloud.firestore_v1.query"),
    # firedantic
    (r"\bget_async_client\b", "get_client"),
    # firedantic-extras public names
    (r"\basync_cursor_paginate\b", "cursor_paginate"),
    (r"\basync_count_model\b", "count_model"),
    # AsyncBareModel -> BareModel, AsyncCollectionSync -> CollectionSync,
    # AsyncClient -> Client, AsyncMock -> Mock, AsyncIterator -> Iterator ...
    (r"\bAsync([A-Z][A-Za-z0-9_]*)", r"\1"),
    # Syntax
    (r"\basync def\b", "def"),
    (r"\basync for\b", "for"),
    (r"\basync with\b", "with"),
    (r"\bawait ", ""),
    # unittest.mock: AsyncMock assertions -> Mock assertions
    (r"\bassert_awaited", "assert_called"),
    (r"\bassert_not_awaited\b", "assert_not_called"),
    (r"\bawait_args", "call_args"),
    (r"\bawait_count\b", "call_count"),
    (r"\b__aiter__\b", "__iter__"),
    (r"\b__anext__\b", "__next__"),
    (r"\bStopAsyncIteration\b", "StopIteration"),
]
COMPILED_SUBS = [(re.compile(pattern), repl) for pattern, repl in SUBS]

# Lines dropped outright (after stripping) rather than substituted.
DROP_LINES = {"@pytest.mark.asyncio"}

BANNER = "# GENERATED FILE — do not edit.\n# Source: {source}. Regenerate with `poetry run python unasync.py`.\n"

SYNC_PACKAGE_INIT = '''"""Sync I/O layer — GENERATED from ``firedantic_extras._async`` by ``unasync.py``.

Do not edit anything in this package by hand; change the async source and
regenerate.  The pre-commit hook does this automatically.
"""
'''


def unasync_line(line: str) -> str:
    for regex, repl in COMPILED_SUBS:
        line = regex.sub(repl, line)
    return line


def unasync_file(in_path: Path, out_path: Path) -> None:
    print(f"{in_path.relative_to(ROOT)} -> {out_path.relative_to(ROOT)}")
    lines = in_path.read_text().splitlines(keepends=True)
    out_lines = [unasync_line(line) for line in lines if line.strip() not in DROP_LINES]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    banner = BANNER.format(source=in_path.relative_to(ROOT).as_posix())
    out_path.write_text(banner + "".join(out_lines), newline="")


def unasync_dir(in_dir: Path, out_dir: Path) -> list[Path]:
    written: list[Path] = []
    for in_path in sorted(in_dir.glob("**/*.py")):
        out_path = out_dir / in_path.relative_to(in_dir)
        unasync_file(in_path, out_path)
        written.append(out_path)
    return written


def main() -> int:
    sync_pkg = PACKAGE / "_sync"
    written = unasync_dir(PACKAGE / "_async", sync_pkg)
    # The package docstring describes the *async* layer; give the generated
    # package its own rather than a mistranslated copy.
    (sync_pkg / "__init__.py").write_text(SYNC_PACKAGE_INIT)
    written += unasync_dir(TESTS / "tests_async", TESTS / "tests_sync")

    # Renames can reorder imports (AsyncMock -> Mock) and leave odd spacing;
    # bring the output up to the repo's ruff config so the files are stable.
    # (S603: argv is a fixed list built here, nothing user-supplied.)
    targets = [str(p) for p in written]
    ruff = [sys.executable, "-m", "ruff"]
    subprocess.run([*ruff, "check", "--select", "I", "--fix", "--quiet", *targets], check=True)  # noqa: S603
    subprocess.run([*ruff, "format", "--quiet", *targets], check=True)  # noqa: S603
    return 0


if __name__ == "__main__":
    sys.exit(main())
