# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.2.1] - 2026-09-28

### Changed

- Published on PyPI as `altissimo-firedantic-extras` (the import name is still
  `firedantic_extras`). Earlier versions were only installable from GitHub
  under the name `firedantic-extras`.
- Depends on [`altissimo-firedantic`](https://pypi.org/project/altissimo-firedantic/)
  `>=0.22.2,<0.23` from PyPI — the Altissimo fork of firedantic, still imported
  as `firedantic` — instead of a git URL.

### Fixed

- Released versions pinned firedantic to its GitHub `main` branch, so installing
  0.2.0 pulled whatever firedantic `main` was at install time, and consuming
  projects' lockfiles recorded `rev=main` even when they pinned a firedantic
  tag. (#32)

## [0.2.0] - 2026-09-28

Requires [firedantic](https://github.com/altissimo-hq/firedantic) 0.22.0 or
later, installed from GitHub (the fork is not on PyPI).

### Added

- Async support. Every Firestore-touching helper now has an async flavour for
  `firedantic.AsyncModel`: `async_cursor_paginate()`, `async_count_model()` and
  `AsyncCollectionSync`, exported from the top-level package and their modules;
  `firedantic_extras.fastapi.pagination` re-exports `async_cursor_paginate` for
  `async def` routes. The async code is the hand-written source and the sync
  flavour is generated from it by `unasync.py` (a pre-commit hook), the same way
  firedantic does it. (#17)
- `$or` / `$and` filters (`firedantic.operators.OR` / `AND`) in
  `cursor_paginate()`, `count_model()` and therefore the Flask and FastAPI
  adapters. Filter dicts are handed straight to firedantic, so anything its
  `find()` accepts works. (#24)
- `SyncResult.skipped_duplicate_keys`, listing the `sync_key` values a
  `CollectionSync` run left alone because of `on_duplicate_keys="skip"`; the
  summary line shows `skipped_duplicates=N`. (#20)
- This changelog.

### Changed

- `cursor_paginate()` runs on firedantic's native `find(start_after=...)` and
  `count_model()` on `Model.count()`. Hydration follows firedantic's stored-value
  conversions, and the sort is firedantic's full ordering: the caller's fields,
  then inequality-filtered fields, then `__name__`. The `__name__` tiebreak now
  follows the direction of the last sort field instead of always ascending;
  cursors are unchanged (document IDs). (#24)
- `CollectionSync` writes through firedantic's batched `save(batch=...)` and
  `delete(batch=...)` instead of a raw client batch, so ID generation, field
  aliases, stored-value conversion and `__db_config__` routing match a plain
  `save()`. An update writes to the existing document's ID through a copy of
  the incoming model, leaving the caller's instance untouched. (#25)
- The `fastapi`, `flask` and `bigquery` extras are declared as optional
  dependencies, so `pip install firedantic-extras[flask]` actually installs
  flask; `fastapi-pagination`, which nothing used, is no longer pulled in by the
  `fastapi` extra. (#22)
- firedantic is pinned to the GitHub repository's `main` branch (0.22.0); the lockfile
  records the exact commit. (#23)

### Fixed

- `CollectionSync(on_duplicate_keys="skip")` overwrote one of the duplicate
  documents with the incoming item, and `"update_all"` added the incoming item
  as a new document instead of updating every duplicate. Both now do what they
  say: `"skip"` leaves the duplicates and the incoming item untouched,
  `"update_all"` updates each duplicate. (#20)
- `cursor_paginate()` with an inequality filter on a field that is not in
  `order_by` was rejected by Firestore ("order by clause cannot contain more
  fields after the key"). (#24)
- `CollectionSync` reported every model with an enum, date, decimal, UUID or
  similar field as changed on every run, because firedantic 0.20 stores those
  in converted form and the diff compared the raw model dump. The incoming side
  is now compared in stored form, and the document ID is dropped by its alias.
  (#25)

### Removed

- The private helpers `firedantic_extras.query._apply_filter_dict`,
  `_build_query`, `_fetch_cursor_snapshot` and `_hydrate`, which copied
  firedantic internals that firedantic 0.21 provides. (#24)

### Security

- Refreshed transitive dependencies to resolve all 19 open advisories
  (cryptography, starlette, anyio, pyasn1, urllib3, idna, pytest). (#21)

## [0.1.8] and earlier

No changelog was kept; see the git history.

[Unreleased]: https://github.com/altissimo-hq/firedantic-extras/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/altissimo-hq/firedantic-extras/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/altissimo-hq/firedantic-extras/compare/5340a71...v0.2.0
