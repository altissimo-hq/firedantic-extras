# Firedantic Extras

Add-on utilities for [Firedantic](https://github.com/altissimo-hq/firedantic) — the async-native Pydantic + Firestore ODM.

[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org)

## Overview

Firedantic Extras is a companion library that provides higher-level utilities
built on top of Firedantic models. Each module solves a specific, recurring
problem that arises when using Firedantic in production:

| Module                   | Purpose                                                       |
| ------------------------ | ------------------------------------------------------------- |
| **`update_collection`**  | Batch-sync a list of models to a Firestore collection         |
| **`fastapi.pagination`** | Cursor-based pagination for Firedantic queries in FastAPI     |
| **`bigquery.schema`**    | Generate BigQuery table schemas from Firedantic model classes |

## Installation

```bash
# Core (includes update_collection)
pip install firedantic-extras

# With FastAPI pagination support
pip install firedantic-extras[fastapi]

# With BigQuery schema generation
pip install firedantic-extras[bigquery]

# Everything
pip install firedantic-extras[all]
```

---

## `update_collection` — Collection Sync

Synchronize a Firestore collection to match a given list of Firedantic models.
Documents are added, updated, or deleted as needed, using batched writes that
respect Firestore's 500-document batch limit.

### Why?

Manually diffing existing documents against a desired state is tedious and
error-prone. `CollectionSync` handles the full add/update/delete lifecycle
in a single class, with support for dry-run mode, field-level diffing, and
configurable sync keys.

The comparison is done against **raw Firestore data** (not re-hydrated
models), so stale or extra fields stored in Firestore are visible and will
trigger updates — ensuring Firestore always converges to the exact shape
described by the model.

### Quick Start

```python
from firedantic import Model
from firedantic_extras import CollectionSync

class User(Model):
    __collection__ = "users"
    name: str
    email: str
    active: bool = True

desired = [
    User(id="u1", name="Alice", email="alice@example.com"),
    User(id="u2", name="Bob",   email="bob@example.com"),
    User(id="u3", name="Carol", email="carol@example.com"),
]

# Additive sync (default) — adds new docs, updates changed docs,
# but does NOT delete docs missing from the list.
result = CollectionSync.sync(User, desired)
print(result.summary())
# SyncResult(adds=3, updates=0, deletes=0, skips=0)

# Full sync — also deletes docs not in the desired list.
result = CollectionSync.sync(User, desired, delete_items=True)

# Dry run with field-level diff output — preview changes without writing.
result = CollectionSync.sync(
    User, desired, delete_items=True, diff=True, dry_run=True,
)
print(result.summary())
# SyncResult(adds=0, updates=1, deletes=2, skips=0, DRY RUN)

# Inspect field-level diffs for updated documents.
for key, doc_diff in result.diffs.items():
    for change in doc_diff.changes:
        print(f"  {change.field}: {change.before!r} → {change.after!r}")
```

### API Reference

#### `CollectionSync`

```python
class CollectionSync:
    """Reconcile a Firestore collection to match a desired list of models."""

    def __init__(
        self,
        model: type[BareModel],
        items: Sequence[BareModel],
        *,
        delete_items: bool = False,
        dry_run: bool = False,
        diff: bool = False,
        output_writer: Callable[[str], None] | None = print,
        sync_key: str | None = None,
        on_duplicate_keys: OnDuplicateKeys = "raise",
        on_error: OnError = "raise",
        chunk_size: int = 500,
    ) -> None: ...

    def run(self) -> SyncResult:
        """Execute the sync and return a SyncResult."""

    @classmethod
    def sync(
        cls,
        model: type[BareModel],
        items: Sequence[BareModel],
        **kwargs,
    ) -> SyncResult:
        """Convenience class method — construct and run in one call."""
```

> **Note:** `UpdateCollection` is available as a backward-compatible alias
> for `CollectionSync`.

#### Parameters

<!-- markdownlint-disable MD033 -->

| Parameter           | Default      | Description                                                                                                               |
| ------------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------- |
| `model`             | _(required)_ | The Firedantic model class for the target collection                                                                      |
| `items`             | _(required)_ | Desired state — every model that should exist after sync                                                                  |
| `delete_items`      | `False`      | If `True`, documents not in `items` are deleted. <br>**Safe default prevents accidental data loss.**                      |
| `dry_run`           | `False`      | If `True`, logs planned changes without writing                                                                           |
| `diff`              | `False`      | If `True`, collects field-level diffs for updates                                                                         |
| `sync_key`          | `None`       | Field to match incoming items to existing docs. <br>`None` uses the document ID; set to e.g. `"email"` for non-ID matches |
| `on_duplicate_keys` | `"raise"`    | What to do when `sync_key` matches >1 doc: `"raise"`, `"skip"`, or `"update_all"`                                         |
| `on_error`          | `"raise"`    | Per-document error strategy: `"raise"`, `"collect"`, or `"skip"`                                                          |
| `chunk_size`        | `500`        | Max operations per Firestore batch write (capped at 500)                                                                  |
| `output_writer`     | `print`      | Callable for progress output; pass `None` to suppress                                                                     |

<!-- markdownlint-enable MD033 -->

#### `SyncResult`

```python
@dataclass
class SyncResult:
    adds: int = 0
    updates: int = 0
    deletes: int = 0
    skips: int = 0
    diffs: dict[str, DocumentDiff]   # populated when diff=True
    errors: list[SyncError]          # populated when on_error != "raise"
    dry_run: bool = False

    @property
    def has_errors(self) -> bool: ...

    @property
    def total_changes(self) -> int:
        """adds + updates + deletes (excludes skips)."""

    def summary(self) -> str:
        """One-liner: 'SyncResult(adds=1, updates=2, ...)'"""
```

#### Supporting types

```python
@dataclass
class FieldDiff:
    """A single field-level change."""
    field: str
    before: Any   # value in Firestore (_MISSING if absent)
    after: Any    # value in desired model (_MISSING if absent)

@dataclass
class DocumentDiff:
    """All field-level changes for one document."""
    doc_id: str
    sync_key_value: str
    changes: list[FieldDiff]

@dataclass
class SyncError:
    """An error for one document (when on_error != 'raise')."""
    sync_key_value: str
    error: Exception
```

#### `build_sync_plan` (advanced)

The pure-function core of `CollectionSync`, exposed for testing and
inspection without any Firestore I/O:

```python
def build_sync_plan(
    desired: dict[str, BareModel],
    existing_models: dict[str, BareModel],
    existing_raw: dict[str, dict[str, Any]],
    doc_id_field: str,
    delete_items: bool = False,
    diff: bool = False,
) -> _SyncPlan:
    """Compute adds/updates/deletes/skips from pure data — no Firestore calls."""
```

---

## `fastapi.pagination` — Cursor-Based Pagination

Integrates Firedantic queries with
[fastapi-pagination](https://github.com/uriyyo/fastapi-pagination) to provide
Firestore-native cursor-based pagination for API endpoints.

### Why?

Firestore does not support offset-based pagination efficiently — it still reads
and charges for all skipped documents. Cursor-based pagination using
`start_after` is the idiomatic approach, and this module bridges the gap
between Firedantic's query interface and FastAPI's pagination framework.

### Quick Start

```python
from fastapi import Depends, FastAPI
from fastapi_pagination.cursor import CursorPage, CursorParams

from firedantic_extras.fastapi import paginate

app = FastAPI()

class Product(Model):
    __collection__ = "products"
    name: str
    price: float
    category: str
    created_at: datetime


@app.get("/products", response_model=CursorPage[Product])
def list_products(
    params: CursorParams = Depends(),
    category: str | None = None,
):
    filter_dict = {"category": category} if category else None
    return paginate(Product, params, filter_dict=filter_dict, sort_field="created_at")
```

**Request:**

```http
GET /products?size=10
GET /products?size=10&cursor=eyJpZCI6ICJhYmMxMjMiLCAidmFsIjogIi4uLiIsICJkaXIiOiAibmV4dCJ9
```

**Response:**

```json
{
  "items": [ ... ],
  "next_cursor": "eyJpZCI6ICJ4eXo3ODkiLCAidmFsIjogIi4uLiIsICJkaXIiOiAibmV4dCJ9",
  "previous_cursor": null
}
```

### API

```python
def paginate(
    model_cls: type[Model],
    params: CursorParams,
    filter_dict: dict[str, Any] | None = None,
    sort_field: str = "created_at",
) -> CursorPage[Model]:
    """Paginate a Firedantic model using Firestore cursors.

    Uses fastapi-pagination's CursorPage/CursorParams for seamless
    integration with FastAPI endpoints.

    Args:
        model_cls: The Firedantic model class to query.
        params: CursorParams from fastapi-pagination (size, cursor).
        filter_dict: Optional equality filters applied as .where() clauses.
        sort_field: Field to order results by (must have a Firestore index).

    Returns:
        A CursorPage containing items and navigation cursors.
    """
```

```python
def encode_cursor(id_: str, val: Any, direction: str) -> str:
    """Encode cursor data (doc id, sort value, direction) to base64."""

def decode_cursor(cursor: str) -> dict[str, Any]:
    """Decode a base64 cursor. Raises HTTP 400 on invalid format."""
```

**Navigation:** The cursor encodes the document ID, sort field value, and
direction (`"next"` or `"prev"`). Bidirectional navigation is supported —
the response includes both `next_cursor` and `previous_cursor` when
applicable.

---

## `bigquery.schema` — Schema Generation

Automatically generates BigQuery table schemas from Firedantic model classes,
mapping Pydantic field types to their BigQuery equivalents.

### Why?

When you maintain Firedantic models as your source of truth for Firestore
documents and also need to export that data to BigQuery, keeping schemas in
sync manually is fragile. This module derives the BigQuery schema directly from
your model definitions.

### Quick Start

```python
from firedantic_extras.bigquery import model_to_bq_schema, models_to_bq_schemas

class Sample(Model):
    __collection__ = "samples"
    sample_id: str
    barcode: str
    collected_at: datetime
    results: dict[str, float]
    tags: list[str]

# Single model
schema = model_to_bq_schema(Sample)
# [
#     SchemaField("sample_id", "STRING", mode="REQUIRED"),
#     SchemaField("barcode", "STRING", mode="REQUIRED"),
#     SchemaField("collected_at", "TIMESTAMP", mode="REQUIRED"),
#     SchemaField("results", "RECORD", mode="REQUIRED", fields=[...]),
#     SchemaField("tags", "STRING", mode="REPEATED"),
# ]

# Multiple models → dict keyed by collection name
schemas = models_to_bq_schemas([Sample, City])
# {"samples": [...], "cities": [...]}
```

### Type Mapping

| Python / Pydantic Type      | BigQuery Type            |
| --------------------------- | ------------------------ |
| `str`                       | `STRING`                 |
| `int`                       | `INTEGER`                |
| `float`                     | `FLOAT`                  |
| `bool`                      | `BOOLEAN`                |
| `datetime`                  | `TIMESTAMP`              |
| `date`                      | `DATE`                   |
| `time`                      | `TIME`                   |
| `bytes`                     | `BYTES`                  |
| `Decimal`                   | `NUMERIC`                |
| `dict` / nested `BaseModel` | `RECORD`                 |
| `list[T]`                   | `T` with mode `REPEATED` |
| `Optional[T]`               | `T` with mode `NULLABLE` |

### API

```python
def model_to_bq_schema(
    model: type[Model],
    *,
    exclude_fields: set[str] | None = None,
    extra_fields: list[SchemaField] | None = None,
) -> list[SchemaField]:
    """Generate a BigQuery schema from a Firedantic model.

    Args:
        model: The Firedantic model class to convert.
        exclude_fields: Field names to omit from the schema.
        extra_fields: Additional SchemaFields to append (e.g., metadata
            columns not in the model).

    Returns:
        A list of google.cloud.bigquery.SchemaField objects.
    """


def models_to_bq_schemas(
    models: list[type[Model]],
    **kwargs,
) -> dict[str, list[SchemaField]]:
    """Generate schemas for multiple models, keyed by collection name.

    Args:
        models: List of Firedantic model classes.
        **kwargs: Passed through to model_to_bq_schema.

    Returns:
        Dict mapping __collection__ names to their BigQuery schemas.
    """
```

---

## Development

```bash
# Clone and install
git clone https://github.com/altissimo-hq/firedantic-extras.git
cd firedantic-extras
poetry install --with dev --all-extras

# Run unit tests (default — no emulator needed)
poetry run pytest

# Lint and format
poetry run ruff check --fix .
poetry run ruff format .

# Pre-commit hooks (installed automatically)
poetry run pre-commit run --all-files
```

### Integration Tests (Firestore Emulator)

Integration tests exercise the full Firestore round-trip and require the
[Firebase Emulator Suite](https://firebase.google.com/docs/emulator-suite).

```bash
# Prerequisites: Firebase CLI (https://firebase.google.com/docs/cli)
npm install -g firebase-tools

# Terminal 1 — start the emulator (Firestore on port 8686)
./scripts/start_emulator.sh

# Terminal 2 — run integration tests only
FIRESTORE_EMULATOR_HOST=127.0.0.1:8686 poetry run pytest -m integration -v

# Or run everything (unit + integration)
FIRESTORE_EMULATOR_HOST=127.0.0.1:8686 poetry run pytest -m "" -v
```

The default `pytest` command excludes integration tests via `addopts` in
`pyproject.toml`, so `poetry run pytest` (and pre-commit) always runs
fast, emulator-free unit tests.

## License

BSD 3-Clause. See [LICENSE](LICENSE) for details.
