# Firedantic Extras

Add-on utilities for [Firedantic](https://github.com/altissimo-hq/firedantic) — the async-native Pydantic + Firestore ODM.

[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org)

## Overview

Firedantic Extras is a companion library that provides higher-level utilities
built on top of Firedantic models. Each module solves a specific, recurring
problem that arises when using Firedantic in production:

| Module                   | Purpose                                                               |
| ------------------------ | --------------------------------------------------------------------- |
| **`update_collection`**  | Batch-sync a list of models to a Firestore collection                 |
| **`cursor_pagination`**  | Framework-agnostic cursor-based pagination for Firedantic models      |
| **`query`**              | `count_model()` aggregation and `build_prefix_filters()` range search |
| **`fastapi.pagination`** | FastAPI adapter (`PaginationParams`) for `cursor_paginate`            |
| **`bigquery.schema`**    | Generate BigQuery table schemas from Firedantic model classes         |

---

## `cursor_pagination` — Cursor-Based Pagination

Efficient, stable, bidirectional cursor pagination for any Firedantic model.
Works independently of any web framework — use it from Flask, FastAPI,
background workers, or anywhere else.

### Why?

`Model.find()` returns every document in the collection. For large collections
this becomes slow and expensive. `cursor_paginate()` fetches only one page at a
time using Firestore's `start_after` cursor, so response time stays constant no
matter how many documents exist.

Key design decisions:

- **Document ID as cursor** — stable, type-safe, no field serialization needed.
- **`__name__` tiebreaker** — a final `order_by("__name__")` prevents silent
  deduplication at page boundaries when the primary sort field has duplicates.
- **Reversed sort for `prev`** — backward pagination reverses all sort
  directions and uses `start_after` (instead of `end_before + limit_to_last`),
  which is fully supported by both the Firestore SDK and the emulator.
- **Sentinel row** — requests `limit + 1` rows to determine `has_next` /
  `has_prev` without an extra `COUNT` query.

### Quick Start

```python
from firedantic import Model
from firedantic_extras import cursor_paginate

class Product(Model):
    __collection__ = "products"
    name: str
    price: float
    category: str

# --- Page 1 (forward, no cursor) ---
page = cursor_paginate(Product, limit=20, order_by="name")
for p in page.items:
    print(p.name)

print(page.has_next)    # True  — more items exist
print(page.has_prev)    # False — we're on the first page

# --- Page 2 (next) ---
page2 = cursor_paginate(
    Product,
    limit=20,
    order_by="name",
    cursor=page.next_cursor,
    direction="next",
)

# --- Back to Page 1 (prev) ---
page1_again = cursor_paginate(
    Product,
    limit=20,
    order_by="name",
    cursor=page2.prev_cursor,
    direction="prev",
)

# --- Last page (no cursor, going backward) ---
last_page = cursor_paginate(Product, limit=20, order_by="name", direction="prev")
print(last_page.has_next)  # False — nothing after this
print(last_page.has_prev)  # True  — earlier pages exist
```

### Filtering

Pass a `filter_` dict using the same format as `Model.find()`:

```python
# Equality filter
page = cursor_paginate(
    Product,
    limit=20,
    order_by="name",
    filter_={"category": "electronics"},
)

# Comparison operators
page = cursor_paginate(
    Product,
    limit=20,
    order_by=[("price", "ASCENDING"), ("name", "ASCENDING")],
    filter_={"price": {">=": 10.0, "<": 100.0}},
)
```

### Compound Sort

Pass a list of `(field, direction)` tuples for multi-field ordering:

```python
from google.cloud.firestore_v1 import ASCENDING, DESCENDING

page = cursor_paginate(
    Product,
    limit=20,
    order_by=[("category", ASCENDING), ("price", DESCENDING)],
)
```

### Include Total Count

```python
page = cursor_paginate(Product, limit=20, order_by="name", include_total=True)
print(page.total)  # e.g. 4231 — one extra server-side COUNT aggregation
```

### API Reference

```python
def cursor_paginate(
    model_class: type[BareModel],
    *,
    limit: int,
    order_by: str | list[str | tuple[str, str]] | None = None,
    cursor: str | None = None,
    direction: Literal["next", "prev"] = "next",
    filter_: FilterDict | None = None,
    include_total: bool = False,
) -> CursorPage[BareModel]:
    ...
```

| Parameter       | Default  | Description                                                                                     |
| --------------- | -------- | ----------------------------------------------------------------------------------------------- |
| `model_class`   | _(req.)_ | The Firedantic model class to query                                                             |
| `limit`         | _(req.)_ | Number of items per page (≥ 1)                                                                  |
| `order_by`      | `None`   | Field name, or list of `(field, direction)` tuples. A `__name__` tiebreaker is always appended. |
| `cursor`        | `None`   | Document ID from a previous page's `next_cursor` or `prev_cursor`                               |
| `direction`     | `"next"` | `"next"` to go forward, `"prev"` to go backward                                                 |
| `filter_`       | `None`   | Equality / comparison filters in Firedantic's `find()` format                                   |
| `include_total` | `False`  | If `True`, runs an extra server-side `COUNT` aggregation and populates `CursorPage.total`       |

```python
@dataclass
class CursorPage(Generic[ModelT]):
    items: list[ModelT]       # hydrated model instances for this page
    has_next: bool            # True if a next page exists (going forward)
    has_prev: bool            # True if a previous page exists (going backward)
    next_cursor: str | None   # pass as cursor + direction="next" to advance
    prev_cursor: str | None   # pass as cursor + direction="prev" to go back
    total: int | None         # total doc count, only set when include_total=True
```

---

## `query` — Count and Prefix Search

### `count_model` — Server-Side Aggregation

Get the number of documents matching a filter without fetching any data:

```python
from firedantic_extras import count_model

# Count all documents in a collection
total = count_model(Product)

# Count with a filter
electronics_count = count_model(Product, filter_={"category": "electronics"})
```

Uses Firestore's native `COUNT` aggregation — no documents are transferred.

```python
def count_model(
    model_class: type[BareModel],
    *,
    filter_: FilterDict | None = None,
) -> int:
    ...
```

### `build_prefix_filters` — Prefix-Range Search

Generate a pair of Firedantic-compatible filters that implement a prefix search
using Firestore's range query pattern:

```python
from firedantic_extras import build_prefix_filters, cursor_paginate

# Find all products whose name starts with "lap"
filters = build_prefix_filters("name", "lap")
# Returns: {"name": {">=": "lap", "<": "lap\uf8ff"}}

page = cursor_paginate(
    Product,
    limit=20,
    order_by="name",
    filter_=filters,
)
```

The upper bound uses the Unicode sentinel `\uf8ff` (the highest character in
the Basic Multilingual Plane), so any string that starts with the prefix sorts
before it.

```python
def build_prefix_filters(field: str, prefix: str) -> FilterDict:
    ...
```

---

## `fastapi.pagination` — FastAPI Adapter

`PaginationParams` is a FastAPI dependency that extracts `cursor`, `direction`,
and `limit` from query-string parameters, ready to pass straight to
`cursor_paginate`.

### Quick Start

```python
from fastapi import Depends, FastAPI
from firedantic_extras import cursor_paginate, CursorPage
from firedantic_extras.fastapi.pagination import PaginationParams

app = FastAPI()

class Product(Model):
    __collection__ = "products"
    name: str
    price: float
    category: str

@app.get("/products")
def list_products(
    pagination: PaginationParams = Depends(),
    category: str | None = None,
) -> CursorPage[Product]:
    filter_ = {"category": category} if category else None
    return cursor_paginate(
        Product,
        limit=pagination.limit,
        order_by="name",
        cursor=pagination.cursor,
        direction=pagination.direction,
        filter_=filter_,
    )
```

**Request examples:**

```http
GET /products?limit=20
GET /products?limit=20&cursor=<next_cursor>&direction=next
GET /products?limit=20&cursor=<prev_cursor>&direction=prev
```

**Response:**

```json
{
  "items": [ ... ],
  "has_next": true,
  "has_prev": false,
  "next_cursor": "abc123",
  "prev_cursor": null,
  "total": null
}
```

### API

```python
class PaginationParams:
    def __init__(
        self,
        cursor: str | None = Query(default=None),
        direction: Literal["next", "prev"] = Query(default="next"),
        limit: int = Query(default=20, ge=1, le=500),
    ) -> None: ...
```

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

## `bigquery.schema` — Schema Generation

Automatically generates BigQuery table schemas from Firedantic model classes,
mapping Pydantic field types to their BigQuery equivalents.

### Why?

When you maintain Firedantic models as your source of truth for Firestore
documents and also need to export that data to BigQuery, keeping schemas in
sync manually is fragile. This module derives the BigQuery schema directly from
your model definitions — including `REQUIRED`/`NULLABLE` mode based on whether
Pydantic fields are required or optional, and `REPEATED` mode for list fields.

### Quick Start

```python
from firedantic_extras.bigquery import model_to_bq_schema, schema_to_dict

class Sample(Model):
    __collection__ = "samples"
    sample_id: str          # required → REQUIRED
    barcode: str
    collected_at: datetime
    results: dict[str, float]   # dict → JSON
    tags: list[str]             # list[str] → REPEATED STRING
    notes: str | None = None    # optional → NULLABLE

schema = model_to_bq_schema(Sample)
# [
#   SchemaField("id",           "STRING",    mode="NULLABLE"),   ← always first
#   SchemaField("sample_id",    "STRING",    mode="REQUIRED"),
#   SchemaField("barcode",      "STRING",    mode="REQUIRED"),
#   SchemaField("collected_at", "TIMESTAMP", mode="REQUIRED"),
#   SchemaField("results",      "JSON",      mode="NULLABLE"),
#   SchemaField("tags",         "STRING",    mode="REPEATED"),
#   SchemaField("notes",        "STRING",    mode="NULLABLE"),
# ]
```

### Type Mapping

| Python / Pydantic Type        | BigQuery Type     | Mode                |
| ----------------------------- | ----------------- | ------------------- |
| `str`, `Enum`, `Literal[...]` | `STRING`          | `REQUIRED/NULLABLE` |
| `int`                         | `INTEGER`         | `REQUIRED/NULLABLE` |
| `float`, `Decimal`            | `FLOAT`/`NUMERIC` | `REQUIRED/NULLABLE` |
| `bool`                        | `BOOLEAN`         | `REQUIRED/NULLABLE` |
| `datetime`                    | `TIMESTAMP`       | `REQUIRED/NULLABLE` |
| `date`                        | `DATE`            | `REQUIRED/NULLABLE` |
| `time`                        | `TIME`            | `REQUIRED/NULLABLE` |
| `bytes`                       | `BYTES`           | `REQUIRED/NULLABLE` |
| `dict` / `dict[str, X]`       | `JSON`            | `NULLABLE`          |
| `Any` / unknown               | `JSON`            | `NULLABLE`          |
| Nested `BaseModel`            | `RECORD`          | `REQUIRED/NULLABLE` |
| `list[scalar]`                | scalar type       | `REPEATED`          |
| `list[BaseModel]`             | `RECORD`          | `REPEATED`          |
| `list[dict]` / `list[Any]`    | `JSON`            | `NULLABLE`          |

**Mode rules:**

- Required Pydantic field (`field: T`) → `REQUIRED`
- `Optional[T]` / `T | None` / field with a default → `NULLABLE`
- `list[T]` → `REPEATED` (BQ does not support `REQUIRED` for repeated fields)
- `id` is always `STRING NULLABLE` (first field, regardless of model definition)

### Backward Compatibility — `json_fields`

When migrating from hand-written schemas where nested objects were stored as
JSON, use `json_fields` to keep specific fields as `JSON NULLABLE` regardless
of what the model says:

```python
# populations: list[Population] would normally → REPEATED RECORD
# but our existing BQ table has it as JSON — keep it for now
schema = model_to_bq_schema(Kit, json_fields={"populations", "acquired_from"})
```

This lets you migrate one table at a time without breaking existing queries.

### Full API

```python
def model_to_bq_schema(
    model_class: type[BaseModel],
    *,
    json_fields: set[str] | None = None,
    exclude_fields: set[str] | None = None,
    extra_fields: list[SchemaField] | None = None,
) -> list[SchemaField]:
    """Generate a BigQuery schema from a Firedantic / Pydantic model.

    Args:
        model_class: The Pydantic model class to introspect.
        json_fields: Field names to force to JSON NULLABLE (backward-compat).
        exclude_fields: Field names to omit from the schema entirely.
        extra_fields: Additional SchemaFields to append at the end
            (e.g., load-time metadata columns not in the model).
    """


def models_to_bq_schemas(
    model_classes: list[type[BaseModel]],
    **kwargs,
) -> dict[str, list[SchemaField]]:
    """Generate schemas for multiple models, keyed by __collection__ name.

    Args:
        model_classes: Firedantic model classes (must have __collection__).
        **kwargs: Forwarded to model_to_bq_schema.

    Returns:
        Dict mapping __collection__ names to their BigQuery schemas.
    """


def schema_to_dict(schema: list[SchemaField]) -> list[dict]:
    """Serialise a schema to a JSON-serialisable list of dicts.

    Output matches the BigQuery REST API representation and can be stored
    in a JSON file or round-tripped via Client.schema_from_json().
    """


def compare_schemas(
    a: list[SchemaField],
    b: list[SchemaField],
) -> SchemaDiff:
    """Diff two BigQuery schemas at the top level (field names and BQ types).

    Useful for verifying a model-derived schema against an existing live BQ
    table schema before cutting over from hand-written definitions.

    Returns a SchemaDiff with:
      .only_in_a       — fields in a but not b
      .only_in_b       — fields in b but not a
      .type_mismatches — [(field, type_in_a, type_in_b), ...]
      .is_equal        — True if schemas are identical
    """
```

### Migration Example for `json2bq`

```python
from firedantic_extras.bigquery.schema import model_to_bq_schema, compare_schemas

# Map (dataset, table) to (ModelClass, fields_to_keep_as_json)
MODEL_MAP = {
    ("darwinsark", "kits"):    (Kit,    {"populations", "acquired_from"}),
    ("darwinsark", "animals"): (Animal, {"consent", "breeds"}),
    # tables without a model fall back to BQ autodetect (old behaviour)
}

def create_schema(dataset_name, table_name):
    entry = MODEL_MAP.get((dataset_name, table_name))
    if entry is None:
        return None
    model_class, json_fields = entry
    return model_to_bq_schema(model_class, json_fields=json_fields)

# Verify new schema matches existing table before switching over:
existing = client.get_table("darwinsark.kits").schema
generated = create_schema("darwinsark", "kits")
diff = compare_schemas(existing, generated)
if not diff.is_equal:
    print("Fields only in live table:", diff.only_in_a)
    print("Fields only in model:     ", diff.only_in_b)
    print("Type mismatches:          ", diff.type_mismatches)
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
