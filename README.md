# duckdb-pipeline

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Single-process DuckDB ETL: serialized writes, typed table writers, streaming extraction, window analytics, and pipeline reporting — the safe pattern for embedded DuckDB.

## 🚀 Overview

DuckDB is blazingly fast but single-writer: two threads touching one connection is a corruption recipe. `duckdb-pipeline` wraps that constraint into an API — `SerializedConnection` guards every statement with a lock, `TableWriter` builds DDL from typed `ColumnSpec`s and inserts dicts in batches, `Extractor` streams results in chunks for huge result sets, and `AnalyticsRunner` times queries and runs rank-window aggregates out of the box.

## ✨ Features

- **Serialized writes:** threading.Lock around every statement; `try_acquire`/`release` for cooperative scheduling
- **Typed schema:** `ColumnSpec("amount", "DOUBLE")` → generated `CREATE TABLE IF NOT EXISTS` with quoted identifiers
- **Dict insertion:** batched `INSERT` from plain Python dicts; missing columns rejected before any write
- **Streaming reads:** `stream(sql, chunk_rows)` yields fixed-size chunks without materializing everything
- **Window analytics:** partition/rank aggregates in one call
- **Pipeline reports:** compose write steps; get rows-moved + duration totals
- **Context-manager lifecycle:** `with DuckPipeline() as dp:` closes cleanly
- **Zero hard dependencies** (duckdb optional at install, required at runtime)

## 🚧 Structure

```
duckdb-pipeline/
├── src/duck_pipeline/
│   ├── __init__.py
│   ├── contracts.py
│   └── core.py
├── tests/
│   └── test_core.py
├── README.md
└── pyproject.toml
```

## 📦 Installation

```bash
git clone https://github.com/supremeloki/duckdb-pipeline.git
cd duckdb-pipeline
python -m venv .venv
.venv\Scripts\activate
pip install -e ".[dev,engine]"
```

## 📋 Requirements

- Python 3.11+
- Runtime: `duckdb >= 0.9`
- Dev: pytest

## 🏃 Quick Start

```python
from duck_pipeline import ColumnSpec, DuckPipeline, TableRef

with DuckPipeline("warehouse.duckdb") as dp:
    writer = dp.writer(
        TableRef(table="sales"),
        [ColumnSpec("region", "VARCHAR"), ColumnSpec("amount", "DOUBLE")],
    )
    writer.insert_dicts([
        {"region": "north", "amount": 300.0},
        {"region": "south", "amount": 50.5},
    ])

    ranked = dp.analytics.window_aggregate("sales", partition_by="region", metric="amount")
    total = dp.extractor.count("sales")
```

### Streaming large results

```python
for chunk in dp.connection.stream("SELECT * FROM big_table", chunk_rows=50_000):
    process(chunk)
```

## 🔧 Error Handling

```text
PipelineError
├── ConnectionBusyError     # lock contention on the single connection
├── SchemaMismatchError     # reserved for strict schema checks
└── ExtractionFailedError   # query execution failure
```

## 🧪 Testing

```bash
pytest tests/ -v
```

## 📝 Code Quality

- Full type hints (`X | None` style), frozen contracts
- Zero comments — names carry the meaning
- `ruff` clean

## 📄 License

MIT — see [LICENSE](LICENSE).

## 👤 Author

**Kooroush Masoumi**

---

⭐ Star this repo if you find it useful!
