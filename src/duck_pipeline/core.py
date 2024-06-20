from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator, Sequence

import duckdb

from .contracts import (
    ColumnSpec,
    ConnectionBusyError,
    ExtractionFailedError,
    IsolationLevel,
    PipelineError,
    QueryStats,
    TableRef,
    WriteResult,
)


class SerializedConnection:
    def __init__(self, database: str | Path = ":memory:") -> None:
        self._connection = duckdb.connect(str(database))
        self._lock = threading.Lock()

    def __enter__(self) -> "SerializedConnection":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    @property
    def raw(self) -> duckdb.DuckDBPyConnection:
        return self._connection

    def try_acquire(self) -> bool:
        return self._lock.acquire(blocking=False)

    def release(self) -> None:
        if self._lock.locked():
            self._lock.release()

    def execute_write(self, sql: str, params: Sequence[Any] = ()) -> int:
        with self._lock:
            self._connection.execute(sql, list(params))
            return 0

    def execute_query(self, sql: str, params: Sequence[Any] = ()) -> list[tuple]:
        with self._lock:
            cursor = self._connection.execute(sql, list(params))
            return cursor.fetchall() if cursor.description else []

    def stream(self, sql: str, chunk_rows: int = 10_000) -> Iterator[list[tuple]]:
        with self._lock:
            cursor = self._connection.execute(sql)
            if not cursor.description:
                return
            while True:
                chunk = cursor.fetchmany(chunk_rows)
                if not chunk:
                    break
                yield chunk

    def close(self) -> None:
        with self._lock:
            self._connection.close()


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


class TableWriter:
    def __init__(self, connection: SerializedConnection, table: TableRef,
                 columns: Sequence[ColumnSpec]) -> None:
        self._connection = connection
        self._table = table
        self._columns = tuple(columns)

    @property
    def create_sql(self) -> str:
        column_defs = ", ".join(col.ddl_fragment() for col in self._columns)
        return f"CREATE TABLE IF NOT EXISTS {_quote_identifier(self._table.qualified)} ({column_defs})"

    def ensure_table(self) -> None:
        self._connection.execute_write(self.create_sql)

    def insert_dicts(self, rows: list[dict[str, Any]], batch_size: int = 5000) -> WriteResult:
        if not rows:
            return WriteResult(rows_written=0, table=self._table.qualified, duration_ms=0.0)
        missing = {col.name for col in self._columns} - set(rows[0])
        if missing:
            raise PipelineError(f"rows missing columns: {sorted(missing)}")
        started = time.perf_counter()
        self.ensure_table()
        names = [col.name for col in self._columns]
        placeholders = ", ".join("?" for _ in names)
        quoted = ", ".join(_quote_identifier(n) for n in names)
        insert_sql = (
            f"INSERT INTO {_quote_identifier(self._table.qualified)} ({quoted}) "
            f"VALUES ({placeholders})"
        )
        written = 0
        for start in range(0, len(rows), batch_size):
            batch = rows[start:start + batch_size]
            values = [tuple(row[name] for name in names) for row in batch]
            for row_values in values:
                self._connection.execute_write(insert_sql, row_values)
            written += len(batch)
        duration = (time.perf_counter() - started) * 1000
        return WriteResult(rows_written=written, table=self._table.qualified,
                           duration_ms=duration)

    def register_dataframe(self, df: Any, view_name: str) -> None:
        self._connection.raw.register(view_name, df)


class Extractor:
    def __init__(self, connection: SerializedConnection) -> None:
        self._connection = connection

    def fetch_all(self, sql: str, params: Sequence[Any] = ()) -> list[tuple]:
        try:
            return self._connection.execute_query(sql, params)
        except duckdb.Error as exc:
            raise ExtractionFailedError(f"query failed: {exc}") from exc

    def to_dicts(self, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        with self._connection._lock:
            cursor = self._connection.raw.execute(sql, list(params))
            columns = [d[0] for d in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def count(self, table: str) -> int:
        exists = self._connection.execute_query(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [table]
        )
        if not exists[0][0]:
            return 0
        result = self._connection.execute_query(f"SELECT COUNT(*) FROM {_quote_identifier(table)}")
        return int(result[0][0])


class AnalyticsRunner:
    def __init__(self, connection: SerializedConnection) -> None:
        self._connection = connection

    def timed(self, sql: str, params: Sequence[Any] = ()) -> QueryStats:
        started = time.perf_counter()
        rows = self._connection.execute_query(sql, params)
        duration = (time.perf_counter() - started) * 1000
        return QueryStats(query=sql, row_count=len(rows), duration_ms=duration)

    def window_aggregate(self, table: str, partition_by: str, metric: str) -> list[tuple]:
        sql = f"""
            SELECT {partition_by},
                   SUM({metric}) AS total,
                   ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY SUM({metric}) DESC) AS rank
            FROM {_quote_identifier(table)}
            GROUP BY {partition_by}
            ORDER BY rank
        """
        return self._connection.execute_query(sql)


class DuckPipeline:
    def __init__(self, database: str | Path = ":memory:",
                 isolation: IsolationLevel = IsolationLevel.SERIALIZABLE) -> None:
        self.connection = SerializedConnection(database)
        self.extractor = Extractor(self.connection)
        self.analytics = AnalyticsRunner(self.connection)

    def writer(self, table: TableRef, columns: Sequence[ColumnSpec]) -> TableWriter:
        return TableWriter(self.connection, table, columns)

    def run_steps(self, steps: Sequence[Callable[[], WriteResult]]) -> Any:
        from .contracts import PipelineReport
        report = PipelineReport()
        for step in steps:
            result = step()
            report = report.extend(result.table, result.rows_written, result.duration_ms)
        return report

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "DuckPipeline":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
