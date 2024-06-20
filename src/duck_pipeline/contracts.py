from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class PipelineError(Exception):
    pass


class ConnectionBusyError(PipelineError):
    pass


class SchemaMismatchError(PipelineError):
    pass


class ExtractionFailedError(PipelineError):
    pass


class IsolationLevel(str, Enum):
    READ_UNCOMMITTED = "read_uncommitted"
    SERIALIZABLE = "serializable"


@dataclass(frozen=True)
class TableRef:
    schema: str = "main"
    table: str = ""

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.table}" if self.schema != "main" else self.table


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    duck_type: str

    def ddl_fragment(self) -> str:
        safe_name = self.name.replace('"', '""')
        return f'"{safe_name}" {self.duck_type}'


@dataclass(frozen=True)
class WriteResult:
    rows_written: int
    table: str
    duration_ms: float


@dataclass(frozen=True)
class QueryStats:
    query: str
    row_count: int
    duration_ms: float


@dataclass(frozen=True)
class PipelineReport:
    steps: tuple[str, ...] = field(default_factory=tuple)
    total_rows_moved: int = 0
    total_duration_ms: float = 0.0

    def extend(self, step: str, rows: int, duration_ms: float) -> "PipelineReport":
        return PipelineReport(
            steps=self.steps + (step,),
            total_rows_moved=self.total_rows_moved + rows,
            total_duration_ms=self.total_duration_ms + duration_ms,
        )
