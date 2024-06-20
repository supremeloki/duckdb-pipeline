import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pytest

from duck_pipeline import (
    ColumnSpec,
    ConnectionBusyError,
    DuckPipeline,
    PipelineError,
    TableRef,
)


@pytest.fixture
def pipeline():
    with DuckPipeline() as dp:
        yield dp


SALES_COLUMNS = [
    ColumnSpec("region", "VARCHAR"),
    ColumnSpec("amount", "DOUBLE"),
]


def test_insert_and_count(pipeline):
    writer = pipeline.writer(TableRef(table="sales"), SALES_COLUMNS)
    result = writer.insert_dicts([
        {"region": "north", "amount": 120.0},
        {"region": "south", "amount": 80.5},
    ])
    assert result.rows_written == 2
    assert pipeline.extractor.count("sales") == 2


def test_empty_insert_is_noop(pipeline):
    writer = pipeline.writer(TableRef(table="empty_t"), SALES_COLUMNS)
    result = writer.insert_dicts([])
    assert result.rows_written == 0
    assert not pipeline.extractor.count("empty_t")


def test_missing_column_rejected(pipeline):
    writer = pipeline.writer(TableRef(table="strict"), SALES_COLUMNS)
    with pytest.raises(PipelineError, match="missing columns"):
        writer.insert_dicts([{"region": "x"}])


def test_window_aggregate_ranks_regions(pipeline):
    writer = pipeline.writer(TableRef(table="sales"), SALES_COLUMNS)
    writer.insert_dicts([
        {"region": "north", "amount": 300.0},
        {"region": "north", "amount": 100.0},
        {"region": "south", "amount": 50.0},
    ])
    rows = pipeline.analytics.window_aggregate("sales", "region", "amount")
    by_region = {row[0]: row[1] for row in rows}
    assert by_region["north"] == 400.0
    assert by_region["south"] == 50.0
    north_rank = [row[2] for row in rows if row[0] == "north"][0]
    assert north_rank == 1


def test_timed_query_reports_stats(pipeline):
    stats = pipeline.analytics.timed("SELECT 42 AS answer")
    assert stats.row_count == 1
    assert stats.duration_ms >= 0


def test_streaming_chunks(pipeline):
    writer = pipeline.writer(TableRef(table="nums"), [ColumnSpec("n", "INTEGER")])
    writer.insert_dicts([{"n": i} for i in range(25)])
    chunks = list(pipeline.connection.stream("SELECT n FROM nums", chunk_rows=10))
    assert [len(c) for c in chunks] == [10, 10, 5]


def test_pipeline_report_accumulates(pipeline):
    writer = pipeline.writer(TableRef(table="t"), SALES_COLUMNS)

    def step_one():
        return writer.insert_dicts([{"region": "a", "amount": 1.0}])

    def step_two():
        return writer.insert_dicts([{"region": "b", "amount": 2.0}] * 3)

    report = pipeline.run_steps([step_one, step_two])
    assert len(report.steps) == 2
    assert report.total_rows_moved == 4


def test_concurrent_write_lock_enforced(pipeline):
    writer = pipeline.writer(TableRef(table="locked"), SALES_COLUMNS)
    writer.ensure_table()
    acquired = pipeline.connection.try_acquire()
    assert acquired is True
    try:
        second_attempt = pipeline.connection.try_acquire()
        assert second_attempt is False
    finally:
        pipeline.connection.release()
