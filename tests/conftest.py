"""Shared fixtures: small, reproducible Meta DWH databases built in a temporary folder."""

from datetime import date
from pathlib import Path

import duckdb
import pytest

from meta_dwh import pipeline
from meta_dwh.generate_events import generate_events, write_raw_events

# One week around the team change of kenji.arakawa (2026-11-04) in the HR seed.
TEST_SEED = 7
TEST_START = date(2026, 11, 1)
TEST_DAYS = 7
TEST_EVENTS_PER_DAY = 60


def build_empty_dwh(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Create all tables and views and load the HR seed, with no events yet."""
    con = pipeline.connect(db_path)
    pipeline.run_sql_folder(con, pipeline.SQL_DIR / "ddl")
    pipeline.load_team_assignments(con, pipeline.SEEDS_DIR / "team_assignments.csv")
    return con


def run_transforms(con: duckdb.DuckDBPyConnection) -> None:
    pipeline.run_sql_folder(con, pipeline.SQL_DIR / "transform")


@pytest.fixture(scope="session")
def events() -> list[dict]:
    return generate_events(TEST_SEED, TEST_START, TEST_DAYS, TEST_EVENTS_PER_DAY)


@pytest.fixture
def loaded_dwh(tmp_path, events) -> duckdb.DuckDBPyConnection:
    """A DWH where all test events were loaded and transformed in one run."""
    con = build_empty_dwh(tmp_path / "dwh.duckdb")
    write_raw_events(con, events)
    run_transforms(con)
    yield con
    con.close()


def scalar(con: duckdb.DuckDBPyConnection, sql: str):
    """Return the single value of a one-row, one-column query."""
    return con.sql(sql).fetchall()[0][0]