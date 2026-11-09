"""End-to-end tests of the SQL pipeline on a small synthetic dataset."""

from datetime import timedelta

from conftest import build_empty_dwh, run_transforms, scalar
from meta_dwh.generate_events import write_raw_events

TABLES = ["fact_query", "fact_query_table_access", "agg_team_usage_daily", "agg_table_usage_daily"]


def snapshot(con) -> dict[str, list[tuple]]:
    """Every row of every loaded table, rounded and sorted, to compare two databases."""
    result = {}
    for table in TABLES:
        rows = con.sql(f"SELECT * FROM {table}").fetchall()
        result[table] = sorted(
            tuple(round(v, 6) if isinstance(v, float) else v for v in row) for row in rows
        )
    return result


def split_by_night(events: list[dict]) -> list[list[dict]]:
    """Group events by the night (02:00 UTC) after which they arrived, like a daily batch."""
    batches: dict = {}
    for row in events:
        night = (row["ingested_at"] - timedelta(hours=2)).date()
        batches.setdefault(night, []).append(row)
    return [batches[night] for night in sorted(batches)]


def test_staging_removes_duplicate_deliveries(loaded_dwh):
    raw = scalar(loaded_dwh, "SELECT count(*) FROM raw_query_events")
    distinct = scalar(loaded_dwh, "SELECT count(DISTINCT query_id) FROM raw_query_events")
    staged = scalar(loaded_dwh, "SELECT count(*) FROM stg_query_events")

    assert raw > distinct, "test data should contain duplicates"
    assert staged == distinct


def test_timestamps_are_stored_in_utc(loaded_dwh):
    shifted = scalar(loaded_dwh, """
        SELECT count(*) FROM raw_query_events
        WHERE CAST(payload->>'$.endTime' AS TIMESTAMPTZ) AT TIME ZONE 'UTC' <> query_end_time
    """)
    assert shifted == 0


def test_rerunning_transforms_changes_nothing(loaded_dwh):
    before = snapshot(loaded_dwh)
    run_transforms(loaded_dwh)
    assert snapshot(loaded_dwh) == before


def test_nightly_incremental_load_equals_full_load(tmp_path, events, loaded_dwh):
    incremental = build_empty_dwh(tmp_path / "incremental.duckdb")
    for batch in split_by_night(events):
        write_raw_events(incremental, batch)
        run_transforms(incremental)

    assert snapshot(incremental) == snapshot(loaded_dwh)
    incremental.close()


def test_late_event_updates_its_original_day(tmp_path, events):
    late = max(events, key=lambda r: r["ingested_at"] - r["query_end_time"])
    # Everything that landed before the late event (ingested_at grows with arrival order).
    earlier = [r for r in events if r["ingested_at"] < late["ingested_at"] and r is not late]
    con = build_empty_dwh(tmp_path / "late.duckdb")

    write_raw_events(con, earlier)
    run_transforms(con)
    day = late["query_end_time"].date()
    before = scalar(con, f"SELECT sum(query_count) FROM agg_team_usage_daily WHERE query_date = DATE '{day}'")

    write_raw_events(con, [late])
    run_transforms(con)
    after = scalar(con, f"SELECT sum(query_count) FROM agg_team_usage_daily WHERE query_date = DATE '{day}'")

    assert after == before + 1
    con.close()


def test_team_totals_add_up_to_all_queries(loaded_dwh):
    by_team = scalar(loaded_dwh, "SELECT sum(cpu_seconds) FROM agg_team_usage_daily")
    total = scalar(loaded_dwh, "SELECT sum(cpu_seconds) FROM stg_query_events")
    assert abs(by_team - total) < 1e-6


def test_allocated_cpu_adds_up_to_query_cpu(loaded_dwh):
    mismatches = scalar(loaded_dwh, """
        SELECT count(*) FROM (
            SELECT q.query_id
            FROM fact_query AS q
            JOIN fact_query_table_access AS a USING (query_id)
            GROUP BY q.query_id, q.cpu_seconds
            HAVING abs(q.cpu_seconds - sum(a.allocated_cpu_seconds)) > 1e-6
        )
    """)
    assert mismatches == 0


def test_team_is_resolved_at_query_date(loaded_dwh):
    teams = loaded_dwh.sql("""
        SELECT team_name, min(query_date), max(query_date)
        FROM fact_query
        WHERE user_name = 'kenji.arakawa'
        GROUP BY team_name
        ORDER BY 2
    """).fetchall()
    assert [t[0] for t in teams] == ["product_analytics", "data_platform"]
    assert str(teams[0][2]) < "2026-11-04" <= str(teams[1][1])


def test_users_missing_from_hr_go_to_unknown(loaded_dwh):
    unknown = {row[0] for row in loaded_dwh.sql(
        "SELECT DISTINCT user_name FROM fact_query WHERE team_name = 'unknown'"
    ).fetchall()}
    assert unknown == {"svc_airflow", "svc_reporting", "ext.mirela.popescu"}