"""Tests for the synthetic event generator."""

from datetime import date, datetime

from meta_dwh.generate_events import generate_events


def test_same_seed_gives_same_events():
    first = generate_events(1, date(2026, 11, 1), 2, 50)
    second = generate_events(1, date(2026, 11, 1), 2, 50)
    assert first == second


def test_events_are_internally_consistent(events):
    for row in events:
        event = row["payload"]
        stats = event["statistics"]
        inputs = event["ioMetadata"]["inputs"]

        create = datetime.fromisoformat(event["createTime"])
        start = datetime.fromisoformat(event["executionStartTime"])
        end = datetime.fromisoformat(event["endTime"])
        assert create <= start <= end
        assert stats["wallTime"] <= stats["cpuTime"]
        assert sum(i["physicalInputBytes"] for i in inputs) == stats["physicalInputBytes"]

        if event["metadata"]["queryState"] == "FAILED":
            assert event["ioMetadata"]["output"] is None
            assert stats["writtenBytes"] == 0


def test_duplicates_are_redeliveries_of_the_same_event(events):
    by_id: dict[str, list[dict]] = {}
    for row in events:
        by_id.setdefault(row["query_id"], []).append(row)
    duplicated = [rows for rows in by_id.values() if len(rows) > 1]

    assert duplicated, "the generator should inject some duplicates"
    for first, second in duplicated:
        assert first["payload"] == second["payload"]
        assert first["ingested_at"] != second["ingested_at"]