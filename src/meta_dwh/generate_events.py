"""Synthetic Trino event generator.

Plays the role of Trino's event listener.
"""

import random
import string
import json
import duckdb
from datetime import date, datetime, time, timedelta, timezone


# Users that run queries, including edge cases as service accounts (svc) or external contractors (ext)
USERS: list[str] = [
    "ingrid.solberg",
    "tomasz.wrobel",
    "amara.okonkwo",
    "kenji.arakawa",
    "leilani.kahale",
    "ruairi.oconnell",
    "nadia.haddad",
    "thiago.bezerra",
    "priya.raghunathan",
    "elif.demirci",
    "ext.mirela.popescu",
    "svc_airflow",
    "svc_reporting",
]


# Queryable Iceberg tables with typical bytes scanned per query
TABLES: dict[str, int] = {
    "iceberg.telemetry.ide_events": 120000000000,
    "iceberg.telemetry.plugin_installs": 8000000000,
    "iceberg.marketing.campaign_clicks": 15000000000,
    "iceberg.marketing.newsletter_subscribers": 150000000,
    "iceberg.sales.licenses": 3500000000,
    "iceberg.sales.subscriptions": 1200000000,
    "iceberg.sales.invoices": 900000000,
    "iceberg.support.tickets": 600000000,
    "iceberg.finance.exchange_rates": 2000000,
    "iceberg.core.products": 200000,
    "iceberg.core.countries": 50000,
}

# Client tools
SOURCES: list[str] = [
    "superset",
    "tableau",
    "metabase",
    "jupyter",
    "datagrip",
    "trino-cli",
    "dbt",
    "airflow",
]

BYTES_PER_CPU_SECOND = 100000000 # 1 cpu second per each 100 MB scanned
MIN_PARALLELISM = 4 # limit number of cores working at the same time
MAX_PARALLELISM = 64
MAX_QUEUED_SECONDS = 5.0 # max time waiting
TABLE_COUNT_WEIGHTS = [70,20,10] # weights so the amount of tables read hasn't the same probability
FAILURE_RATE = 0.05 # probability for an event to fail
WRITE_RATE = 0.1 # probability for an event to write in a table
# Names of the possible errors
ERROR_NAMES: list[str] = [
    "EXCEEDED_LOCAL_MEMORY_LIMIT",
    "EXCEEDED_TIME_LIMIT",
    "EXCEEDED_CPU_LIMIT",
    "QUERY_REJECTED",
]

# Delivery issues caused by the event pipe

LATE_ARRIVAL_RATE = 0.03
DUPLICATE_RATE = 0.02
MAX_INGESTION_DELAY_SECONDS = 120
MAX_LATE_HOURS = 48
SECONDS_PER_DAY = 86_400

def make_event(rng: random.Random, query_id: str, end_time: datetime) -> dict:
    """Create one Trino QueryCompletedEvent that finished at end_time (UTC)."""

    user = rng.choice(USERS)
    source = rng.choice(SOURCES)
    n_tables = rng.choices([1, 2, 3], weights=TABLE_COUNT_WEIGHTS)[0]
    tables = rng.sample(list(TABLES), n_tables)

    inputs = []
    for table in tables:
        catalog, schema, name = table.split(".")
        input_bytes = int(TABLES[table] * rng.uniform(0.5, 1.5))
        inputs.append({
            "catalogName": catalog,
            "schema": schema,
            "table": name,
            "physicalInputBytes": input_bytes,
        })

    # Resources: coherent with the amount of data read.
    total_bytes = sum(table_input["physicalInputBytes"] for table_input in inputs)
    cpu_seconds = total_bytes / BYTES_PER_CPU_SECOND * rng.uniform(0.5, 2.0)

    query_state = "FINISHED"
    failure_info = None

    if(rng.random() < FAILURE_RATE):
        query_state = "FAILED"
        failure_info = {"errorCode": {"name": rng.choice(ERROR_NAMES)}}
        cpu_seconds = cpu_seconds * rng.uniform(0.1,0.9)
    
    parallelism = rng.randint(MIN_PARALLELISM, MAX_PARALLELISM)
    wall_seconds = cpu_seconds / parallelism
    queued_seconds = rng.uniform(0, MAX_QUEUED_SECONDS)
    peak_memory_bytes = int(total_bytes * rng.uniform(0.01, 0.1))

    # Times derived backwards from the end, so create <= start <= end always holds.
    execution_start = end_time - timedelta(seconds=wall_seconds)
    create_time = execution_start - timedelta(seconds=queued_seconds)

    # Failed queries never write, so only finished ones can have an output.
    output = None
    written_bytes = 0

    if rng.random() < WRITE_RATE and query_state == "FINISHED":
        target = rng.choice([t for t in TABLES if t not in tables])
        catalog, schema, name = target.split(".")
        output = {"catalogName": catalog, "schema": schema, "table": name}
        written_bytes = int(total_bytes * rng.uniform(0.05, 0.5))




    return {
        "metadata": {"queryId": query_id, "queryState": query_state},
        "context": {"user": user, "source": source},
        "createTime": create_time.isoformat(),
        "executionStartTime": execution_start.isoformat(),
        "endTime": end_time.isoformat(),
        "statistics": {
            "cpuTime": round(cpu_seconds, 3),
            "wallTime": round(wall_seconds, 3),
            "queuedTime": round(queued_seconds, 3),
            "peakUserMemoryBytes": peak_memory_bytes,
            "physicalInputBytes": total_bytes,
            "writtenBytes": written_bytes,  
        },
        "ioMetadata": {"inputs": inputs, "output": output},
        "failureInfo": failure_info,
    }

def _to_naive_utc(moment: datetime) -> datetime:

    """Return moment as a naive UTC datetime"""

    return moment.astimezone(timezone.utc).replace(tzinfo=None)

def generate_events(seed: int, start_date: date, days: int, events_per_day: int) -> list[dict]:

    """Generate a batch of raw event rows, as they would land in raw_query_events.

    Each row has query_id, query_end_time, ingested_at and payload.
    Late arrivals and duplicates are injected on purpose as edge cases.
    """
    rng = random.Random(seed)
    rows = []
    counter = 0

    for day_offset in range(days):
        day = start_date + timedelta(days=day_offset)
        day_start = datetime.combine(day, time.min, tzinfo=timezone.utc)

        for _ in range(events_per_day):
            counter += 1
            end_time = day_start + timedelta(seconds=rng.uniform(0, SECONDS_PER_DAY))
            suffix = "".join(rng.choices(string.ascii_lowercase, k=5))
            query_id = f"{end_time:%Y%m%d_%H%M%S}_{counter:05d}_{suffix}"
            event = make_event(rng, query_id, end_time)

            # Most events arrive within seconds; a few arrive hours (or days) later.
            if rng.random() < LATE_ARRIVAL_RATE:
                delay = timedelta(hours=rng.uniform(1, MAX_LATE_HOURS))
            else:
                delay = timedelta(seconds=rng.uniform(1, MAX_INGESTION_DELAY_SECONDS))

            row = {
                "query_id": query_id,
                "query_end_time": end_time,
                "ingested_at": end_time + delay,
                "payload": event,
            }
            rows.append(row)

            # Re-delivery of the same event: a NEW dict, so the original row is not modified.
            if rng.random() < DUPLICATE_RATE:
                redelivery = timedelta(minutes=rng.uniform(1, 30))
                rows.append({**row, "ingested_at": row["ingested_at"] + redelivery})

    rows.sort(key=lambda r: r["ingested_at"])
    return rows

def write_raw_events(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    """Insert generated rows into raw_query_events in a single bulk statement."""
    con.executemany(
        "INSERT INTO raw_query_events VALUES (?, ?, ?, ?)",
        [
            (
                row["query_id"],
                _to_naive_utc(row["query_end_time"]),
                _to_naive_utc(row["ingested_at"]),
                json.dumps(row["payload"]),
            )
            for row in rows
        ],
    )  