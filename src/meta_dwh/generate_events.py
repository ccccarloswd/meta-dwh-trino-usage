"""Synthetic Trino event generator.

Plays the role of Trino's event listener.
"""

import random

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


def make_event(rng: random.Random, query_id: str) -> dict:
    """Create one synthetic Trino QueryCompletedEvent."""
    user = rng.choice(USERS)
    source = rng.choice(SOURCES)
    n_tables = rng.randint(1, 3)
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

    return {
        "metadata": {"queryId": query_id, "queryState": "FINISHED"},
        "context": {"user": user, "source": source},
        "ioMetadata": {"inputs": inputs, "output": None},
    }