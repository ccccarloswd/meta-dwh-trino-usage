""" Synthetic Trino event generator

Plays the role of Trino's event listener"""

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