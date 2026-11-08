-- One row per Trino completed event
-- Duplicates are expected (no primary key)
CREATE TABLE IF NOT EXISTS raw_query_events(
    query_id VARCHAR NOT NULL,
    query_end_time TIMESTAMP NOT NULL, -- When the query finishes (UTC)
    ingested_at TIMESTAMP NOT NULL, -- When the event arrived (UTC)
    payload JSON NOT NULL -- Full event
);