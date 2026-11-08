-- Fact table of table accesses, loaded incrementally after fact_query.
-- Grain: one row per query x table x access type
-- Query CPU is split across the tables READ, proportionally to bytes read from each.
CREATE TABLE IF NOT EXISTS fact_query_table_access (
    query_id              VARCHAR   NOT NULL,
    full_table_name       VARCHAR   NOT NULL,  -- catalog.schema.table
    access_type           VARCHAR   NOT NULL,  -- 'read' or 'write'
    query_date            DATE      NOT NULL,
    user_name             VARCHAR   NOT NULL,
    team_name             VARCHAR   NOT NULL,
    query_state           VARCHAR   NOT NULL,
    input_bytes           BIGINT,              -- reads only
    written_bytes         BIGINT,              -- writes only
    allocated_cpu_seconds DOUBLE,              -- reads only; sums to the query CPU
    ingested_at           TIMESTAMP NOT NULL,  -- watermark for incremental loads
    PRIMARY KEY (query_id, full_table_name, access_type)
);