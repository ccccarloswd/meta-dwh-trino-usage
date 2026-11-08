-- Daily usage per table, rebuilt per affected day from fact_query_table_access.
-- Grain: one row per day x table. All metrics are additive except distinct_users.
CREATE TABLE IF NOT EXISTS agg_table_usage_daily (
    query_date            DATE      NOT NULL,
    full_table_name       VARCHAR   NOT NULL,
    distinct_users        BIGINT    NOT NULL,  -- readers and writers; never sum across days
    read_count            BIGINT    NOT NULL,
    write_count           BIGINT    NOT NULL,
    allocated_cpu_seconds DOUBLE    NOT NULL,
    input_bytes           BIGINT    NOT NULL,
    written_bytes         BIGINT    NOT NULL,
    last_ingested_at      TIMESTAMP NOT NULL,  -- watermark for incremental loads
    PRIMARY KEY (query_date, full_table_name)
);