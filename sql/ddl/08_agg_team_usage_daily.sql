-- Daily resource usage per team, rebuilt per affected day from fact_query.
-- Grain: one row per day x team. All metrics are additive except distinct_users.
CREATE TABLE IF NOT EXISTS agg_team_usage_daily (
    query_date         DATE      NOT NULL,
    team_name          VARCHAR   NOT NULL,
    query_count        BIGINT    NOT NULL,
    failed_query_count BIGINT    NOT NULL,
    distinct_users     BIGINT    NOT NULL,  -- per day only: never sum across days
    cpu_seconds        DOUBLE    NOT NULL,
    input_bytes        BIGINT    NOT NULL,
    written_bytes      BIGINT    NOT NULL,
    last_ingested_at   TIMESTAMP NOT NULL,  -- watermark for incremental loads
    PRIMARY KEY (query_date, team_name)
);