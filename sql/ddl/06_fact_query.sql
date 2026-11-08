-- Fact table of Trino queries, loaded incrementally from staging.
-- Grain: one row per query. Team is resolved at load time.
CREATE TABLE IF NOT EXISTS fact_query (
    query_id          VARCHAR   NOT NULL PRIMARY KEY,
    query_date        DATE      NOT NULL,  -- UTC day the query ended
    end_time          TIMESTAMP NOT NULL,
    user_name         VARCHAR   NOT NULL,
    team_name         VARCHAR   NOT NULL,  -- 'unknown' when the user is not in the HR export
    source            VARCHAR,
    query_state       VARCHAR   NOT NULL,
    error_name        VARCHAR,
    cpu_seconds       DOUBLE    NOT NULL,
    wall_seconds      DOUBLE    NOT NULL,
    queued_seconds    DOUBLE    NOT NULL,
    peak_memory_bytes BIGINT    NOT NULL,
    input_bytes       BIGINT    NOT NULL,
    written_bytes     BIGINT    NOT NULL,
    ingested_at       TIMESTAMP NOT NULL   -- watermark for incremental loads
);