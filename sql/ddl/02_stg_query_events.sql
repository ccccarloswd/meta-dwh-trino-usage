-- Staging view of Trino query events.
-- Grain: one row per query. Duplicate deliveries are removed (first arrival wins)
-- and the JSON payload is flattened into typed, unit-suffixed columns.
CREATE OR REPLACE VIEW stg_query_events AS
WITH ranked AS (
    SELECT
        r.*,
        ROW_NUMBER() OVER (PARTITION BY query_id ORDER BY ingested_at) AS delivery_rank
    FROM raw_query_events AS r
)
SELECT
    query_id,
    payload->>'$.metadata.queryState'                                       AS query_state,
    payload->>'$.context.user'                                              AS user_name,
    payload->>'$.context.source'                                            AS source,
    CAST(payload->>'$.createTime' AS TIMESTAMPTZ) AT TIME ZONE 'UTC'         AS create_time,
    CAST(payload->>'$.executionStartTime' AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS execution_start_time,
    query_end_time                                                          AS end_time,
    CAST(query_end_time AS DATE)                                            AS query_date,
    CAST(payload->>'$.statistics.cpuTime' AS DOUBLE)                        AS cpu_seconds,
    CAST(payload->>'$.statistics.wallTime' AS DOUBLE)                       AS wall_seconds,
    CAST(payload->>'$.statistics.queuedTime' AS DOUBLE)                     AS queued_seconds,
    CAST(payload->>'$.statistics.peakUserMemoryBytes' AS BIGINT)            AS peak_memory_bytes,
    CAST(payload->>'$.statistics.physicalInputBytes' AS BIGINT)             AS input_bytes,
    CAST(payload->>'$.statistics.writtenBytes' AS BIGINT)                   AS written_bytes,
    payload->>'$.failureInfo.errorCode.name'                                AS error_name,
    ingested_at
FROM ranked
WHERE delivery_rank = 1;