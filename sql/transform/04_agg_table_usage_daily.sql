-- Incremental rebuild of agg_table_usage_daily, same pattern as the team aggregate.
BEGIN TRANSACTION;

-- Must be computed before the DELETE, which removes the rows holding the watermark.
CREATE OR REPLACE TEMP TABLE affected_dates AS
SELECT DISTINCT query_date
FROM fact_query_table_access
WHERE ingested_at > (
    SELECT COALESCE(max(last_ingested_at), TIMESTAMP '1970-01-01') - INTERVAL 1 HOUR
    FROM agg_table_usage_daily
);

DELETE FROM agg_table_usage_daily
WHERE query_date IN (SELECT query_date FROM affected_dates);

INSERT INTO agg_table_usage_daily
SELECT
    query_date,
    full_table_name,
    COUNT(DISTINCT user_name)                     AS distinct_users,
    COUNT(*) FILTER (WHERE access_type = 'read')  AS read_count,
    COUNT(*) FILTER (WHERE access_type = 'write') AS write_count,
    COALESCE(SUM(allocated_cpu_seconds), 0)       AS allocated_cpu_seconds,
    COALESCE(SUM(input_bytes), 0)                 AS input_bytes,
    COALESCE(SUM(written_bytes), 0)               AS written_bytes,
    MAX(ingested_at)                              AS last_ingested_at
FROM fact_query_table_access
WHERE query_date IN (SELECT query_date FROM affected_dates)
GROUP BY query_date, full_table_name;

COMMIT;