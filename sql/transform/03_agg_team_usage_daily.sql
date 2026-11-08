-- Incremental rebuild of agg_team_usage_daily: every day that received new fact rows
-- (late events included) is deleted and fully recomputed. Atomic and idempotent.
BEGIN TRANSACTION;

-- Must be computed before the DELETE, which removes the rows holding the watermark.
CREATE OR REPLACE TEMP TABLE affected_dates AS
SELECT DISTINCT query_date
FROM fact_query
WHERE ingested_at > (
    SELECT COALESCE(max(last_ingested_at), TIMESTAMP '1970-01-01') - INTERVAL 1 HOUR
    FROM agg_team_usage_daily
);

DELETE FROM agg_team_usage_daily
WHERE query_date IN (SELECT query_date FROM affected_dates);

INSERT INTO agg_team_usage_daily
SELECT
    query_date,
    team_name,
    COUNT(*)                                       AS query_count,
    COUNT(*) FILTER (WHERE query_state = 'FAILED') AS failed_query_count,
    COUNT(DISTINCT user_name)                      AS distinct_users,
    SUM(cpu_seconds)                               AS cpu_seconds,
    SUM(input_bytes)                               AS input_bytes,
    SUM(written_bytes)                             AS written_bytes,
    MAX(ingested_at)                               AS last_ingested_at
FROM fact_query
WHERE query_date IN (SELECT query_date FROM affected_dates)
GROUP BY query_date, team_name;

COMMIT;