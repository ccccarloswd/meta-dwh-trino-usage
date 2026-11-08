-- Question: how many resources did team Y consume during a period?

-- Additive metrics, so the daily aggregate can be summed over any period.
WITH params AS (
    SELECT
        'data_platform'   AS team_name,
        DATE '2026-10-26' AS date_from,  -- inclusive
        DATE '2026-11-09' AS date_to     -- exclusive
)
SELECT
    p.team_name,
    SUM(t.query_count)                              AS queries,
    SUM(t.failed_query_count)                       AS failed_queries,
    ROUND(SUM(t.cpu_seconds) / 3600, 1)             AS cpu_hours,
    ROUND(SUM(t.input_bytes) / POWER(1024, 4), 2)   AS input_tib,
    ROUND(SUM(t.written_bytes) / POWER(1024, 4), 2) AS written_tib
FROM agg_team_usage_daily AS t
JOIN params AS p
  ON t.team_name = p.team_name
 AND t.query_date >= p.date_from
 AND t.query_date < p.date_to
GROUP BY p.team_name;