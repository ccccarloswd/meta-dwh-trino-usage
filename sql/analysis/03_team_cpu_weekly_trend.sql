-- Question: weekly CPU per team and week over week change.

WITH weekly AS (
    SELECT
        DATE_TRUNC('week', query_date) AS week_start,
        team_name,
        SUM(cpu_seconds) / 3600        AS cpu_hours
    FROM agg_team_usage_daily
    GROUP BY 1, 2
)
SELECT
    week_start,
    team_name,
    ROUND(cpu_hours, 1) AS cpu_hours,
    ROUND(100 * (cpu_hours / LAG(cpu_hours) OVER (PARTITION BY team_name ORDER BY week_start) - 1), 1)
        AS pct_change_vs_prev_week
FROM weekly
ORDER BY team_name, week_start;