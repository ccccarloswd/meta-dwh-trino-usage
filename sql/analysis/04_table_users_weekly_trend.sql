-- Q4 (history): weekly distinct users per table, counted exactly per week from the fact.
SELECT
    DATE_TRUNC('week', query_date) AS week_start,
    full_table_name,
    COUNT(DISTINCT user_name)      AS distinct_users
FROM fact_query_table_access
GROUP BY 1, 2
ORDER BY full_table_name, week_start;