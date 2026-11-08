-- Question: how many distinct users queried table X during a period?

-- Exact count over the fact table (distinct users are not additive across days).
WITH params AS (
    SELECT
        'iceberg.sales.licenses' AS table_name,
        DATE '2026-10-26'        AS date_from,  -- inclusive
        DATE '2026-11-09'        AS date_to     -- exclusive
)
SELECT
    p.table_name,
    COUNT(DISTINCT a.user_name)                                         AS distinct_users,
    COUNT(DISTINCT a.user_name) FILTER (WHERE a.access_type = 'read')   AS distinct_readers,
    COUNT(DISTINCT a.user_name) FILTER (WHERE a.access_type = 'write')  AS distinct_writers,
    COUNT(DISTINCT a.query_id)                                          AS queries
FROM fact_query_table_access AS a
JOIN params AS p
  ON a.full_table_name = p.table_name
 AND a.query_date >= p.date_from
 AND a.query_date < p.date_to
GROUP BY p.table_name;