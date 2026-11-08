-- Incremental, idempotent load of fact_query_table_access (runs after fact_query).
-- CPU allocation: share = bytes read from the table / bytes read by the query;
-- equal split among the tables read when the query read no bytes at all.
MERGE INTO fact_query_table_access AS t
USING (
    WITH new_queries AS (
        SELECT *
        FROM fact_query
        WHERE ingested_at > (
            SELECT COALESCE(max(ingested_at), TIMESTAMP '1970-01-01') - INTERVAL 1 HOUR
            FROM fact_query_table_access
        )
    ),
    accesses AS (
        SELECT
            a.query_id,
            a.full_table_name,
            a.access_type,
            q.query_date,
            q.user_name,
            q.team_name,
            q.query_state,
            a.input_bytes,
            q.written_bytes,
            q.cpu_seconds,
            q.ingested_at,
            SUM(a.input_bytes) OVER (PARTITION BY a.query_id, a.access_type) AS query_read_bytes,
            COUNT(*) OVER (PARTITION BY a.query_id, a.access_type)          AS query_read_tables
        FROM stg_query_table_access AS a
        JOIN new_queries AS q
          ON a.query_id = q.query_id
    )
    SELECT
        query_id,
        full_table_name,
        access_type,
        query_date,
        user_name,
        team_name,
        query_state,
        input_bytes,
        CASE WHEN access_type = 'write' THEN written_bytes END AS written_bytes,
        CASE
            WHEN access_type = 'write' THEN NULL
            WHEN query_read_bytes > 0
                THEN cpu_seconds * CAST(input_bytes AS DOUBLE) / query_read_bytes
            ELSE cpu_seconds / query_read_tables
        END AS allocated_cpu_seconds,
        ingested_at
    FROM accesses
) AS s
ON  t.query_id = s.query_id
AND t.full_table_name = s.full_table_name
AND t.access_type = s.access_type
WHEN NOT MATCHED THEN INSERT *;