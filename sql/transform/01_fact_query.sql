-- Incremental, idempotent load of fact_query.
-- Only rows ingested after the current watermark (minus a 1-hour safety overlap) are read,
-- so late events are picked up whatever their query_date. MERGE on query_id makes reruns safe.
MERGE INTO fact_query AS f
USING (
    SELECT
        q.query_id,
        q.query_date,
        q.end_time,
        q.user_name,
        COALESCE(d.team_name, 'unknown') AS team_name,
        q.source,
        q.query_state,
        q.error_name,
        q.cpu_seconds,
        q.wall_seconds,
        q.queued_seconds,
        q.peak_memory_bytes,
        q.input_bytes,
        q.written_bytes,
        q.ingested_at
    FROM stg_query_events AS q
    LEFT JOIN dim_user_team AS d
      ON q.user_name = d.user_name
     AND q.query_date >= d.valid_from
     AND q.query_date < d.valid_to
    WHERE q.ingested_at > (
        SELECT COALESCE(max(ingested_at), TIMESTAMP '1970-01-01') - INTERVAL 1 HOUR
        FROM fact_query
    )
) AS s
ON f.query_id = s.query_id
WHEN NOT MATCHED THEN INSERT *;