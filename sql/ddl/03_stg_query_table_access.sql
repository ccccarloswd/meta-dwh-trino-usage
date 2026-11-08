-- Staging view of table accesses.
-- Grain: one row per query x table accessed: reads from ioMetadata.inputs,
-- the write target from ioMetadata.output. Reuses the delivery kept by stg_query_events.
CREATE OR REPLACE VIEW stg_query_table_access AS
WITH deduped AS (
    SELECT r.query_id, r.payload
    FROM raw_query_events AS r
    JOIN stg_query_events AS s
      ON r.query_id = s.query_id
     AND r.ingested_at = s.ingested_at
),
reads AS (
    SELECT
        d.query_id,
        'read'                                           AS access_type,
        i.value->>'$.catalogName'                        AS catalog_name,
        i.value->>'$.schema'                             AS schema_name,
        i.value->>'$.table'                              AS table_name,
        CAST(i.value->>'$.physicalInputBytes' AS BIGINT) AS input_bytes
    FROM deduped AS d,
         json_each(d.payload, '$.ioMetadata.inputs') AS i
),
writes AS (
    SELECT
        query_id,
        'write'                                     AS access_type,
        payload->>'$.ioMetadata.output.catalogName' AS catalog_name,
        payload->>'$.ioMetadata.output.schema'      AS schema_name,
        payload->>'$.ioMetadata.output.table'       AS table_name,
        CAST(NULL AS BIGINT)                        AS input_bytes  -- bytes read do not apply
    FROM deduped
    WHERE payload->>'$.ioMetadata.output' IS NOT NULL
),
all_access AS (
    SELECT * FROM reads
    UNION ALL
    SELECT * FROM writes
)
SELECT
    query_id,
    access_type,
    catalog_name || '.' || schema_name || '.' || table_name AS full_table_name,
    catalog_name,
    schema_name,
    table_name,
    input_bytes
FROM all_access;