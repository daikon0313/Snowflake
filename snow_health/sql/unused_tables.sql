WITH access_history_flattened AS (
    SELECT
        access_history.query_id,
        access_history.query_start_time,
        access_history.user_name,
        objects_accessed.value:objectId::integer AS table_id,
        objects_accessed.value:objectName::text AS object_name,
        objects_accessed.value:objectDomain::text AS object_domain,
        objects_accessed.value:columns AS columns_array
    FROM snowflake.account_usage.access_history, LATERAL FLATTEN(access_history.base_objects_accessed) AS objects_accessed
)
, table_access_history AS (
    SELECT
        query_id,
        query_start_time,
        user_name,
        object_name AS fully_qualified_table_name,
        table_id
    FROM access_history_flattened
    WHERE object_domain = 'Table'
)
, table_access_summary AS (
    SELECT
        table_id,
        MAX(query_start_time) AS last_accessed_at,
        MAX_BY(user_name, query_start_time) AS last_accessed_by,
        MAX_BY(query_id, query_start_time) AS last_query_id
    FROM table_access_history
    GROUP BY 1
)
, table_storage_metrics AS (
    SELECT
        id AS table_id,
        table_catalog || '.' || table_schema || '.' || table_name AS fully_qualified_table_name,
        (active_bytes + time_travel_bytes + failsafe_bytes + retained_for_clone_bytes)/POWER(1024,4) AS total_storage_tb
    FROM snowflake.account_usage.table_storage_metrics
    WHERE NOT deleted
)
SELECT
    table_storage_metrics.*,
    table_access_summary.* EXCLUDE (table_id)
FROM table_storage_metrics
LEFT JOIN table_access_summary
    ON table_storage_metrics.table_id = table_access_summary.table_id
WHERE COALESCE(last_accessed_at, DATE'1900-01-01') < (CURRENT_DATE - 30)
ORDER BY table_storage_metrics.total_storage_tb DESC;