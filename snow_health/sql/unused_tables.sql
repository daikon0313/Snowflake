with access_history_flattened as (
    select
        access_history.query_id,
        access_history.query_start_time,
        access_history.user_name,
        objects_accessed.value:objectid::integer as table_id,
        objects_accessed.value:objectname::text as object_name,
        objects_accessed.value:objectdomain::text as object_domain,
        objects_accessed.value:columns as columns_array
    from snowflake.account_usage.access_history, lateral flatten(access_history.base_objects_accessed) as objects_accessed
)
, table_access_history as (
    select
        query_id,
        query_start_time,
        user_name,
        object_name as fully_qualified_table_name,
        table_id
    from access_history_flattened
    where object_domain = 'Table'
)
, table_access_summary as (
    select
        table_id,
        max(query_start_time) as last_accessed_at,
        max_by(user_name, query_start_time) as last_accessed_by,
        max_by(query_id, query_start_time) as last_query_id
    from table_access_history
    group by 1
)
, table_storage_metrics as (
    select
        id as table_id,
        table_catalog || '.' || table_schema || '.' || table_name as fully_qualified_table_name,
        (active_bytes + time_travel_bytes + failsafe_bytes + retained_for_clone_bytes)/power(1024,4) as total_storage_tb
    from snowflake.account_usage.table_storage_metrics
    where not deleted
)
select
    table_storage_metrics.*,
    table_access_summary.* exclude (table_id)
from table_storage_metrics
left join table_access_summary
    on table_storage_metrics.table_id = table_access_summary.table_id
where coalesce(last_accessed_at, date'1900-01-01') < (current_date - 30)
order by table_storage_metrics.total_storage_tb desc;