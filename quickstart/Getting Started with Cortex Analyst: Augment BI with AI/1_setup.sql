-- bringing gen ai to etl: redefining data engineering through llms
-- https://medium.com/snowflake/llm-powered-etl-snowflakes-leap-into-data-warehouse-ai-1aebcaee8025

/*--
• database, schema, warehouse, and stage creation
--*/

use role securityadmin;

-- create or replace role accountadmin;
grant database role snowflake.cortex_user to role accountadmin;

grant role accountadmin to user <user_name>;

use role accountadmin;

-- create demo database
-- create or replace database d_harato_db;

-- create schema
create or replace schema d_harato_db.sq_revenue_timeseries;

-- create warehouse
-- create or replace warehouse d_harato_wh
--     warehouse_size = 'large'
--     warehouse_type = 'standard'
--     auto_suspend = 60
--     auto_resume = true
--     initially_suspended = true
-- comment = 'warehouse for cortex analyst demo';

grant usage on warehouse d_harato_wh to role accountadmin;
grant operate on warehouse d_harato_wh to role accountadmin;

grant ownership on schema d_harato_db.sq_revenue_timeseries to role accountadmin;
grant ownership on database d_harato_db to role accountadmin;


use role accountadmin;

-- use the created warehouse
use warehouse d_harato_wh;

use database d_harato_db;
use schema d_harato_db.sq_revenue_timeseries;

-- create stage for raw data
create or replace stage raw_data directory = (enable = true);

/*--
• fact and dimension table creation
--*/

-- fact table: daily_revenue
create or replace table d_harato_db.sq_revenue_timeseries.daily_revenue (
    date date,
    revenue float,
    cogs float,
    forecasted_revenue float,
    product_id int,
    region_id int
);

-- dimension table: product_dim
create or replace table d_harato_db.sq_revenue_timeseries.product_dim (
    product_id int,
    product_line varchar(16777216)
);

-- dimension table: region_dim
create or replace table d_harato_db.sq_revenue_timeseries.region_dim (
    region_id int,
    sales_region varchar(16777216),
    state varchar(16777216)
);




COPY INTO d_harato_db.sq_revenue_timeseries.DAILY_REVENUE
FROM @raw_data
FILES = ('daily_revenue.csv')
FILE_FORMAT = (
    TYPE=CSV,
    SKIP_HEADER=1,
    FIELD_DELIMITER=',',
    TRIM_SPACE=FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO
    EMPTY_FIELD_AS_NULL = FALSE
    error_on_column_count_mismatch=false
)

ON_ERROR=CONTINUE
FORCE = TRUE ;



COPY INTO d_harato_db.sq_revenue_timeseries.PRODUCT_DIM
FROM @raw_data
FILES = ('product.csv')
FILE_FORMAT = (
    TYPE=CSV,
    SKIP_HEADER=1,
    FIELD_DELIMITER=',',
    TRIM_SPACE=FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO
    EMPTY_FIELD_AS_NULL = FALSE
    error_on_column_count_mismatch=false
)

ON_ERROR=CONTINUE
FORCE = TRUE ;



COPY INTO d_harato_db.sq_revenue_timeseries.REGION_DIM
FROM @raw_data
FILES = ('region.csv')
FILE_FORMAT = (
    TYPE=CSV,
    SKIP_HEADER=1,
    FIELD_DELIMITER=',',
    TRIM_SPACE=FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO
    EMPTY_FIELD_AS_NULL = FALSE
    error_on_column_count_mismatch=false
)

ON_ERROR=CONTINUE
FORCE = TRUE ;


CREATE OR REPLACE CORTEX SEARCH SERVICE product_line_search_service
ON product_dimension
WAREHOUSE = cortex_analyst_wh
TARGET_LAG = '1 hour'
AS (
    SELECT DISTINCT product_line AS product_dimension FROM product_dim
);