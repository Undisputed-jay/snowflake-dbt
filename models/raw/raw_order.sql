SELECT 
    *
FROM {{ source("Snowflake Source Raw", "orders") }}