WITH raw AS (
    SELECT
        *
    FROM {{ source("Snowflake Source Staging", "customer") }}
),

final AS (
    SELECT
        ID AS customer_id,
        first_name,
        last_name
    FROM raw
)

SELECT * FROM final