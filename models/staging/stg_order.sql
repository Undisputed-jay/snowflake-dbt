WITH raw AS (
    SELECT
        *
    FROM {{ source("Snowflake Source Staging", "orders") }}
),

final AS (
    SELECT
        ID AS order_id,
        user_id AS customer_id,
        order_date,
        status
    FROM raw
)

SELECT * FROM final