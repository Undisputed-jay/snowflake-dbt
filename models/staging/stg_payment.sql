WITH raw AS (
    SELECT
        *
    FROM {{ source("Snowflake Source Staging", "payment") }}
),

final AS (
    SELECT
        ID AS payment_id,
        order_id,
        payment_method AS payment_mode,
        amount/100 AS sales_amount
    FROM raw
)

SELECT * FROM final