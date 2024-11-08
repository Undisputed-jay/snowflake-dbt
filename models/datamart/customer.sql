-- this will return this as dbt_ayodele_analytics
-- you can also specify a different database in the config

{{ config(
    schema = "analytics"
) }} 

with
    customer as (select * from {{ ref("stg_customer") }}),

    orders as (select * from {{ ref("stg_order") }}),

    payment as (select * from {{ ref("stg_payment") }}),

    customer_level_details as (
        select 
            c.first_name,
            c.last_name,
            c.customer_id,
            MIN(o.order_date) AS first_order,
            MAX(o.order_date) AS most_recent_order
        from customer as c
        left join orders as o 
        on c.customer_id = o.customer_id
        group by 1,2,3
    ),

    payment_details as (
        select
            o.customer_id,
            SUM(p.sales_amount) AS payment_amount
        from payment p
        left join orders o
        on p.order_id = o.order_id
        group by o.customer_id
    ),

    final_table as (
        select
            cl.customer_id,          
            cl.first_name,
            cl.last_name,
            cl.first_order,
            cl.most_recent_order,
            pl.payment_amount
        from customer_level_details cl
        left join payment_details pl
        on cl.customer_id = pl.customer_id
    )

select *
from final_table
