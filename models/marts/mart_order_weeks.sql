{{
    config(
        enabled=True,
        materialized='table'
    )
}}

with source as (
    SELECT 
        hub_order.order_pk,
        sat_order_details.status,       
        {{ dbt_date.week_start(" sat_order_details.order_date") }} as order_week
     from {{ ref("hub_order") }} as hub_order
     left join {{ ref("sat_order_details") }} as sat_order_details on hub_order.order_pk = sat_order_details.order_pk
)

SELECT 
    order_week,    
    status,
    count(order_week)

    from source group by order_week, status
