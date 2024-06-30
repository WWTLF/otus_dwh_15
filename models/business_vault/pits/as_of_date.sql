{{ config(materialized='table') }}

{%- set datepart = "minute" -%}
{%- set start_date = "'2024-06-30 12:20:00'::timestamp" -%}
{%- set end_date = "'2024-06-30 13:30:00'::timestamp" -%}

WITH as_of_date AS (
    {{ dbt_utils.date_spine(datepart=datepart, 
                            start_date=start_date,
                            end_date=end_date) }}
)

SELECT DATE_{{datepart}} as AS_OF_DATE FROM as_of_date