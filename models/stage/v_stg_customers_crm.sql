{{
    config(
        enabled=True
    )
}}

{%- set yaml_metadata -%}
source_model: 'source_customers_crm'
derived_columns:
  CUSTOMER_KEY: 'id'
  RECORD_SOURCE: '!CSV_CUSTOMERS_CRM'
hashed_columns:
  CUSTOMER_PK: 'id'
  CRM_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'country'
      - 'age'      
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}


WITH staging AS (
{{ automate_dv.stage(include_source_columns=true,
                  source_model=source_model,
                  derived_columns=derived_columns,
                  hashed_columns=hashed_columns,
                  ranked_columns=none) }}
)

SELECT *, 
       {{ var('load_date') }}::timestamp AS LOAD_DATE,
       {{ var('load_date') }}::timestamp AS EFFECTIVE_FROM
FROM staging