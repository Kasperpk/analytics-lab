-- Silver HICP inflation: typed, one row per country/month/category.
{{ config(materialized='table', schema='silver') }}

with cleaned as (
    select
        geo                                     as country_code,
        coicop,
        time                                    as time_period,
        try_cast(obs_value as double)           as inflation_rate_pct,
        dataset_code,
        extracted_at
    from {{ ref('bronze_eurostat_hicp') }}
    where obs_value is not null
      and obs_value != ''
      and length(geo) = 2
)

select
    country_code,
    coicop                                      as price_category,
    case coicop
        when 'CP00' then 'All items'
        when 'CP01' then 'Food and non-alcoholic beverages'
        when 'CP04' then 'Housing, water, electricity, gas'
        when 'CP07' then 'Transport'
        when 'NRG'  then 'Energy'
        else coicop
    end                                         as price_category_label,
    time_period                                as month,
    inflation_rate_pct,
    dataset_code,
    extracted_at
from cleaned
