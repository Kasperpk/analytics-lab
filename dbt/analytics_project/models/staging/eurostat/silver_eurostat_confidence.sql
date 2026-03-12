-- Silver consumer confidence: typed, one row per country/month.
{{ config(materialized='table', schema='silver') }}

with cleaned as (
    select
        geo                                     as country_code,
        time                                    as time_period,
        try_cast(obs_value as double)           as confidence_index,
        dataset_code,
        extracted_at
    from {{ ref('bronze_eurostat_confidence') }}
    where obs_value is not null
      and obs_value != ''
      and length(geo) = 2
)

select
    country_code,
    time_period                                 as month,
    confidence_index,
    dataset_code,
    extracted_at
from cleaned
