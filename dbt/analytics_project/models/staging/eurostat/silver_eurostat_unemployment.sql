-- Silver unemployment: typed, filtered, one row per country/month/age group/sex.
{{ config(materialized='table', schema='silver') }}

with cleaned as (
    select
        geo                                     as country_code,
        age,
        sex,
        time                                    as time_period,
        try_cast(obs_value as double)           as unemployment_rate_pct,
        dataset_code,
        extracted_at
    from {{ ref('bronze_eurostat_unemployment') }}
    where obs_value is not null
      and obs_value != ''
      and length(geo) = 2
)

select
    country_code,
    age                                         as age_group,
    sex,
    time_period                                 as month,
    unemployment_rate_pct,
    dataset_code,
    extracted_at
from cleaned
