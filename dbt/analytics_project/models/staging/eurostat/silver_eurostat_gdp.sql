-- Silver GDP: typed, filtered to valid countries, long-form with one row per country/year/indicator/unit.
{{ config(materialized='table', schema='silver') }}

with cleaned as (
    select
        geo                                     as country_code,
        unit,
        na_item,
        time                                    as time_period,
        try_cast(obs_value as double)           as obs_value,
        dataset_code,
        extracted_at
    from {{ ref('bronze_eurostat_gdp') }}
    where obs_value is not null
      and obs_value != ''
      and length(geo) = 2
)

select
    country_code,
    unit,
    na_item                                     as gdp_component,
    time_period                                 as year,
    obs_value                                   as value_meur,
    dataset_code,
    extracted_at
from cleaned
where try_cast(time_period as integer) is not null
