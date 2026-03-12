-- Silver government finances: typed, one row per country/year/indicator/unit.
{{ config(materialized='table', schema='silver') }}

with cleaned as (
    select
        geo                                     as country_code,
        na_item,
        unit,
        time                                    as time_period,
        try_cast(obs_value as double)           as obs_value,
        dataset_code,
        extracted_at
    from {{ ref('bronze_eurostat_gov_finance') }}
    where obs_value is not null
      and obs_value != ''
      and length(geo) = 2
)

select
    country_code,
    na_item                                     as indicator_code,
    case na_item
        when 'B9' then 'Government deficit/surplus'
        when 'GD' then 'Government gross debt'
        else na_item
    end                                         as indicator_label,
    unit,
    time_period                                 as year,
    obs_value,
    dataset_code,
    extracted_at
from cleaned
where try_cast(time_period as integer) is not null
