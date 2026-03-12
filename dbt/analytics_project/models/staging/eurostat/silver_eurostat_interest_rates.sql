-- Silver interest rates: typed, one row per country/month/maturity.
{{ config(materialized='table', schema='silver') }}

with cleaned as (
    select
        geo                                     as country_code,
        int_rt,
        time                                    as time_period,
        try_cast(obs_value as double)           as interest_rate_pct,
        dataset_code,
        extracted_at
    from {{ ref('bronze_eurostat_interest_rates') }}
    where obs_value is not null
      and obs_value != ''
      and length(geo) = 2
)

select
    country_code,
    int_rt                                      as maturity_code,
    case int_rt
        when 'IRT_M3'  then '3-month'
        when 'IRT_M6'  then '6-month'
        when 'IRT_M12' then '12-month'
        else int_rt
    end                                         as maturity_label,
    time_period                                 as month,
    interest_rate_pct,
    dataset_code,
    extracted_at
from cleaned
