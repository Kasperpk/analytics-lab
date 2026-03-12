-- Macro dashboard — wide monthly snapshot joining all key indicators per country.
-- Designed as the single entry-point table for cross-indicator analysis.
{{ config(materialized='table') }}

with inflation as (
    select
        country_code,
        month                                   as period,
        headline_inflation_pct,
        energy_inflation_pct,
        food_inflation_pct
    from {{ ref('gold_fct_inflation') }}
),

unemployment as (
    select
        country_code,
        month                                   as period,
        unemployment_rate_total,
        youth_unemployment_rate
    from {{ ref('gold_fct_unemployment') }}
),

confidence as (
    select
        country_code,
        month                                   as period,
        confidence_index
    from {{ ref('silver_eurostat_confidence') }}
),

interest_rates as (
    select
        country_code,
        month                                   as period,
        max(case when maturity_code = 'IRT_M3'  then interest_rate_pct end) as rate_3m,
        max(case when maturity_code = 'IRT_M12' then interest_rate_pct end) as rate_12m
    from {{ ref('silver_eurostat_interest_rates') }}
    group by country_code, month
),

-- Build a spine of all country + period combinations from monthly datasets
spine as (
    select distinct country_code, period from inflation
    union
    select distinct country_code, period from unemployment
    union
    select distinct country_code, period from confidence
    union
    select distinct country_code, period from interest_rates
)

select
    s.country_code,
    s.period,
    i.headline_inflation_pct,
    i.energy_inflation_pct,
    i.food_inflation_pct,
    u.unemployment_rate_total,
    u.youth_unemployment_rate,
    c.confidence_index                          as consumer_confidence,
    ir.rate_3m                                  as interest_rate_3m,
    ir.rate_12m                                 as interest_rate_12m
from spine s
left join inflation i
    on s.country_code = i.country_code and s.period = i.period
left join unemployment u
    on s.country_code = u.country_code and s.period = u.period
left join confidence c
    on s.country_code = c.country_code and s.period = c.period
left join interest_rates ir
    on s.country_code = ir.country_code and s.period = ir.period
