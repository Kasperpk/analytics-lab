-- Labour market overview — monthly unemployment by country with demographic breakdowns.
{{ config(materialized='table') }}

with total as (
    select
        country_code,
        month,
        unemployment_rate_pct as unemployment_rate_total
    from {{ ref('silver_eurostat_unemployment') }}
    where age_group = 'TOTAL'
      and sex = 'T'
),

youth as (
    select
        country_code,
        month,
        unemployment_rate_pct as youth_unemployment_rate
    from {{ ref('silver_eurostat_unemployment') }}
    where age_group = 'Y_LT25'
      and sex = 'T'
),

by_sex as (
    select
        country_code,
        month,
        max(case when sex = 'M' then unemployment_rate_pct end) as unemployment_rate_male,
        max(case when sex = 'F' then unemployment_rate_pct end) as unemployment_rate_female
    from {{ ref('silver_eurostat_unemployment') }}
    where age_group = 'TOTAL'
      and sex in ('M', 'F')
    group by country_code, month
)

select
    t.country_code,
    t.month,
    t.unemployment_rate_total,
    y.youth_unemployment_rate,
    s.unemployment_rate_male,
    s.unemployment_rate_female,
    round(s.unemployment_rate_female - s.unemployment_rate_male, 2) as gender_gap_pp
from total t
left join youth y
    on t.country_code = y.country_code
    and t.month = y.month
left join by_sex s
    on t.country_code = s.country_code
    and t.month = s.month
