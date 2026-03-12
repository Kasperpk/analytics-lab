-- Price stability overview — monthly inflation by country with category breakdown.
{{ config(materialized='table') }}

select
    country_code,
    month,
    max(case when price_category = 'CP00' then inflation_rate_pct end) as headline_inflation_pct,
    max(case when price_category = 'CP01' then inflation_rate_pct end) as food_inflation_pct,
    max(case when price_category = 'CP04' then inflation_rate_pct end) as housing_inflation_pct,
    max(case when price_category = 'CP07' then inflation_rate_pct end) as transport_inflation_pct,
    max(case when price_category = 'NRG'  then inflation_rate_pct end) as energy_inflation_pct
from {{ ref('silver_eurostat_hicp') }}
group by country_code, month
