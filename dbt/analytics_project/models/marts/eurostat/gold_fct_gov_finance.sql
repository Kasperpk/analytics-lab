-- Government finances overview — annual deficit/surplus and debt by country.
{{ config(materialized='table') }}

select
    country_code,
    year,
    max(case when indicator_code = 'B9' and unit = 'PC_GDP'  then obs_value end)  as deficit_surplus_pct_gdp,
    max(case when indicator_code = 'B9' and unit = 'MIO_EUR' then obs_value end)  as deficit_surplus_meur,
    max(case when indicator_code = 'GD' and unit = 'PC_GDP'  then obs_value end)  as gross_debt_pct_gdp,
    max(case when indicator_code = 'GD' and unit = 'MIO_EUR' then obs_value end)  as gross_debt_meur
from {{ ref('silver_eurostat_gov_finance') }}
group by country_code, year
