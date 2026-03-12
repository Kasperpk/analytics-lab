-- GDP overview — annual GDP by country with headline and component breakdown.
-- Pivots key components into separate columns for easy comparison.
{{ config(materialized='table') }}

with current_prices as (
    select
        country_code,
        year,
        gdp_component,
        value_meur
    from {{ ref('silver_eurostat_gdp') }}
    where unit = 'CP_MEUR'
),

volumes as (
    select
        country_code,
        year,
        gdp_component,
        value_meur as value_clv_meur
    from {{ ref('silver_eurostat_gdp') }}
    where unit = 'CLV10_MEUR'
),

-- Pivot headline GDP (B1GQ) current prices
headline as (
    select
        country_code,
        year,
        value_meur as gdp_current_meur
    from current_prices
    where gdp_component = 'B1GQ'
),

-- Year-over-year GDP growth from chain-linked volumes
gdp_volumes as (
    select
        country_code,
        year,
        value_clv_meur as gdp_volume_meur
    from volumes
    where gdp_component = 'B1GQ'
),

gdp_growth as (
    select
        c.country_code,
        c.year,
        c.gdp_volume_meur,
        p.gdp_volume_meur as prev_gdp_volume_meur,
        case
            when p.gdp_volume_meur is not null and p.gdp_volume_meur != 0
            then round(((c.gdp_volume_meur - p.gdp_volume_meur) / p.gdp_volume_meur) * 100, 2)
        end as real_gdp_growth_pct
    from gdp_volumes c
    left join gdp_volumes p
        on c.country_code = p.country_code
        and cast(c.year as integer) = cast(p.year as integer) + 1
),

-- Pivot demand-side components (current prices)
components as (
    select
        country_code,
        year,
        max(case when gdp_component = 'P3'  then value_meur end) as consumption_meur,
        max(case when gdp_component = 'P5G' then value_meur end) as investment_meur,
        max(case when gdp_component = 'P6'  then value_meur end) as exports_meur,
        max(case when gdp_component = 'P7'  then value_meur end) as imports_meur,
        max(case when gdp_component = 'B11' then value_meur end) as net_exports_meur
    from current_prices
    group by country_code, year
)

select
    h.country_code,
    h.year,
    h.gdp_current_meur,
    g.gdp_volume_meur,
    g.real_gdp_growth_pct,
    c.consumption_meur,
    c.investment_meur,
    c.exports_meur,
    c.imports_meur,
    c.net_exports_meur
from headline h
left join gdp_growth g
    on h.country_code = g.country_code
    and h.year = g.year
left join components c
    on h.country_code = c.country_code
    and h.year = c.year
