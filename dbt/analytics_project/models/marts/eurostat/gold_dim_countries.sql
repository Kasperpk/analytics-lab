-- Country dimension — one row per country code appearing across all Eurostat datasets.
-- Maps ISO-2 codes to full names and EU/Euro area membership.
{{ config(materialized='table') }}

with all_countries as (
    select distinct country_code from {{ ref('silver_eurostat_gdp') }}
    union
    select distinct country_code from {{ ref('silver_eurostat_unemployment') }}
    union
    select distinct country_code from {{ ref('silver_eurostat_hicp') }}
    union
    select distinct country_code from {{ ref('silver_eurostat_gov_finance') }}
    union
    select distinct country_code from {{ ref('silver_eurostat_confidence') }}
    union
    select distinct country_code from {{ ref('silver_eurostat_interest_rates') }}
),

country_meta as (
    select
        country_code,
        case country_code
            when 'AT' then 'Austria'
            when 'BE' then 'Belgium'
            when 'BG' then 'Bulgaria'
            when 'HR' then 'Croatia'
            when 'CY' then 'Cyprus'
            when 'CZ' then 'Czechia'
            when 'DK' then 'Denmark'
            when 'EE' then 'Estonia'
            when 'FI' then 'Finland'
            when 'FR' then 'France'
            when 'DE' then 'Germany'
            when 'EL' then 'Greece'
            when 'HU' then 'Hungary'
            when 'IE' then 'Ireland'
            when 'IT' then 'Italy'
            when 'LV' then 'Latvia'
            when 'LT' then 'Lithuania'
            when 'LU' then 'Luxembourg'
            when 'MT' then 'Malta'
            when 'NL' then 'Netherlands'
            when 'PL' then 'Poland'
            when 'PT' then 'Portugal'
            when 'RO' then 'Romania'
            when 'SK' then 'Slovakia'
            when 'SI' then 'Slovenia'
            when 'ES' then 'Spain'
            when 'SE' then 'Sweden'
            when 'NO' then 'Norway'
            when 'IS' then 'Iceland'
            when 'CH' then 'Switzerland'
            when 'LI' then 'Liechtenstein'
            when 'RS' then 'Serbia'
            when 'ME' then 'Montenegro'
            when 'MK' then 'North Macedonia'
            when 'AL' then 'Albania'
            when 'BA' then 'Bosnia and Herzegovina'
            when 'TR' then 'Turkey'
            when 'UK' then 'United Kingdom'
            when 'US' then 'United States'
            when 'JP' then 'Japan'
            when 'EA' then 'Euro area'
            when 'EU' then 'European Union'
            else country_code
        end as country_name,
        case
            when country_code in (
                'AT','BE','BG','HR','CY','CZ','DK','EE','FI','FR',
                'DE','EL','HU','IE','IT','LV','LT','LU','MT','NL',
                'PL','PT','RO','SK','SI','ES','SE'
            ) then true
            else false
        end as is_eu_member,
        case
            when country_code in (
                'AT','BE','HR','CY','EE','FI','FR','DE','EL','IE',
                'IT','LV','LT','LU','MT','NL','PT','SK','SI','ES'
            ) then true
            else false
        end as is_eurozone
    from all_countries
)

select * from country_meta
