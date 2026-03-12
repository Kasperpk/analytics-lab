{{ config(materialized='table', schema='bronze') }}

select
    *,
    'gdp' as _source_dataset
from read_parquet('../../data/raw/eurostat/gdp.parquet')
