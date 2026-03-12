{{ config(materialized='table', schema='bronze') }}

select
    *,
    'hicp_inflation' as _source_dataset
from read_parquet('../../data/raw/eurostat/hicp_inflation.parquet')
