{{ config(materialized='table', schema='bronze') }}

select
    *,
    'interest_rates' as _source_dataset
from read_parquet('../../data/raw/eurostat/interest_rates.parquet')
