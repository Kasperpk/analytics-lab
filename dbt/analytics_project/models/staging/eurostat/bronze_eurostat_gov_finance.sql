{{ config(materialized='table', schema='bronze') }}

select
    *,
    'gov_deficit_debt' as _source_dataset
from read_parquet('../../data/raw/eurostat/gov_deficit_debt.parquet')
