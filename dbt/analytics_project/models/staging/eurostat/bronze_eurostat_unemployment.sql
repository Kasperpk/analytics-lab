{{ config(materialized='table', schema='bronze') }}

select
    *,
    'unemployment' as _source_dataset
from read_parquet('../../data/raw/eurostat/unemployment.parquet')
