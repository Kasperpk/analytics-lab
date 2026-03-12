{{ config(materialized='table', schema='bronze') }}

select
    *,
    'consumer_confidence' as _source_dataset
from read_parquet('../../data/raw/eurostat/consumer_confidence.parquet')
