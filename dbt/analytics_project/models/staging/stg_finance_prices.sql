{{ config(materialized='table') }}

select
    date,
    ticker,
    open,
    high,
    low,
    close,
    volume,
    ingested_at
from read_parquet('../../data/raw/finance_prices_*.parquet')