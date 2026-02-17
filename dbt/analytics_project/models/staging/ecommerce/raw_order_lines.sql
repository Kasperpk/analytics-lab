{{ config(materialized='view') }}

select
    *,
    filename as _source_file,
    try_cast(regexp_extract(filename, 'dt=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1) as date) as _file_date
from read_csv_auto(
    '../../data/raw/dt=*/order_lines.csv',
    all_varchar=true,
    union_by_name=true,
    filename=true,
    ignore_errors=true,
    delim=',',
    quote='',
    escape='',
    strict_mode=false,
    null_padding=true
)
