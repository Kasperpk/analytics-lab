{{ config(materialized='incremental', schema='bronze') }}

with src as (
    select
        *,
        filename as _source_file,
        try_cast(regexp_extract(filename, 'dt=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1) as date) as _file_date
    from read_csv_auto(
        '../../data/raw/dt=*/products.csv',
        all_varchar=true,
        union_by_name=true,
        filename=true,
        ignore_errors=true,
        delim=',',
        quote='"',
        escape='"',
        strict_mode=false,
        null_padding=true
    )
)

select *
from src
{% if is_incremental() %}
where _source_file not in (select distinct _source_file from {{ this }})
{% endif %}
