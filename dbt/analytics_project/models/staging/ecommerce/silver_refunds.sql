{{ config(materialized='table', schema='silver') }}

with typed as (
    select
        refund_id,
        order_line_id,
        order_id,
        try_cast(refund_ts as timestamp) as refund_ts,
        try_cast(refund_amount as double) as refund_amount,
        refund_reason,
        refund_status,
        _file_date,
        _source_file
    from {{ ref('bronze_refunds') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by refund_id
            order by _file_date desc, refund_ts desc nulls last
        ) as _rn
    from typed
    where refund_id is not null
)

select
    refund_id,
    order_line_id,
    order_id,
    refund_ts,
    refund_amount,
    refund_reason,
    refund_status,
    _file_date,
    _source_file
from ranked
where _rn = 1
    and refund_id like 'R20%'
    and order_line_id like 'L20%'
