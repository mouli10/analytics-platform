with tx as (
    select
        date_trunc('day', created_at) as revenue_date,
        status,
        amount_cents
    from {{ ref('stg_transactions') }}
    where status = 'succeeded'
)

select
    revenue_date,
    sum(amount_cents) / 100.0 as revenue_usd,
    count(*) as transaction_count
from tx
group by 1
order by 1
