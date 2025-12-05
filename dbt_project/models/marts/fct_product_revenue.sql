with tx as (
    select
        date_trunc('day', created_at) as revenue_date,
        product_id,
        amount_cents
    from {{ ref('stg_transactions') }}
    where status = 'succeeded'
)

select
    revenue_date,
    product_id,
    sum(amount_cents) / 100.0 as revenue_usd,
    count(*) as transaction_count
from tx
group by 1, 2
order by 1, 2
