with source as (
    select
        TRANSACTION_ID as transaction_id,
        CUSTOMER_ID as customer_id,
        PRODUCT_ID as product_id,
        AMOUNT_CENTS as amount_cents,
        CURRENCY as currency,
        STATUS as status,
        CREATED_AT as created_at
    from {{ source('raw', 'transactions_raw') }}
)

select * from source
