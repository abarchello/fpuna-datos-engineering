{{ config(materialized='view') }}

with source as (
    select * from {{ source('ecommerce', 'order_item_refunds') }}
),

renamed as (
    select
        order_item_refund_id,
        created_at,
        order_item_id,
        order_id,
        refund_amount_usd,
        date_trunc('day', created_at)                   as refund_date

    from source
)

select * from renamed
