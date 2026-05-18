{{ config(materialized='view') }}

with source as (
    select * from {{ source('ecommerce', 'order_items') }}
),

renamed as (
    select
        order_item_id,
        created_at,
        order_id,
        product_id,
        is_primary_item,
        price_usd,
        cogs_usd,
        price_usd - cogs_usd                            as margin_usd

    from source
)

select * from renamed
