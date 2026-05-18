{{ config(materialized='view') }}

with source as (
    select * from {{ source('ecommerce', 'orders') }}
),

renamed as (
    select
        order_id,
        created_at,
        website_session_id,
        user_id,
        primary_product_id,
        items_purchased,
        price_usd,
        cogs_usd,
        price_usd - cogs_usd                            as margin_usd,
        round((price_usd - cogs_usd) / price_usd * 100, 2) as margin_pct,
        date_trunc('day', created_at)                   as order_date,
        date_trunc('week', created_at)                  as order_week,
        date_trunc('month', created_at)                 as order_month,
        extract(year from created_at)                   as order_year

    from source
)

select * from renamed
