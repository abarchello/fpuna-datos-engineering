{{ config(materialized='table') }}

with order_items as (
    select
        product_id,
        date_trunc('month', created_at)                 as order_month,
        count(*)                                        as units_sold,
        sum(price_usd)                                  as revenue,
        sum(cogs_usd)                                   as cogs,
        sum(margin_usd)                                 as margin

    from {{ ref('stg_order_items') }}
    group by product_id, date_trunc('month', created_at)
),

refunds as (
    select
        oi.product_id,
        date_trunc('month', r.created_at)               as refund_month,
        count(*)                                        as refund_count,
        sum(r.refund_amount_usd)                        as refund_amount

    from {{ ref('stg_refunds') }} r
    inner join {{ ref('stg_order_items') }} oi
        on r.order_item_id = oi.order_item_id
    group by oi.product_id, date_trunc('month', r.created_at)
)

select
    oi.product_id,
    oi.order_month,
    oi.units_sold,
    oi.revenue,
    oi.cogs,
    oi.margin,
    round(oi.margin * 100.0 / nullif(oi.revenue, 0), 2) as margin_pct,
    coalesce(r.refund_count, 0)                         as refund_count,
    coalesce(r.refund_amount, 0)                        as refund_amount,
    oi.revenue - coalesce(r.refund_amount, 0)           as net_revenue

from order_items oi
left join refunds r
    on oi.product_id = r.product_id
    and oi.order_month = r.refund_month
order by oi.order_month, oi.revenue desc
