{{ config(materialized='table') }}

-- One Big Table: cada orden con todo su contexto de sesion y producto
select
    o.order_id,
    o.created_at                                        as order_date,
    o.order_month,
    o.website_session_id,
    o.user_id,
    o.primary_product_id,
    o.items_purchased,
    o.price_usd,
    o.cogs_usd,
    o.margin_usd,
    o.margin_pct,

    -- Contexto de sesion
    s.utm_source,
    s.utm_campaign,
    s.device_type,
    s.is_repeat_session,
    s.http_referer,

    -- Metricas de refunds
    coalesce(r.refund_amount_usd, 0)                    as refund_amount,
    case when r.order_item_refund_id is not null
         then true else false
    end                                                 as has_refund,

    -- Revenue neto
    o.price_usd - coalesce(r.refund_amount_usd, 0)     as net_revenue

from {{ ref('stg_orders') }} o
left join {{ ref('stg_sessions') }} s
    on o.website_session_id = s.website_session_id
left join {{ ref('stg_refunds') }} r
    on o.order_id = r.order_id
