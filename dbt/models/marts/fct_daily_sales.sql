{{ config(materialized='table') }}

with sessions as (
    select
        session_date,
        count(*)                                        as total_sessions,
        count(distinct user_id)                         as unique_users,
        sum(case when is_repeat_session then 1 else 0 end) as repeat_sessions

    from {{ ref('stg_sessions') }}
    group by session_date
),

orders as (
    select
        order_date,
        count(*)                                        as total_orders,
        sum(price_usd)                                  as revenue,
        sum(cogs_usd)                                   as cogs,
        sum(margin_usd)                                 as margin,
        sum(items_purchased)                            as items_sold,
        count(distinct user_id)                         as unique_buyers

    from {{ ref('stg_orders') }}
    group by order_date
),

refunds as (
    select
        refund_date,
        count(*)                                        as total_refunds,
        sum(refund_amount_usd)                          as refund_amount

    from {{ ref('stg_refunds') }}
    group by refund_date
)

select
    s.session_date                                                          as date,
    s.total_sessions,
    s.unique_users,
    s.repeat_sessions,
    coalesce(o.total_orders, 0)                                             as total_orders,
    coalesce(o.unique_buyers, 0)                                            as unique_buyers,
    coalesce(o.items_sold, 0)                                               as items_sold,
    round(coalesce(o.total_orders, 0) * 100.0 / nullif(s.total_sessions, 0), 2) as conversion_rate_pct,
    coalesce(o.revenue, 0)                                                  as revenue,
    coalesce(o.cogs, 0)                                                     as cogs,
    coalesce(o.margin, 0)                                                   as margin,
    coalesce(r.total_refunds, 0)                                            as total_refunds,
    coalesce(r.refund_amount, 0)                                            as refund_amount,
    coalesce(o.revenue, 0) - coalesce(r.refund_amount, 0)                  as net_revenue

from sessions s
left join orders o on s.session_date = o.order_date
left join refunds r on s.session_date = r.refund_date
order by s.session_date
