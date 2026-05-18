{{ config(materialized='table') }}

with sessions as (
    select
        session_month,
        utm_source,
        utm_campaign,
        device_type,
        count(*)                                        as total_sessions,
        count(distinct user_id)                         as unique_users,
        sum(case when is_repeat_session then 1 else 0 end) as repeat_sessions

    from {{ ref('stg_sessions') }}
    group by session_month, utm_source, utm_campaign, device_type
),

orders as (
    select
        s.session_month,
        s.utm_source,
        s.utm_campaign,
        s.device_type,
        count(o.order_id)                               as total_orders,
        sum(o.price_usd)                                as revenue,
        sum(o.margin_usd)                               as margin

    from {{ ref('stg_sessions') }} s
    inner join {{ ref('stg_orders') }} o
        on s.website_session_id = o.website_session_id
    group by s.session_month, s.utm_source, s.utm_campaign, s.device_type
)

select
    s.session_month,
    s.utm_source,
    s.utm_campaign,
    s.device_type,
    s.total_sessions,
    s.unique_users,
    coalesce(o.total_orders, 0)                         as total_orders,
    round(coalesce(o.total_orders, 0) * 100.0 / nullif(s.total_sessions, 0), 2) as conversion_rate_pct,
    coalesce(o.revenue, 0)                              as revenue,
    coalesce(o.margin, 0)                               as margin,
    round(coalesce(o.margin, 0) * 100.0 / nullif(coalesce(o.revenue, 1), 0), 2) as margin_pct

from sessions s
left join orders o
    on s.session_month = o.session_month
    and s.utm_source = o.utm_source
    and s.utm_campaign = o.utm_campaign
    and s.device_type = o.device_type
order by s.session_month, revenue desc
