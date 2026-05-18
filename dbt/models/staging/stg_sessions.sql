{{ config(materialized='view') }}

with source as (
    select * from {{ source('ecommerce', 'website_sessions') }}
),

renamed as (
    select
        website_session_id,
        created_at,
        user_id,
        (is_repeat_session = 1)                         as is_repeat_session,
        coalesce(utm_source, 'direct')                  as utm_source,
        coalesce(utm_campaign, 'none')                  as utm_campaign,
        utm_content,
        device_type,
        http_referer,
        date_trunc('day', created_at)                   as session_date,
        date_trunc('week', created_at)                  as session_week,
        date_trunc('month', created_at)                 as session_month,
        extract(year from created_at)                   as session_year

    from source
)

select * from renamed
