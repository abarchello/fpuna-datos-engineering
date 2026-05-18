{{ config(materialized='view') }}

with source as (
    select * from {{ source('ecommerce', 'website_pageviews') }}
),

renamed as (
    select
        website_pageview_id,
        created_at,
        website_session_id,
        pageview_url,
        date_trunc('day', created_at)                   as pageview_date

    from source
)

select * from renamed
