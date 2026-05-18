{{ config(materialized='view') }}

-- Modelo intermedio: actividad combinada de Issues y PRs por mes
with issues as (
    select
        created_month                               as activity_month,
        'issue'                                     as activity_type,
        count(*)                                    as total_count,
        sum(case when is_closed then 1 else 0 end)  as closed_count,
        avg(days_to_close)                          as avg_days_to_resolve,
        count(distinct author_login)                as unique_contributors

    from {{ ref('stg_github_issues') }}
    group by created_month
),

pull_requests as (
    select
        created_month                               as activity_month,
        'pull_request'                              as activity_type,
        count(*)                                    as total_count,
        sum(case when is_merged then 1 else 0 end)  as closed_count,
        avg(days_to_merge)                          as avg_days_to_resolve,
        count(distinct author_login)                as unique_contributors

    from {{ ref('stg_github_pull_requests') }}
    group by created_month
),

combined as (
    select * from issues
    union all
    select * from pull_requests
)

select
    activity_month,
    activity_type,
    total_count,
    closed_count,
    round(closed_count * 100.0 / nullif(total_count, 0), 2)    as close_rate_pct,
    round(avg_days_to_resolve, 1)                              as avg_days_to_resolve,
    unique_contributors

from combined
order by activity_month, activity_type
