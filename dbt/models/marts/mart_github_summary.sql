{{ config(materialized='table') }}

-- Resumen mensual de actividad del repositorio GitHub
with activity as (
    select * from {{ ref('int_github_activity') }}
),

monthly_summary as (
    select
        activity_month,
        sum(total_count)                                        as total_activities,
        sum(case when activity_type = 'issue' then total_count else 0 end)          as total_issues,
        sum(case when activity_type = 'pull_request' then total_count else 0 end)   as total_prs,
        sum(case when activity_type = 'issue' then closed_count else 0 end)         as closed_issues,
        sum(case when activity_type = 'pull_request' then closed_count else 0 end)  as merged_prs,
        max(unique_contributors)                                                     as unique_contributors,
        avg(avg_days_to_resolve)                                                     as avg_days_to_resolve

    from activity
    group by activity_month
)

select
    activity_month,
    total_activities,
    total_issues,
    total_prs,
    closed_issues,
    merged_prs,
    round(closed_issues * 100.0 / nullif(total_issues, 0), 2)  as issue_close_rate_pct,
    round(merged_prs * 100.0 / nullif(total_prs, 0), 2)        as pr_merge_rate_pct,
    unique_contributors,
    round(avg_days_to_resolve, 1)                              as avg_days_to_resolve

from monthly_summary
order by activity_month
