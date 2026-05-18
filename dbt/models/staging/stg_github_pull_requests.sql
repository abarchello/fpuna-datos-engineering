{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'pull_requests') }}
),

renamed as (
    select
        id                                              as pr_id,
        number                                          as pr_number,
        title,
        state,
        created_at,
        updated_at,
        closed_at,
        merged_at,
        user.login                                      as author_login,
        head.ref                                        as source_branch,
        base.ref                                        as target_branch,
        case when merged_at is not null
             then true else false
        end                                             as is_merged,
        case when state = 'closed' and merged_at is null
             then true else false
        end                                             as is_rejected,
        case when merged_at is not null
             then date_diff('day', created_at, merged_at)
             else null
        end                                             as days_to_merge,
        date_trunc('month', created_at)                 as created_month,
        date_trunc('week', created_at)                  as created_week

    from source
)

select * from renamed
