{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'issues') }}
),

renamed as (
    select
        id                                              as issue_id,
        number                                          as issue_number,
        title,
        state,
        body,
        created_at,
        updated_at,
        closed_at,
        user.login                                      as author_login,
        user.type                                       as author_type,
        len(coalesce(labels, []))                       as label_count,
        comments                                        as comment_count,
        milestone.title                                 as milestone_title,
        case when state = 'closed' and closed_at is not null
             then date_diff('day', created_at, closed_at)
             else null
        end                                             as days_to_close,
        case when state = 'closed' then true else false end as is_closed,
        date_trunc('month', created_at)                 as created_month,
        date_trunc('week', created_at)                  as created_week

    from source
)

select * from renamed
