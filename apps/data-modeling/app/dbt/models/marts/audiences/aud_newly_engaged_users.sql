{{ config(materialized='table') }}

{% set new_user_window_days = var('newly_engaged_window_days', 14) %}

with suppressed as (
    select
        cast(user_id as int) as user_id
    from {{ ref('seed_suppressed_users') }}
)
select
    du.user_id,
    du.user_login,
    du.first_event_ts,
    du.first_meaningful_event_ts,
    du.last_event_ts,
    current_timestamp as audience_generated_at
from {{ ref('dim_users') }} du
left join suppressed s
    on s.user_id = du.user_id
where s.user_id is null
  and du.first_event_ts
      >= (current_timestamp - interval '{{ new_user_window_days }} day')
