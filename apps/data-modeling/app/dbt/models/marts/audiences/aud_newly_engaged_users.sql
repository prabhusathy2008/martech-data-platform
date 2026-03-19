{{ config(materialized='table') }}

{% set new_user_window_days = var('newly_engaged_window_days', 14) %}

with suppressed as (
    select
        cast(user_id as text) as user_id,
        lower(user_login) as user_login
    from {{ ref('seed_suppressed_users') }}
),
base as (
    select
        f.user_id,
        lower(f.user_login) as user_login,
        f.first_event_ts,
        f.first_meaningful_event_ts,
        f.last_event_ts
    from {{ ref('int_user_first_engagement') }} f
)
select
    b.user_id,
    b.user_login,
    b.first_event_ts,
    b.first_meaningful_event_ts,
    b.last_event_ts,
    current_timestamp as audience_generated_at
from base b
left join suppressed s
    on s.user_id = b.user_id
    or s.user_login = b.user_login
where s.user_id is null
  and s.user_login is null
  and coalesce(b.first_meaningful_event_ts, b.first_event_ts)
      >= (current_timestamp - interval '{{ new_user_window_days }} day')
