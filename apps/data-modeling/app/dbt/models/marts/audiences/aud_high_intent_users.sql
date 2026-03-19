{{ config(materialized='table') }}

{% set score_threshold = var('high_intent_score_threshold', 10) %}
{% set meaningful_threshold = var('high_intent_meaningful_threshold', 5) %}

with suppressed as (
    select
        cast(user_id as text) as user_id,
        lower(user_login) as user_login
    from {{ ref('seed_suppressed_users') }}
),
base as (
    select
        r.user_id,
        lower(r.user_login) as user_login,
        r.events_last_n_days,
        r.meaningful_actions_last_n_days,
        r.score_last_n_days,
        r.last_event_ts
    from {{ ref('int_user_recent_activity_window') }} r
)
select
    b.user_id,
    b.user_login,
    b.events_last_n_days,
    b.meaningful_actions_last_n_days,
    b.score_last_n_days,
    b.last_event_ts,
    current_timestamp as audience_generated_at
from base b
left join suppressed s
    on s.user_id = b.user_id
    or s.user_login = b.user_login
where s.user_id is null
  and s.user_login is null
  and (
      b.score_last_n_days >= {{ score_threshold }}
      or b.meaningful_actions_last_n_days >= {{ meaningful_threshold }}
  )
