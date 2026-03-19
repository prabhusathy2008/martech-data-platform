{{ config(materialized='table') }}

{% set score_threshold = var('high_intent_score_threshold', 10) %}
{% set meaningful_threshold = var('high_intent_meaningful_threshold', 5) %}
{% set window_days = var('audience_window_days', 30) %}

with suppressed as (
    select
        cast(user_id as int) as user_id
    from {{ ref('seed_suppressed_users') }}
),
user_window as (
    select
        user_id,
        sum(events_count) as events_last_n_days,
        sum(meaningful_events_count) as meaningful_actions_last_n_days,
        sum(engagement_score) as score_last_n_days
    from {{ ref('fct_user_repo_engagement') }}
    where event_date >= (current_date - interval '{{ window_days }} day')
    group by 1
)
select
    uw.user_id,
    du.user_login,
    uw.events_last_n_days,
    uw.meaningful_actions_last_n_days,
    uw.score_last_n_days,
    current_timestamp as audience_generated_at
from user_window uw
join {{ ref('dim_users') }} du
    on du.user_id = uw.user_id
left join suppressed s
    on s.user_id = uw.user_id
where s.user_id is null
  and (
      uw.score_last_n_days >= {{ score_threshold }}
      or uw.meaningful_actions_last_n_days >= {{ meaningful_threshold }}
  )
