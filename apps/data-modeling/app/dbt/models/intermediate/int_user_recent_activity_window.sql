{{-
    config(
        materialized='view',
        description='Engagement metrics for users in the recent activity window.'
    )
-}}

{% set window_days = var('audience_window_days', 30) %}

select
    user_id,
    min(user_login) as user_login,
    count(*) as events_last_n_days,
    sum(case when is_meaningful_action then 1 else 0 end) as meaningful_actions_last_n_days,
    sum(action_weight) as score_last_n_days,
    max(event_ts) as last_event_ts
from {{ ref('int_events_enriched') }}
where event_date >= (current_date - interval '{{ window_days }} day')
group by 1