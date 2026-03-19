{{-
    config(
        materialized='view',
        description='Daily per-user engagement metrics.'
    )
-}}

select
    user_id,
    event_date,
    min(user_login) as user_login,
    count(*) as events_count,
    sum(case when is_meaningful_action then 1 else 0 end) as meaningful_actions_count,
    sum(action_weight) as weighted_score
from {{ ref('int_events_enriched') }}
group by 1, 2
