{{ config(materialized = 'table') }}

select
    event_date,
    user_id,
    repo_id,
    event_type,
    count(*) as events_count,
    sum(case when is_meaningful_action then 1 else 0 end) as meaningful_events_count,
    sum(action_weight) as engagement_score,
    current_timestamp as transformed_at
from {{ ref('int_events_enriched') }}
where
    event_date is not null
    and user_id is not null
    and repo_id is not null
    and event_type is not null
group by 1, 2, 3, 4