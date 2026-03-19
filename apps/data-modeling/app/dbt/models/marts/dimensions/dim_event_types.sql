{{ config(materialized='table') }}

select
    event_type,
    max(case when is_meaningful_action then 1 else 0 end)::boolean as is_meaningful_action,
    max(action_weight) as default_action_weight
from {{ ref('int_events_enriched') }}
where event_type is not null
group by 1
