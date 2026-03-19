{{ config(materialized='table') }}

select
    user_id,
    min(user_login) as user_login,
    min(first_event_ts) as first_event_ts,
    max(last_event_ts) as last_event_ts,
    min(first_meaningful_event_ts) as first_meaningful_event_ts,
    max(last_meaningful_event_ts) as last_meaningful_event_ts
from {{ ref('int_user_first_engagement') }}
group by 1
