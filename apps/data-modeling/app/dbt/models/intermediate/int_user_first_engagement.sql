{{-
    config(
        materialized='view',
        description='First and last engagement timestamps per user.'
    )
-}}

select
    user_id,
    min(user_login) as user_login,
    min(event_ts) as first_event_ts,
    max(event_ts) as last_event_ts,
    min(case when is_meaningful_action then event_ts end) as first_meaningful_event_ts,
    max(case when is_meaningful_action then event_ts end) as last_meaningful_event_ts
from {{ ref('int_events_enriched') }}
group by 1