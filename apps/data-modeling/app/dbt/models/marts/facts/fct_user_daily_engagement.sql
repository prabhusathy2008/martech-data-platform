{{ config(materialized='table') }}

select
    event_date,
    user_id,
    user_login,
    events_count,
    meaningful_actions_count,
    weighted_score,
    current_timestamp as transformed_at
from {{ ref('int_user_daily_activity') }}
