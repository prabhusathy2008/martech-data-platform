{{-
    config(
        materialized = 'view',
        description  = 'Derives engagement attributes (weights, flags, date parts) from stg_raw_events.'
    )
-}}

select
    event_id,
    event_ts,
    cast(event_ts as date) as event_date,
    date_trunc('hour', event_ts) as event_hour,
    user_id,
    user_login,
    repo_source,
    event_type,
    payload,
    case
        when event_type in (
            'PUSHEVENT',
            'PULLREQUESTEVENT',
            'ISSUESEVENT',
            'ISSUECOMMENTEVENT',
            'PULLREQUESTREVIEWCOMMENTEVENT',
            'CREATEEVENT',
            'WATCHEVENT'
        ) then true
        else false
    end as is_meaningful_action,
    case
        when event_type = 'PUSHEVENT' then 3
        when event_type = 'PULLREQUESTEVENT' then 5
        when event_type in ('ISSUESEVENT', 'ISSUECOMMENTEVENT', 'PULLREQUESTREVIEWCOMMENTEVENT') then 2
        when event_type in ('CREATEEVENT', 'WATCHEVENT') then 1
        else 0
    end as action_weight
from {{ ref('stg_raw_events') }}
