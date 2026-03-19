{{-
    config(
        materialized = 'view',
        description  = 'Light cleaning of raw.raw_events.'
    )
-}}

select
    event_id,
    to_timestamp(event_ts, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as event_ts,
    user_id::int as user_id,
    lower(trim(user_login)) as user_login,
    lower(trim(repo_source)) as repo_source,
    upper(trim(event_type)) as event_type,
    payload
from {{ source('raw', 'raw_events') }}
where event_id is not null