{{-
    config(
        materialized = 'incremental',
        unique_key = 'event_id',
        incremental_strategy = 'merge',
        on_schema_change = 'sync_all_columns',
        description = 'Enriches staged GitHub events with calendar fields, action semantics, and engagement weights.'
    )
-}}

{% set incremental_backfill_days = var('incremental_backfill_days', 1) %}

select
    event_id,
    event_ts,
    cast(event_ts as date) as event_date,
    user_id,
    user_login,
    repo_id,
    repo_source,
    event_type,
    payload,
    loaded_at,
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
        when event_type = 'PULLREQUESTEVENT' then 5
        when event_type = 'PUSHEVENT' then 3
        when event_type in ('ISSUESEVENT', 'ISSUECOMMENTEVENT', 'PULLREQUESTREVIEWCOMMENTEVENT') then 2
        when event_type in ('CREATEEVENT', 'WATCHEVENT') then 1
        else 0
    end as action_weight
from {{ ref('stg_raw_events') }}
where event_id is not null
    {% if is_incremental() %}
    and loaded_at >= (
                select coalesce(
                        max(loaded_at) - interval '{{ incremental_backfill_days }} day',
                        '1900-01-01'::timestamptz
                )
                from {{ this }}
    )
    {% endif %}
