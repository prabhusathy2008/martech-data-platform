{{-
    config(
        materialized = 'incremental',
        unique_key = 'event_id',
        incremental_strategy = 'merge',
        on_schema_change = 'sync_all_columns',
        description = 'Incremental canonical event staging model with light cleaning and a rolling backfill window.'
    )
-}}

{% set incremental_backfill_days = var('incremental_backfill_days', 1) %}

with source_events as (
    select
        event_id,
        to_timestamp(payload ->> 'created_at', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as event_ts,
        (payload -> 'actor' ->> 'id')::int as user_id,
        lower(trim(payload -> 'actor' ->> 'login')) as user_login,
        (payload -> 'repo' ->> 'id')::bigint as repo_id,
        lower(trim(payload -> 'repo' ->> 'name')) as repo_source,
        upper(trim(payload ->> 'type')) as event_type,
        payload,
        ingested_file,
        loaded_at
    from {{ source('raw', 'raw_events') }}
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
)
select
    event_id,
    event_ts,
    user_id,
    user_login,
    repo_id,
    repo_source,
    event_type,
    payload,
    ingested_file,
    loaded_at
from source_events