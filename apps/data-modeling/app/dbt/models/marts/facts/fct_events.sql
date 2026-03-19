{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

{% set incremental_backfill_days = var('incremental_backfill_days', 2) %}

select
    event_id,
    event_ts,
    user_id,
    repo_source,
    event_type,
    payload,
    is_meaningful_action,
    action_weight,
    current_timestamp as transformed_at
from {{ ref('int_events_enriched') }}
where event_id is not null
{% if is_incremental() %}
  and event_ts >= (
            select coalesce(
                    max(event_ts) - interval '{{ incremental_backfill_days }} day',
                    '1900-01-01'::timestamptz
            )
      from {{ this }}
  )
{% endif %}
