{{ config(materialized='table') }}

select distinct
    repo_id,
    repo_source
from {{ ref('int_events_enriched') }}
where repo_id is not null
  and repo_source is not null
