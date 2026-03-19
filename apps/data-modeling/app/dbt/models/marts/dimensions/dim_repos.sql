{{ config(materialized='table') }}

select distinct
    repo_source
from {{ ref('int_events_enriched') }}
where repo_source is not null
