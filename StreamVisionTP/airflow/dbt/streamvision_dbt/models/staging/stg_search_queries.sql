-- Staging: search queries
-- File: models/staging/stg_search_queries.sql

{{
    config(
        materialized='view',
        tags=['staging', 'search_queries']
    )
}}

SELECT
    id AS query_id,
    user_id,
    query_text,
    search_date,
    results_count,
    clicked_content_id,
    search_filters,
    session_id,
    _loaded_at
FROM {{ source('staging', 'stg_search_queries') }}
