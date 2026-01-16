-- Staging: watchlist
-- File: models/staging/stg_watchlist.sql

{{
    config(
        materialized='view',
        tags=['staging', 'watchlist']
    )
}}

SELECT
    id AS watchlist_id,
    user_id,
    content_id,
    added_date AS added_at,
    watched,
    watched_date,
    _loaded_at
FROM {{ source('staging', 'stg_watchlist') }}
