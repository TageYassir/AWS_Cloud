-- Staging: ratings events
-- File: models/staging/stg_ratings.sql

{{
    config(
        materialized='view',
        tags=['staging', 'ratings']
    )
}}

SELECT
    id AS rating_id,
    user_id,
    content_id,
    rating AS rating_value,
    rating_date AS rated_at,
    review_text,
    helpful_count,
    _loaded_at
FROM {{ source('staging', 'stg_ratings') }}
