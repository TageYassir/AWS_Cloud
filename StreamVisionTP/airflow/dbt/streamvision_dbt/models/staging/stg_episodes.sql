-- Staging: episodes
-- File: models/staging/stg_episodes.sql

{{
    config(
        materialized='view',
        tags=['staging', 'episodes']
    )
}}

SELECT
    id AS episode_id,
    tv_show_id,
    season_number,
    episode_number,
    title,
    duration_minutes,
    release_date,
    director,
    imdb_rating,
    description,
    _loaded_at
FROM {{ source('staging', 'stg_episodes') }}
