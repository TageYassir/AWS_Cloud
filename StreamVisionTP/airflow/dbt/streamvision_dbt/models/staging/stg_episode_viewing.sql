-- Staging: episode viewing events
-- File: models/staging/stg_episode_viewing.sql

{{
    config(
        materialized='view',
        tags=['staging', 'episode_viewing']
    )
}}

SELECT
    id AS episode_viewing_id,
    viewing_session_id,
    episode_id,
    user_id,
    start_time AS started_at,
    end_time AS ended_at,
    duration_watched AS duration_seconds,
    completion_rate,
    _loaded_at
FROM {{ source('staging', 'stg_episode_viewing') }}
