-- Staging: viewing sessions
-- File: models/staging/stg_viewing_sessions.sql

{{
    config(
        materialized='view',
        tags=['staging', 'viewing_sessions']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('staging', 'stg_viewing_sessions') }}
),

cleaned AS (
    SELECT
        id AS viewing_session_id,
        user_id,
        content_id,
        session_start,
        session_end,
        duration_seconds,
        completion_rate,
        buffering_count,
        avg_bitrate,
        platform,
        device_type,
        quality,
        city,
        ip_address,
        _loaded_at
    FROM source
    WHERE session_start >= '{{ var("start_date") }}'
      AND duration_seconds > 0
)

SELECT * FROM cleaned
