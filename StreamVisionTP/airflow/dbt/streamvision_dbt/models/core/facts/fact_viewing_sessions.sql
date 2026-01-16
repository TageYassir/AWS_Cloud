{{ config(materialized='table', tags=['core', 'fact', 'viewing_sessions']) }}

WITH vs AS (
    SELECT * FROM {{ ref('stg_viewing_sessions') }}
),

final AS (
    SELECT
        viewing_session_id AS viewing_session_key,

        user_id AS user_key,
        content_id AS content_key,
        DATE(session_start) AS date_key,

        platform,
        device_type,
        quality,
        city,

        session_start,
        session_end,

        duration_seconds,
        completion_rate,
        buffering_count,
        avg_bitrate,

        CASE
            WHEN completion_rate >= 90 THEN 'Complete'
            WHEN completion_rate >= 50 THEN 'Partial'
            ELSE 'Abandoned'
        END AS completion_status,

        CURRENT_TIMESTAMP AS dbt_updated_at
    FROM vs
)

SELECT * FROM final
