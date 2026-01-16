{{ config(
  materialized='table',
  tags=['marts', 'user_engagement']
) }}

WITH daily_engagement AS (
    SELECT
        DATE_TRUNC('day', f.session_start) AS engagement_date,
        u.country AS country_code,
        u.age_group AS customer_tier,
        u.subscription_plan,
        COUNT(DISTINCT f.user_id) AS daily_active_users,
        COUNT(DISTINCT f.content_id) AS unique_content_viewed,
        COUNT(*) AS total_sessions,                 -- instead of COUNT(f.id)
        SUM(f.duration_seconds) AS total_watch_time_seconds,
        AVG(f.completion_rate) AS avg_completion_rate,
        AVG(f.buffering_count) AS avg_buffering_count,
        COUNT_IF(f.platform = 'web') AS web_sessions,
        COUNT_IF(f.platform LIKE 'mobile%') AS mobile_sessions,
        COUNT_IF(f.platform = 'smart_tv') AS tv_sessions,
        COUNT_IF(f.quality IN ('4K','HDR')) AS high_quality_sessions,
        COUNT_IF(f.quality = 'Full HD') AS full_hd_sessions,
        COUNT_IF(f.quality = 'HD') AS hd_sessions,
        COUNT_IF(f.quality = 'SD') AS sd_sessions
    FROM {{ ref('stg_viewing_sessions') }} AS f
    JOIN {{ ref('stg_users') }} AS u
      ON f.user_id = u.user_id                     -- was u.id
    GROUP BY
      DATE_TRUNC('day', f.session_start),
      u.country,
      u.age_group,
      u.subscription_plan
)

SELECT
    *,
    ROUND(total_watch_time_seconds / 3600.0, 2) AS total_watch_time_hours,
    ROUND(total_sessions / NULLIF(daily_active_users, 0), 2) AS sessions_per_user,
    ROUND(total_watch_time_seconds / NULLIF(total_sessions, 0), 2) AS avg_session_duration_seconds,
    ROUND(web_sessions * 100.0 / NULLIF(total_sessions, 0), 1) AS web_sessions_pct,
    ROUND(mobile_sessions * 100.0 / NULLIF(total_sessions, 0), 1) AS mobile_sessions_pct,
    ROUND(tv_sessions * 100.0 / NULLIF(total_sessions, 0), 1) AS tv_sessions_pct,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM daily_engagement
ORDER BY engagement_date DESC, total_watch_time_seconds DESC