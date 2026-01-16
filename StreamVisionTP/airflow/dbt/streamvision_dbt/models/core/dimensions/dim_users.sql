{{ config(
    materialized='table',
    tags=['core', 'dimension', 'users']
) }}

WITH users AS (
    SELECT * 
    FROM {{ source('staging', 'stg_users') }}
),

viewing_stats AS (
    SELECT
        user_id,
        MIN(session_start) AS first_viewing_date,
        MAX(session_start) AS last_viewing_date,
        COUNT(id) AS total_sessions,
        SUM(duration_seconds) AS total_watch_time_seconds,
        AVG(completion_rate) AS avg_completion_rate
    FROM {{ source('staging', 'stg_viewing_sessions') }}
    GROUP BY user_id
),

subscription_events AS (
    SELECT
        user_id,
        MIN(event_date) AS first_subscription_date,
        MAX(event_date) AS last_subscription_event_date,
        COUNT(CASE WHEN event_type = 'subscription_start' THEN 1 END) AS subscription_starts,
        COUNT(CASE WHEN event_type = 'cancellation' THEN 1 END) AS cancellations
    FROM {{ source('staging', 'stg_subscription_events') }}
    GROUP BY user_id
),

final AS (
    SELECT
        u.id AS user_key,

        u.email,
        u.username,
        u.first_name,
        u.last_name,
        u.first_name || ' ' || u.last_name AS full_name,

        UPPER(u.country) AS country_code,
        u.age_group,

        u.subscription_plan,
        u.subscription_start,
        u.subscription_end,
        u.payment_method,

        CASE
            WHEN u.subscription_plan IN (
                {{ "'" + "' , '".join(var('premium_plans')) + "'" }}
            ) THEN 'Premium'
            WHEN u.subscription_plan = 'free_trial' THEN 'Free Trial'
            ELSE 'Basic'
        END AS customer_tier,

        u.is_active,
        u.last_login,
        u.created_at AS registration_date,
        u.device_preference,

        COALESCE(vs.total_sessions, 0) AS total_viewing_sessions,
        COALESCE(vs.total_watch_time_seconds, 0) AS total_watch_time_seconds,
        COALESCE(vs.avg_completion_rate, 0) AS avg_completion_rate,
        vs.first_viewing_date,
        vs.last_viewing_date,

        se.first_subscription_date,
        se.last_subscription_event_date,
        COALESCE(se.subscription_starts, 0) AS subscription_starts,
        COALESCE(se.cancellations, 0) AS cancellations,

        CASE
            WHEN vs.last_viewing_date IS NOT NULL 
                 AND DATEDIFF(day, vs.last_viewing_date, CURRENT_DATE) <= {{ var('active_days_threshold') }} THEN 'Active'
            WHEN vs.last_viewing_date IS NOT NULL THEN 'Inactive'
            ELSE 'Never Watched'
        END AS engagement_status,

        CURRENT_TIMESTAMP AS dbt_updated_at

    FROM users u
    LEFT JOIN viewing_stats vs ON u.id = vs.user_id
    LEFT JOIN subscription_events se ON u.id = se.user_id
)

SELECT * 
FROM final
