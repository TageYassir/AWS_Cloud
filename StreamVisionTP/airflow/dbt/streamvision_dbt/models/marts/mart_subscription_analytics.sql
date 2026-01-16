{{ config(
  materialized='table',
  tags=['marts', 'subscription_analytics']
) }}

WITH subscription_events AS (
    SELECT *
    FROM {{ ref('stg_subscription_events') }}
),

subscription_summary AS (
    SELECT
        user_id,
        event_type,
        event_at AS event_date,   -- use staging column name, alias to event_date
        previous_plan,
        new_plan,
        COUNT(*) OVER (PARTITION BY user_id, event_type) AS event_count
    FROM subscription_events
)

SELECT
    *,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM subscription_summary
ORDER BY event_date DESC, user_id