-- Staging: subscription events
-- File: models/staging/stg_subscription_events.sql

{{
    config(
        materialized='view',
        tags=['staging', 'subscription_events']
    )
}}

SELECT
    id AS event_id,
    user_id,
    event_type,
    event_date AS event_at,
    previous_plan,
    new_plan,
    amount,
    currency,
    payment_gateway,
    transaction_id,
    _loaded_at
FROM {{ source('staging', 'stg_subscription_events') }}
