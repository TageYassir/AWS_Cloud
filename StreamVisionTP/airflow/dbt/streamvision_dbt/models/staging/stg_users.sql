-- Staging: users
-- File: models/staging/stg_users.sql

{{
    config(
        materialized='view',
        tags=['staging', 'users']
    )
}}

WITH src AS (
    SELECT * FROM {{ source('staging', 'stg_users') }}
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY COALESCE(_loaded_at, CURRENT_TIMESTAMP()) DESC) AS _rn
    FROM src
)

SELECT
    id AS user_id,
    email,
    username,
    first_name,
    last_name,
    country,
    age_group,
    subscription_plan,
    subscription_start,
    subscription_end,
    created_at,
    last_login,
    is_active,
    payment_method,
    device_preference,
    _loaded_at
FROM ranked
WHERE _rn = 1
