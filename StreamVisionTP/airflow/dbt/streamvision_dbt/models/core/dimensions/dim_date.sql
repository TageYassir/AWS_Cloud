{{ config(
    materialized='table',
    tags=['core', 'dimension', 'date']
) }}

-- Dimension: date (distinct dates from viewing sessions)
WITH dates AS (
    SELECT DISTINCT CAST(DATE_TRUNC('day', session_start) AS DATE) AS dt
    FROM {{ source('staging', 'stg_viewing_sessions') }}
)

SELECT
    dt AS date,
    EXTRACT(year FROM dt)::INT AS year,
    EXTRACT(month FROM dt)::INT AS month,
    EXTRACT(day FROM dt)::INT AS day,
    TO_CHAR(dt, 'YYYY-MM-DD') AS date_string
FROM dates
ORDER BY dt
