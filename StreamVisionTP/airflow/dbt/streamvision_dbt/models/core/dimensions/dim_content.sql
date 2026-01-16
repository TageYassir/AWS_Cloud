{{ config(materialized='table', tags=['core', 'dimension', 'content']) }}

WITH content AS (
    SELECT * FROM {{ source('staging', 'stg_content') }}
),

viewing_stats AS (
    SELECT
        content_id,
        COUNT(viewing_session_id) AS total_sessions,
        COUNT(DISTINCT user_id) AS unique_viewers,
        SUM(duration_seconds) AS total_watch_time_seconds,
        AVG(completion_rate) AS avg_completion_rate
    FROM {{ ref('stg_viewing_sessions') }}
    GROUP BY content_id
),

rating_stats AS (
    SELECT
        content_id,
        COUNT(*) AS total_ratings,
        AVG(rating) AS avg_rating,
        COUNT_IF(rating = 5) AS five_star_ratings
    FROM {{ source('staging', 'stg_ratings') }}
    GROUP BY content_id
),

final AS (
    SELECT
        c.id AS content_key,

        c.title,
        c.content_type,
        c.genre,
        c.subgenre,
        c.description,

        c.release_year,
        c.duration_minutes,
        c.director,
        c.main_actor,
        c.imdb_rating,
        c.content_rating,

        c.is_original,
        c.added_date,

        c.available_countries,
        c.tags,

        COALESCE(vs.total_sessions, 0) AS total_viewing_sessions,
        COALESCE(vs.unique_viewers, 0) AS unique_viewers,
        COALESCE(vs.total_watch_time_seconds, 0) AS total_watch_time_seconds,
        COALESCE(vs.avg_completion_rate, 0) AS avg_completion_rate,

        COALESCE(rs.total_ratings, 0) AS total_ratings,
        COALESCE(rs.avg_rating, 0) AS avg_rating,
        COALESCE(rs.five_star_ratings, 0) AS five_star_ratings,

        CURRENT_TIMESTAMP AS dbt_updated_at
    FROM content c
    LEFT JOIN viewing_stats vs ON c.id = vs.content_id
    LEFT JOIN rating_stats rs ON c.id = rs.content_id
)

SELECT * FROM final
