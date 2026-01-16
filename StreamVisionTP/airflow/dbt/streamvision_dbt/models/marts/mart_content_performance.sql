{{ config(
  materialized='table',
  tags=['marts', 'content_performance']
) }}

WITH content_performance AS (
    SELECT
        c.content_id AS content_id,         -- was c.id
        c.title,
        c.content_type,
        c.genre,
        c.subgenre,
        c.release_year,
        c.duration_minutes,
        c.is_original,
        COUNT(*) AS total_sessions,         -- no f.id column in staging, count rows
        COUNT(DISTINCT f.user_id) AS unique_viewers,
        SUM(f.duration_seconds) AS total_watch_time_seconds,
        AVG(f.completion_rate) AS avg_completion_rate,
        c.imdb_rating AS avg_rating,
        0 AS total_ratings,
        0 AS five_star_ratings,
        MIN(f.session_start) AS first_viewing_date,
        MAX(f.session_start) AS last_viewing_date
    FROM {{ ref('stg_viewing_sessions') }} AS f
    JOIN {{ ref('stg_content') }} AS c
        ON f.content_id = c.content_id      -- was c.id
    GROUP BY
        c.content_id,                       -- was c.id
        c.title,
        c.content_type,
        c.genre,
        c.subgenre,
        c.release_year,
        c.duration_minutes,
        c.is_original,
        c.imdb_rating
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY total_watch_time_seconds DESC) AS watch_time_rank,
        SUM(total_watch_time_seconds) OVER (
            ORDER BY total_watch_time_seconds DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_watch_time,
        SUM(total_watch_time_seconds) OVER () AS total_watch_time_all
    FROM content_performance
),
final AS (
    SELECT
        *,
        ROUND(total_watch_time_seconds * 100.0 / NULLIF(total_watch_time_all,0), 3) AS watch_time_pct,
        ROUND(cumulative_watch_time * 100.0 / NULLIF(total_watch_time_all,0), 2) AS cumulative_watch_time_pct,
        CASE
            WHEN ROUND(cumulative_watch_time * 100.0 / NULLIF(total_watch_time_all,0), 2) <= 80 THEN 'A'
            WHEN ROUND(cumulative_watch_time * 100.0 / NULLIF(total_watch_time_all,0), 2) <= 95 THEN 'B'
            ELSE 'C'
        END AS abc_class,
        CASE
            WHEN watch_time_rank <= 10 THEN 'Top 10'
            WHEN watch_time_rank <= 50 THEN 'Top 50'
            WHEN watch_time_rank <= 100 THEN 'Top 100'
            ELSE 'Long Tail'
        END AS popularity_tier,
        ROUND(
            (COALESCE(avg_completion_rate, 0) * 0.4) +
            (COALESCE(avg_rating, 0) * 0.3) +
            (LN(GREATEST(COALESCE(total_sessions, 1), 1)) * 0.3),
            2
        ) AS content_score,
        CURRENT_TIMESTAMP() AS dbt_updated_at
    FROM ranked
)

SELECT *
FROM final
ORDER BY watch_time_rank