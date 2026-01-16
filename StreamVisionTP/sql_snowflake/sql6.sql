-- ============================================
-- Chargement des données depuis S3 vers STAGING - StreamVision
-- Date : 2026-01-05
-- ============================================

Use STREAMVISION_WH;
USE WAREHOUSE LOADING_WH;
USE SCHEMA STAGING;

-- 1. Chargement de STG_USERS
COPY INTO stg_users (
    id, email, username, first_name, last_name, country, age_group,
    subscription_plan, subscription_start, subscription_end,
    created_at, last_login, is_active, payment_method, device_preference
)
FROM @RAW.s3_raw_stage/postgres/users/2026-01-05/
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

SELECT 'Chargement stg_users terminé : ' || COUNT(*) || ' lignes' AS resultat FROM stg_users;

-- 2. Chargement de STG_CONTENT
COPY INTO stg_content (
    id, title, content_type, genre, subgenre, release_year,
    duration_minutes, director, main_actor, imdb_rating,
    content_rating, is_original, added_date, available_countries,
    tags, description
)
FROM @RAW.s3_raw_stage/postgres/content/2026-01-05/
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

SELECT 'Chargement stg_content terminé : ' || COUNT(*) || ' lignes' AS resultat FROM stg_content;

-- 3. Chargement de STG_VIEWING_SESSIONS
COPY INTO stg_viewing_sessions (
    id, user_id, content_id, session_start, session_end, duration_seconds,
    platform, device_type, quality, completion_rate,
    buffering_count, avg_bitrate, city, ip_address
)
FROM @RAW.s3_raw_stage/postgres/viewing_sessions/2026-01-05/
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

SELECT 'Chargement stg_viewing_sessions terminé : ' || COUNT(*) || ' lignes' AS resultat FROM stg_viewing_sessions;

-- 4. Chargement de STG_RATINGS
COPY INTO stg_ratings (
    id, user_id, content_id, rating, rating_date, review_text, helpful_count
)
FROM @RAW.s3_raw_stage/postgres/ratings/2026-01-05/
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

SELECT 'Chargement stg_ratings terminé : ' || COUNT(*) || ' lignes' AS resultat FROM stg_ratings;

-- 5. Chargement de STG_WATCHLIST
COPY INTO stg_watchlist (
    id, user_id, content_id, added_date, watched, watched_date
)
FROM @RAW.s3_raw_stage/postgres/watchlist/2026-01-05/
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

SELECT 'Chargement stg_watchlist terminé : ' || COUNT(*) || ' lignes' AS resultat FROM stg_watchlist;

-- 6. Chargement de STG_SUBSCRIPTION_EVENTS
COPY INTO stg_subscription_events (
    id, user_id, event_type, event_date, previous_plan, new_plan,
    amount, currency, payment_gateway, transaction_id
)
FROM @RAW.s3_raw_stage/postgres/subscription_events/2026-01-05/
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

SELECT 'Chargement stg_subscription_events terminé : ' || COUNT(*) || ' lignes' AS resultat FROM stg_subscription_events;

-- 7. Chargement de STG_SEARCH_QUERIES
COPY INTO stg_search_queries (
    id, user_id, query_text, search_date, results_count, clicked_content_id,
    search_filters, session_id
)
FROM @RAW.s3_raw_stage/postgres/search_queries/2026-01-05/
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

SELECT 'Chargement stg_search_queries terminé : ' || COUNT(*) || ' lignes' AS resultat FROM stg_search_queries;

-- 8. Chargement de STG_EPISODES
COPY INTO stg_episodes (
    id, tv_show_id, season_number, episode_number, title,
    duration_minutes, release_date, director, imdb_rating, description
)
FROM @RAW.s3_raw_stage/postgres/episodes/2026-01-05/
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

SELECT 'Chargement stg_episodes terminé : ' || COUNT(*) || ' lignes' AS resultat FROM stg_episodes;

-- 9. Chargement de STG_EPISODE_VIEWING
COPY INTO stg_episode_viewing (
    id, viewing_session_id, episode_id, user_id, start_time, end_time,
    duration_watched, completion_rate
)
FROM @RAW.s3_raw_stage/postgres/episode_viewing/2026-01-05/
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

SELECT 'Chargement stg_episode_viewing terminé : ' || COUNT(*) || ' lignes' AS resultat FROM stg_episode_viewing;

-- Récapitulatif final
SELECT 'stg_users' AS table_name, COUNT(*) AS row_count FROM stg_users
UNION ALL
SELECT 'stg_content', COUNT(*) FROM stg_content
UNION ALL
SELECT 'stg_viewing_sessions', COUNT(*) FROM stg_viewing_sessions
UNION ALL
SELECT 'stg_ratings', COUNT(*) FROM stg_ratings
UNION ALL
SELECT 'stg_watchlist', COUNT(*) FROM stg_watchlist
UNION ALL
SELECT 'stg_subscription_events', COUNT(*) FROM stg_subscription_events
UNION ALL
SELECT 'stg_search_queries', COUNT(*) FROM stg_search_queries
UNION ALL
SELECT 'stg_episodes', COUNT(*) FROM stg_episodes
UNION ALL
SELECT 'stg_episode_viewing', COUNT(*) FROM stg_episode_viewing;
